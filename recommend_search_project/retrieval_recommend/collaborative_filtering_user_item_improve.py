import os
import sys
import tempfile

import matplotlib.pyplot as plt
from pymongo import MongoClient, UpdateOne
import pandas as pd
from functools import reduce

import numpy as np
import tensorflow as tf
import tensorflow_datasets as tfds

import tensorflow_recommenders as tfrs


BEHAVIORS = {
    "apply": 5.0,
    "search": 4.0,
    "like": 4.0,
    "view": 3.0,
    "ignore": 1.0,
    "dislike": 0.0,
    "hated": 0.0,
}

MONGO_DB = sys.argv[1]


def create_rating(behavior):
    return BEHAVIORS.get(behavior, 0)


def create_skill_text(lst_skill):
    return ','.join(lst_skill)


def tokenization(t):
    return tf.strings.split(t, ',')


def tokenization_1(t):
    return tf.strings.split(t, ' ')


def scale_5(arr):
    arr = (arr - min(arr) + 1)/(max(arr) - min(arr))
    return list(map(float, arr*5/max(arr)))


def update_item_recs(user, recs, score):
    return UpdateOne({"_id": user.decode("ascii")}, {
        "$set": {
            "items": dict(zip(list(map(lambda x: x.decode("ascii"), recs)), scale_5(score)))
        }
    }, upsert=True)


class UserModel(tf.keras.Model):

  def __init__(self):
    super().__init__()

    self.user_embedding = tf.keras.Sequential([
        tf.keras.layers.StringLookup(
            vocabulary=unique_user_ids, mask_token=None),
        tf.keras.layers.Embedding(len(unique_user_ids) + 1, 32),
    ])

  def call(self, inputs):
    # Take the input dictionary, pass it through each input layer,
    # and concatenate the result.
    return tf.concat([
        self.user_embedding(inputs["user_id"])
    ], axis=1)


class QueryModel(tf.keras.Model):
  """Model for encoding user queries."""

  def __init__(self, layer_sizes):
    """Model for encoding user queries.

    Args:
      layer_sizes:
        A list of integers where the i-th entry represents the number of units
        the i-th layer contains.
    """
    super().__init__()

    # We first use the user model for generating embeddings.
    self.embedding_model = UserModel()

    # Then construct the layers.
    self.dense_layers = tf.keras.Sequential()

    # Use the ReLU activation for all but the last layer.
    for layer_size in layer_sizes[:-1]:
      self.dense_layers.add(tf.keras.layers.Dense(layer_size, activation="relu"))

    # No activation for the last layer.
    for layer_size in layer_sizes[-1:]:
      self.dense_layers.add(tf.keras.layers.Dense(layer_size))

  def call(self, inputs):
    feature_embedding = self.embedding_model(inputs)
    print(feature_embedding)
    return self.dense_layers(feature_embedding)


class MovieModel(tf.keras.Model):

    def __init__(self):
        super().__init__()

        max_tokens = 10_000

        self.job_id_embedding = tf.keras.Sequential([
            tf.keras.layers.StringLookup(
                vocabulary=unique_job_ids, mask_token=None),
            tf.keras.layers.Embedding(len(unique_job_ids) + 1, 32)
        ])
        self.location_embedding = tf.keras.Sequential([
            tf.keras.layers.StringLookup(
                vocabulary=unique_locations, mask_token=None),
            tf.keras.layers.Embedding(len(unique_locations) + 1, 32)
        ])
        self.level_embedding = tf.keras.Sequential([
            tf.keras.layers.StringLookup(
                vocabulary=unique_levels, mask_token=None),
            tf.keras.layers.Embedding(len(unique_levels) + 1, 32)
        ])
        self.category_embedding = tf.keras.Sequential([
            tf.keras.layers.StringLookup(
                vocabulary=unique_category, mask_token=None),
            tf.keras.layers.Embedding(len(unique_category) + 1, 64)
        ])
        self.skill_embedding = tf.keras.Sequential([
            tf.keras.layers.TextVectorization(
                max_tokens=50,
                standardize=None,
                split=tokenization,
                vocabulary=unique_skills,
                pad_to_max_tokens=True
            ),
            tf.keras.layers.Embedding(len(unique_skills), 32),
            tf.keras.layers.GlobalAvgPool1D()
        ])

    #     self.job_title_embedding = tf.keras.Sequential([
    #       tf.keras.layers.TextVectorization(
    #           max_tokens=50,
    #           standardize=None,
    #           split=tokenization_1,
    #           vocabulary=job_title_vocabs,
    #           pad_to_max_tokens=True
    #       ),
    #       tf.keras.layers.Embedding(len(job_title_vocabs) + 1, 32),
    #       tf.keras.layers.GlobalAvgPool1D()
    #     ])

    def call(self, features):
        return tf.concat([
            self.job_id_embedding(features["job_id"]),
            self.category_embedding(features["category"]),
            self.location_embedding(features["location"]),
            self.level_embedding(features["level"]),
            self.skill_embedding(features["skill_text"])
        ], axis=1)


class CandidateModel(tf.keras.Model):
  """Model for encoding movies."""

  def __init__(self, layer_sizes):
    """Model for encoding movies.

    Args:
      layer_sizes:
        A list of integers where the i-th entry represents the number of units
        the i-th layer contains.
    """
    super().__init__()

    self.embedding_model = MovieModel()

    # Then construct the layers.
    self.dense_layers = tf.keras.Sequential()

    # Use the ReLU activation for all but the last layer.
    for layer_size in layer_sizes[:-1]:
      self.dense_layers.add(tf.keras.layers.Dense(layer_size, activation="relu"))

    # No activation for the last layer.
    for layer_size in layer_sizes[-1:]:
      self.dense_layers.add(tf.keras.layers.Dense(layer_size))

  def call(self, inputs):
    feature_embedding = self.embedding_model(inputs)
    return self.dense_layers(feature_embedding)


class MovielensModel(tfrs.models.Model):

    def __init__(self, layer_sizes):
        super().__init__()
        self.query_model = QueryModel(layer_sizes)
        self.candidate_model = CandidateModel(layer_sizes)
        self.task = tfrs.tasks.Retrieval(
            metrics=tfrs.metrics.FactorizedTopK(
                candidates=movies.batch(128).map(self.candidate_model),
            ),
        )

    def compute_loss(self, features, training=False):
        # We only pass the user id and timestamp features into the query model. This
        # is to ensure that the training inputs would have the same keys as the
        # query inputs. Otherwise the discrepancy in input structure would cause an
        # error when loading the query model after saving it.
        query_embeddings = self.query_model({
            "user_id": features["user_id"]
        })
        movie_embeddings = self.candidate_model({
            "job_id": features["job_id"],
            "job_title": features["job_title"],
            "category": features["category"],
            "location": features["location"],
            "skill_text": features["skill_text"],
            "level": features["level"],
        })

        return self.task(
            query_embeddings, movie_embeddings, compute_metrics=not training)


if __name__ == '__main__':
    '''Load data from Mongo'''
    mongo = MongoClient()
    data = list(mongo[MONGO_DB]["events"].find({}, {"_id": 0}))
    movies = list(mongo[MONGO_DB]["data"].find({}, {"_id": 0, "tag": 0}))
    df_meta = pd.DataFrame(movies)
    df = pd.DataFrame(data)

    '''Create Dataset'''
    df_meta["skill_text"] = df_meta.skill.apply(create_skill_text)

    df["user_rating"] = df.behavior.apply(create_rating)
    ratings = df[["userid", "jobId", "user_rating"]].merge(df_meta, on="jobId", how="left")

    tensor_slices = {
        "user_id": ratings.userid.values.tolist(),
        "job_id": ratings.jobId.values.tolist(),
        "location": ratings.location.values.tolist(),
        "category": ratings.category.values.tolist(),
        "level": ratings.level.values.tolist(),
        "job_title": ratings.title.values.tolist(),
        "skill_text": ratings.skill_text.values.tolist()
    }

    movies = tf.data.Dataset.from_tensor_slices({
        "job_id": df_meta.jobId.values.tolist(),
        "category": df_meta.category.values.tolist(),
        "location": df_meta.location.values.tolist(),
        "level": df_meta.level.values.tolist(),
        "job_title": df_meta.title.values.tolist(),
        "skill_text": df_meta.skill_text.values.tolist()
    })
    ratings = tf.data.Dataset.from_tensor_slices(tensor_slices)

    ''' Create Vocabs '''
    unique_job_titles = np.unique(np.concatenate(list(ratings.batch(1_000).map(
        lambda x: x["job_title"]))))
    unique_job_ids = np.unique(np.concatenate(list(ratings.batch(1_000).map(
        lambda x: x["job_id"]))))
    unique_user_ids = np.unique(np.concatenate(list(ratings.batch(1_000).map(
        lambda x: x["user_id"]))))
    unique_locations = np.unique(np.concatenate(list(ratings.batch(1_000).map(
        lambda x: x["location"]))))
    unique_levels = np.unique(np.concatenate(list(ratings.batch(1_000).map(
        lambda x: x["level"]))))
    unique_skills = np.unique(reduce(lambda x, y: x + y, df_meta.skill.values.tolist()))
    job_title_vocabs = np.unique(reduce(lambda x, y: x + y, df_meta.title.str.split(" ").tolist()))
    unique_category = np.unique(df_meta.category.values.tolist())

    '''Create data train'''
    tf.random.set_seed(42)
    shuffled = ratings.shuffle(100_000, seed=42, reshuffle_each_iteration=False)

    train = shuffled.take(80_000)
    test = shuffled.skip(80_000).take(20_000)

    cached_train = train.shuffle(100_000).batch(2048)
    cached_test = test.batch(4096).cache()

    num_epochs = 250

    model = MovielensModel([128, 64, 32])
    model.compile(optimizer=tf.keras.optimizers.Adagrad(0.01))

    one_layer_history = model.fit(
        cached_train,
        validation_data=cached_test,
        validation_freq=5,
        epochs=num_epochs,
        verbose=0)

    index = tfrs.layers.factorized_top_k.BruteForce(model.query_model, k=20)
    index.index_from_dataset(
        movies.batch(100).map(lambda x: (x["job_id"], model.candidate_model(x))))

    print(f"Recommendation {unique_user_ids[0]}")
    print(index({"user_id": np.array([unique_user_ids[0]])}))

    path = "./model"
    # index.save(path)
    # tf.saved_model.save(
    #     index
    # )
    tf.saved_model.save(index, export_dir=path)
    """Bulk user-item to Mongo"""
    score, preds = index({"user_id": unique_user_ids.reshape(len(unique_user_ids), 1)})
    mongo[MONGO_DB].User_Recs.bulk_write(
        list(map(lambda u, r, s: update_item_recs(u, r, s), unique_user_ids, preds.numpy(), score.numpy())), )

