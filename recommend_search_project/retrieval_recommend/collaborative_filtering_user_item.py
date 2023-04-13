import os
import tempfile
from pymongo import MongoClient, UpdateOne
import pandas as pd
import matplotlib.pyplot as plt

import numpy as np
import tensorflow as tf
import tensorflow_datasets as tfds
import datetime
import tensorflow_recommenders as tfrs

MONGO_DB = "data_3"


BEHAVIORS = {
    "apply": 5.0,
    "search": 4.0,
    "like": 4.0,
    "view": 3.0,
    "ignore": 1.0,
    "dislike": 0.0,
    "hated": 0.0,
}

def create_rating(behavior):
    return BEHAVIORS.get(behavior, 0)


def scale_5(arr):
    arr = (arr - min(arr) + 1)/(max(arr) - min(arr))
    return list(map(float, arr*5/max(arr)))


def update_item_recs(user, recs, score):
    return UpdateOne({"_id": user.decode("ascii")}, {
        "$set": {
            "items": dict(zip(list(map(lambda x: x.decode("ascii"), recs)), scale_5(score)))
        }
    }, upsert=True)


def update_data(user, pred):
    return UpdateOne({"_id": user.decode("ascii")}, {"$set": {"items": list(map(lambda x: x.decode("ascii"),pred))}}, upsert=True)


def tokenization(t):
    return tf.strings.split(t, ',')


def tokenization_1(t):
    return tf.strings.split(t, ' ')


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

    self.title_embedding = tf.keras.Sequential([
      tf.keras.layers.StringLookup(
          vocabulary=unique_movie_titles,mask_token=None),
      tf.keras.layers.Embedding(len(unique_movie_titles) + 1, 32)
    ])
    self.location_embedding = tf.keras.Sequential([
      tf.keras.layers.StringLookup(
          vocabulary=unique_locations,mask_token=None),
      tf.keras.layers.Embedding(len(unique_locations) + 1, 32)
    ])
    self.level_embedding = tf.keras.Sequential([
      tf.keras.layers.StringLookup(
          vocabulary=unique_levels,mask_token=None),
      tf.keras.layers.Embedding(len(unique_levels) + 1, 32)
    ])

  def call(self, features):
    return tf.concat([
        self.title_embedding(features["movie_title"]),
        self.location_embedding(features["location"]),
        self.level_embedding(features["level"])
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
        "movie_title": features["movie_title"],
        "location": features["location"],
        "level": features["level"],
    })

    return self.task(
        query_embeddings, movie_embeddings, compute_metrics=not training)



if __name__ == '__main__':
    mongo = MongoClient()
    data = list(mongo["data_3"]["events"].find({}, {"_id": 0}))
    movies = list(mongo["data_3"]["data"].find({}, {"_id": 0, "tag": 0}))
    df_meta = pd.DataFrame(movies)
    df = pd.DataFrame(data)
    df["user_rating"] = df.behavior.apply(create_rating)
    ratings = df[["userid", "jobId", "location", "level", "user_rating"]]

    tensor_slices = {
        "user_id": ratings.userid.values.tolist(),
        "movie_title": ratings.jobId.values.tolist(),
        "location": ratings.location.values.tolist(),
        "level": ratings.level.values.tolist(),

    }

    movies = tf.data.Dataset.from_tensor_slices({
        "movie_title": df_meta.jobId.values.tolist(),
        "location": df_meta.location.values.tolist(),
        "level": df_meta.level.values.tolist(),
    })
    ratings = tf.data.Dataset.from_tensor_slices(tensor_slices)
    unique_movie_titles = np.unique(np.concatenate(list(ratings.batch(1_000).map(
        lambda x: x["movie_title"]))))
    unique_user_ids = np.unique(np.concatenate(list(ratings.batch(1_000).map(
        lambda x: x["user_id"]))))
    unique_locations = np.unique(np.concatenate(list(ratings.batch(1_000).map(
        lambda x: x["location"]))))
    unique_levels = np.unique(np.concatenate(list(ratings.batch(1_000).map(
        lambda x: x["level"]))))

    tf.random.set_seed(42)
    shuffled = ratings.shuffle(100_000, seed=42, reshuffle_each_iteration=False)

    train = shuffled.take(80_000)
    test = shuffled.skip(80_000).take(20_000)

    cached_train = train.shuffle(100_000).batch(2048)
    cached_test = test.batch(4096).cache()

    num_epochs = 300

    model = MovielensModel([256, 128, 64])
    model.compile(optimizer=tf.keras.optimizers.Adagrad(0.1))

    model.fit(
        cached_train,
        validation_data=cached_test,
        validation_freq=5,
        epochs=num_epochs,
        verbose=0)

    index = tfrs.layers.factorized_top_k.BruteForce(model.query_model, k=20)
    index.index_from_dataset(
        movies.batch(100).map(lambda x: (x["movie_title"], model.candidate_model(x))))

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
    mongo[MONGO_DB].User_Recs.bulk_write(list(map(lambda u, r, s: update_item_recs(u, r, s), unique_user_ids, preds.numpy(), score.numpy())), )




