from pymongo import MongoClient, UpdateOne
import pandas as pd
import itertools
from functools import reduce
import os
import tempfile
import numpy as np
import tensorflow as tf
import tensorflow_datasets as tfds

import tensorflow_recommenders as tfrs


def create_pair_items(lst_items):
    return list(itertools.combinations(lst_items, 2))


def create_skill_text(lst_skill):
    return ','.join(lst_skill)


def tokenization(t):
    return tf.strings.split(t, ',')


def scale_5(arr):
    arr = (arr - min(arr) + 1)/(max(arr) - min(arr))
    return list(map(float, arr*5/max(arr)))


def update_item_recs(item, recs, score):
    return UpdateOne({"_id": item}, {
        "$set": {
            "items": dict(zip(list(map(lambda x: x.decode("ascii"), recs)), scale_5(score)))
        }
    }, upsert=True)


class Movie1Model(tf.keras.Model):

    def __init__(self):
        super().__init__()

        max_tokens = 10_000

        self.title_embedding = tf.keras.Sequential([
            tf.keras.layers.StringLookup(
                vocabulary=unique_items, mask_token=None),
            tf.keras.layers.Embedding(len(unique_items) + 1, 32)
        ])
        self.location_embedding = tf.keras.Sequential([
            tf.keras.layers.StringLookup(
                vocabulary=unique_location, mask_token=None),
            tf.keras.layers.Embedding(len(unique_location) + 1, 32)
        ])
        self.level_embedding = tf.keras.Sequential([
            tf.keras.layers.StringLookup(
                vocabulary=unique_level, mask_token=None),
            tf.keras.layers.Embedding(len(unique_level) + 1, 32)
        ])
        self.skill_embedding = tf.keras.Sequential([
            tf.keras.layers.TextVectorization(
                max_tokens=50,
                vocabulary=unique_skill,
                standardize=None,
                split=tokenization,
                pad_to_max_tokens=True
            ),
            tf.keras.layers.Embedding(len(unique_skill) + 1, 32),
            tf.keras.layers.GlobalAvgPool1D()
        ])

    def call(self, features):
        return tf.concat([
            self.title_embedding(features["item1"]),
            self.location_embedding(features["location_item1"]),
            self.level_embedding(features["level_item1"]),
            self.skill_embedding(features["skill_text_item1"])
        ], axis=1)


class Movie2Model(tf.keras.Model):

    def __init__(self):
        super().__init__()

        max_tokens = 10_000

        self.title_embedding = tf.keras.Sequential([
            tf.keras.layers.StringLookup(
                vocabulary=unique_items, mask_token=None),
            tf.keras.layers.Embedding(len(unique_items) + 1, 32)
        ])
        self.location_embedding = tf.keras.Sequential([
            tf.keras.layers.StringLookup(
                vocabulary=unique_location, mask_token=None),
            tf.keras.layers.Embedding(len(unique_location) + 1, 32)
        ])
        self.level_embedding = tf.keras.Sequential([
            tf.keras.layers.StringLookup(
                vocabulary=unique_level, mask_token=None),
            tf.keras.layers.Embedding(len(unique_level) + 1, 32)
        ])
        self.skill_embedding = tf.keras.Sequential([
            tf.keras.layers.TextVectorization(
                max_tokens=50,
                vocabulary=unique_skill,
                standardize=None,
                split=tokenization,
                pad_to_max_tokens=True
            ),
            tf.keras.layers.Embedding(len(unique_skill) + 1, 32),
            tf.keras.layers.GlobalAvgPool1D()
        ])

    def call(self, features):
        return tf.concat([
            self.title_embedding(features["item2"]),
            self.location_embedding(features["location_item2"]),
            self.level_embedding(features["level_item2"]),
            self.skill_embedding(features["skill_text_item2"])
        ], axis=1)


class MovielensModel(tfrs.Model):

    def __init__(self):
        super().__init__()
        self.movie_1_model: tf.keras.Model = Movie1Model()
        self.movie_2_model: tf.keras.Model = Movie2Model()
        self.task: tf.keras.layers.Layer = tfrs.tasks.Retrieval(
            metrics=tfrs.metrics.FactorizedTopK(
                candidates=jobs.batch(128).map(self.movie_2_model)
            )
        )

    def compute_loss(self, features, training=False) -> tf.Tensor:
        # We pick out the user features and pass them into the user model.
        movie_1_embeddings = self.movie_1_model(features)
        # And pick out the movie features and pass them into the movie model,
        # getting embeddings back.
        movie_2_embeddings = self.movie_2_model(features)

        # The task computes the loss and the metrics.
        return self.task(movie_1_embeddings, movie_2_embeddings)


if __name__ == '__main__':
    mongo = MongoClient()
    data = list(mongo.data_3.events.find({}, {"userid": 1, "jobId": 1}))
    meta_data = list(mongo.data_3.data.find({}, {"tag": 0, "_id": 0}))
    df = pd.DataFrame(data)
    df_1 = df.groupby(["userid"]).aggregate({"jobId": list}).reset_index()
    df_1["num_items"] = df_1.jobId.str.len()
    item_item_data = reduce(lambda x, y: x + y, list([create_pair_items(i) for i in df_1.jobId.values.tolist()]))
    df_2 = pd.DataFrame(item_item_data, columns=["item1", "item2"])
    meta_df = pd.DataFrame(meta_data)
    meta_df["skill_text"] = meta_df.skill.apply(create_skill_text)
    df_2 = df_2.merge(meta_df, how="left", left_on="item1", right_on="jobId")
    df_2 = df_2.merge(meta_df, how="left", left_on="item2", right_on="jobId", suffixes=("_item1", "_item2"))

    ''' Create dataset '''
    tensor_slices = {
        "item1": df_2.item1.values.tolist(),
        "title_item1": df_2.title_item1.values.tolist(),
        "location_item1": df_2.location_item1.values.tolist(),
        "level_item1": df_2.level_item1.values.tolist(),
        "skill_text_item1": df_2.skill_text_item1.values.tolist(),
        "item2": df_2.item2.values.tolist(),
        "title_item2": df_2.title_item2.values.tolist(),
        "location_item2": df_2.location_item2.values.tolist(),
        "level_item2": df_2.level_item2.values.tolist(),
        "skill_text_item2": df_2.skill_text_item2.values.tolist()
    }

    jobs = tf.data.Dataset.from_tensor_slices({
        "item2": meta_df.jobId.values.tolist(),
        "location_item2": meta_df.location.values.tolist(),
        "level_item2": meta_df.level.values.tolist(),
        "skill_text_item2": meta_df.skill_text.values.tolist()
    })

    items = tf.data.Dataset.from_tensor_slices(tensor_slices)
    unique_items = np.unique(meta_df.jobId.values.tolist())
    unique_location = np.unique(meta_df.location.values.tolist())
    unique_level = np.unique(meta_df.level.values.tolist())
    unique_skill = np.unique(reduce(lambda x, y: x + y, meta_df.skill.values.tolist()))

    tf.random.set_seed(42)
    shuffled = items.shuffle(100_000, seed=42, reshuffle_each_iteration=False)

    train = shuffled.take(80_000)
    test = shuffled.skip(80_000).take(20_000)

    cached_train = train.shuffle(100_000).batch(2048)
    cached_test = test.batch(4096).cache()

    model = MovielensModel()
    model.compile(optimizer=tf.keras.optimizers.Adagrad(learning_rate=0.1))

    model.fit(cached_train, epochs=3)
    # Create a model that takes in raw query features, and
    index = tfrs.layers.factorized_top_k.BruteForce(model.movie_1_model, k=20)
    # recommends movies out of the entire movies dataset.
    index.index_from_dataset(
        jobs.batch(100).map(lambda x: (x["item2"], model.movie_2_model(x)))
    )

    path = "/home/spark/ylv/recommend_search_project/item_model"
    tf.saved_model.save(
        index,
        path,
        options=tf.saved_model.SaveOptions(namespace_whitelist=["brute_force_1"])
    )
    # loaded_model = tf.saved_model.load(path)
    n, _ = meta_df.shape
    score, pred = index({"item1": np.array(meta_df.jobId.values.tolist()).reshape(n, 1),
                         "location_item1": np.array(meta_df.location.values.tolist()).reshape(n, 1),
                         "level_item1": np.array(meta_df.level.values.tolist()).reshape(n, 1),
                         "skill_text_item1": np.array(meta_df.skill_text.values.tolist()).reshape(n, 1)
                         })
    mongo.data_3.Item_Recs.bulk_write(list(map(lambda i, r, s: update_item_recs(i, r, s), meta_df.jobId.values.tolist(), pred.numpy(), score.numpy())))