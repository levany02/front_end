from elasticsearch import helpers
from elasticsearch.client import Elasticsearch
import random

import pandas as pd
from pymongo import MongoClient, UpdateOne


def get_list_item(recs):
    return [i["item"] for i in recs]


df = pd.read_parquet("/home/spark/ylv/recommend_scala/UserMapper.parquet")
df_1 = pd.read_parquet("/home/spark/ylv/recommend_scala/UserRecs.parquet")
df_1["itemId"] = df_1.recommendations.apply(get_list_item)
df_2 = pd.read_parquet("/home/spark/ylv/recommend_scala/ItemMapper.parquet")
itemMaper = dict(zip(df_2.item.values, df_2.job.values))

def map_job(jobIds):
    return [itemMaper[i] for i in jobIds]

df_1["item"] = df_1.itemId.apply(map_job)
print(itemMaper)
df_1 = df_1.merge(df, on="user")
print(df_1[["item", "userid"]].to_dict(orient="records")[:5])

#con = MongoClient()

#data_bulk = [UpdateOne({"_id": i["userid"]}, {"$set": i}, upsert=True) for i in df_1[["item", "userid"]].to_dict(orient="records")]
#con.ecom_ur.UserRecs.bulk_write(data_bulk)


#set variables
elastichost = 'localhost'
port        = '9200'
outputIndex = 'user_recs'
counter     = 0
saveSize    = 3
eventCount  = 50 
es = Elasticsearch([{'host': elastichost, 'port' : port}])
actions = []

for lp in df_1[["item", "userid"]].to_dict(orient="records"):
    action = {
        "_index": outputIndex,
        '_op_type': 'index',
        "_type": "_doc",
        "_id": lp["userid"],
        "_source": lp
            }
    actions.append(action)
    counter += counter
    if len(actions) >= saveSize:
          helpers.bulk(es, actions)
          del actions[0:len(actions)]

if len(actions) > 0:
  helpers.bulk(es, actions)

print('All Finished')

