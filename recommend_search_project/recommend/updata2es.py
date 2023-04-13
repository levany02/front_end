import pandas as pd
from pymongo import MongoClient, UpdateOne


def get_list_item(recs):
    return [i["item"] for i in recs]


def get_list_location(recs):
    return [i["locationId"] for i in recs]


def get_list_level(recs):
    return [i["levelId"] for i in recs]


def get_list_user(recs):
    return [i["user"] for i in recs]


df = pd.read_parquet("/home/spark/ylv/recommend_scala/UserMapper.parquet")
df_1 = pd.read_parquet("/home/spark/ylv/recommend_scala/UserRecs.parquet")
df_1["itemId"] = df_1.recommendations.apply(get_list_item)
df_2 = pd.read_parquet("/home/spark/ylv/recommend_scala/ItemMapper.parquet")
df_3 = pd.read_parquet("/home/spark/ylv/recommend_scala/ItemRecs.parquet")
df_3["userId"] = df_3.recommendations.apply(get_list_user)
itemMaper = dict(zip(df_2.item.values, df_2.job.values))
userMaper = dict(zip(df.user.values, df.userid.values))

#print(userMaper)

def map_job(jobIds):
    return [itemMaper[i] for i in jobIds]

def map_user(users):
    return [userMaper[i] for i in users]



df_3["user"] = df_3.userId.apply(map_user)
#print(itemMaper)
df_1 = df_1.merge(df, on="user")
df_3 = df_3.merge(df_2, on="item")



#print(df_1[["item", "userid"]].to_dict(orient="records")[:5])
#print(df_3[["item", "userid"]].to_dict(orient="records")[:5])

con = MongoClient()

data_bulk = [UpdateOne({"_id": i["job"]}, {"$set": i}, upsert=True) for i in df_3[["job", "user"]].to_dict(orient="records")]
con.ecom_ur.ItemRecs.bulk_write(data_bulk)

print(df_3.head(5))




