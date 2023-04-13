from pymongo import MongoClient

BEHAVIOR_VALUES = {
    "apply": 5,
    "search": 4,
    "like": 4,
    "view": 3,
    "ignore": 0,
    "dislike": -3,
    "hated": -3
}


def create_rating_behavior(behaviors, username):
    user_feature_recommend_shortterm = {
        "userid": "",
        "item": {},
        "itemId": {},
        "location": {},
        "level": {},
        "skill": {}
    }

    def create_rating(behavior):
        if behavior["job"] in user_feature_recommend_shortterm["item"]:
            user_feature_recommend_shortterm["item"][behavior["job"]] = (user_feature_recommend_shortterm["item"][
                                                                             behavior["job"]] + BEHAVIOR_VALUES[
                                                                             behavior["behavior"]]) / 2
        else:
            user_feature_recommend_shortterm["item"][behavior["job"]] = BEHAVIOR_VALUES[behavior["behavior"]]

        if behavior["location"] in user_feature_recommend_shortterm["location"]:
            user_feature_recommend_shortterm["location"][behavior["location"]] = (user_feature_recommend_shortterm[
                                                                                      "location"][
                                                                                      behavior["location"]] +
                                                                                  BEHAVIOR_VALUES[
                                                                                      behavior["behavior"]]) / 2
        else:
            user_feature_recommend_shortterm["location"][behavior["location"]] = BEHAVIOR_VALUES[behavior["behavior"]]

        if behavior["level"] in user_feature_recommend_shortterm["level"]:
            user_feature_recommend_shortterm["level"][behavior["level"]] = (user_feature_recommend_shortterm["level"][
                                                                                behavior["level"]] + BEHAVIOR_VALUES[
                                                                                behavior["behavior"]]) / 2
        else:
            user_feature_recommend_shortterm["level"][behavior["level"]] = BEHAVIOR_VALUES[behavior["behavior"]]

    list(map(create_rating, behaviors))
    return user_feature_recommend_shortterm

mongo = MongoClient()
user_name = "User_Senior_Data_HCM_12"
items = mongo["data_1"]["User_Recs"].find_one({"_id": user_name})
itemId_recs = items["itemId"]
lst_itemId = list(items["itemId"].keys())
lst_score = list(items["itemId"].values())

abc = list(mongo["data_1"]["data"].aggregate([{"$match": {"jobId": {"$in": lst_itemId}}}, {"$addFields": {"idx": {"$indexOfArray": [lst_itemId, "$jobId"]}}}, {"$addFields": {"score": {"$arrayElemAt": [lst_score, "$idx"]}}}, {"$project": {"tag": 0, "_id": 0}}, {"$sort": {"idx": 1}}]))

behaviors = list(mongo["data_1"]["events"].find({"userid": user_name}, {"_id": 0, "time": 0}).sort("_id", -1).limit(10))
user_feature_recommend_shortterm = create_rating_behavior(behaviors, user_name)

# for doc in abc:
#     score = doc["score"] + \
#             (user_feature_recommend_shortterm["item"].get(doc["title"], 0) + items["item"].get(doc["title"], 0))/2 + \
#             (user_feature_recommend_shortterm["location"].get(doc["location"], 0) + items["location"].get(doc["location"], 0)) / 2 + \
#             (user_feature_recommend_shortterm["level"].get(doc["level"], 0) + items["level"].get(doc["level"], 0)) / 2

a = [{"_score": doc["score"] + \
            (user_feature_recommend_shortterm["item"].get(doc["title"], 0) + items["item"].get(doc["title"], 0))/2 + \
            (user_feature_recommend_shortterm["location"].get(doc["location"], 0) + items["location"].get(doc["location"], 0)) / 2 + \
            (user_feature_recommend_shortterm["level"].get(doc["level"], 0) + items["level"].get(doc["level"], 0)) / 2,
            "title": doc["title"],
            "level": doc["level"],
            "location": doc["location"],
            "salary": doc["salary"]
  } for doc in abc]
ranking = sorted(a, key=lambda x: x['_score'], reverse=True)

from elasticsearch import Elasticsearch

es = Elasticsearch()

search_txt = " ".join(
    list(set([i for i, _ in list(filter(lambda x: x[1] >= 2.5, list(user_feature_recommend_shortterm["item"].items())))] + \
    [i for i, _ in list(filter(lambda x: x[1] >= 2.5, list(user_feature_recommend_shortterm["location"].items())))] + \
    [i for i, _ in list(filter(lambda x: x[1] >= 2.5, list(user_feature_recommend_shortterm["level"].items())))])))

res = es.search(index="test_index_2", body={"size":2,"query":{"bool":{"should":[{"multi_match":{"query":search_txt,"fields":["title","location","level"],"type":"cross_fields","operator":"or"}}]}}})

print(res)

data = [{"score": doc["_score"] + \
        user_feature_recommend_shortterm["item"].get(doc["_source"]["title"], 0) + \
        user_feature_recommend_shortterm["location"].get(doc["_source"]["location"], 0)+ \
        user_feature_recommend_shortterm["level"].get(doc["_source"]["level"], 0) ,
        "title": doc["_source"]["title"],
        "jobId": doc["_source"]["jobId"],
        "skill": doc["_source"]["skill"],
        "level": doc["_source"]["level"],
        "location": doc["_source"]["location"],
        "salary": doc["_source"]["salary"]} for doc in res["hits"]["hits"]]

data = sorted(data, key=lambda x: x['score'], reverse=True)

print(data)