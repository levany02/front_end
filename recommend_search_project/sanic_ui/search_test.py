import asyncio
import aiohttp
from pymongo import MongoClient
import json as jsont
import requests

mongo = MongoClient()

LOCATIONS = {
    "Ho Chi Minh": "HCM",
    "Da Nang": "DN",
    "Ha Noi": "HN",
    "other": "other"
}

BEHAVIOR_VALUES = {
    "apply": 5,
    "search": 4,
    "like": 4,
    "view": 3,
    "ignore": 1,
    "dislike": 0,
    "hated": 0
}

MONGO_DATABASE = "data_1"
MONGO_COLLECTION_EVENTS = "data"
MONGO_COLLECTION_HISTORY = "history1"
MONGO_COLLECTION_RECS = "User_Recs"


def create_feature_rating(rating_items):
    items = dict()
    for i in rating_items:
        if i["name"] in items:
            items[i["name"]] = (items[i["name"]] + i["value"])/2
        else:
            items[i["name"]] = i["value"]
    return items


def create_query(userid, k=10):
    pipeline = [{"$match": {"userid": userid}},
                {"$sort": {"_id": -1}},
                {"$limit": k},
                {"$project":
                 {
                     "userid": 1,
                     "rating":
                         {
                             "$switch":
                                 {
                                     "branches":
                                         [{"case": {"$eq": ["$behavior", "apply"]},"then": 5},
                                          {"case": {"$eq": ["$behavior", "search"]},"then": 4},
                                          {"case": {"$eq": ["$behavior", "like"]},"then": 4},
                                          {"case": {"$eq": ["$behavior", "view"]},"then": 3},
                                          {"case": {"$eq": ["$behavior", "ignore"]},"then": 1},
                                          {"case": {"$eq": ["$behavior", "hated"]},"then": 0},
                                          {"case": {"$eq": ["$behavior", "dislike"]},"then": 0}],
                                     "default": 3}},

                     "location": 1,
                     "level": 1,
                     "job": 1,
                     "jobId": 1
                 }
            },
            {
              "$group": {
                  "_id": "$userid",
                  "item": {"$push": {"name": "$job", "value": "$rating"}},
                  "itemId": {"$push": {"name": "$jobId", "value": "$rating"}},
                  "level": {"$push": {"name": "$level", "value": "$rating"}},
                  "location": {"$push": {"name": "$location", "value": "$rating"}}
             }
            }
        ]
    return pipeline


def search_result(keyword="developer", username="", location=None, level=None):
    _filter = []
    if location not in [None, "", "all"]:
        _filter.append({"term": {"location": LOCATIONS[location]}})
    if level not in [None, "", "all"]:
        _filter.append({"term": {"level": level}})

    user_feature_recommend = mongo["data_1"]["User_Recs"].find_one({"_id": username})
    try:
        _doc = list(mongo["data_1"]["events"].aggregate(create_query(userid=username)))[0]
        user_feature_recommend_shortterm = {
            "userid": _doc["_id"],
            "item": create_feature_rating(_doc["item"]),
            "itemId": create_feature_rating(_doc["itemId"]),
            "location": create_feature_rating(_doc["location"]),
            "level": create_feature_rating(_doc["level"]),
            "skill": {}
        }
    except Exception as err:
        print(err)
        user_feature_recommend_shortterm = {
            "userid": "",
            "item": {},
            "itemId": {},
            "location": {},
            "level": {},
            "skill": {}
        }
    query = {
              "size": 100,
              "query": {
                "bool": {
                  "must": [
                    {"multi_match": {
                      "query": keyword,
                      "fields": ["title"]
                    }}
                  ],
                  "filter": _filter
                }
              }

            }
    print("abc: ", user_feature_recommend_shortterm)
    user_feature_recommend = user_feature_recommend if user_feature_recommend is not None else dict(item={}, level={}, location={}, itemId={})

    data = requests.post("http://localhost:9200/test_index_2/_search", data=jsont.dumps(query), headers={'Content-type': 'application/json'})
    data = jsont.loads(data.text)
    print(data["hits"]["hits"][:10])
    ranking = [{"_score": _doc["_score"] + \
                          (user_feature_recommend["itemId"].get(_doc["_source"]["jobId"], 0) + user_feature_recommend_shortterm["itemId"].get(_doc["_source"]["jobId"], 0))/2 + \
                          (user_feature_recommend["item"].get(_doc["_source"]["title"], 0)  + user_feature_recommend_shortterm["item"].get(_doc["_source"]["title"], 0))/2+ \
                          (user_feature_recommend["location"].get(_doc["_source"]["location"], 0) + user_feature_recommend_shortterm["location"].get(_doc["_source"]["location"], 0))/2+ \
                           (user_feature_recommend["level"].get(_doc["_source"]["level"], 0)+ user_feature_recommend_shortterm["level"].get(_doc["_source"]["level"], 0))/2, "_source": _doc["_source"]}
               for _doc in data["hits"]["hits"]]

    # print(ranking)

    ranking = sorted(ranking, key=lambda x: x['_score'], reverse=True)
    res = [i for i in ranking]
    # return {"message": "Results : ", "results":res, "username": username, "keyword": keyword}
    return res


if __name__ == '__main__':
    res = search_result("Data", "User_Data_Scientist_Senior_HCM_test02")
    print(res[:10])
