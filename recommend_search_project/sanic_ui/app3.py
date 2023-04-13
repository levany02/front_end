from sanic import Sanic
from sanic.response import text, json
import json as jsont
import aiohttp
import asyncio
from pymongo import MongoClient
from bson import ObjectId, json_util
from sanic_openapi import doc
import datetime

import numpy as np
import os
os.environ["CUDA_VISIBLE_DEVICES"]="-1"
import tensorflow as tf
import sys

app = Sanic("MyHelloWorldApp")
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

MONGO_DATABASE = sys.argv[1]
INDEX = sys.argv[2]
MONGO_COLLECTION_EVENTS = "data"
MONGO_COLLECTION_HISTORY = "history1"
MONGO_COLLECTION_RECS = "User_Recs"
# ITEM_MODEL_PATH = "/home/spark/ylv/recommend_search_project/item_model"
USER_MODEL_PATH = "/home/spark/ylv/recommend_search_project/model"


USER_MODEL = tf.saved_model.load("/home/spark/ylv/recommend_search_project/model")
ITEM_MODEL = tf.saved_model.load("/home/spark/ylv/recommend_search_project/item_model")


def create_feature_rating(rating_items):
    items = dict()
    for i in rating_items:
        if i["name"] in items:
            items[i["name"]] = (items[i["name"]] + i["value"])/2
        else:
            items[i["name"]] = i["value"]
    return items


def create_rating_behavior(behaviors, username):
    user_feature_recommend_shortterm = {
        "userid": "",
        "item": {},
        "itemId": {},
        "location": {},
        "category": {},
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

        if behavior["category"] in user_feature_recommend_shortterm["category"]:
            user_feature_recommend_shortterm["category"][behavior["category"]] = (user_feature_recommend_shortterm["category"][
                                                                                behavior["category"]] + BEHAVIOR_VALUES[
                                                                                behavior["behavior"]]) / 2
        else:
            user_feature_recommend_shortterm["category"][behavior["category"]] = BEHAVIOR_VALUES[behavior["behavior"]]

    list(map(create_rating, behaviors))
    return user_feature_recommend_shortterm


@app.get("/")
async def hello_world(request):
    return text("hello world")


@app.get("/recommend-item")
async def recommend_items(request):
    _, pred = ITEM_MODEL(
        {"item1": np.array(["Job_Senior_Data_Scientist_HN_Senior_WZANXM"]),
         "location_item1": np.array(["other"]),
         "level_item1": np.array(["Senior"]),
         "skill_text_item1": np.array(["MLops,Machine Learning,Python,Deep Learning,Machine Learning"])
         }
    )
    pred = list(map(lambda x: x.decode('ascii'), pred.numpy()[0]))
    return json({"data": pred})


@app.get("/recommend-user")
async def recommend_items(request):
    _score, pred = USER_MODEL({"user_id": np.array(["User_Fresher_NodeJS_Developer_DN_15"])})
    pred = list(map(lambda x: x.decode('ascii'), pred.numpy()[0]))
    return json({"data": pred})


@app.route("/search-page")
@app.ext.template("boostrap_test2.html")
async def navi_search_page(request):
    user_name = request.args.get("username", "User_Default")
    last_item = list(mongo[MONGO_DATABASE].events.find({"userid": user_name}).sort("_id", -1).limit(1))
    if len(last_item) > 0:
        user_model_recs = mongo[MONGO_DATABASE].User_Recs.find_one({"_id": user_name})
        user_model_recs = list(user_model_recs["items"].keys()) if user_model_recs is not None else []
        item_model_recs = mongo[MONGO_DATABASE].Item_Recs.find_one({"_id": last_item[0]["jobId"]})
        item_model_recs = list(item_model_recs["items"].keys()) if item_model_recs is not None else []
        lst_items = user_model_recs[:10] + item_model_recs[:10]
        ranking = list(mongo[MONGO_DATABASE].data.aggregate([{"$match": {"jobId": {"$in": lst_items}}}, {"$addFields": {"idx": {"$indexOfArray": [lst_items, "$jobId"]}}}, {"$sort": {"idx": 1}},{"$unset": ["tag", "_id"]}]))
    else:
        ranking = list(mongo[MONGO_DATABASE].data.find({},{"tag": 0, "_id": 0}).limit(10))
    return {"message": "Your job : ", "results": ranking, "username": user_name, "keyword": ""}


@app.route('/events')
async def background_process_test(request):
    print(request.args)
    args = request.args["eventName"][0]
    status = "Applied" if args.lower() == "apply" else ("Viewed" if args.lower() == "view" else ("Ignored" if args.lower() == "ignore" else "Disliked"))
    _doc = {
        "userid":request.args["userId"][0],
        "jobId": request.args["jobId"][0],
        "job": request.args["job"][0],
        "skill": request.args["skill"],
        "category": request.args["category"][0],
        "level": request.args["level"][0],
        "location": request.args["location"][0],
        "salary": 1000,
        "behavior": request.args["eventName"][0],
        "time": datetime.datetime.now()
    }
    print(_doc)
    mongo[MONGO_DATABASE]["events"].update_one({"userid": _doc["userid"], "jobId": _doc["jobId"]}, {"$set": _doc}, upsert=True)
    return json({"status": status})


@app.route("/search", methods=["GET"])
@app.ext.template("boostrap_test2.html")
async def search_result(request):
    params = request.args
    keyword = params.get("keyword", "Data")
    print(keyword)
    username = params.get("username", "User_Default")
    location = params.get("location", None)
    level = params.get("level", None)
    _filter = []
    if location not in [None, "", "all"]:
        _filter.append({"term": {"location": LOCATIONS[location]}})
    if level not in [None, "", "all"]:
        _filter.append({"term": {"level": level}})
    query = {
              "size": 40,
              "query": {
                "bool": {
                  "should": [
                    {
                      "multi_match": {
                        "query": keyword,
                        "fields": ["title"]
                      }
                    },
                    {
                      "match": {
                        "category": keyword
                      }
                    },
                    {
                      "match": {
                        "skill": keyword
                      }
                    }
                  ],
                  "filter": _filter
                }
              }
            }

    async with aiohttp.ClientSession() as session:
        async with session.post(f"http://localhost:9200/{INDEX}/_search", data=jsont.dumps(query), headers={'Content-type': 'application/json'}) as res:
            data = await res.json()

    historical_items = list(mongo[MONGO_DATABASE].events.find({"userid": username}).sort("_id", -1).limit(10))
    user_model_recs = {}
    item_model_recs = {}
    content_base_weight = {"userid": "", "item": {}, "itemId": {}, "location": {}, "category": {}, "level": {}, "skill": {}}
    if len(historical_items) > 0:
        try:
            content_base_weight = create_rating_behavior(historical_items, '')
        except Exception as err:
            print("err:", err)
            pass
        print("*"*60)
        print(content_base_weight)
        user_model_recs = mongo[MONGO_DATABASE].User_Recs.find_one({"_id": username})
        user_model_recs = user_model_recs["items"] if user_model_recs is not None else {}
        item_model_recs = mongo[MONGO_DATABASE].Item_Recs.find_one({"_id": historical_items[0]["jobId"]})
        item_model_recs = item_model_recs["items"] if item_model_recs is not None else {}

    res = [i["_source"] for i in data["hits"]["hits"]]
    ranking = [{"score": doc["_score"] + \
                         user_model_recs.get(doc["_source"]["jobId"], 0)  + \
                         # item_model_recs.get(doc["_source"]["jobId"], 0) + \
                         content_base_weight["item"].get(doc["_source"]["title"], 0) + \
                         content_base_weight["category"].get(doc["_source"]["category"], 0) + \
                         content_base_weight["location"].get(doc["_source"]["location"], 0) + \
                         content_base_weight["level"].get(doc["_source"]["level"], 0) +\
                   0,
                "title": doc["_source"]["title"],
                "jobId": doc["_source"]["jobId"],
                "skill": doc["_source"]["skill"],
                "level": doc["_source"]["level"],
                "category": doc["_source"]["category"],
                "location": doc["_source"]["location"],
                "image": doc["_source"]["image"],
                "salary": doc["_source"]["salary"]} for doc in data["hits"]["hits"]]
    ranking = sorted(ranking, key=lambda x: x['score'], reverse=True)
    print(ranking)
    print(keyword)
    return {"message": f"Results (10/{data['hits']['total']['value']}): ", "results":ranking[:10], "username": username, "keyword": keyword}


@app.get("/history")
@app.ext.template("show_history.html")
async def recommend_items(request):
    params = request.args
    username = params.get("username", "User_Default")
    history = list(mongo[MONGO_DATABASE].events.find({"userid": username}, {"_id": 0, "time": 0}).sort("_id", -1).limit(10))
    history = list(mongo[MONGO_DATABASE].events.aggregate([{"$match": {"userid": username}}, {
        "$group": {"_id": {"jobId": "$jobId", "behavior": "$behavior"}, "job": {"$first": "$job"},
                   "skill": {"$first": "$skill"}, "location": {"$first": "$location"}, "level": {"$first": "$level"},
                   "view_count": {"$sum": 1}}}, {"$unset": ["time"]}]))
    print(history)
    return {"events": history}


@app.get("/explain")
@app.ext.template("explain.html")
async def recommend_items(request):
    params = request.args
    username = params.get("username", "User_Default")
    history = list(mongo[MONGO_DATABASE].events.aggregate([{"$match": {"userid": username}}, {"$sort": {"_id": -1}}, {"$group": {"_id": "$userid", "items": {"$addToSet": "$jobId"}}}]))
    items = history[0]["items"]
    same_view_user = list(mongo[MONGO_DATABASE].events.find({"jobId": {"$in": items}}, {"userid": 1, "_id": 0}))
    same_view_user = [i["userid"] for i in same_view_user]
    matched_job = list(mongo[MONGO_DATABASE].events.aggregate([{"$match": {"userid": {"$in": same_view_user}}}, {"$group": {"_id": "$jobId", "view_count": {"$sum": 1}, "skill": {"$first": "$skill"}, "job": {"$first": "$job"}, "location": {"$first": "$location"}, "level": {"$first": "$level"}}}, {"$sort": {"view_count": -1}}]))
    try:
        user_recs = mongo[MONGO_DATABASE].User_Recs.find_one({"_id": username})
        user_recs = list(user_recs["items"].keys())
        user_recs = mongo[MONGO_DATABASE].data.aggregate([{"$match": {"jobId": {"$in": user_recs}}}, {"$addFields": {"idx": {"$indexOfArray": [user_recs, "$jobId"]}}}, {"$sort": {"idx": 1}}, {"$unset": ["tag", "_id", "time"]}])
    except:
        user_recs = []
    try:
        item_recs = mongo[MONGO_DATABASE].Item_Recs.find_one({"_id": items[0]})
        item_recs = list(item_recs["items"].keys())
        item_recs = mongo[MONGO_DATABASE].data.aggregate(
            [{"$match": {"jobId": {"$in": item_recs}}}, {"$addFields": {"idx": {"$indexOfArray": [item_recs, "$jobId"]}}},
             {"$sort": {"idx": 1}}, {"$unset": ["tag", "_id", "time"]}])
    except:
        item_recs
    history = list(
        mongo[MONGO_DATABASE].events.find({"userid": username}, {"_id": 0, "time": 0}).sort("_id", -1).limit(10))
    return {"histories": history, "events": matched_job, "user_recs": user_recs, "item_recs": item_recs}


if __name__ == '__main__':
    app.run(host="0.0.0.0", workers=4, port=8080, debug=True)
