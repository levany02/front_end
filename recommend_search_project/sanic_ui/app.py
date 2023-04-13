from sanic import Sanic
from sanic.response import text, json
import json as jsont
import aiohttp
import asyncio
from pymongo import MongoClient
from bson import ObjectId, json_util
from sanic_openapi import doc
import datetime

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


@app.get("/")
async def hello_world(request):
    return text("hello world")


@app.get("/foo", strict_slashes=True)
@app.ext.template("foo.html")
async def handeler(request):
    return {"seq": ["one", "two"]}


@app.route("/search", methods=["GET"])
@app.ext.template("boostrap_test2.html")
async def search_result(request):
    params = request.args
    keyword = params.get("keyword", "Middle")
    username = params.get("username", "User_Default")
    location = params.get("location", None)
    level = params.get("level", None)
    _filter = []
    if location not in [None, "", "all"]:
        _filter.append({"term": {"location": LOCATIONS[location]}})
    if level not in [None, "", "all"]:
        _filter.append({"term": {"level": level}})

    user_feature_recommend = mongo["data_1"]["User_Recs"].find_one({"_id": username})
    try:
        behaviors = list(
            mongo["data_1"]["events"].find({"userid": username}, {"_id": 0, "time": 0}).sort("_id", -1).limit(10))
        user_feature_recommend_shortterm = create_rating_behavior(behaviors, username)
    except Exception as err:
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
    print("abc", user_feature_recommend_shortterm)
    print("abc", user_feature_recommend)
    user_feature_recommend = user_feature_recommend if user_feature_recommend is not None else dict(item={}, level={}, location={}, itemId={})
    async with aiohttp.ClientSession() as session:
        async with session.post("http://localhost:9200/test_index_2/_search", data=jsont.dumps(query), headers={'Content-type': 'application/json'}) as res:
            data = await res.json()
    ranking = [{"_score": _doc["_score"] + \
                          (user_feature_recommend["itemId"].get(_doc["_source"]["jobId"], 0) + user_feature_recommend_shortterm["itemId"].get(_doc["_source"]["jobId"], 0))/2 + \
                          (user_feature_recommend["item"].get(_doc["_source"]["title"], 0)  + user_feature_recommend_shortterm["item"].get(_doc["_source"]["title"], 0))/2+ \
                          (user_feature_recommend["location"].get(_doc["_source"]["location"], 0) + user_feature_recommend_shortterm["location"].get(_doc["_source"]["location"], 0))/2+ \
                           (user_feature_recommend["level"].get(_doc["_source"]["level"], 0)+ user_feature_recommend_shortterm["level"].get(_doc["_source"]["level"], 0))/2, "_source": _doc["_source"]}
               for _doc in data["hits"]["hits"]]
    ranking = sorted(ranking, key=lambda x: x['_score'], reverse=True)
    res = [i["_source"] for i in ranking][:10]
    return {"message": f"Results (10/{data['hits']['total']['value']}): ", "results":res, "username": username, "keyword": keyword}


@app.route("/search-test", methods=["GET"])
async def search_result(request):
    params = request.args
    keyword = params.get("keyword", "Middle")
    username = params.get("username", "User_Default")
    location = params.get("location", None)
    level = params.get("level", None)
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
        user_feature_recommend_shortterm = {
            "userid": "",
            "item": {},
            "itemId": {},
            "location": {},
            "level": {},
            "skill": {}
        }
    query = {
              "size": 20,
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
    user_feature_recommend = user_feature_recommend if user_feature_recommend is not None else dict(item={}, level={}, location={}, itemId={})
    async with aiohttp.ClientSession() as session:
        async with session.post("http://localhost:9200/test_index_2/_search", data=jsont.dumps(query), headers={'Content-type': 'application/json'}) as res:
            data = await res.json()
    ranking = [{"_score": _doc["_score"] + \
                          (user_feature_recommend["itemId"].get(_doc["_source"]["jobId"], 0) + user_feature_recommend_shortterm["itemId"].get(_doc["_source"]["jobId"], 0))/2 + \
                          (user_feature_recommend["item"].get(_doc["_source"]["title"], 0)  + user_feature_recommend_shortterm["item"].get(_doc["_source"]["title"], 0))/2+ \
                          (user_feature_recommend["location"].get(_doc["_source"]["location"], 0) + user_feature_recommend_shortterm["location"].get(_doc["_source"]["location"], 0))/2+ \
                           (user_feature_recommend["level"].get(_doc["_source"]["level"], 0)+ user_feature_recommend_shortterm["level"].get(_doc["_source"]["level"], 0))/2, "_source": _doc["_source"]}
               for _doc in data["hits"]["hits"]]
    ranking = sorted(ranking, key=lambda x: x['_score'], reverse=True)
    res = [i["_source"] for i in ranking][:10]
    print("data result: ", data['hits']['total']['value'])
    return json({"message": f"Results (10/{data['hits']['total']['value']}): ", "results":res, "username": username, "keyword": keyword})


@app.route("/search-page")
@app.ext.template("boostrap_test2.html")
async def navi_search_page(request):
    user_name = request.args.get("username", "User_Default")
    keyword = request.args.get("keyword", "")
    query = mongo["data_1"].User_Recs.aggregate([{"$match": {"userid": user_name}},
                                                 {"$project": {"_id": 0, "recItem": {"$objectToArray": "$itemId"}}},
                                                 {"$unwind": "$recItem"},
                                                 {"$project": {"recItem": "$recItem.k"}},
                                                 {"$lookup": {"from": "data", "localField": "recItem", "foreignField": "jobId", "as": "user_rec_mapping"}},
                                                 {"$project": {"descript": {"$arrayElemAt":["$user_rec_mapping", 0]}}},
                                                 {"$project": {"jobId": "$descript.jobId", "title": "$descript.title", "level": "$descript.level", "location": "$descript.location", "skill": "$descript.skill", "salary": "$descript.salary"}}])
    data = list(query)[:10]
    if len(data) < 1:
        try:
            _doc = list(mongo["data_1"]["events"].aggregate(create_query(userid=user_name)))[0]
            user_feature_recommend = {
                "userid": _doc["_id"],
                "item": create_feature_rating(_doc["item"]),
                "itemId": create_feature_rating(_doc["itemId"]),
                "location": create_feature_rating(_doc["location"]),
                "level": create_feature_rating(_doc["level"]),
                "skill": {}
            }
            """create query ES shorterm"""
            search_txt = " ".join([i for i, _ in list(filter(lambda x: x[1] >= 2.5, list(user_feature_recommend["item"].items())))] + \
                            [i for i, _ in list(filter(lambda x: x[1] >= 2.5,list(user_feature_recommend["location"].items())))] + \
                            [i for i, _ in list(filter(lambda x: x[1] >= 2.5, list(user_feature_recommend["level"].items())))])
            query_es = {
                      "size": 100,
                      "query": {
                        "bool": {
                          "should": [
                            {
                              "multi_match": {
                                "query": search_txt,
                                "fields": ["title", "location", "level"],
                                "type": "cross_fields",
                                "operator": "or"
                              }
                            }
                          ]
                        }
                      }
                    }
            async with aiohttp.ClientSession() as session:
                async with session.post("http://localhost:9200/test_index_2/_search", data=jsont.dumps(query_es), headers={'Content-type': 'application/json'}) as res:
                    res_es = await res.json()
            ranking = [{"_score": _doc["_score"] + user_feature_recommend["itemId"].get(_doc["_source"]["jobId"], 0) +
                                  user_feature_recommend["item"].get(_doc["_source"]["title"], 0) + user_feature_recommend[
                                      "location"].get(_doc["_source"]["location"], 0) + user_feature_recommend["level"].get(
                _doc["_source"]["level"], 0), "_source": _doc["_source"]} for _doc in res_es["hits"]["hits"]]
            ranking = sorted(ranking, key=lambda x: x['_score'], reverse=True)
            data = [i["_source"] for i in ranking][:10]
        except Exception as err:
            data = []
    if len(data) < 1:
        data = list(mongo["data_1"]["data"].aggregate([{"$limit": 10},{"$project": {"title": 1, "level":1, "location": 1, "skill": 1, "jobId": 1}}]))
    return {"message": "Your job recommendation : ", "results": data, "username": user_name, "keyword": keyword}


@app.route("/v2/search-page")
@app.ext.template("boostrap_test2.html")
async def navi_search_page_test(request):
    user_name = request.args.get("username", "User_Default")
    """ USER SHORTERM BEHAVIOR """
    behaviors = list(
        mongo["data_1"]["events"].find({"userid": user_name}, {"_id": 0, "time": 0}).sort("_id", -1).limit(10))
    user_feature_recommend_shortterm = create_rating_behavior(behaviors, user_name)
    items = mongo["data_1"]["User_Recs"].find_one({"_id": user_name})
    if items is not None:
        """ CASE 1: LOGGED IN USER : TRAINED """
        lst_itemId = list(items["itemId"].keys())
        lst_score = list(items["itemId"].values())

        data = list(mongo["data_1"]["data"].aggregate(
            [{"$match": {"jobId": {"$in": lst_itemId}}}, {"$addFields": {"idx": {"$indexOfArray": [lst_itemId, "$jobId"]}}},
             {"$addFields": {"score": {"$arrayElemAt": [lst_score, "$idx"]}}}, {"$project": {"tag": 0, "_id": 0}},
             {"$sort": {"idx": 1}}]))
        ranking = [{"score": doc["score"] + \
                    (user_feature_recommend_shortterm["item"].get(doc["title"], 0) + items["item"].get(doc["title"],
                                                                                                       0)) / 2 + \
                    (user_feature_recommend_shortterm["location"].get(doc["location"], 0) + items["location"].get(
                        doc["location"], 0)) / 2 + \
                    (user_feature_recommend_shortterm["level"].get(doc["level"], 0) + items["level"].get(doc["level"],
                                                                                                         0)) / 2,
                  "title": doc["title"],
                  "jobId": doc["jobId"],
                  "level": doc["level"],
                  "location": doc["location"],
                  "salary": doc["salary"],
                  "skill": doc["skill"]} for doc in data]
        ranking = sorted(ranking, key=lambda x: x['score'], reverse=True)

    elif len(behaviors) > 0:
        """ CASE 2: LOGGED IN USER : TRAINED NOT YET"""
        search_txt = " ".join(
            list(set([i for i, _ in
                      list(filter(lambda x: x[1] >= 2.5, list(user_feature_recommend_shortterm["item"].items())))]+\
                     [i for i, _ in
                      list(filter(lambda x: x[1] >= 2.5, list(user_feature_recommend_shortterm["location"].items())))]+\
                     [i for i, _ in
                      list(filter(lambda x: x[1] >= 2.5, list(user_feature_recommend_shortterm["level"].items())))])))
        query_es = {"size": 100, "query": {"bool": {"should": [{"multi_match": {"query": search_txt,
                                                                                "fields": ["title", "location",
                                                                                           "level"],
                                                                                "type": "cross_fields",
                                                                                "operator": "or"}}]}}}
        async with aiohttp.ClientSession() as session:
            async with session.post("http://localhost:9200/test_index_2/_search", data=jsont.dumps(query_es),
                                    headers={'Content-type': 'application/json'}) as res:
                es_data = await res.json()
        ranking = [{"score": doc["_score"] + \
                          user_feature_recommend_shortterm["item"].get(doc["_source"]["title"], 0) + \
                          user_feature_recommend_shortterm["location"].get(doc["_source"]["location"], 0) + \
                          user_feature_recommend_shortterm["level"].get(doc["_source"]["level"], 0),
                 "title": doc["_source"]["title"],
                 "jobId": doc["_source"]["jobId"],
                 "skill": doc["_source"]["skill"],
                 "level": doc["_source"]["level"],
                 "location": doc["_source"]["location"],
                 "salary": doc["_source"]["salary"]} for doc in es_data["hits"]["hits"]]
        ranking = sorted(ranking, key=lambda x: x['score'], reverse=True)
    else:
        ranking = list(mongo["data_1"]["data"].aggregate(
            [{"$limit": 10}, {"$project": {"title": 1, "level": 1, "location": 1, "skill": 1, "jobId": 1}}]))
    return {"message": "Your job : ", "results": ranking, "username": user_name, "keyword": ""}


@app.route("/events-page")
@app.ext.template("events.html")
async def navi_events_page(request):
    return {}


@app.route("/change-user")
@app.ext.template("change_user.html")
async def change_user(request):
    return {}


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
        "level": request.args["level"][0],
        "location": request.args["location"][0],
        "salary": 1000,
        "behavior": request.args["eventName"][0],
        "time": datetime.datetime.now()
    }
    print(_doc)
    mongo["data_3"]["events"].update_one({"userid": _doc["userid"], "jobId": _doc["jobId"]}, {"$set": _doc}, upsert=True)
    return json({"status": status})


@app.route('/es')
async def test_es(request):
    async with aiohttp.ClientSession() as session:
        async with session.post("http://localhost:9200/test_index_2/_search", data=jsont.dumps({"query":{"multi_match":{"query":"Data","fields":["title"]}}}), headers={'Content-type': 'application/json'}) as res:
            data = await res.json()
            res = [i["_source"] for i in data["hits"]["hits"]]
    return json({"data": res})


@app.route("/update_mongodb")
async def test_mongo(request):
    username = "User_Data_Scientist_Fresher_HCM_test03"
    user_feature_recommend = mongo["data_1"]["User_Recs"].find_one({"_id": username})
    return json({"user_feature_recommend": user_feature_recommend})


@app.route("/test-search")
async def test_mongo(request):
    username = "User_Data_Scientist_Fresher_HCM_test03"
    user_feature_recommend = mongo["data_1"]["User_Recs"].find_one({"_id": username})
    user_feature_recommend_shortterm = {
        "userid": "",
        "item": {},
        "itemId": {},
        "location": {},
        "level": {},
        "skill": {}
    }
    async with aiohttp.ClientSession() as session:
        async with session.post("http://localhost:9200/test_index_2/_search", data=jsont.dumps({"query":{"multi_match":{"query":"Data","fields":["title"]}}}), headers={'Content-type': 'application/json'}) as res:
            data = await res.json()
            res = [i["_source"] for i in data["hits"]["hits"]]

    ranking = [{"_score": _doc["_score"] + \
                          (user_feature_recommend["itemId"].get(_doc["_source"]["jobId"], 0) +
                           user_feature_recommend_shortterm["itemId"].get(_doc["_source"]["jobId"], 0)) / 2 + \
                          (user_feature_recommend["item"].get(_doc["_source"]["title"], 0) +
                           user_feature_recommend_shortterm["item"].get(_doc["_source"]["title"], 0)) / 2 + \
                          (user_feature_recommend["location"].get(_doc["_source"]["location"], 0) +
                           user_feature_recommend_shortterm["location"].get(_doc["_source"]["location"], 0)) / 2 + \
                          (user_feature_recommend["level"].get(_doc["_source"]["level"], 0) +
                           user_feature_recommend_shortterm["level"].get(_doc["_source"]["level"], 0)) / 2,
                "_source": _doc["_source"]}
               for _doc in data["hits"]["hits"]]
    ranking = sorted(ranking, key=lambda x: x['_score'], reverse=True)
    res = [i["_source"] for i in ranking][:10]
    return json({"user_feature_recommend": user_feature_recommend, "data": res})


@app.route("/history/<username>")
async def history_username(request, username):
    data = list(mongo["data_1"]["events"].find({"userid": username}).sort("_id", -1))
    return json({"data": json_util.loads(data)})


@app.route("/test-behavior")
async def history_username(request):
    tmp = {
        "userid": "",
        "item": {},
        "itemId": {},
        "location": {},
        "level": {},
        "skill": {}
    }
    def create_rating(behavior):
        if behavior["job"] in tmp["item"]:
            tmp["item"][behavior["job"]] = (tmp["item"][behavior["job"]] + BEHAVIOR_VALUES[behavior["behavior"]]) / 2
        else:
            tmp["item"][behavior["job"]] = BEHAVIOR_VALUES[behavior["behavior"]]

        if behavior["location"] in tmp["location"]:
            tmp["location"][behavior["location"]] = (tmp["location"][behavior["location"]] + BEHAVIOR_VALUES[
                behavior["behavior"]]) / 2
        else:
            tmp["location"][behavior["location"]] = BEHAVIOR_VALUES[behavior["behavior"]]

        if behavior["level"] in tmp["level"]:
            tmp["level"][behavior["level"]] = (tmp["level"][behavior["level"]] + BEHAVIOR_VALUES[
                behavior["behavior"]]) / 2
        else:
            tmp["level"][behavior["level"]] = BEHAVIOR_VALUES[behavior["behavior"]]
    username = "User_Data_Scientist_Fresher_HCM_test03"
    behaviors = list(mongo["data_1"]["events"].find({"userid": username}, {"_id": 0, "time": 0}).sort("_id", -1).limit(10))
    list(map(create_rating, behaviors))
    return json(tmp)


if __name__ == '__main__':
    app.run(host="0.0.0.0", workers=4,port=8080, debug=True)
