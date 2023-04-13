from sanic import Sanic
from sanic.response import json
from sanic_openapi import openapi2_blueprint, doc
from pymongo import MongoClient, UpdateOne
from bson import ObjectId
import datetime

app = Sanic(__name__)
app.blueprint(openapi2_blueprint)


@app.post("/engines/events")
@doc.consumes(
    doc.JsonBody(
        {
            "userid": doc.String("UserId"),
            "job": doc.String("Job Title"),
            "behavior": doc.String("apply, search, view, ignore, hated")
        }
    ),
    location="body",
)
async def craete_event(request):
    try:
        source = request.json
        mongodb = MongoClient()
        mongodb.ecom_ur.event1.insert_one(source)
        try:
            print(source)
            #mongodb.ecom_ur.history.bulk_write([UpdateOne({}, {"$set": source}, upsert=True)])
            mongodb.ecom_ur.history.update_one({"userid": source["userid"]}, {"$set":source}, upsert=True)
            print("Update history success")
        except Exception as e:
            print(e)
            pass
        return json({"msg": "suscessful"})
    except Exception as err:
        return json({"msg": err})


@app.post("/recommend/users")
@doc.consumes(
    doc.JsonBody(
        {
            "userid": doc.String("UserId")
        }
    ),
    location="body",
)
async def queires(request):
    try:
        params = request.json
        mongodb = MongoClient()
        cb_recommendations=[]
        recommendations=[]
        if params.get("userid", None) in [None, '', "String"]:
            recommendations = list(mongodb.ecom_ur.meta.find({},{"_id": 0}).limit(5))
            print("recommend: -------------", recommendations)
        else:
            jobIds = mongodb.ecom_ur.UserRecs.find_one({"userid":params["userid"]})
            print("--------------jobiD: ", jobIds)
            try:
                history = mongodb.ecom_ur.history.find_one({"userid":params["userid"]})
                print("--------------history: ", history)
                job_group = history["job"].split("_")[1]
                print("-----------------------1")
                if history["behavior"] in ["ignore", "hated"]:
                    print("-----------------------2")
                    cb_recommendations = list(mongodb.ecom_ur.meta.find({"title": {"$not": {"$regex": job_group}}}, {"_id": 0}).limit(3))
                else:
                    print("-----------------------2")
                    cb_recommendations = list(mongodb.ecom_ur.meta.find({"title": {"$regex": job_group}}, {"_id": 0}).limit(3))

                print("cb recommend: ", cb_recommendations)
                print("history: ", history)
            except Exception as err:
                print("CB err: ", err)
                pass
            try:
                recommendations = list(mongodb.ecom_ur.meta.aggregate([{"$match": {"title": {"$in": jobIds["item"]}}}, {"$project": {"index": {"$indexOfArray":[jobIds["item"], "$title"]}, "title": 1, "salary": 1, "skill": 1, "location": 1}}, {"$sort": {"index": 1}}, {"$project": {"index": 0, "_id": 0}}]))
                print("recommend: -------------", recommendations)
            except:
                pass
        recommendations = recommendations + (cb_recommendations if len(cb_recommendations) > 0 else [])
        return json({"status": "success", "recommends": recommendations})
    except Exception as err:
        print(err)
        return json({"status": "failed", "recommends": []})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8001)

