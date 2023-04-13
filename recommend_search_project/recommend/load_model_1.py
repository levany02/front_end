import pandas as pd
from pymongo import MongoClient, UpdateOne

MONGO_DATABASE = "data_1"


def get_list_item(recs):
    return [i["item"] for i in recs]


def get_list_user(recs):
    return [i["user"] for i in recs]


def get_list_location(recs):
    return [i["locationId"] for i in recs]


def get_list_level(recs):
    return [i["levelId"] for i in recs]


def get_list_skill(recs):
    return [i["skillId"] for i in recs]


if __name__ == '__main__':
    user_mapper = pd.read_parquet("/home/spark/ylv/recommend_scala/alsModel/UserMapper.parquet")
    item_id_mapper = pd.read_parquet("/home/spark/ylv/recommend_scala/alsModel/ItemIdMapper.parquet")
    item_name_mapper = pd.read_parquet("/home/spark/ylv/recommend_scala/alsModel/ItemNameMapper.parquet")
    location_mapper = pd.read_parquet("/home/spark/ylv/recommend_scala/alsModel/LocationMapper.parquet")
    # level_mapper = pd.read_parquet("/home/spark/ylv/recommend_scala/alsModel/UserMapper.parquet")

    user_item_id_recs = pd.read_parquet("/home/spark/ylv/recommend_scala/alsModel/userRecsItemId.parquet")
    user_item_name_recs = pd.read_parquet("/home/spark/ylv/recommend_scala/alsModel/userRecsItemName.parquet")
    location_recs = pd.read_parquet("/home/spark/ylv/recommend_scala/alsModel/userRecsLocation.parquet")
    level_recs = pd.read_parquet("/home/spark/ylv/recommend_scala/alsModel/userRecsItemLevel.parquet")

    # user_recs["itemId"] = user_recs.recommendations.apply(get_list_item)
    # location_recs["locationId"] = location_recs.recommendations.apply(get_list_location)
    # level_recs["levelId"] = level_recs.recommendations.apply(get_list_level)
    user_recs = user_item_id_recs.merge(user_mapper, on="user")

    itemIdMaper = dict(zip(item_id_mapper.item_id.values, item_id_mapper.jobId.values))
    itemNameMaper = dict(zip(item_name_mapper.item_name.values, item_name_mapper.job.values))
    userMaper = dict(zip(user_mapper.user.values, user_mapper.userid.values))
    # levelMaper = dict(zip(level_recs.levelId.values, user_mapper.level.values))
    levelMapper = {1: "Fresher", 2: "Junior", 3: "Middle", 4: "Senior", 5: "Leader"}
    locationMaper = dict(zip(location_mapper.locationId.values, location_mapper.location.values))
    print(itemNameMaper)

    # Skill
    user_skill_mapper = pd.read_parquet("/home/spark/ylv/recommend_scala/alsModel/skill/UserMapper.parquet")
    skill_mapper = pd.read_parquet("/home/spark/ylv/recommend_scala/alsModel/skill/SkillMapper.parquet")
    user_recs_skill = pd.read_parquet("/home/spark/ylv/recommend_scala/alsModel/skill/userRecsSkill.parquet")


    # user_recs_skill["skillId"] = user_recs_skill.recommendations.apply(get_list_skill)
    skillMaper = dict(zip(skill_mapper.skillId.values, skill_mapper.skill.values))


    def map_skill(jobIds):
        # return [skillMaper[i] for i in jobIds]
        return [{skillMaper[int(list(i.values())[0])]: list(i.values())[1]} for i in jobIds]

    print(user_recs_skill.head())
    user_recs_skill["skill"] = user_recs_skill.recommendations.apply(map_skill)
    user_recs_skill = user_recs_skill.merge(user_skill_mapper, on="user")

    print(user_recs_skill.head())


    def map_job_id(jobIds):
        # return [itemMaper[i] for i in jobIds]
        tmp = [(itemIdMaper[int(list(i.values())[0])], list(i.values())[1]) for i in jobIds]
        return dict(tmp)

    def map_job_name(jobIds):
        # return [itemMaper[i] for i in jobIds]
        tmp = [(itemNameMaper[int(list(i.values())[0])], list(i.values())[1]) for i in jobIds]
        return dict(tmp)

        # return [tmp.update({itemMaper[int(list(i.values())[0])], list(i.values())[1]}) for i in jobIds]
    def map_user(users):
        # return [userMaper[i] for i in users]
        return dict([(userMaper[int(list(i.values())[0])], list(i.values())[1]) for i in users])

    def map_location(users):
        # return [locationMaper[i] for i in users]
        return dict([(locationMaper[int(list(i.values())[0])], list(i.values())[1]) for i in users])


    def map_level(users):
        # return [levelMapper[i] for i in users]
        return dict([(levelMapper[int(list(i.values())[0])],list(i.values())[1]) for i in users])

    # user_recs = user_recs.merge(location_recs, on="user", suffixes=("_item", "_location"))
    # user_recs = user_recs.merge(level_recs, on="user", suffixes=("_item", "_level"))

    user_recs["itemId"] = user_recs.recommendations.apply(map_job_id)
    user_item_name_recs["item"] = user_item_name_recs.recommendations.apply(map_job_name)
    location_recs["location"] = location_recs.recommendations.apply(map_location)
    level_recs["level"] = level_recs.recommendations.apply(map_level)

    user_recs = user_recs.merge(user_item_name_recs, on="user", suffixes=("_itemid", "_item"))
    user_recs = user_recs.merge(location_recs, on="user", suffixes=("_itemid", "_location"))
    user_recs = user_recs.merge(level_recs, on="user", suffixes=("_itemid", "_level"))

    # df_all = user_recs[["userid", "item", "location", "level"]].merge(user_recs_skill[["userid", "skill"]], on="userid")
    # print(user_recs[["userid", "item", "location", "level"]].head(50))
    print(user_recs[["userid", "itemId", "item", "location", "level"]].head())

    # print(user_recs[user_recs["userid"]=="User_Leader_Front-end_DN_38"][["userid", "item", "location", "level"]].values)
    #
    # print(user_recs.columns)
    print(user_recs.head(2)[["userid", "itemId", "item", "location", "level"]].to_dict(orient="records"))

    mongo = MongoClient()

    data_bulk = [UpdateOne({"_id": _doc["userid"]}, {"$set": _doc}, upsert=True) for _doc in user_recs[["userid", "itemId", "item", "location", "level"]].to_dict(orient="records")]
    mongo[MONGO_DATABASE]["User_Recs"].bulk_write(data_bulk)
