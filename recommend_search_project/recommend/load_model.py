import pandas as pd
from pymongo import MongoClient, UpdateOne


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


user_mapper = pd.read_parquet("/home/spark/ylv/recommend_scala/alsModel/UserMapper.parquet")
item_mapper = pd.read_parquet("/home/spark/ylv/recommend_scala/alsModel/ItemMapper.parquet")
location_mapper = pd.read_parquet("/home/spark/ylv/recommend_scala/alsModel/LocationMapper.parquet")
# level_mapper = pd.read_parquet("/home/spark/ylv/recommend_scala/alsModel/UserMapper.parquet")

user_recs = pd.read_parquet("/home/spark/ylv/recommend_scala/alsModel/userRecsItem.parquet")
location_recs = pd.read_parquet("/home/spark/ylv/recommend_scala/alsModel/userRecsLocation.parquet")
level_recs = pd.read_parquet("/home/spark/ylv/recommend_scala/alsModel/userRecsItemLevel.parquet")

user_recs["itemId"] = user_recs.recommendations.apply(get_list_item)
location_recs["locationId"] = location_recs.recommendations.apply(get_list_location)
level_recs["levelId"] = level_recs.recommendations.apply(get_list_level)
user_recs = user_recs.merge(user_mapper, on="user")

itemMaper = dict(zip(item_mapper.item.values, item_mapper.job.values))
userMaper = dict(zip(user_mapper.user.values, user_mapper.userid.values))
# levelMaper = dict(zip(level_recs.levelId.values, user_mapper.level.values))
levelMapper = {1: "Fresher", 2: "Junior", 3: "Middle", 4: "Senior", 5: "Leader"}
locationMaper = dict(zip(location_mapper.locationId.values, location_mapper.location.values))

user_recs = user_recs.merge(location_recs, on="user", suffixes=("_item", "_location"))
user_recs = user_recs.merge(level_recs, on="user", suffixes=("_item", "_level"))

# Skill
user_skill_mapper = pd.read_parquet("/home/spark/ylv/recommend_scala/alsModel/skill/UserMapper.parquet")
skill_mapper = pd.read_parquet("/home/spark/ylv/recommend_scala/alsModel/skill/SkillMapper.parquet")
user_recs_skill = pd.read_parquet("/home/spark/ylv/recommend_scala/alsModel/skill/userRecsSkill.parquet")
user_recs_skill["skillId"] = user_recs_skill.recommendations.apply(get_list_skill)
skillMaper = dict(zip(skill_mapper.skillId.values, skill_mapper.skill.values))


def map_skill(jobIds):
    return [skillMaper[i] for i in jobIds]

user_recs_skill["skill"] = user_recs_skill.skillId.apply(map_skill)
user_recs_skill = user_recs_skill.merge(user_skill_mapper, on="user")


def map_job(jobIds):
    return [itemMaper[i] for i in jobIds]


def map_user(users):
    return [userMaper[i] for i in users]


def map_location(users):
    return [locationMaper[i] for i in users]


def map_level(users):
    return [levelMapper[i] for i in users]


user_recs["item"] = user_recs.itemId.apply(map_job)
user_recs["location"] = user_recs.locationId.apply(map_location)
user_recs["level"] = user_recs.levelId.apply(map_level)

df_all = user_recs[["userid", "item", "location", "level"]].merge(user_recs_skill[["userid", "skill"]], on="userid")
# print(user_recs[["userid", "item", "location", "level"]].head(50))
print(df_all.head())