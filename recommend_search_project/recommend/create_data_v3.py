import random
from multiprocessing import Pool
from pymongo import MongoClient, InsertOne, UpdateOne
import string
import datetime
import sys

flag = sys.argv[1]
print(flag)


LANGUAGES = ["Javascript", "Python", "Go", "Java", "Kotlin", "PHP", "C#", "Swift", "R", "Ruby", "C", "C++", "Matlab", "TypeScript", "Scala", "SQL", "HTML", "CSS", "NoSQL", "Rust", "Perl"]
LOCATIONS = ["HN", "HCM", "DN", "other"]
JOB_TITLES = ["Backend", "Front-end", "Devops","Data"]
BEHAVIOR = ["apply", "search", "view", "ignore", "hated"]

GROUP_JOBS = {
    "Data": ["Java", "Python", "Scala", "Data Engineering", "Spark", "HDFS", "MapReduce", "Machine Learning"],
    "Backend": ["Backend", "NodeJS", "Golang", "PHP", "Java", ".NET", "English", "System", "ReactJS", "Python", "JavaScript","API"],
    "Front-end": ["ReactJS", "JavaScript", "Typescript", "MongoDB", "Agile", "HTML", "jQuery", "Front-End"],
    "Devops": ["Linux", "Python", "Perl", "DevOps", "Azure", "Aws"]
}

GROUP_JOBS_NEW = {
    "Data Scientist": ["Python", "Data Analytic", "Machine Learning", "Deep Learning", "Probability &  Statistic", "Big-data", "Visualization"],
    "Machine Learning": ["Scala", "Python", "Machine Learning", "MLops", "Deep Learning"],
    "Data Analyst": ["Python", "Data Analytic", "Visualization", "SQL"],
    "Data Engineer": ["Java", "Python", "Scala", "Data Engineering", "Spark", "HDFS", "MapReduce", "Machine Learning"],
    "Big Data Engineer": ["Java", "Python", "Scala", "Data Engineering", "Spark", "HDFS", "MapReduce", "Machine Learning"],
    "Python Developer" : ["Python", "Flask", "Django", "SQL", "API"],
    "Java Engineer" : ["Java", "Spring", "SQL"],
    "Backend": ["Backend", "NodeJS", "Golang", "PHP", "Java", ".NET", "English", "System", "ReactJS", "Python", "JavaScript","API"],
    "Front-end": ["ReactJS", "JavaScript", "Typescript", "MongoDB", "Agile", "HTML", "jQuery", "Front-End"],
    "Devops": ["Linux", "Python", "Perl", "DevOps", "Azure", "Aws"]
}

SALARY_LEVELS = {
    "Fresher": {"MIN": 500, "MAX":1500, "level": 1},
    "Junior": {"MIN": 1000, "MAX":2000, "level": 2},
    "Middle": {"MIN": 1500, "MAX":2500, "level": 3},
    "Senior": {"MIN": 2000, "MAX": 3000, "level": 4},
    "Leader": {"MIN": 3000, "MAX": 5000, "level": 5},
}

MONGO_DATABASE = "data_2"
MIN = 1000
MAX = 10000
CHARS=string.ascii_uppercase + string.digits


def create_jobs(_):
    jobs_titles = 'job_' + random.choice(JOB_TITLES) + '_' + ''.join(random.choices(CHARS, k=6))
    level = random.choice(list(SALARY_LEVELS.keys()))
    return {"title": jobs_titles,
            "location": random.choice(LOCATIONS),
            "skill": create_skill(jobs_titles),
            "salary": random.randint(SALARY_LEVELS[level]["MIN"], SALARY_LEVELS[level]["MAX"]),
            "level": level
            }


def create_behavior(user, job_title):
    if "Data" in user:
        if "Front-end" in job_title:
            return random.choice(["ignore", "hated"])
        elif ("Backend" in job_title) or ("Devops" in job_title):
            return random.choice(["view", "ignore"])
        else:
            return random.choice(["apply", "search"])
    elif "Devops" in user:
        if "Front-end" in job_title:
            return random.choice(["ignore", "hated"])
        elif ("Data" in job_title):
            return random.choice(["view", "ignore"])
        else:
            return random.choice(["apply", "search"])
    elif "Front-end" in user:
        if "Front-end" in job_title:
            return random.choice(["apply", "search"])
        elif ("Backend" in job_title):
            return random.choice(["view", "ignore"])
        else:
            return random.choice(["ignore", "hated"])
    else:
        if "Data" in job_title:
            return random.choice(["ignore", "hated"])
        elif ("Front-end" in job_title):
            return random.choice(["view", "ignore"])
        else:
            return random.choice(["apply", "search"])


def create_skill(job_title):
    if "Data" in job_title:
        return random.choices(GROUP_JOBS["Data"], k=5)
    elif "Devops" in job_title:
        return random.choices(GROUP_JOBS["Devops"], k=5)
    elif "Front-end" in job_title:
        return random.choices(GROUP_JOBS["Front-end"], k=5)
    else:
        return random.choices(GROUP_JOBS["Backend"], k=5)


def create_events(user, job_title):
    return {
        "userid": user,
        "job": job_title,
        "behavior": create_behavior(user, job_title),
        "time": datetime.datetime.now() + datetime.timedelta(days=random.randint(-150, 0))
    }


def create_user_events(user, job):
    return {
        "userid": user,
        "job": job["title"],
        "skill": job["skill"],
        "level": job["level"],
        "location": job["location"],
        "salary": job["salary"],
        "behavior": create_behavior(user, job["title"]),
        "time": datetime.datetime.now() + datetime.timedelta(days=random.randint(-150, 0))
    }


def userid_generator(_):
    return 'user_' + random.choice(list(SALARY_LEVELS.keys())) + "_" + random.choice(JOB_TITLES) + '_' + ''.join(random.choices(CHARS, k=6))


BEHAVIORS_FORM = {
    "Data": {
        "apply": {
                    "values": ["Data"],
                    "weight": [1.0]
                  },
        "search": {
                    "values": ["Data"],
                    "weight": [1.0]
                },
        "view": {
                    "values": ["Data", "Backend"],
                    "weight": [0.9, 0.1]
        },
        "ignore": {
                    "values":["Backend", "Front-end", "Devops"],
                    "weight": [0.1, 0.7, 0.2]
                   },
        "hated": {
                    "values": ["Front-end"],
                    "weight": [1.0]
                }
    },
    "Backend": {
        "apply": {
                    "values": ["Backend"],
                    "weight": [1]
        },
        "search": {
                    "values": ["Backend"],
                    "weight": [1.0]
        },
        "view": {
                    "values": ["Backend"],
                    "weight": [1.0]
        },
        "ignore": {
                    "values": ["Front-end", "Data", "Devops"],
                    "weight": [0.6, 0.2, 0.2]
        },
        "hated": {
                    "values": ["Front-end"],
                    "weight": [1.0]
        }
    },
    "Front-end": {
        "apply": {
                    "values": ["Front-end"],
                    "weight": [1.0]
        },
        "search": {
                    "values": ["Front-end"],
                    "weight": [1.0]
        },
        "view": {
                    "values": ["Front-end"],
                    "weight": [1.0]
        },
        "ignore": {
                    "values": ["Backend", "Data", "Devops"],
                    "weight": [0.3, 0.4, 0.3]
        },
        "hated": {
                    "values": ["Data", "Devops"],
                    "weight": [0.6, 0.4]
        }
    },
    "Devops": {
        "apply": {
                    "values": ["Devops"],
                    "weight": [1.0]
        },
        "search": {
                    "values": ["Devops"],
                    "weight": [1.0]
        },
        "view": {
                    "values": ["Backend", "Devops"],
                    "weight": [0.2, 0.8]
        },
        "ignore": {
                    "values": ["Data", "Front-end"],
                    "weight": [0.5, 0.5]
        },
        "hated": {
                    "values": ["Data", "Front-end"],
                    "weight": [0.5, 0.5]
        }
    }
}

LOCATIONS_USER_BEHAVIOR_FORM = {
    "HCM": {
        "apply": {
                    "values": ["HCM_User","General_User"],
                    "weight": [0.8, 0.2]
        },
        "search": {
                    "values": ["HCM_User","General_User"],
                    "weight": [0.8, 0.2]
        },
        "view": {
                    "values": ["HCM_User","General_User"],
                    "weight": [0.8, 0.2]
        },
        "ignore": {
                    "values": ["HN_User", "DN_User", "General_User"],
                    "weight": [0.3, 0.3, 0.4]
        },
        "hated": {
                    "values": ["HN_User", "DN_User", "General_User"],
                    "weight": [0.3, 0.3, 0.4]
        }
    },
    "HN": {
        "apply": {
                    "values": ["HN_User", "General_User"],
                    "weight": [0.8, 0.2]
        },
        "search": {
                    "values": ["HN_User","General_User"],
                    "weight": [0.8, 0.2]
        },
        "view": {
                    "values": ["HN_User", "HCM_User", "DN_User", "General_User"],
                    "weight": [0.4, 0.3, 0.2, 0.1]
        },
        "ignore": {
                    "values": ["HCM_User", "DN_User", "General_User"],
                    "weight": [0.3, 0.3, 0.4]
        },
        "hated": {
                    "values": ["HCM_User", "DN_User", "General_User"],
                    "weight": [0.3, 0.3, 0.4]
        }
    },
    "DN": {
        "apply": {
                    "values": ["DN_User"],
                    "weight": [1.0]
        },
        "search": {
                    "values":["DN_User"],
                    "weight": [1.0]
        },
        "view": {
                    "values": ["DN_User"],
                    "weight": [1.0]
        },
        "ignore": {
                    "values": ["HN_User", "HCM_User", "General_User"],
                    "weight": [0.3, 0.3, 0.4]
        },
        "hated": {
                    "values": ["HN_User", "HCM_User", "General_User"],
                    "weight": [0.3, 0.3, 0.4]
        }
    },
    "other": {
        "apply": {
                    "values": ["HN_User", "HCM_User", "DN_User", "General_User"],
                    "weight": [0.25, 0.25, 0.25, 0.25]
        },
        "search": {
                    "values": ["HN_User", "HCM_User", "DN_User", "General_User"],
                    "weight": [0.25, 0.25, 0.25, 0.25]
        },
        "view": {
                    "values": ["HN_User", "HCM_User", "DN_User", "General_User"],
                    "weight": [0.25, 0.25, 0.25, 0.25]
        },
        "ignore":{
                    "values": ["HN_User", "HCM_User", "DN_User", "General_User"],
                    "weight": [0.25, 0.25, 0.25, 0.25]
        },
        "hated": {
                    "values": ["HN_User", "HCM_User", "DN_User", "General_User"],
                    "weight": [0.25, 0.25, 0.25, 0.25]
        }
    }
}

LOCATIONS_BEHAVIOR_FORM = {
    "DN_User": {
        "apply": {
                    "values": ["DN"],
                    "weight": [1.0]
        },
        "search": {
                    "values": ["DN"],
                    "weight": [1.0]
        },
        "view": {
                    "values": ["DN", "HCM", "HN"],
                    "weight": [1.0, 0.0, 0.0]
        },
        "ignore": {
                    "values": ["HCM", "HN", "other"],
                    "weight": [0.3, 0.3, 0.4]
        },
        "hated": {
                    "values": ["other"],
                    "weight": [1.0]
        }
    },
    "HCM_User": {
        "apply": {
                    "values": ["HCM"],
                    "weight": [1.0]
        },
        "search": {
                    "values": ["HCM"],
                    "weight": [1.0]
        },
        "view": {
                    "values": ["HCM", "HN", "DN"],
                    "weight": [1.0, 0.0, 0.0]
        },
        "ignore": {
                    "values": ["HN", "DN", "other"],
                    "weight": [0.3, 0.3, 0.4]
        },
        "hated": {
                    "values": ["HN", "DN", "other"],
                    "weight": [0.3, 0.3, 0.4]
        }
    },
    "HN_User": {
        "apply": {
                    "values": ["HN"],
                    "weight": [1.0]
        },
        "search": {
                    "values": ["HN"],
                    "weight": [1.0]
        },
        "view": {
                    "values": ["HN", "HCM", "DN"],
                    "weight": [1.0, 0.0, 0.0]
        },
        "ignore": {
                    "values": ["other", "HCM", "DN"],
                    "weight": [0.5, 0.3, 0.2]
        },
        "hated": {
                    "values": ["other", "HCM", "DN"],
                    "weight": [0.5, 0.3, 0.2]
        },
    },
    "General_User": {
        "apply": {
                    "values": ["HN", "HCM", "DN", "other"],
                    "weight": [0.25, 0.25, 0.25, 0.25]
        },
        "search": {
                    "values": ["HN", "HCM", "DN", "other"],
                    "weight": [0.25, 0.25, 0.25, 0.25]
        },
        "view": {
                    "values": ["HN", "HCM", "DN", "other"],
                    "weight": [0.25, 0.25, 0.25, 0.25]
        },
        "ignore":{
                    "values": ["HN", "HCM", "DN", "other"],
                    "weight": [0.25, 0.25, 0.25, 0.25]
        },
        "hated": {
                    "values": ["HN", "HCM", "DN", "other"],
                    "weight": [0.25, 0.25, 0.25, 0.25]
        }
    }
}

LEVEL_BEHAVIOR_FORM = {
    "Leader": {
        "apply": {
                    "values": ["Leader", "Senior"],
                    "weight": [0.7, 0.3]
        },
        "search": {
                    "values": ["Leader", "Senior"],
                    "weight": [0.7, 0.3]
        },
        "view": {
                    "values": ["Leader", "Senior"],
                    "weight": [0.8, 0.2]
        },
        "ignore": {
                    "values": ["Senior", "Middle", "Junior", "Fresher"],
                    "weight": [0.1, 0.2, 0.3, 0.4]
        },
        "hated": {
                    "values": ["Junior", "Fresher"],
                    "weight": [0.3, 0.7]
        }
    },
    "Senior": {
        "apply": {
                    "values": ["Leader", "Senior"],
                    "weight": [0.4, 0.6]
        },
        "search": {
                    "values": ["Leader", "Senior"],
                    "weight": [0.4, 0.6]
        },
        "view": {
                    "values": ["Leader", "Senior"],
                    "weight": [0.4, 0.6]
        },
        "ignore": {
                    "values": ["Middle", "Junior", "Fresher"],
                    "weight": [0.2, 0.3, 0.5]
        },
        "hated": {
                    "values": ["Junior", "Fresher"],
                    "weight": [0.4, 0.6]
        }
    },
    "Middle": {
        "apply": {
                    "values": ["Senior", "Middle"],
                    "weight": [0.3, 0.7]
        },
        "search": {
                    "values": ["Senior", "Middle"],
                    "weight": [0.3, 0.7]
        },
        "view": {
                    "values": ["Senior", "Middle"],
                    "weight": [0.3, 0.7]
        },
        "ignore": {
                    "values": ["Leader"],
                    "weight": [1.0]
        },
        "hated": {
                    "values": ["Leader"],
                    "weight": [1.0]
        }
    },
    "Junior": {
        "apply": {
                    "values": ["Junior", "Middle"],
                    "weight": [0.7, 0.3]
        },
        "search": {
                    "values": ["Junior", "Middle", "Senior"],
                    "weight": [0.6, 0.3, 0.1]
        },
        "view": {
                    "values": ["Junior", "Middle", "Senior"],
                    "weight": [0.6, 0.3, 0.1]
        },
        "ignore": {
                    "values": ["Senior", "Middle", "Leader"],
                    "weight": [0.4, 0.2, 0.4]
        },
        "hated": {
                    "values": ["Senior", "Leader"],
                    "weight": [0.5, 0.5]
        }
    },
    "Fresher": {
        "apply": {
                    "values": ["Fresher", "Junior"],
                    "weight": [0.6, 0.4]
        },
        "search": {
                    "values": ["Fresher", "Junior"],
                    "weight": [0.6, 0.4]
        },
        "view": {
                    "values": ["Fresher", "Junior", "Middle"],
                    "weight": [0.5, 0.4, 0.1]
        },
        "ignore": {
                    "values": ["Junior", "Middle", "Senior", "Leader"],
                    "weight": [0.1, 0.2, 0.3, 0.4]
        },
        "hated": {
                    "values": ["Senior", "Leader"],
                    "weight": [0.5, 0.5]
        }
    }

}


def create_job_by_params(job_type='', job_level=''):
    title = f"{job_level} {job_type} {random.choice(['Scientist', 'Engineer', 'Analyst']) if job_type=='Data' else (random.choice(['Developer', 'Engineer']) if job_type in ['Backend', 'Front-end'] else '')}".strip().replace("  ", " ")
    if job_level=='':
        job_level = random.choice(list(SALARY_LEVELS.keys()))
    location = random.choices(LOCATIONS, [0.2, 0.3, 0.1, 0.4], k=1)[0]
    skills = random.choices(GROUP_JOBS[job_type], k=5)
    salary = random.randint(SALARY_LEVELS[job_level]["MIN"], SALARY_LEVELS[job_level]["MAX"])
    job_id = f"Job_{title.replace(' ', '_')}_{location}_{job_level}_{''.join(random.choices(CHARS, k=6))}"
    tags = {
        "apply": [
            {
                "userid": create_user_by_params(job_type, job_level, random.choices(LOCATIONS_USER_BEHAVIOR_FORM[location]["apply"]["values"], LOCATIONS_USER_BEHAVIOR_FORM[location]["apply"]["weight"], k=1)[0], "apply"),
                "job": title,
                "jobId": job_id,
                "skill": skills,
                "level": job_level,
                "location": location,
                "salary": salary,
                "behavior": "apply",
                "time": datetime.datetime.now() + datetime.timedelta(days=random.randint(-150, 0))
            }
             for _ in range(5)],
        "search": [{
                "userid": create_user_by_params(job_type, job_level, random.choices(LOCATIONS_USER_BEHAVIOR_FORM[location]["search"]["values"], LOCATIONS_USER_BEHAVIOR_FORM[location]["search"]["weight"], k=1)[0], "search"),
                "job": title,
                "jobId": job_id,
                "skill": skills,
                "level": job_level,
                "location": location,
                "salary": salary,
                "behavior": "search",
                "time": datetime.datetime.now() + datetime.timedelta(days=random.randint(-150, 0))
            } for _ in range(20)],
        "view": [{
                "userid": create_user_by_params(job_type, job_level, random.choices(LOCATIONS_USER_BEHAVIOR_FORM[location]["view"]["values"], LOCATIONS_USER_BEHAVIOR_FORM[location]["view"]["weight"], k=1)[0],"view"),
                "job": title,
                "jobId": job_id,
                "skill": skills,
                "level": job_level,
                "location": location,
                "salary": salary,
                "behavior": "view",
                "time": datetime.datetime.now() + datetime.timedelta(days=random.randint(-150, 0))
            } for _ in range(70)],
        "ignore": [{
                "userid": create_user_by_params(job_type, job_level, random.choices(LOCATIONS_USER_BEHAVIOR_FORM[location]["ignore"]["values"], LOCATIONS_USER_BEHAVIOR_FORM[location]["ignore"]["weight"], k=1)[0],"ignore"),
                "job": title,
                "jobId": job_id,
                "skill": skills,
                "level": job_level,
                "location": location,
                "salary": salary,
                "behavior": "ignore",
                "time": datetime.datetime.now() + datetime.timedelta(days=random.randint(-150, 0))
            } for _ in range(50)],
        "hated": [{
                "userid": create_user_by_params(job_type, job_level, random.choices(LOCATIONS_USER_BEHAVIOR_FORM[location]["hated"]["values"], LOCATIONS_USER_BEHAVIOR_FORM[location]["hated"]["weight"], k=1)[0],"hated"),
                "job": title,
                "jobId": job_id,
                "skill": skills,
                "level": job_level,
                "location": location,
                "salary": salary,
                "behavior": "hated",
                "time": datetime.datetime.now() + datetime.timedelta(days=random.randint(-150, 0))
            } for _ in range(7)] + [{
                "userid": create_user_specific(job_type, random.choices(LEVEL_BEHAVIOR_FORM[job_level]["hated"]["values"], LEVEL_BEHAVIOR_FORM[job_level]["hated"]["weight"], k=1)[0], location),
                "job": title,
                "jobId": job_id,
                "skill": skills,
                "level": job_level,
                "location": location,
                "salary": salary,
                "behavior": "hated",
                "time": datetime.datetime.now() + datetime.timedelta(days=random.randint(-150, 0))
            } for _ in range(3)]
    }

    job = {
        "jobId": job_id,
        "title": title,
        "location": location,
        "skill": random.choices(GROUP_JOBS[job_type], k=5),
        "salary": random.randint(SALARY_LEVELS[job_level]["MIN"], SALARY_LEVELS[job_level]["MAX"]),
        "level": job_level,
        "tag": tags
     }
    return job


def create_user_by_params(job_type, level, user_location, behavior):
    # level = random.choices(["Leader", "Senior", "Middle", "Junior", "Fresher"], [0.2, 0.2, 0.2, 0.2, 0.2], k=1)[0]
    # location = random.choices(["DN_User", "HN_User", "HCM_User", "General_User"], [0.1, 0.2, 0.3, 0.4], k=1)[0]
    location = user_location
    level_apply = random.choices(LEVEL_BEHAVIOR_FORM[level][behavior]["values"], LEVEL_BEHAVIOR_FORM[level][behavior]["weight"])[0]
    jop_apply = random.choices(BEHAVIORS_FORM[job_type][behavior]["values"], BEHAVIORS_FORM[job_type][behavior]["weight"])[0]
    location_apply = random.choices(LOCATIONS_BEHAVIOR_FORM[location][behavior]["values"], LOCATIONS_BEHAVIOR_FORM[location][behavior]["weight"])[0]
    return f"User_{level_apply}_{jop_apply}_{location_apply}_{random.randint(0, 100)}"


def create_user_specific(job_type, level, location):
    return f"User_{level}_{job_type}_{location}_{random.randint(0, 100)}"


if __name__ == '__main__':
    pool = Pool(processes=4)
    jobs = pool.starmap(create_job_by_params, [(random.choice(["Data", "Backend", "Devops", "Front-end"]), random.choices(["Leader", "Senior", "Middle", "Junior", "Fresher"], [0.1, 0.2, 0.2, 0.25, 0.25], k=1)[0]) for _ in range(500)])
    update_meta = list(map(lambda x: InsertOne(x), jobs))
    mongo = MongoClient()
    print("bulk data data....")
    mongo[MONGO_DATABASE].data.bulk_write(update_meta)
    # mongo.data_1.data.create_index({'jobId': 1})
    apply_user = list(mongo[MONGO_DATABASE].data.aggregate([{"$project": {"apply": "$tag.apply"}}, {"$unwind": "$apply"}, {
        "$project": {"userid": "$apply.userid", "jobId": "$apply.jobId", "job": "$apply.job", "skill": "$apply.skill", "level": "$apply.level",
                     "location": "$apply.location", "salary": "$apply.salary", "behavior": "$apply.behavior",
                     "time": "$apply.time"}}, {"$project": {"_id": 0}}]))
    search_user = list(mongo[MONGO_DATABASE].data.aggregate([{"$project": {"apply": "$tag.search"}}, {"$unwind": "$apply"}, {
        "$project": {"userid": "$apply.userid", "jobId": "$apply.jobId", "job": "$apply.job", "skill": "$apply.skill", "level": "$apply.level",
                     "location": "$apply.location", "salary": "$apply.salary", "behavior": "$apply.behavior",
                     "time": "$apply.time"}}, {"$project": {"_id": 0}}]))
    view_user = list(mongo[MONGO_DATABASE].data.aggregate([{"$project": {"apply": "$tag.view"}}, {"$unwind": "$apply"}, {
        "$project": {"userid": "$apply.userid", "jobId": "$apply.jobId", "job": "$apply.job", "skill": "$apply.skill", "level": "$apply.level",
                     "location": "$apply.location", "salary": "$apply.salary", "behavior": "$apply.behavior",
                     "time": "$apply.time"}}, {"$project": {"_id": 0}}]))
    ignore_user = list(mongo[MONGO_DATABASE].data.aggregate([{"$project": {"apply": "$tag.ignore"}}, {"$unwind": "$apply"}, {
        "$project": {"userid": "$apply.userid", "jobId": "$apply.jobId", "job": "$apply.job", "skill": "$apply.skill", "level": "$apply.level",
                     "location": "$apply.location", "salary": "$apply.salary", "behavior": "$apply.behavior",
                     "time": "$apply.time"}}, {"$project": {"_id": 0}}]))
    hated_user = list(mongo[MONGO_DATABASE].data.aggregate([{"$project": {"apply": "$tag.hated"}}, {"$unwind": "$apply"}, {
        "$project": {"userid": "$apply.userid", "jobId": "$apply.jobId", "job": "$apply.job", "skill": "$apply.skill", "level": "$apply.level",
                     "location": "$apply.location", "salary": "$apply.salary", "behavior": "$apply.behavior",
                     "time": "$apply.time"}}, {"$project": {"_id": 0}}]))

    # events_data = list(map(lambda x: InsertOne(x), apply_user + search_user + view_user + ignore_user + hated_user))
    events_data = list(map(lambda x: InsertOne(x), apply_user + search_user + view_user))
    mongo[MONGO_DATABASE].events.bulk_write(events_data)
    # mongo.data_1.events.create_index({'jobId': 1},{'unique': False})
    # mongo.data_1.events.create_index({'userid': 1}, {'unique': False})


