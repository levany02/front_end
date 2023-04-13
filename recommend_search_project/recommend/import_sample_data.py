import random
from multiprocessing import Pool
from pymongo import MongoClient, InsertOne, UpdateOne
import string
import datetime


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


MIN = 1000
MAX = 10000
CHARS=string.ascii_uppercase + string.digits


def create_jobs(_):
    jobs_titles = 'job_' + random.choice(JOB_TITLES) + '_' + ''.join(random.choices(CHARS, k=6))
    return {"title": jobs_titles,
            "location": random.choice(LOCATIONS),
            "skill": create_skill(jobs_titles),
            "salary": random.randint(MIN, MAX)
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
        return random.choices(GROUP_JOBS["Data"], k=4)
    elif "Devops" in job_title:
        return random.choices(GROUP_JOBS["Devops"], k=4)
    elif "Front-end" in job_title:
        return random.choices(GROUP_JOBS["Front-end"], k=4)
    else:
        return random.choices(GROUP_JOBS["Backend"], k=4)


def create_events(user, job_title):
    return {
        "userid": user,
        "job": job_title,
        # "skill": create_skill(job_title),
        "behavior": create_behavior(user, job_title),
        "time": datetime.datetime.now() + datetime.timedelta(days=random.randint(-150, 0))
    }


def userid_generator(_):
    return 'user_' + random.choice(JOB_TITLES) + '_' + ''.join(random.choices(CHARS, k=6))


if __name__ == '__main__':
    pool = Pool(processes=4)
    users = pool.map(userid_generator, range(1000))
    jobs = pool.map(create_jobs, range(500))
    jobs_titles = list(map(lambda x: x['title'], jobs))
    #
    events = pool.starmap(create_events, [(random.choice(users), random.choice(jobs_titles)) for _ in range(1000000)])
    print(events[:5])
    update_meta = list(map(lambda x: InsertOne(x), jobs))
    bulk_data = list(map(lambda x: InsertOne(x), events))
    mongo = MongoClient()
    print("bulk data metadata....")
    mongo.ecom_ur.meta.bulk_write(update_meta)
    print("bulk data events....")
    mongo.ecom_ur.events.bulk_write(bulk_data)

