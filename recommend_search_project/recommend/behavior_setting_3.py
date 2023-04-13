import random
import datetime
import string
from pymongo import MongoClient, UpdateOne, InsertOne
import sys

MONGO_DATABASE = sys.argv[1]

MIN = 1000
MAX = 10000
CHARS=string.ascii_uppercase + string.digits

GROUP_JOBS_NEW = {
    "Data Scientist": ["Python", "Data Analytic", "Machine Learning", "Deep Learning", "AI","Probability &  Statistic", "Big-data", "Visualization", "NLP", "Computer Vision"],
    "Machine Learning": ["Scala", "Python", "Machine Learning", "MLops", "Deep Learning", "NLP", "Computer Vision"],
    "Data Analyst": ["Python", "Data Analytic", "Visualization", "SQL", "PowerBI"],
    "Data Engineer": ["Java", "Python", "Scala", "Data Engineering", "Spark", "HDFS", "MapReduce", "Machine Learning", "Backend"],
    "Python Developer" : ["Python", "Flask", "Django", "SQL", "API", "Backend"],
    "Java Engineer" : ["Java", "Spring", "SQL", "Backend"],
    "NodeJS Developer": ["NodeJS", "JavaScript", "ReactJS", "Backend"],
    # "Backend Developer": ["Backend", "NodeJS", "Golang", "PHP", "Java", ".NET", "English", "System", "ReactJS", "Python", "JavaScript","API"],
    "Front-end Developer": ["ReactJS", "JavaScript", "Typescript","Agile", "HTML", "jQuery", "Front-End", "Adobe Illustrator (AI)"],
    "Fullstack Developer": ["NodeJS", "ReactJS", "SQL", "Front-End", "Backend (familar AI, chatbot, chat GPT)"],
    "Devops Engineer": ["Linux", "Python", "Perl", "DevOps", "Azure", "Aws"]
}


GROUP_JOB_NAME = {
    "Data Scientist": ["Data Scientist", "Data Science", "Data Researcher", "NLP Engineer", "Statistician (in Finance)", "Quantitive Reseacher"],
    "Machine Learning": ["AI Engineer", "Machine Learning Engineer", "Artificial Intelligent Engineer", "Python Developer (strong Machine Leaning)", "NLP Engineer"],
    "Data Analyst": ["Data Analyst", "Business Intelligence", "Data Analystics", "Data Associate"],
    "Data Engineer": ["Data Engineer", "Python Developer (Big-data, PySpark)","Big-Data Engineer (Java/Scala)", "Java Developer (Scala, Big-data, Data Engineer)", "Software Engineer (Python, Java, Scala, Big-data, Hadoop)"],
    "Python Developer" : ["Python Developer", "Backend Developer ( Python )", "Software Engineer ( Python )", "Fullstack Developer (Python, Django)", "Devops (Bash, Python)"],
    "Java Engineer" : ["Java Developer", "Backend Developer ( Java )", "Software Engineer ( Java )", "Fullstack Developer (Java, Spring)"],
    "NodeJS Developer": ["NodeJS Developer", "Backend Developer ( NodeJS )", "Software Engineer ( NodeJS )", "Fullstack Developer (NodeJS, Express)"],
    "Devops Engineer": ["Devops Engineer", "Devops (prefer Machine Learning)", "Cloud Engineer (support Data Engineer)", "System Administrator (Data Center Management)"],
    "Front-end Developer": ["Front-end Developer", "Fullstack (prefer Front-end)", "ReactJs/NodeJs Developer", "IOS Developer", "Android Developer", "Designer Abobe Illustrator (AI)"],
    "Fullstack Developer": ["Fullstack Developer", "Fullstack Developer (for AI project)", "Fullstack Developer (NodeJS, Express)", "NodeJs Developer (with ReactJs)"]
}

MAPPING_JOB_NAME = {
    "Data Scientist": "Data Science",
    "Machine Learning": "Machine Learning",
    "Data Analyst": "Data Analytic",
    "Data Engineer": "Data Engineering",
    "Python Developer": "Python Backend",
    "Java Engineer": "Java Backend",
    "NodeJS Developer": "NodeJS Backend",
    "Devops Engineer": "Devops",
    "Front-end Developer": "Front-end",
    "Fullstack Developer": "Fullstack"
}


BEHAVIORS_FORM_NEW = {
    "Data Scientist": {
        "apply": {
                    "values": ["Data Scientist"],
                    "weight": [1.0]
                  },
        "search": {
                    "values": ["Data Scientist"],
                    "weight": [1.0]
                },
        "view": {
                    "values": ["Data Scientist", ],
                    "weight": [1.0]
        },
        "ignore": {
                    "values": ["Backend", "Front-end", "Devops"],
                    "weight": [0.1, 0.7, 0.2]
                   },
        "hated": {
                    "values": ["Front-end"],
                    "weight": [1.0]
                }
    },
    "Machine Learning": {
        "apply": {
                    "values": ["Machine Learning", "Data Scientist"],
                    "weight": [0.8, 0.2]
        },
        "search": {
                    "values": ["Machine Learning", "Data Scientist"],
                    "weight": [0.8, 0.2]
        },
        "view": {
                    "values": ["Machine Learning", "Data Scientist"],
                    "weight": [0.8, 0.2]
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
    "Data Analyst": {
        "apply": {
                    "values": ["Data Analyst"],
                    "weight": [1.0]
        },
        "search": {
                    "values": ["Data Analyst"],
                    "weight": [1.0]
        },
        "view": {
                    "values": ["Data Analyst", "Data Scientist"],
                    "weight": [0.8, 0.2]
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
    "Data Engineer": {
        "apply": {
                    "values": ["Data Engineer"],
                    "weight": [1.0]
        },
        "search": {
                    "values": ["Data Engineer"],
                    "weight": [1.0]
        },
        "view": {
                    "values": ["Data Engineer", "Data Scientist"],
                    "weight": [0.9, 0.1]
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
    "Big Data Engineer": {
        "apply": {
                    "values": ["Data Engineer", "Big Data Engineer"],
                    "weight": [0.5, 0.5]
        },
        "search": {
                    "values": ["Data Engineer", "Big Data Engineer"],
                    "weight": [0.5, 0.5]
        },
        "view": {
                    "values": ["Data Engineer", "Big Data Engineer"],
                    "weight": [0.5, 0.5]
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
    "Python Developer": {
        "apply": {
                    "values": ["Python Developer"],
                    "weight": [1.0]
        },
        "search": {
                    "values": ["Python Developer"],
                    "weight": [1.0]
        },
        "view": {
                    "values": ["Python Developer"],
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
    "Java Engineer": {
        "apply": {
                    "values": ["Java Engineer"],
                    "weight": [1.0]
        },
        "search": {
                    "values": ["Java Engineer"],
                    "weight": [1.0]
        },
        "view": {
                    "values": ["Java Engineer"],
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
    "NodeJS Developer": {
        "apply": {
            "values": ["NodeJS Developer", "Fullstack Developer"],
            "weight": [0.8, 0.2]
        },
        "search": {
            "values": ["NodeJS Developer"],
            "weight": [1.0]
        },
        "view": {
            "values": ["NodeJS Developer", "Fullstack Developer"],
            "weight": [0.8, 0.2]
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
    "Backend Developer": {
        "apply": {
            "values": ["Backend Developer"],
            "weight": [1.0]
        },
        "search": {
            "values": ["Backend Developer"],
            "weight": [1.0]
        },
        "view": {
            "values": ["Backend Developer"],
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
    "Fullstack Developer": {
        "apply": {
            "values": ["Fullstack Developer", "Front-end Developer", "NodeJS Developer"],
            "weight": [0.8, 0.1, 0.1]
        },
        "search": {
            "values": ["Fullstack Developer"],
            "weight": [1.0]
        },
        "view": {
            "values": ["Fullstack Developer", "Front-end Developer", "NodeJS Developer"],
            "weight": [0.8, 0.1, 0.1]
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
    "Front-end Developer": {
        "apply": {
            "values": ["Front-end Developer"],
            "weight": [1.0]
        },
        "search": {
            "values": ["Front-end Developer"],
            "weight": [1.0]
        },
        "view": {
            "values": ["Front-end Developer"],
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
    "Devops Engineer": {
        "apply": {
            "values": ["Devops Engineer"],
            "weight": [1.0]
        },
        "search": {
            "values": ["Devops Engineer"],
            "weight": [1.0]
        },
        "view": {
            "values": ["Devops Engineer"],
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
                    "values": ["HCM", "HN"],
                    "weight": [0.5, 0.5]
        },
        "hated": {
                    "values": ["HCM", "HN"],
                    "weight": [0.5, 0.5]
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
                    "values": ["HN", "DN"],
                    "weight": [0.5, 0.5]
        },
        "hated": {
                    "values": ["HN", "DN"],
                    "weight": [0.5, 0.5]
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
                    "values": ["HCM", "DN"],
                    "weight": [0.5, 0.5]
        },
        "hated": {
                    "values": ["HCM", "DN"],
                    "weight": [0.5, 0.5]
        },
    },
    "General_User": {
        "apply": {
                    "values": ["HN", "HCM", "DN"],
                    "weight": [0.35, 0.35, 0.3]
        },
        "search": {
                    "values": ["HN", "HCM", "DN"],
                    "weight": [0.35, 0.35, 0.3]
        },
        "view": {
                    "values": ["HN", "HCM", "DN"],
                    "weight": [0.35, 0.35, 0.3]
        },
        "ignore":{
                    "values": ["HN", "HCM", "DN"],
                    "weight": [0.35, 0.35, 0.3]
        },
        "hated": {
                    "values": ["HN", "HCM", "DN"],
                    "weight": [0.35, 0.35, 0.3]
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
                    "values": ["HN_User","General_User"],
                    "weight": [0.8, 0.2]
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
                    "values": ["Junior", "Middle"],
                    "weight": [0.3, 0.7]
        },
        "search": {
                    "values": ["Junior", "Middle"],
                    "weight": [0.3, 0.7]
        },
        "view": {
                    "values": ["Junior", "Middle"],
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
                    "values": ["Junior", "Fresher"],
                    "weight": [0.8, 0.2]
        },
        "search": {
                    "values": ["Junior", "Fresher"],
                    "weight": [0.8, 0.2]
        },
        "view": {
                    "values": ["Junior", "Fresher"],
                    "weight": [0.8, 0.2]
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
                    "values": ["Fresher"],
                    "weight": [1.0]
        },
        "search": {
                    "values": ["Fresher"],
                    "weight": [1.0]
        },
        "view": {
                    "values": ["Fresher"],
                    "weight": [1.0]
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


LOCATIONS = ["HN", "HCM", "DN"]
SALARY_LEVELS = {
    "Fresher": {"MIN": 500, "MAX":1500, "level": 1},
    "Junior": {"MIN": 1000, "MAX":2000, "level": 2},
    "Middle": {"MIN": 1500, "MAX":2500, "level": 3},
    "Senior": {"MIN": 2000, "MAX": 3000, "level": 4},
    "Leader": {"MIN": 3000, "MAX": 5000, "level": 5},
}
COMPANY = {
    "Home Credit": "https://www.vietnamworks.com/_next/image?url=https%3A%2F%2Fimages.vietnamworks.com%2Fpictureofcompany%2F5a%2F10480385.png&w=256&q=75",
    "FPT Telecom": "https://www.vietnamworks.com/_next/image?url=https%3A%2F%2Fimages.vietnamworks.com%2Fpictureofcompany%2F1c%2F10843820.png&w=256&q=75",
    "Generali": "https://www.vietnamworks.com/_next/image?url=https%3A%2F%2Fimages.vietnamworks.com%2Fpictureofcompany%2F10%2F10158370.jpg&w=256&q=75",
    "Trustingsocial": "https://www.vietnamworks.com/_next/image?url=https%3A%2F%2Fimages.vietnamworks.com%2Fpictureofcompany%2F15%2F10523523.png&w=256&q=75",
    "VietnamWork": "https://www.vietnamworks.com/_next/image?url=https%3A%2F%2Fimages.vietnamworks.com%2Fpictureofcompany%2F6e%2F10922087.png&w=256&q=75",
    "Abort": "https://www.vietnamworks.com/_next/image?url=https%3A%2F%2Fimages.vietnamworks.com%2Fpictureofcompany%2Fdb%2F11125410.png&w=256&q=75",
    "MUFG": "https://www.vietnamworks.com/_next/image?url=https%3A%2F%2Fimages.vietnamworks.com%2Fpictureofcompany%2Fd4%2F10182458.jpg&w=256&q=75",
    "Bosch": "https://www.vietnamworks.com/_next/image?url=https%3A%2F%2Fimages.vietnamworks.com%2Fpictureofcompany%2Ff9%2F11069133.png&w=256&q=75"
}

def create_user_by_params(job_type, level, user_location, behavior):
    # level = random.choices(["Leader", "Senior", "Middle", "Junior", "Fresher"], [0.2, 0.2, 0.2, 0.2, 0.2], k=1)[0]
    # location = random.choices(["DN_User", "HN_User", "HCM_User", "General_User"], [0.1, 0.2, 0.3, 0.4], k=1)[0]
    location = user_location
    level_apply = random.choices(LEVEL_BEHAVIOR_FORM[level][behavior]["values"], LEVEL_BEHAVIOR_FORM[level][behavior]["weight"])[0]
    jop_apply = random.choices(BEHAVIORS_FORM_NEW[job_type][behavior]["values"], BEHAVIORS_FORM_NEW[job_type][behavior]["weight"])[0]
    location_apply = random.choices(LOCATIONS_BEHAVIOR_FORM[location][behavior]["values"], LOCATIONS_BEHAVIOR_FORM[location][behavior]["weight"])[0]
    return f"User_{level_apply}_{jop_apply.replace(' ', '_')}_{location_apply}_{random.randint(0, 20)}"


def create_job_by_params(job_type='', job_level=''):
    job_name = f"{job_level} {job_type}".strip().replace("  ", " ")
    job_title = f"{job_level} {random.choice(GROUP_JOB_NAME[job_type])}".strip().replace("  ", " ")
    if job_level=='':
        job_level = random.choice(list(SALARY_LEVELS.keys()))

    location = random.choices(LOCATIONS, [0.4, 0.35, 0.25], k=1)[0]
    skills = random.choices(GROUP_JOBS_NEW[job_type], k=5)
    salary = random.randint(SALARY_LEVELS[job_level]["MIN"], SALARY_LEVELS[job_level]["MAX"])
    job_id = f"Job_{job_name.replace(' ', '_')}_{location}_{job_level}_{''.join(random.choices(CHARS, k=6))}"
    img = random.choice(list(COMPANY.values()))
    tags = {
        "apply": [
            {
                "userid": create_user_by_params(job_type, job_level, random.choices(LOCATIONS_USER_BEHAVIOR_FORM[location]["apply"]["values"], LOCATIONS_USER_BEHAVIOR_FORM[location]["apply"]["weight"], k=1)[0], "apply"),
                "job": job_title,
                "jobId": job_id,
                "skill": skills,
                "level": job_level,
                "location": location,
                "salary": salary,
                "behavior": "apply",
                "time": datetime.datetime.now() + datetime.timedelta(days=random.randint(-150, 0))
            }
             for _ in range(10)],
        "search": [{
                "userid": create_user_by_params(job_type, job_level, random.choices(LOCATIONS_USER_BEHAVIOR_FORM[location]["search"]["values"], LOCATIONS_USER_BEHAVIOR_FORM[location]["search"]["weight"], k=1)[0], "search"),
                "job": job_title,
                "jobId": job_id,
                "skill": skills,
                "level": job_level,
                "location": location,
                "salary": salary,
                "behavior": "search",
                "time": datetime.datetime.now() + datetime.timedelta(days=random.randint(-150, 0))
            } for _ in range(30)],
        "view": [{
                "userid": create_user_by_params(job_type, job_level, random.choices(LOCATIONS_USER_BEHAVIOR_FORM[location]["view"]["values"], LOCATIONS_USER_BEHAVIOR_FORM[location]["view"]["weight"], k=1)[0],"view"),
                "job": job_title,
                "jobId": job_id,
                "skill": skills,
                "level": job_level,
                "location": location,
                "salary": salary,
                "behavior": "view",
                "time": datetime.datetime.now() + datetime.timedelta(days=random.randint(-150, 0))
            } for _ in range(100)]
    }

    job = {
        "jobId": job_id,
        "title": job_title,
        "category": MAPPING_JOB_NAME[job_type],
        "location": location,
        "skill": random.choices(GROUP_JOBS_NEW[job_type], k=5),
        "salary": random.randint(SALARY_LEVELS[job_level]["MIN"], SALARY_LEVELS[job_level]["MAX"]),
        "level": job_level,
        "tag": tags,
        "image": img
     }
    return job


if __name__ == '__main__':
    from multiprocessing import Pool
    pool = Pool(processes=4)
    random.seed(42)
    jobs = pool.starmap(create_job_by_params, [(random.choice(list(GROUP_JOBS_NEW.keys())), random.choices(["Leader", "Senior", "Middle", "Junior", "Fresher"], [0.1, 0.2, 0.2, 0.25, 0.25], k=1)[0]) for _ in range(100)])
    print(jobs[0])
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