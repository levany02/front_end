from pymongo import MongoClient, UpdateOne
import pandas as pd


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    '''Load data from Mongo'''
    database = "vnw_job"
    col = "items"

    mongo = MongoClient("mongodb://10.122.6.17:27017")
    df_meta = pd.DataFrame(list(mongo[database][col].find()))
    df_meta['title'] = df_meta.categoricalProps.apply(lambda x: x["jobTitle"][0])
    df_meta['skill'] = df_meta.categoricalProps.apply(lambda x: x["skills"])
    df_meta['category'] = df_meta.categoricalProps.apply(lambda x: x["industries"])
    df_meta['location'] = df_meta.categoricalProps.apply(lambda x: x["locations"])
    df_meta['level'] = df_meta.categoricalProps.apply(lambda x: x["jobLevel"])
    df_meta['availableDate'] = df_meta.dateProps.apply(lambda x: x["availableDate"])
    df_meta["jobId"] = df_meta["_id"]

    print(df_meta[["jobId", "title", "skill", "category", "location", "level", "availableDate"]].head())
    data = df_meta[["jobId", "title", "skill", "category", "location", "level", "availableDate"]].to_dict(orient='records')
    m = MongoClient()
    bulk_data = []
    for i in data:
        i["image"] = "/images/bosch.png"
        bulk_data.append(UpdateOne({"_id": i["jobId"]}, {"$set": i}, upsert=True))
    m['vnw_job']['data'].bulk_write(bulk_data)



# See PyCharm help at https://www.jetbrains.com/help/pycharm/
