from pymongo import MongoClient


def insert_into_mongodb(charging_stations, uri, db_name="openchargemap", collection_name="charging_stations",
                        batch_size=1000):
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]

    for i in range(0, len(charging_stations), batch_size):
        batch = charging_stations[i:i + batch_size]
        collection.insert_many(batch)