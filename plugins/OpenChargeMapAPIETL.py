import luigi
import requests
from pymongo import MongoClient
import json

class ExtractFromAPITask(luigi.Task):
    uri = luigi.Parameter()
    db_name = luigi.Parameter()
    collection_name = luigi.Parameter()
    
    def fetch_charging_stations(self):
        url = 'https://api.openchargemap.io/v3/poi/?output=json&countrycode=US&maxresults=80000&key=dab05a64-2552-44b8-85d0-6033399f9931'
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print('Failed to fetch data:', response.status_code)
            return None

    def output(self):
        return luigi.LocalTarget("charging_stations.json")

    def run(self):
        charging_stations = self.fetch_charging_stations()
        if charging_stations:
            with self.output().open('w') as f:
                json.dump(charging_stations, f)

class InsertIntoMongoTask(luigi.Task):
    uri = luigi.Parameter()
    db_name = luigi.Parameter()
    collection_name = luigi.Parameter()

    def requires(self):
        return ExtractFromAPITask(uri=self.uri, db_name=self.db_name, collection_name=self.collection_name)

    def run(self):
        client = MongoClient(self.uri)
        db = client[self.db_name]
        collection = db[self.collection_name]

        with self.input().open('r') as infile:
            data = json.load(infile)

            for charging_station in data:
                collection.insert_one(charging_station)