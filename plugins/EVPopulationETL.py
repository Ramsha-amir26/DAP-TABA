#!/usr/bin/env python
# coding: utf-8

# In[2]:


import luigi
import pymongo
import psycopg2
from datetime import datetime
import json
import pandas as pd

# MongoDB connection details
MONGODB_URI = "mongodb+srv://ramsha0amir:gYk6WWjS0ACv6kFQ@cluster0.a0p69eg.mongodb.net/"
DATABASE_NAME = "ev_database"
COLLECTION_NAME = "ev_population"

# PostgreSQL connection details
POSTGRES_DB_NAME = "d7o2v0t05ltbk9"
POSTGRES_USER = "uat366enhv0hv4"
POSTGRES_PASSWORD = "p25ce6cd8f98ee06dfa822ae01a93833c68649897452d765f20897e60414c63ca"
POSTGRES_HOST = "c7gljno857ucsl.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com"
POSTGRES_PORT = 5432

class ExtractDataFromMongoDB(luigi.Task):
    def output(self):
        return luigi.LocalTarget("extract_data_from_mongodb_task_complete.txt")

    def run(self):
        client = pymongo.MongoClient(MONGODB_URI)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        mongo_data = list(collection.find().limit(100))
        
        for doc in mongo_data:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])

        with self.output().open('w') as f:
            json.dump(mongo_data, f)

class TransformData(luigi.Task):
    def requires(self):
        return ExtractDataFromMongoDB()

    def output(self):
        return luigi.LocalTarget("transform_data_task_complete.txt")

    def run(self):
        with self.input().open() as infile:
            mongo_data = json.load(infile)
        
        df = pd.DataFrame(mongo_data)
        
        self.remove_duplicate_rows(df)
        self.remove_null_values(df)
    
        transformed_data = []
        for index, row in df.iterrows():
                          transformed_data.append({
                "County": row["County"],
                "State": row["State"],
                "Vehicle Primary Use": row["Vehicle Primary Use"],
                "Date": self.format_date(row["Date"]),
                "Battery Electric Vehicles (BEVs)": row["Battery Electric Vehicles (BEVs)"],
                "Plug-In Hybrid Electric Vehicles (PHEVs)": row["Plug-In Hybrid Electric Vehicles (PHEVs)"],
                "Electric Vehicle (EV) Total": row["Electric Vehicle (EV) Total"],
                "Non-Electric Vehicle Total": row["Non-Electric Vehicle Total"],
                "Total Vehicles": row["Total Vehicles"],
                "Percent Electric Vehicles": row["Percent Electric Vehicles"]
            })

        with self.output().open('w') as f:
            f.write(json.dumps(transformed_data))

    def format_date(self, date_str):
        try:
            # Convert date string to datetime object
            date_obj = datetime.strptime(date_str, "%B %d %Y")
            # Format datetime object as "YYYY-MM-DD" string
            formatted_date = date_obj.strftime("%Y-%m-%d")
            return formatted_date
        except (ValueError, TypeError):
            return None
    def remove_duplicate_rows(self, df):
        duplicate_rows = df[df.duplicated()]
        if not duplicate_rows.empty:
            print("Duplicate rows found:")
            print(duplicate_rows)
            df.drop_duplicates(inplace=True)

    def remove_null_values(self, df):
        df.dropna(subset=["County", "State"], inplace=True)


class LoadDataIntoPostgreSQL(luigi.Task):
    def requires(self):
        return TransformData()

    def output(self):
        return luigi.LocalTarget("load_data_into_postgresql_task_complete.txt")

    def run(self):
        with self.input().open() as infile:
            transformed_data = json.load(infile)

        conn = psycopg2.connect(
            dbname=POSTGRES_DB_NAME,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )
        cursor = conn.cursor()

        # Insert counties
        self.insert_counties(cursor, transformed_data)

        # Insert vehicle usage types
        self.insert_vehicle_usage_types(cursor, transformed_data)

        # Insert electric vehicle data
        self.insert_electric_vehicle_data(cursor, transformed_data)

        conn.commit()
        cursor.close()
        conn.close()

        with self.output().open('w') as f:
            f.write("LoadDataIntoPostgreSQL completed successfully")

    def insert_counties(self, cursor, transformed_data):
        counties = set((row["County"], row["State"]) for row in transformed_data)
        for county, state in counties:
            cursor.execute("INSERT INTO counties (name, state) VALUES (%s, %s) ON CONFLICT DO NOTHING", (county, state))

    def insert_vehicle_usage_types(self, cursor, transformed_data):
        vehicle_usage_types = set(row["Vehicle Primary Use"] for row in transformed_data)
        for vehicle_type in vehicle_usage_types:
            cursor.execute("INSERT INTO vehicle_primary_uses (name) VALUES (%s) ON CONFLICT DO NOTHING", (vehicle_type,))

    def insert_electric_vehicle_data(self, cursor, transformed_data):
        for data_row in transformed_data:
            county_id = self.get_county_id(cursor, data_row["County"], data_row["State"])
            vehicle_primary_use_id = self.get_vehicle_primary_use(cursor, data_row["Vehicle Primary Use"])

            cursor.execute("""
                INSERT INTO electric_vehicles (county_id, vehicle_primary_use_id, date, bev, phev, ev_total, non_ev_total, total_vehicles, percent_ev)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                county_id,
                vehicle_primary_use_id,
                data_row["Date"],
                data_row["Battery Electric Vehicles (BEVs)"],
                data_row["Plug-In Hybrid Electric Vehicles (PHEVs)"],
                data_row["Electric Vehicle (EV) Total"],
                data_row["Non-Electric Vehicle Total"],
                data_row["Total Vehicles"],
                data_row["Percent Electric Vehicles"]
            ))

    def get_county_id(self, cursor, county_name, state_name):
        cursor.execute("SELECT id FROM counties WHERE name = %s AND state = %s", (county_name, state_name))
        county = cursor.fetchone()
        if county is not None:
            return county[0]

    def get_vehicle_primary_use(self, cursor, primary_use):
        cursor.execute("SELECT id FROM vehicle_primary_uses WHERE name = %s", (primary_use,))
        primary_use_id = cursor.fetchone()
        if primary_use_id is not None:
            return primary_use_id[0]
