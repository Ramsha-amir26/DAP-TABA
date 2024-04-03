import luigi
from read_records_mongo import read_data_from_mongodb
import psycopg2
import pandas as pd
import os
import json
import numpy as np

class Extract(luigi.Task):

    mongo_connection_string = luigi.Parameter()
    mongo_db_name = luigi.Parameter()
    collection_name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget("data/mongo_data.csv")

    def run(self):
        data = read_data_from_mongodb(self.mongo_connection_string, self.mongo_db_name, self.collection_name)
        data.to_csv(self.output().path, index=False)



class Transform(luigi.Task):

    mongo_connection_string = luigi.Parameter()
    mongo_db_name = luigi.Parameter()
    collection_name = luigi.Parameter()

    def requires(self):
        return Extract(
            mongo_connection_string=self.mongo_connection_string,
            mongo_db_name=self.mongo_db_name,
            collection_name=self.collection_name
        )
    
    def input(self):
        return luigi.LocalTarget("data/mongo_data.csv")

    def output(self):
        return luigi.LocalTarget("data/transformed_data.csv") 
    
    def extract_id(self, json_str):
        # Convert JSON-like string to dictionary
        try:
            json_str = str(json_str).replace("'",'"')
            json_dict = json.loads(json_str)
        except:
            print(json_str)
        return json_dict['ID']


    def run(self):
        # Transforming data for postgres
        df = pd.read_csv(self.input().path)
        df = df.fillna('NULL')
        df = df.replace(np.nan, 'NULL')

        if (self.collection_name == 'ev_charging_stations'):
            df = df.drop(columns=['DataProvider', 'OperatorInfo', 'UsageType', 'SubmissionStatus', 'UUID', 'StatusType'])

            df['ID'] = df['AddressInfo'].apply(self.extract_id)
            df = df.drop(columns=['AddressInfo'])

        
        df = df.replace('', 'NULL', regex=True)
        df = df.replace("''", '"', regex=True)
        df = df.drop(columns=['_id'])

        # update changes to new file
        df.to_csv(self.output().path, index=False)


class Load(luigi.Task):

    mongo_connection_string = luigi.Parameter()
    postgres_host = luigi.Parameter()
    postgres_port = luigi.IntParameter(default=5432)
    mongo_db_name = luigi.Parameter()
    postgres_db_name = luigi.Parameter()
    collection_name = luigi.Parameter()

    def requires(self):
        return Transform(
            mongo_connection_string=self.mongo_connection_string,
            mongo_db_name=self.mongo_db_name,
            collection_name=self.collection_name
        )
    
    def input(self):
        return luigi.LocalTarget("data/transformed_data.csv")

    def run(self):
        conn = psycopg2.connect(
            host=self.postgres_host,
            port=self.postgres_port,
            dbname=self.postgres_db_name,
            user='sammam', 
            password='mysecretpassword'
        )
        cursor = conn.cursor()

        df = pd.read_csv(self.input().path, keep_default_na=False)

        columns_init = ','.join([f'"{col}" VARCHAR(10000)' for col in df.columns])

        q = f"CREATE TABLE IF NOT EXISTS {self.collection_name} ({columns_init})"
        cursor.execute(q)
        conn.commit()

        for index, row in df.iterrows():

            values = tuple(row.values)
            placeholders = ', '.join(['%s'] * len(values))
            try:
                sql_query = f"INSERT INTO {self.collection_name} VALUES ({placeholders})"
                cursor.execute(sql_query, values)
            except:
                print(values)
                print('\n')
            conn.commit()

        cursor.close()
        conn.close()

        ## remove temperory files
        if os.path.exists(self.input().path):
            os.remove(self.input().path)
        
        mongo_file_path = os.path.join(os.getcwd(), 'data', 'mongo_data.csv')
        if os.path.exists(mongo_file_path):
            os.remove(mongo_file_path)


if __name__ == "__main__":
    luigi.run()
