import luigi
from read_records_mongo import read_data_from_mongodb
import psycopg2
import pandas as pd
import os
import numpy as np
import ast

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
    
    # transformation for ev_charging_stations datset
    def modifyDT3(self, df):
            df = df.drop(columns=['DataProvider', 'OperatorInfo', 'UsageType', 'SubmissionStatus', 'StatusType', 'PercentageSimilarity',
                        'GeneralComments', 'DataProvidersReference', 'UserComments', 'ID', 'GeneralComments', 'MediaItems'])

            df['AddressInfo'] = df['AddressInfo'].apply(ast.literal_eval)
            df2 = pd.DataFrame(df['AddressInfo'].tolist())
            df2 = df2.fillna("NULL")

            df2 = df2.drop(columns=['ID', 'Country', 'CountryID', 'ContactTelephone1', 'ContactTelephone2', 'ContactEmail', 'AccessComments', 'RelatedURL'])
            df = df.drop(columns=['AddressInfo'])

            df = pd.concat([df, df2], axis=1)
            df = df.astype('str')

            return df


    def run(self):
        # Transforming data for postgres
        df = pd.read_csv(self.input().path, low_memory=False, lineterminator='\n')
        df = df.fillna('NULL')
        df = df.replace(np.nan, 'NULL')

        if (self.collection_name == 'ev_charging_stations'):
            df = self.modifyDT3(df)
        
        # string manipulation
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

        df = pd.read_csv(self.input().path, keep_default_na=False, low_memory=False, lineterminator='\n')

        columns_init = ','.join([f'"{col}" VARCHAR(5000)' for col in df.columns])

        q = f"CREATE TABLE IF NOT EXISTS {self.collection_name} ({columns_init})"
        cursor.execute(q)
        conn.commit()

        for index, row in df.iterrows():

            values = tuple(row.values)
            placeholders = ', '.join(['%s'] * len(values))
            sql_query = f"INSERT INTO {self.collection_name} VALUES ({placeholders})"
            cursor.execute(sql_query, values)
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
