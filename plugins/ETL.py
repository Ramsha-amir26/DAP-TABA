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
        df = df.dropna()
        df = df.fillna('NULL')
        df = df.replace(np.nan, 'NULL')

        if (self.collection_name == 'ev_charging_stations'):
            df = self.modifyDT3(df)

        df['City MPG'] = df['City MPG'].str.split('/').str[0]
        df['Hwy MPG'] = df['Hwy MPG'].str.split('/').str[0]
        df['Cmb MPG'] = df['Cmb MPG'].str.split('/').str[0]
        df['Comb CO2'] = df['Comb CO2'].str.split('/').str[0]
        
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


        for index, row in df.iterrows():

            row = dict(row)
            emission_values = (row['Stnd'], row['Cert Region'], row['Stnd Description'])
            detail_values = (row['Underhood ID'], row['Stnd'], row['Model'], row['Displ'], row['Cyl'], row['Trans'], row['Drive'], row['Fuel'], row['Veh Class'], row['Air Pollution Score'], 
                row['City MPG'], row['Hwy MPG'], row['Cmb MPG'], row['Greenhouse Gas Score'], row['SmartWay'], row['Comb CO2'])

            placeholders = ', '.join(['%s'] * len(emission_values))
            sql_query = f"INSERT INTO emission_standard VALUES ({placeholders}) ON CONFLICT (stnd) DO NOTHING"
            cursor.execute(sql_query, emission_values)
            conn.commit()
        
            placeholders = ', '.join(['%s'] * len(detail_values))
            sql_query = f"INSERT INTO car_details VALUES ({placeholders}) ON CONFLICT (underhood_id, stnd) DO NOTHING"
            cursor.execute(sql_query, detail_values)
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
