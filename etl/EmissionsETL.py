import luigi
from plugins.read_records_mongo import read_data_from_mongodb
import psycopg2
import pandas as pd
import os

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
    
    def split_values(self, x):
        split_values = x.split('/')
        if len(split_values) == 1:
            return pd.Series([split_values[0], '0'])
        else:
            return pd.Series(split_values)

    def transformToInteger(self, df):
        try:
            df[['City MPG', 'City MPG Alternate']] = df['City MPG'].apply(self.split_values)
            df[['Hwy MPG', 'Hwy MPG Alternate']]= df['Hwy MPG'].apply(self.split_values)
            df[['Cmb MPG', 'Cmb MPG Alternate']]= df['Cmb MPG'].apply(self.split_values)
            df[['Comb CO2', 'Comb CO2 Alternate']] = df['Comb CO2'].apply(self.split_values)
        except:
            print(df['City MPG'].str.split('/'))

        return df
    
    def remove_outliers(self, df):
        cols = ['Displ', 'Cyl', 'Greenhouse Gas Score']

        for col in cols:
            quartiles = df[col].quantile([0.20, 0.80])
            q1 = quartiles.loc[0.20]
            q3 = quartiles.loc[0.80]

            low_bound = q1 - 1.5 * (q3 - q1)
            upp_bound = q3 + 1.5 * (q3 - q1)

            df = df[(df[col] >= low_bound) & (df[col] <= upp_bound)]

        return df


    def run(self):
        # Transforming data for postgres
        df = pd.read_csv(self.input().path, low_memory=False, lineterminator='\n')

        # drop rows containing null values
        df = df.dropna()

        ## Format columns to take integer value
        df = self.transformToInteger(df)
        
        ## remove outliers
        df = self.remove_outliers(df)

        # Define the desired order of columns
        desired_order = ['_id', 'Underhood ID', 'Stnd','Model', 'Displ', 'Cyl', 'Trans', 'Drive', 'Fuel', 'Veh Class', 'Air Pollution Score',
                         'City MPG', 'City MPG Alternate', 'Hwy MPG', 'Hwy MPG Alternate', 'Cmb MPG', 'Cmb MPG Alternate',
                         'Greenhouse Gas Score', 'SmartWay', 'Comb CO2', 'Comb CO2 Alternate', 'Stnd Description', 'Cert Region']

        # Rearrange columns
        df = df[desired_order]
        
        # string manipulation
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
    postgres_db_username = luigi.Parameter()
    postgres_db_password = luigi.Parameter()

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
            host = self.postgres_host,
            port = self.postgres_port,
            dbname = self.postgres_db_name,
            user = self.postgres_db_username, 
            password = self.postgres_db_password
        )

        cursor = conn.cursor() 
        df = pd.read_csv(self.input().path, keep_default_na=False, low_memory=False, lineterminator='\n')

        for index, row in df.iterrows():
            row = dict(row)
            emission_values = (row['Stnd'], row['Cert Region'], row['Stnd Description'])
            detail_values  = tuple(list(row.values())[0: 20])

            placeholders = ', '.join(['%s'] * len(emission_values))
            sql_query = f"INSERT INTO emission_standard VALUES ({placeholders}) ON CONFLICT (stnd) DO NOTHING"
            cursor.execute(sql_query, emission_values)
        
            placeholders = ', '.join(['%s'] * len(detail_values))
            sql_query = f"INSERT INTO car_details VALUES ({placeholders}) ON CONFLICT (underhood_id, stnd) DO NOTHING"
            cursor.execute(sql_query, detail_values)

            print("index is ", index)
        conn.commit()

        cursor.close()
        conn.close()

        # remove temperory files
        if os.path.exists(self.input().path):
            os.remove(self.input().path)
        
        mongo_file_path = os.path.join(os.getcwd(), 'data', 'mongo_data.csv')
        if os.path.exists(mongo_file_path):
            os.remove(mongo_file_path)


if __name__ == "__main__":
    from luigi import build
    build([Load()], local_scheduler=True)


