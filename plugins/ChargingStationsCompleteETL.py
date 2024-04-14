import luigi
import pymongo
import psycopg2
from datetime import datetime
import json
import pandas as pd

MONGODB_URI = "mongodb+srv://ramsha0amir:gYk6WWjS0ACv6kFQ@cluster0.a0p69eg.mongodb.net/"
DATABASE_NAME = "ev_database"
COLLECTION_NAME = "ev_charging_stations"

POSTGRES_DB_NAME = "d98hjdk7pfqndj"
POSTGRES_USER = "u81drfdtd54dcp"
POSTGRES_PASSWORD = "p65a4a6d8d6e8d6b26cd2248db325179a98ed96c458a615ccfc4b4870a7c045a8"
POSTGRES_HOST = "c9pbiquf6p6pfn.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com"
POSTGRES_PORT = 5432


class ExtractDataFromMongoDB(luigi.Task):
    def output(self):
        return luigi.LocalTarget("extract_data_from_mongodb_task_complete.txt")

    def run(self):
        client = pymongo.MongoClient(MONGODB_URI)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        mongo_data = list(collection.find().limit(10))

        for doc in mongo_data:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])

        with self.output().open('w') as f:
            json.dump(mongo_data, f)


class TransformData(luigi.Task):
    def requires(self):
        return ExtractDataFromMongoDB()

    def output(self):
        return luigi.LocalTarget("transformed_data.json")

    def run(self):
        with self.input().open('r') as f:
            mongo_data = json.load(f)

        transformed_data = []

        for entry in mongo_data:
            transform_entry = {
                'ID': entry['_id'] if '_id' in entry else None,
                'UUID': entry.get('UUID'),
                'IsRecentlyVerified': entry.get('IsRecentlyVerified'),
                'DateLastVerified': entry.get('DateLastVerified'),
                'DateCreated': entry.get('DateCreated'),
                'SubmissionStatus': {
                    'SubmissionStatusTypeID': entry['SubmissionStatus'].get('ID'),
                    'IsLive': entry['SubmissionStatus'].get('IsLive'),
                    'Title': entry['SubmissionStatus'].get('Title')
                },
                'UsageType': {
                    'UsageTypeID': entry['UsageType'].get('ID'),
                    'IsPayAtLocation': entry['UsageType'].get('IsPayAtLocation'),
                    'IsMembershipRequired': entry['UsageType'].get('IsMembershipRequired'),
                    'IsAccessKeyRequired': entry['UsageType'].get('IsAccessKeyRequired'),
                    'Title': entry['UsageType'].get('Title')
                },
                'Address': {
                    'AddressID': entry['AddressInfo'].get('ID'),
                    'Title': entry['AddressInfo'].get('Title'),
                    'AddressLine1': entry['AddressInfo'].get('AddressLine1'),
                    'AddressLine2': entry['AddressInfo'].get('AddressLine2'),
                    'Town': entry['AddressInfo'].get('Town'),
                    'StateOrProvince': entry['AddressInfo'].get('StateOrProvince'),
                    'Postcode': entry['AddressInfo'].get('Postcode'),
                    'Country': {
                        'CountryID': entry['AddressInfo']['Country'].get('ID'),
                        'ISOCode': entry['AddressInfo']['Country'].get('ISOCode'),
                        'ContinentCode': entry['AddressInfo']['Country'].get('ContinentCode'),
                        'Title': entry['AddressInfo']['Country'].get('Title')
                    },
                    'Latitude': entry['AddressInfo'].get('Latitude'),
                    'Longitude': entry['AddressInfo'].get('Longitude'),
                    'ContactTelephone1': entry['AddressInfo'].get('ContactTelephone1'),
                    'ContactTelephone2': entry['AddressInfo'].get('ContactTelephone2'),
                    'ContactEmail': entry['AddressInfo'].get('ContactEmail'),
                    'AccessComments': entry['AddressInfo'].get('AccessComments'),
                    'RelatedURL': entry['AddressInfo'].get('RelatedURL'),
                    'Distance': entry['AddressInfo'].get('Distance'),
                    'DistanceUnit': entry['AddressInfo'].get('DistanceUnit')
                },
                'Connections': []
            }

            for connection in entry['Connections']:
                transformed_connection = {
                    'ConnectionID': connection.get('ID'),
                    'ConnectionType': connection['ConnectionType'].get('FormalName'),
                    'StatusType': {
                        'IsOperational': connection['StatusType'].get('IsOperational') if connection[
                                                                                              'StatusType'] is not None else None,
                        'IsUserSelectable': connection['StatusType'].get('IsUserSelectable') if connection[
                                                                                                    'StatusType'] is not None else None,
                        'Title': connection['StatusType'].get('Title') if connection['StatusType'] is not None else None
                    },
                    'Level': {
                        'LevelID': connection.get('ID'),
                        'Comments': connection['Level'].get('Comments') if connection['Level'] is not None else None,
                        'IsFastChargeCapable': connection['Level'].get('IsFastChargeCapable') if connection[
                                                                                                     'Level'] is not None else None,
                        'Title': connection['Level'].get('Title') if connection['Level'] is not None else None
                    },
                    'CurrentType': {
                        'CurrentTypeID': connection.get('ID'),
                        'Description': connection['CurrentType'].get('Description') if connection[
                                                                                           'CurrentType'] is not None else None,
                        'Title': connection['CurrentType'].get('Title') if connection[
                                                                               'CurrentType'] is not None else None
                    },
                    'Quantity': connection.get('Quantity'),
                    'PowerKW': connection.get('PowerKW'),
                    'Comments': connection.get('Comments')
                }
                transform_entry['Connections'].append(transformed_connection)

            transformed_data.append(transform_entry)

        with self.output().open('w') as f:
            json.dump(transformed_data, f)


class LoadDataToPostgres(luigi.Task):
    input_file = luigi.Parameter(default="transformed_data.json")
    host = luigi.Parameter(default=POSTGRES_HOST)
    database = luigi.Parameter(default=POSTGRES_DB_NAME)
    user = luigi.Parameter(default=POSTGRES_USER)
    password = luigi.Parameter(default=POSTGRES_PASSWORD)

    def requires(self):
        return TransformData()

    def output(self):
        return luigi.LocalTarget("json_loaded_to_postgres.txt")

    def run(self):
        with open(self.input_file, 'r') as f:
            data = json.load(f)

        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password
        )

        # Open a cursor to perform database operations
        cur = conn.cursor()

        # Insert data into PostgreSQL
        for item in data:
            address = item["Address"]
            country = address["Country"]
            cur.execute(sql.SQL("""
                INSERT INTO Country (ISOCode, ContinentCode, Title)
                VALUES (%s, %s, %s)
                ON CONFLICT (Title) DO NOTHING
                RETURNING CountryID
                """),
                        (country["ISOCode"], country["ContinentCode"], country["Title"])
                        )
            row = cur.fetchone()
            if row:
                country_id = row[0]
            else:
                cur.execute(sql.SQL("""
                    SELECT CountryID FROM Country
                    WHERE Title = %s
                    """),
                            (country["Title"],)
                            )
                country_id = cur.fetchone()[0]

            level = item["Connections"][0]["Level"]
            cur.execute(sql.SQL("""
                INSERT INTO Level (Comments, IsFastChargeCapable, Title)
                VALUES (%s, %s, %s)
                ON CONFLICT (Title) DO NOTHING
                RETURNING LevelID
                """),
                        (level["Comments"], level["IsFastChargeCapable"], level["Title"])
                        )
            row = cur.fetchone()
            if row:
                level_id = row[0]
            else:
                cur.execute(sql.SQL("""
                    SELECT LevelID FROM Level
                    WHERE Title = %s
                    """),
                            (level["Title"],)
                            )
                level_id = cur.fetchone()[0]

            current_type = item["Connections"][0]["CurrentType"]
            cur.execute(sql.SQL("""
                INSERT INTO CurrentType (Description, Title)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                RETURNING CurrentTypeID
                """),
                        (current_type["Description"], current_type["Title"])
                        )
            current_type_row = cur.fetchone()
            if current_type_row:
                current_type_id = current_type_row[0]
            else:
                cur.execute(sql.SQL("""
                    SELECT CurrentTypeID FROM CurrentType
                    WHERE Title = %s
                    """),
                            (current_type["Title"],)
                            )
                current_type_id = cur.fetchone()[0]

            status_type = item["Connections"][0]["StatusType"]
            cur.execute(sql.SQL("""
                INSERT INTO StatusType (IsOperational, IsUserSelectable, Title)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING StatusTypeID
                """),
                        (status_type["IsOperational"], status_type["IsUserSelectable"], status_type["Title"])
                        )
            status_type_row = cur.fetchone()
            if status_type_row:
                status_type_id = status_type_row[0]
            else:
                # If the StatusType already exists, get its ID
                cur.execute(sql.SQL("""
                    SELECT StatusTypeID FROM StatusType
                    WHERE Title = %s
                    """),
                            (status_type["Title"],)
                            )
                status_type_id = cur.fetchone()[0]

            connection = item["Connections"][0]
            cur.execute(sql.SQL("""
                INSERT INTO Connection (StatusTypeID, LevelID, CurrentTypeID, Quantity, PowerKW)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING ConnectionID
                """),
                        (status_type_id, level_id, current_type_id, connection["Quantity"], connection["PowerKW"])
                        )
            connection_id = cur.fetchone()[0]

            usage_type = item["UsageType"]
            cur.execute(sql.SQL("""
                INSERT INTO UsageType (IsPayAtLocation, IsMembershipRequired, IsAccessKeyRequired, Title)
                VALUES (%s, %s, %s, %s)
                RETURNING UsageTypeID
                """),
                        (usage_type.get("IsPayAtLocation"), usage_type.get("IsMembershipRequired"),
                         usage_type.get("IsAccessKeyRequired"), usage_type["Title"])
                        )
            usage_type_id = cur.fetchone()[0]

            submission_status = item["SubmissionStatus"]
            cur.execute(sql.SQL("""
                INSERT INTO SubmissionStatus (IsLive, Title)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                RETURNING SubmissionStatusTypeID
                """),
                        (submission_status.get("IsLive"), submission_status["Title"])
                        )
            submission_status_row = cur.fetchone()
            if submission_status_row:
                submission_status_id = submission_status_row[0]
            else:
                cur.execute(sql.SQL("""
                    SELECT SubmissionStatusTypeID FROM SubmissionStatus
                    WHERE Title = %s
                    """),
                            (submission_status["Title"],)
                            )
                submission_status_id = cur.fetchone()[0]

            cur.execute(sql.SQL("""
                INSERT INTO AddressInfo (Title, AddressLine1, AddressLine2, Town, StateOrProvince, Postcode, CountryID, Latitude, Longitude)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING AddressID
                """),
                        (address["Title"], address["AddressLine1"], address.get("AddressLine2"), address["Town"],
                         address["StateOrProvince"], address["Postcode"], country_id, address["Latitude"],
                         address["Longitude"])
                        )
            address_id = cur.fetchone()[0]

            cur.execute(sql.SQL("""
                INSERT INTO OpenChargeMap (UUID, AddressID, ConnectionID, UsageTypeID, IsRecentlyVerified, DateLastVerified, DateCreated, SubmissionStatusTypeID)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """),
                        (item["UUID"], address_id, connection_id, usage_type_id, item["IsRecentlyVerified"],
                         item["DateLastVerified"], item["DateCreated"], submission_status_id)
                        )

        conn.commit()
        cur.close()
        conn.close()

        with self.output().open('w') as f:
            f.write("Data loaded to PostgreSQL successfully")

