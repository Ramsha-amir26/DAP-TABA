import luigi
import pymongo
import psycopg2
from datetime import datetime
import json

MONGODB_URI = "mongodb+srv://ramsha0amir:gYk6WWjS0ACv6kFQ@cluster0.a0p69eg.mongodb.net/"
DATABASE_NAME = "ev_database"
COLLECTION_NAME = "ev_charging_stations"

POSTGRES_DB_NAME = "postgres"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "javamylife"
POSTGRES_HOST = "database-1.ctqg0gsq67st.eu-north-1.rds.amazonaws.com"
POSTGRES_PORT = 5432


class ExtractDataFromMongoDB(luigi.Task):
    def output(self):
        return luigi.LocalTarget("data/extract_data_from_mongodb_task_complete.txt")

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
        return luigi.LocalTarget("data/transformed_data.json")

    def run(self):
        with self.input().open('r') as f:
            mongo_data = json.load(f)

        transformed_data = [self.transform_entry(entry) for entry in mongo_data]

        with self.output().open('w') as f:
            json.dump(transformed_data, f)

    def transform_entry(self, entry):
        transformed_entry = {
            'ID': entry.get('_id'),
            'UUID': entry.get('UUID'),
            'IsRecentlyVerified': entry.get('IsRecentlyVerified'),
            'DateLastVerified': self.format_date(entry.get('DateLastVerified')),
            'DateCreated': self.format_date(entry.get('DateCreated')),
            'SubmissionStatus': self.transform_submission_status(entry),
            'UsageType': self.transform_usage_type(entry),
            'Address': self.transform_address(entry),
            'Connections': [self.transform_connection(connection) for connection in entry['Connections']]
        }
        return transformed_entry

    def transform_submission_status(self, entry):
        submission_status = entry.get('SubmissionStatus', {})
        return {
            'SubmissionStatusTypeID': submission_status.get('ID'),
            'IsLive': submission_status.get('IsLive'),
            'Title': submission_status.get('Title')
        }

    def transform_usage_type(self, entry):
        usage_type = entry.get('UsageType')
        if usage_type:
            return {
                'UsageTypeID': usage_type.get('ID'),
                'IsPayAtLocation': usage_type.get('IsPayAtLocation'),
                'IsMembershipRequired': usage_type.get('IsMembershipRequired'),
                'IsAccessKeyRequired': usage_type.get('IsAccessKeyRequired'),
                'Title': usage_type.get('Title')
            }
        else:
            return {
                'UsageTypeID': None,
                'IsPayAtLocation': None,
                'IsMembershipRequired': None,
                'IsAccessKeyRequired': None,
                'Title': None
            }


    def transform_address(self, entry):
        address_info = entry.get('AddressInfo', {})
        country_info = address_info.get('Country', {})
        return {
            'AddressID': address_info.get('ID'),
            'Title': address_info.get('Title'),
            'AddressLine1': address_info.get('AddressLine1'),
            'Town': address_info.get('Town'),
            'StateOrProvince': address_info.get('StateOrProvince'),
            'Postcode': address_info.get('Postcode'),
            'Country': {
                'CountryID': country_info.get('ID'),
                'ISOCode': country_info.get('ISOCode'),
                'Title': country_info.get('Title')
            },
            'Latitude': address_info.get('Latitude'),
            'Longitude': address_info.get('Longitude'),
            'ContactTelephone1': address_info.get('ContactTelephone1')
        }

    def transform_connection(self, connection):
        if connection.get('StatusType'):
            status_type = connection['StatusType']
            is_operational = status_type.get('IsOperational')
            is_user_selectable = status_type.get('IsUserSelectable')
            title = status_type.get('Title')
        else:
            is_operational = None
            is_user_selectable = None
            title = None

        if connection.get('Level'):
            comments = connection['Level'].get('Comments')
            is_fast_charge_capable = connection['Level'].get('IsFastChargeCapable')
            level_title = connection['Level'].get('Title')
        else:
            comments = None
            is_fast_charge_capable = None
            level_title = None

        if connection.get('CurrentType'):
            description = connection['CurrentType'].get('Description')
            title = connection['CurrentType'].get('Title')
        else:
            description = None
            title = None

        transformed_connection = {
            'ConnectionID': connection.get('ID'),
            'ConnectionType': {
                'ConnectionTypeID': connection.get('ConnectionTypeID'),
                'FormalName': connection['ConnectionType'].get('FormalName'),
                'IsDiscontinued': connection['ConnectionType'].get('IsDiscontinued'),
                'IsObsolete': connection['ConnectionType'].get('IsObsolete'),
                'Title': connection['ConnectionType'].get('Title')
            },
            'StatusType': {
                'IsOperational': is_operational,
                'IsUserSelectable': is_user_selectable,
                'Title': title
            },
            'Level': {
                'LevelID': connection.get('ID'),
                'Comments': comments,
                'IsFastChargeCapable': is_fast_charge_capable,
                'Title': level_title
            },
            'CurrentType': {
                'CurrentTypeID': connection.get('ID'),
                'Description': description,
                'Title': title
            },
            'Quantity': connection.get('Quantity'),
            'PowerKW': connection.get('PowerKW')
        }
        return transformed_connection



    def format_date(self, date_str):
        if date_str is None:
            return None
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
            return date_obj.strftime("%Y%m%d")
        except ValueError:
            print("Invalid date format:", date_str)
            return None

class LoadDataToPostgres(luigi.Task):
    input_file = luigi.Parameter(default="data/transformed_data.json")
    host = luigi.Parameter(default=POSTGRES_HOST)
    database = luigi.Parameter(default=POSTGRES_DB_NAME)
    user = luigi.Parameter(default=POSTGRES_USER)
    password = luigi.Parameter(default=POSTGRES_PASSWORD)

    def requires(self):
        return TransformData()

    def output(self):
        return luigi.LocalTarget("data/json_loaded_to_postgres.txt")

    def run(self):
        data = self.load_json_data()
        conn = self.connect_to_postgres()
        cur = conn.cursor()

        try:
            self.insert_data_to_postgres(data, cur)
            conn.commit()
            with self.output().open('w') as f:
                f.write("Data loaded to PostgreSQL successfully")
        finally:
            cur.close()
            conn.close()

    def load_json_data(self):
        with open(self.input_file, 'r') as f:
            return json.load(f)

    def connect_to_postgres(self):
        return psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password
        )

    def insert_data_to_postgres(self, data, cur):
        for item in data:
            address_id = self.insert_or_get_address_id(item["Address"], cur)
            connection_id = self.insert_connection(item["Connections"], cur)
            usage_type_id = self.insert_or_get_usage_type_id(item["UsageType"], cur)
            submission_status_id = self.insert_or_get_submission_status_id(item["SubmissionStatus"], cur)

            cur.execute("""
                INSERT INTO OpenChargeMap (UUID, AddressID, ConnectionID, UsageTypeID, IsRecentlyVerified, DateLastVerified, DateCreated, SubmissionStatusTypeID)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (item["UUID"], address_id, connection_id, usage_type_id, item["IsRecentlyVerified"], item["DateLastVerified"], item["DateCreated"], submission_status_id)
            )

    def insert_or_get_address_id(self, address_data, cur):
        country_id = self.insert_or_get_country_id(address_data["Country"], cur)

        cur.execute("""
            INSERT INTO AddressInfo (Title, AddressLine1, Town, StateOrProvince, Postcode, CountryID, Latitude, Longitude)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING AddressID
            """,
            (address_data["Title"], address_data["AddressLine1"], address_data["Town"], address_data["StateOrProvince"], address_data["Postcode"], country_id, address_data["Latitude"], address_data["Longitude"])
        )

        return cur.fetchone()[0]

    def insert_or_get_country_id(self, country_data, cur):
        cur.execute("""
            INSERT INTO Country (ISOCode, Title)
            VALUES (%s, %s)
            ON CONFLICT (Title) DO NOTHING
            RETURNING CountryID
            """,
            (country_data["ISOCode"], country_data["Title"])
        )

        row = cur.fetchone()
        if row:
            return row[0]
        else:
            cur.execute("""
                SELECT CountryID FROM Country
                WHERE Title = %s
                """,
                (country_data["Title"],)
            )
            return cur.fetchone()[0]

    def insert_connection(self, connections_data, cur):
        for connection in connections_data:
            connection_type_id = self.insert_or_get_connection_type_id(connection["ConnectionType"], cur)
            level_id = self.insert_or_get_level_id(connection["Level"], cur)
            current_type_id = self.insert_or_get_current_type_id(connection["CurrentType"], cur)
            status_type_id = self.insert_or_get_status_type_id(connection["StatusType"], cur)

            cur.execute("""
                INSERT INTO Connection (StatusTypeID, LevelID, CurrentTypeID, ConnectionTypeID, Quantity, PowerKW)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING ConnectionID
                """,
                (status_type_id, level_id, current_type_id, connection_type_id, connection["Quantity"], connection["PowerKW"])
            )

            return cur.fetchone()[0]

    def insert_or_get_connection_type_id(self, connection_type_data, cur):
        cur.execute("""
            INSERT INTO ConnectionType (FormalName, IsDiscontinued, IsObsolete, Title)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (Title) DO NOTHING
            RETURNING ConnectionTypeID
            """,
            (connection_type_data["FormalName"], connection_type_data.get("IsDiscontinued", False), connection_type_data.get("IsObsolete", False), connection_type_data["Title"])
        )

        row = cur.fetchone()
        if row:
            return row[0]
        else:
            cur.execute("""
                SELECT ConnectionTypeID FROM ConnectionType
                WHERE Title = %s
                """,
                (connection_type_data["Title"],)
            )
            return cur.fetchone()[0]

    def insert_or_get_level_id(self, level_data, cur):
        cur.execute("""
            INSERT INTO Level (Comments, IsFastChargeCapable, Title)
            VALUES (%s, %s, %s)
            ON CONFLICT (Title) DO NOTHING
            RETURNING LevelID
            """,
            (level_data["Comments"], level_data["IsFastChargeCapable"], level_data["Title"])
        )

        row = cur.fetchone()
        if row:
            return row[0]
        else:
            cur.execute("""
                SELECT LevelID FROM Level
                WHERE Title = %s
                """,
                (level_data["Title"],)
            )
            return cur.fetchone()[0]

    def insert_or_get_current_type_id(self, current_type_data, cur):
        cur.execute("""
            INSERT INTO CurrentType (Description, Title)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            RETURNING CurrentTypeID
            """,
            (current_type_data["Description"], current_type_data["Title"])
        )

        row = cur.fetchone()
        if row:
            return row[0]
        else:
            cur.execute("""
                SELECT CurrentTypeID FROM CurrentType
                WHERE Title = %s
                """,
                (current_type_data["Title"],)
            )
            return cur.fetchone()[0]

    def insert_or_get_status_type_id(self, status_type_data, cur):
        cur.execute("""
            INSERT INTO StatusType (IsOperational, IsUserSelectable, Title)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING StatusTypeID
            """,
            (status_type_data["IsOperational"], status_type_data["IsUserSelectable"], status_type_data["Title"])
        )

        row = cur.fetchone()
        if row:
            return row[0]
        else:
            cur.execute("""
                SELECT StatusTypeID FROM StatusType
                WHERE Title = %s
                """,
                (status_type_data["Title"],)
            )
            return cur.fetchone()[0]

    def insert_or_get_usage_type_id(self, usage_type_data, cur):
        cur.execute("""
            INSERT INTO UsageType (IsPayAtLocation, IsMembershipRequired, IsAccessKeyRequired, Title)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (Title) DO NOTHING
            RETURNING UsageTypeID
            """,
            (usage_type_data.get("IsPayAtLocation"), usage_type_data.get("IsMembershipRequired"), usage_type_data.get("IsAccessKeyRequired"), usage_type_data["Title"])
        )

        row = cur.fetchone()
        if row:
            return row[0]
        else:
            cur.execute("""
                SELECT UsageTypeID FROM UsageType
                WHERE Title = %s
                """,
                (usage_type_data["Title"],)
            )
            return cur.fetchone()[0]

    def insert_or_get_submission_status_id(self, submission_status_data, cur):
        cur.execute("""
            INSERT INTO SubmissionStatus (IsLive, Title)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            RETURNING SubmissionStatusTypeID
            """,
            (submission_status_data.get("IsLive"), submission_status_data["Title"])
        )

        row = cur.fetchone()
        if row:
            return row[0]
        else:
            cur.execute("""
                SELECT SubmissionStatusTypeID FROM SubmissionStatus
                WHERE Title = %s
                """,
                (submission_status_data["Title"],)
            )
            return cur.fetchone()[0]

if __name__ == "__main__":
    luigi.run(['LoadDataToPostgres', '--local-scheduler'])
