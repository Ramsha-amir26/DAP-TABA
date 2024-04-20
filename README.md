# DAP-TABA
Database-and-Analytics-Programming TABA

### ETL Pipeline
Do not remove the 'data' folder inside plugins folder

# TO RUN Emissions ETL Pipeline
python3 EmissionsETL.py Load  --mongo-connection-string <connection_string> --mongo-db-name <db_name> --collection-name <collection_name> --postgres-host <host_address> --postgres-port <host_port>  --postgres-db-name <db_name> --postgres-db-username <postgres_db_username> --postgres-db-password <postgres_db_password>