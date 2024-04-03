# DAP-TABA
Database-and-Analytics-Programming TABA

# TO RUN ETL Pipeline
python3 ETL.py Load  --mongo-connection-string <connection_string> --mongo-db-name ev_database --collection-name ev_emissions --postgres-host 0.0.0.0 --postgres-port 5432  --postgres-db-name mydb
