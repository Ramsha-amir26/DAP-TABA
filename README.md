# DAP-TABA
Database and Analytics Programming TABA

## Goal
Analysing the growth and popularity of EVs in USA.

## Datasets
**Vehicles Emissions** : Vehicle specs and emissions dataset

**EV Population** : Vehicle populations in USA.

**OpenCharge Map** : EV Charging stations api data stored as a dataset.

## ETL
In this project Luigi is being used for creating ETL pipelines.
Each dataset has its dedicated pipeline.

## Database
**MongoDB**: Initially used to store raw data from API and CSV files.

**PostgreSQL**:  stores the data after being cleansed from the ETL pipelines.

## ML Models
Random forest regressor ensemble model is being used for emissions and open charging stations dataset.

KNN classifier is being used for ev population dataset.

## Analysis
"Do advantages of ev's (better mileage and less emissions) effect their population among other vehicles"
This analysis is being done on emissions and ev_population dataset.

## Automation
Automating tasks is crucial for maintaining efficiency and consistency in data processing pipelines. 
In this project, automation is employed to execute ETL (Extract, Transform, Load) processes at specific intervals using the schedule library in Python

## Deployment 
**Amazon Web Services (AWS)**: The chosen cloud platform for hosting the PostgreSQL database

## Further Notes 
### ETL Pipeline
Do not remove the 'data' folder inside plugins folder

### TO RUN Emissions ETL Pipeline
python3 EmissionsETL.py Load  --mongo-connection-string <connection_string> --mongo-db-name <db_name> --collection-name <collection_name> --postgres-host <host_address> --postgres-port <host_port>  --postgres-db-name <db_name> --postgres-db-username <postgres_db_username> --postgres-db-password <postgres_db_password>