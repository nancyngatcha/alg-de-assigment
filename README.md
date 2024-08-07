# Shopify ETL Project

## Overview

This project is an ETL pipeline designed to process Shopify configuration data. The pipeline extracts data from an S3 bucket, transforms it using PySpark, and loads it into a PostgreSQL database. The project uses Apache Airflow to orchestrate the workflow.

## Project Structure

    .
    ├── dags
    │   └── etl_dag.py
    ├── logs
    ├── plugins
    ├── scripts
    │   ├── db_utils.py
    │   └── etl.py
    ├── tests
    │   ├── test_db_utils.py
    │   └── test_etl.py
    ├── tmp
    │   └── data
    │       └── 2024-08-07.csv
    ├── Dockerfile
    ├── README.md
    └── docker-compose.yml




## Requirements

- Docker
- Docker Compose
- Python 3
- Apache Airflow
- PySpark
- PostgreSQL
- Boto3
- Pytest

## Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd <repository-directory>
```

### 2. Build Docker Images
Ensure your Docker daemon is running and execute the following command:

```bash
docker-compose build
```

### 3. Initialize Airflow
Initialize the Airflow database and create necessary directories:

```
docker-compose up airflow-init
```
### 4. Start Services
Start all the services defined in the docker-compose.yml file:

```
docker-compose up
```
### 5. Access Airflow
Open your web browser and navigate to http://localhost:8080. Use the default Airflow credentials (airflow / airflow) to log in.

## ETL Process
### Extract
The download_csv_from_s3 function downloads a CSV file from an S3 bucket to the local /tmp directory.

### Transform
The transform function reads the CSV file into a PySpark DataFrame, applies transformations, and writes the transformed data to a Parquet file.

### Load
The load_to_postgres function reads the Parquet file, ensures the target PostgreSQL table and partitions exist, and loads the data into PostgreSQL.

### Clean Up
Temporary files created during the ETL process are removed after the load step to ensure efficient use of storage.

## Airflow DAG
The DAG (shopify_etl_dag.py) orchestrates the ETL process by defining tasks and dependencies. The tasks include:

- download_csv_from_s3
- transform
- load_to_postgres

## Running Tests
### Unit Tests
Unit tests for the ETL and utility functions are located in the tests directory. To run the tests, use the following command:

```
docker-compose run --rm test
```
### Test Files
Ensure test files are placed in the appropriate directories 
