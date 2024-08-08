from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_date
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import psycopg2
import logging
import sys

# Add /opt/airflow/scripts to the Python path
sys.path.append("/opt/airflow/scripts")

from etl import download_csv_from_s3, transform, load_to_postgres

logger = logging.getLogger("airflow.task")
execution_date = "{{ next_ds }}"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": "2024-08-06",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "shopify_etl_temp_files",
    default_args=default_args,
    description="A scalable ETL pipeline for Shopify data using temporary files",
    schedule_interval="0 2 * * *",
    )

valid_S3_prefix = "s3://"
bucket_name = "alg-data-public"

s3_sensor = S3KeySensor(
    task_id = f"check_extracted_file",
    bucket_key = f"{valid_S3_prefix}/{bucket_name}/{execution_date}.csv",
    wildcard_match = False,
    aws_conn_id = 'aws_default',
    dag = dag
)

download_data = PythonOperator(
    task_id = "extract",
    python_callable = download_csv_from_s3,
    op_kwargs = {"execution_date": execution_date },
    provide_context = True,
    dag = dag,
)

transform_data = PythonOperator(
    task_id = "transform",
    python_callable = transform,
    op_kwargs = {"execution_date": execution_date},
    provide_context = True,
    dag = dag,
)

load_data = PythonOperator(
    task_id = "load",
    python_callable = load_to_postgres,
    op_kwargs = {"execution_date": execution_date},
    provide_context = True,
    dag = dag,
)

s3_sensor >> download_data >> transform_data >> load_data
