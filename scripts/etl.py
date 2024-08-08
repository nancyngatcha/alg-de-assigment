from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_date
import boto3
import logging
import shutil

# Add root directory to the Python path
from db_utils import create_table_if_not_exists, create_partition_if_not_exists
logger = logging.getLogger("airflow.task")

bucket_name = "alg-data-public"
table_name = "shopify_configurations"
postgres_url = 'jdbc:postgresql://postgres:5432/airflow'

postgres_conn = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres"
}
postgres_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}
spark = SparkSession.builder \
    .appName("Shopify") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.20") \
    .getOrCreate()


def download_csv_from_s3(**kwargs):
    """
    Download CSV file from S3 bucket
    """
    execution_date = kwargs["execution_date"]
    s3 = boto3.client('s3')
    file_name = f"{execution_date}.csv"
    local_file_path = f"/tmp/data/{file_name}"
    try:
        logger.info(f"Downloading file {file_name} from S3 bucket")
        s3.download_file(bucket_name, file_name, local_file_path)
        logger.info(f"Successfully downloaded the file")
    except Exception as e:
        logger.error(f"Download failed: {e}")
        logger.info(f"Download failed. No problem! We can use the mock file instead")

def transform(**kwargs):
    """
    Read csv file, transform its dataframe and write it into a parquet file
    """
    execution_date = kwargs["execution_date"]
    df = spark.read.csv(f"/tmp/data/{execution_date}.csv", header=True, inferSchema=True)
    df_filtered = df.filter(df.application_id.isNotNull())
    df_transformed = df_filtered.withColumn(
        "has_specific_prefix", when(col("index_prefix") != "shopify_", True).otherwise(False)
    )
    df_transformed = df_transformed.withColumn("date_id", to_date(lit(execution_date)))
    df_transformed.printSchema()
    df_transformed.write.mode('overwrite').parquet(f"/tmp/data/{execution_date}.parquet")
    logger.info(f"DataFrame written to Parquet file at /tmp/data/{execution_date}.parquet")

def load_to_postgres(**kwargs):
    """
    Load dataframe to Postgres
    """
    execution_date = kwargs["execution_date"]
    logger.info(f"Starting load to PostgreSQL for execution date: {execution_date}")
    try:
        create_table_if_not_exists()
        create_partition_if_not_exists(execution_date)
        file_path = f"/tmp/data/{execution_date}.parquet"
        df = spark.read.parquet(file_path)
        logger.info(df.printSchema() )
        df.write.jdbc(url=postgres_url, table= table_name, mode="append", properties=postgres_properties)
        shutil.rmtree(file_path)  # Delete parquet
        #os.remove(file_path.replace(".parquet", ".csv"))  # Cleanup csv file. We won't delete it for now because we use it for the tests
        logger.info(f"Temporary files {file_path} removed")
    except Exception as e:
        logger.error(f"Error during load for execution date: {execution_date}: {e}")
        raise