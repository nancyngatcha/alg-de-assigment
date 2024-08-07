import psycopg2
import logging
from datetime import datetime, timedelta

logger = logging.getLogger("airflow.task")

POSTGRES_CONN = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres"
}

def execute_sql(sql):
    """
    Execute the given SQL command.
    """
    conn = psycopg2.connect(**POSTGRES_CONN)
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()

def create_table_if_not_exists():
    """
    Create a table if it does not already exist.
    """
    query = """
    CREATE TABLE IF NOT EXISTS shopify_configurations (
        application_id SERIAL,
        index_prefix VARCHAR,
        has_specific_prefix BOOLEAN,
        config VARCHAR,
        date_id DATE
    ) PARTITION BY RANGE (date_id);
    """
    execute_sql(query)
    logger.info("Creating table 'shopify_configurations' if not exists")

def create_partition_if_not_exists(execution_date):
    """
    Create a partition for a specific execution_date if it does not already exist.
    """
    partition_name = f"shopify_configurations_{execution_date.replace('-', '_')}"
    start_date = execution_date
    end_date = (datetime.strptime(execution_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    query = f"""
    CREATE TABLE IF NOT EXISTS {partition_name} PARTITION OF shopify_configurations
    FOR VALUES FROM ('{start_date}') TO ('{end_date}');
    """
    logger.info(f"Creating partition {partition_name} if not exists")
    execute_sql(query)
