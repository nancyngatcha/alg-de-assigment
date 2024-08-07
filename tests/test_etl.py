import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from io import BytesIO
import sys
sys.path.append('/opt/airflow/scripts')

import etl


class TestETL(unittest.TestCase):

    @patch('etl.boto3.client')
    def test_download_csv_from_s3(self, mock_boto_client):
        # Mock the S3 client and its methods
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.download_file.return_value = True

        # Call the function
        execution_date = '2024-08-07'
        etl.download_csv_from_s3(execution_date=execution_date)

        # Assert the expected results
        mock_s3.download_file.assert_called_once_with('alg-data-public', f"{execution_date}.csv", f"/tmp/{execution_date}.csv")

    @patch('pyspark.sql.SparkSession.read')
    def test_transform(self, mock_spark_read):
        # Mock the Spark DataFrame and its methods
        mock_df = MagicMock()
        mock_filtered_df = MagicMock()
        mock_transformed_df = MagicMock()

        mock_spark_read.csv.return_value = mock_df
        mock_df.filter.return_value = mock_filtered_df
        mock_filtered_df.withColumn.return_value = mock_transformed_df
        mock_transformed_df.withColumn.return_value = mock_transformed_df

        # Call the function
        execution_date = '2024-08-07'
        etl.transform(execution_date=execution_date)

        # Assert the expected results
        mock_spark_read.csv.assert_called_once_with(f"/tmp/{execution_date}.csv", header=True, inferSchema=True)
        mock_transformed_df.write.mode('overwrite').parquet.assert_called_once_with(f"/tmp/{execution_date}.parquet")



    @patch('etl.create_table_if_not_exists')
    @patch('etl.create_partition_if_not_exists')
    @patch('pyspark.sql.SparkSession.read')
    @patch('shutil.rmtree')
    def test_load_to_postgres(self, mock_rmtree, mock_spark_read, mock_create_partition, mock_create_base_table):
        # Mock the Spark DataFrame and its methods
        mock_df = MagicMock()
        mock_spark_read.parquet.return_value = mock_df

        # Call the function
        execution_date = '2024-08-07'
        etl.load_to_postgres(execution_date=execution_date)

        # Assert the expected results
        mock_create_base_table.assert_called_once()
        mock_create_partition.assert_called_once_with(execution_date)
        mock_spark_read.parquet.assert_called_once_with(f"/tmp/{execution_date}.parquet")
        mock_rmtree.assert_called_once_with(f"/tmp/{execution_date}.parquet")


if __name__ == '__main__':
    unittest.main()