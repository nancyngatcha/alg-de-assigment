import unittest
from unittest.mock import patch, MagicMock
import sys
import os
from datetime import datetime, timedelta

# Add the scripts directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts import db_utils

class TestDBUtils(unittest.TestCase):

    @patch('psycopg2.connect')
    def test_execute_sql(self, mock_connect):
        # Mock the database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # SQL to execute
        sql = "SELECT 1"

        # Call the function
        db_utils.execute_sql(sql)

        # Assert the expected results
        mock_connect.assert_called_once()
        mock_conn.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with(sql)
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch('scripts.db_utils.execute_sql')
    def test_create_table_if_not_exists(self, mock_execute_sql):
        # Call the function
        db_utils.create_table_if_not_exists()

        # SQL expected to be executed
        expected_sql = """
    CREATE TABLE IF NOT EXISTS shopify_configurations (
        application_id SERIAL,
        index_prefix VARCHAR,
        has_specific_prefix BOOLEAN,
        config VARCHAR,
        date_id DATE
    ) PARTITION BY RANGE (date_id);
    """

        # Assert the expected results
        mock_execute_sql.assert_called_once_with(expected_sql)

    @patch('scripts.db_utils.execute_sql')
    def test_create_partition_if_not_exists(self, mock_execute_sql):
        # Define the execution_date
        execution_date = '2024-08-07'
        partition_name = f"shopify_configurations_{execution_date.replace('-', '_')}"
        start_date = execution_date
        end_date = (datetime.strptime(execution_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

        # Call the function
        db_utils.create_partition_if_not_exists(execution_date)

        # SQL expected to be executed
        expected_sql = f"""
    CREATE TABLE IF NOT EXISTS {partition_name} PARTITION OF shopify_configurations
    FOR VALUES FROM ('{start_date}') TO ('{end_date}');
    """

        # Assert the expected results
        mock_execute_sql.assert_called_once_with(expected_sql)

if __name__ == '__main__':
    unittest.main()
