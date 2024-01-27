# test_transfer_to_snowflake.py

from unittest.mock import patch
from dags.main_dag import transfer_to_snowflake

@patch('dags.main_dag.connect')
def test_transfer_to_snowflake(mock_connect):
    # Mock Snowflake connection and cursor
    mock_connection = mock_connect.return_value
    mock_cursor = mock_connection.cursor.return_value

    # Configure the mock to return success
    mock_cursor.execute.return_value = None

    # Call the function under test
    transfer_to_snowflake(data='your_test_data')  # Adjust this based on your test data

    # Assertions
    mock_connect.assert_called_once_with(user='username', password='password', account='account-url', warehouse='warehouse', database='database', schema='schema')
    mock_connection.cursor.assert_called_once_with()
    mock_cursor.execute.assert_called_once_with("INSERT INTO table VALUES your_test_data")
    mock_connection.commit.assert_called_once_with()
