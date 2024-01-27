# test_error_handling.py

import pytest
from unittest.mock import patch, MagicMock, Mock
from requests.exceptions import RequestException
from google.auth.exceptions import DefaultCredentialsError
from google.cloud.exceptions import NotFound
from snowflake.connector.errors import ProgrammingError
from dags.main_dag import fetch_app_usage_logs, acquire_occupancy_logs, extract_booking_logs, transfer_to_snowflake

@patch('dags.main_dag.requests.get')
def test_fetch_app_usage_logs_error_handling(mock_get):
    mock_get.side_effect = RequestException("Network error")
    result = fetch_app_usage_logs()
    assert result is None

@patch('dags.main_dag.requests.get')
def test_acquire_occupancy_logs_error_handling(mock_get):
    mock_get.return_value.raise_for_status.side_effect = RequestException("Server error")
    result = acquire_occupancy_logs()
    assert result is None

@patch('dags.main_dag.extract_booking_logs')
def test_extract_booking_logs_error_handling(mock_extract_booking_logs):
    mock_extract_booking_logs.side_effect = NotFound("Blob not found")
    result = extract_booking_logs()
    assert result is None

@patch('dags.main_dag.connect')
def test_transfer_to_snowflake_error_handling(mock_connect):
    mock_cursor = Mock()
    mock_cursor.execute.side_effect = ProgrammingError("SQL error")
    mock_connect.return_value.cursor.return_value = mock_cursor
    result = transfer_to_snowflake(data='mock_data')
    assert result is None
