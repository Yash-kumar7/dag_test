# test_fetch_app_usage_logs.py

import requests
from unittest.mock import patch
from dags.main_dag import fetch_app_usage_logs

@patch('dag.main_dag.requests.get')
def test_fetch_app_usage_logs(mock_get):
    # Mock response
    mock_response_data = {'log_data': 'app_usage_log_data', 'environment_name': 'env_name'}

    # Configure the mock to return the mock response
    mock_get.return_value.json.return_value = mock_response_data

    # Call the function under test
    result = fetch_app_usage_logs()

    # Assertions
    assert result == mock_response_data  # Adjust this based on your expected result
    mock_get.assert_called_once_with('http://api-endpoint.com/logs')
