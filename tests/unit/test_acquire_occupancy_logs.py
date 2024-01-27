# test_acquire_occupancy_logs.py

import requests
from unittest.mock import patch, MagicMock
from dags.main_dag import acquire_occupancy_logs, transform_logs
import pendulum

class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data

@patch('dags.main_dag.requests.get')
@patch('dags.main_dag.transform_logs')
def test_acquire_occupancy_logs(mock_transform_logs, mock_get):
    # Mock response
    mock_response_data = {'log_data': 'occupancy_log_data', 'specific_column': True}
    mock_response = MockResponse(json_data=mock_response_data, status_code=200)

    # Configure the mock to return the mock response
    mock_get.return_value = mock_response

    # Configure the mock to return the filtered logs
    mock_transform_logs.return_value = [mock_response_data]  # Adjust this based on your transform logic

    # Call the function under test
    result = acquire_occupancy_logs()

    # Assertions
    expected_result = [mock_response_data]  # Adjust this based on your expected result
    assert result == expected_result, f"Unexpected result from acquire_occupancy_logs: {result}"
    mock_get.assert_called_once_with('http://api-endpoint.com/occupancy-logs')
    mock_transform_logs.assert_called_once_with(logs=[mock_response_data])

