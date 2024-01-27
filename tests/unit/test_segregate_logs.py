# test_segregate_logs.py

from unittest.mock import patch
from dags.main_dag import segregate_logs

@patch('dags.main_dag.fetch_app_usage_logs')
def test_segregate_logs(mock_fetch_app_usage_logs):
    # Mock logs
    mock_logs = [{'environment_name': 'env1', 'log_data': 'log1'}, {'environment_name': 'env2', 'log_data': 'log2'}]

    # Configure the mock to return the mock logs
    mock_fetch_app_usage_logs.return_value = mock_logs

    # Call the function under test
    result = segregate_logs()

    # Assertions
    assert result == {'env1': [{'environment_name': 'env1', 'log_data': 'log1'}], 'env2': [{'environment_name': 'env2', 'log_data': 'log2'}]}
    mock_fetch_app_usage_logs.assert_called_once_with()
