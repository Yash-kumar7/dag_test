# test_extract_booking_logs.py

from unittest.mock import patch, MagicMock
from dags.main_dag import extract_booking_logs
from google.cloud import storage

class MockBlob:
    def __init__(self, download_data):
        self.download_data = download_data
        self._content_type = None

    def download_as_text(self):
        return self.download_data

    @property
    def content_type(self):
        return str(self._content_type)

    @content_type.setter
    def content_type(self, value):
        self._content_type = value

@patch('dags.main_dag.storage.Client')
def test_extract_booking_logs(mock_storage_client):
    # Mock data
    mock_download_data = 'booking_log_data'

    try:
        # Configure the mock to return the mock Blob
        mock_blob = MockBlob(download_data=mock_download_data)
        mock_bucket = mock_storage_client.return_value.get_bucket.return_value
        mock_bucket.return_value.get_blob.return_value = mock_blob

        # Set the status code in the mock
        mock_status_code = 200
        mock_http_request = mock_storage_client.return_value.get_bucket.return_value.client._http.request
        mock_http_request.return_value.status_code = mock_status_code

        # Set the content type in the mock
        mock_content_type = 'text/plain'
        mock_blob.content_type = mock_content_type

        # Call the function under test
        result = extract_booking_logs()

        # Assertions
        assert result == mock_download_data  # Adjust this based on your expected result
        mock_storage_client.assert_called_once_with()
        mock_bucket.assert_called_once_with('your-gcp-bucket')
        mock_bucket.return_value.get_blob.assert_called_once_with('booking-logs-path')

        # Include additional debug information
        print("Mock storage client calls:")
        for call in mock_storage_client.mock_calls:
            print(call)
        print("Mock blob calls:")
        for call in mock_blob.mock_calls:
            print(call)
        print("Mock http request calls:")
        for call in mock_http_request.mock_calls:
            print(call)

    except Exception as e:
        print("Exception during test:", e)
        raise  # Re-raise the exception to get more details

# Run the tests
test_extract_booking_logs()
