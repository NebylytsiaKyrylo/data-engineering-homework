from unittest import mock
import pytest
import json
from flask import Flask

from lec02.hw.job2.main import app, run_job2_endpoint


@pytest.fixture
def client():
    """Fixture to create a test client for the Flask application."""
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


@mock.patch("lec02.hw.job2.main.process_sales_data")
def test_run_job2_endpoint_success(mock_process_sales_data, client):
    """Test run_job2_endpoint function behavior with valid input data."""

    # Setup test parameters
    test_raw_dir = "test/raw/dir"
    test_stg_dir = "test/stg/dir"
    test_input = {"raw_dir": test_raw_dir, "stg_dir": test_stg_dir}

    # Call endpoint with test data
    response = client.post(
        "/", data=json.dumps(test_input), content_type="application/json"
    )

    # Assert response status code and content
    assert response.status_code == 201
    response_data = json.loads(response.data)
    assert response_data["message"] == "Job completed successfully."

    # Assert process_sales_data was called with correct parameters
    mock_process_sales_data.assert_called_once_with(
        raw_dir=test_raw_dir, stg_dir=test_stg_dir
    )


def test_run_job2_endpoint_no_input_data(client):
    """Test run_job2_endpoint function behavior when no input data is provided."""

    # Call endpoint with empty JSON object
    response = client.post("/", data=json.dumps({}), content_type="application/json")

    # Assert response status code and error message
    assert response.status_code == 400
    response_data = json.loads(response.data)
    assert response_data["error"] == "No input data received."


def test_run_job2_endpoint_missing_raw_dir(client):
    """Test run_job2_endpoint function behavior when 'raw_dir' parameter is missing."""

    # Setup test input with missing raw_dir
    test_input = {"stg_dir": "test/stg/dir"}

    # Call endpoint with test data
    response = client.post(
        "/", data=json.dumps(test_input), content_type="application/json"
    )

    # Assert response status code and error message
    assert response.status_code == 400
    response_data = json.loads(response.data)
    assert response_data["error"] == "Missing 'raw_dir' parameter in input data."


def test_run_job2_endpoint_missing_stg_dir(client):
    """Test run_job2_endpoint function behavior when 'stg_dir' parameter is missing."""

    # Setup test input with missing stg_dir
    test_input = {"raw_dir": "test/raw/dir"}

    # Call endpoint with test data
    response = client.post(
        "/", data=json.dumps(test_input), content_type="application/json"
    )

    # Assert response status code and error message
    assert response.status_code == 400
    response_data = json.loads(response.data)
    assert response_data["error"] == "Missing 'stg_dir' parameter in input data."


@mock.patch("lec02.hw.job2.main.process_sales_data")
def test_run_job2_endpoint_file_not_found_error(mock_process_sales_data, client):
    """Test run_job2_endpoint function behavior when process_sales_data raises FileNotFoundError."""

    # Setup test parameters
    test_raw_dir = "test/nonexistent/dir"
    test_stg_dir = "test/stg/dir"
    test_input = {"raw_dir": test_raw_dir, "stg_dir": test_stg_dir}

    # Configure mock to raise FileNotFoundError
    error_msg = "Raw directory does not exist"
    mock_process_sales_data.side_effect = FileNotFoundError(error_msg)

    # Call endpoint with test data
    response = client.post(
        "/", data=json.dumps(test_input), content_type="application/json"
    )

    # Assert response status code and error message
    assert response.status_code == 500
    response_data = json.loads(response.data)
    assert f"An error occurred while running job: {error_msg}" in response_data["error"]

    # Assert process_sales_data was called with correct parameters
    mock_process_sales_data.assert_called_once_with(
        raw_dir=test_raw_dir, stg_dir=test_stg_dir
    )


@mock.patch("lec02.hw.job2.main.process_sales_data")
def test_run_job2_endpoint_os_error(mock_process_sales_data, client):
    """Test run_job2_endpoint function behavior when process_sales_data raises OSError."""

    # Setup test parameters
    test_raw_dir = "test/raw/dir"
    test_stg_dir = "test/stg/dir"
    test_input = {"raw_dir": test_raw_dir, "stg_dir": test_stg_dir}

    # Configure mock to raise OSError
    error_msg = "Permission denied"
    mock_process_sales_data.side_effect = OSError(error_msg)

    # Call endpoint with test data
    response = client.post(
        "/", data=json.dumps(test_input), content_type="application/json"
    )

    # Assert response status code and error message
    assert response.status_code == 500
    response_data = json.loads(response.data)
    assert f"An error occurred while running job: {error_msg}" in response_data["error"]

    # Assert process_sales_data was called with correct parameters
    mock_process_sales_data.assert_called_once_with(
        raw_dir=test_raw_dir, stg_dir=test_stg_dir
    )


@mock.patch("lec02.hw.job2.main.process_sales_data")
def test_run_job2_endpoint_io_error(mock_process_sales_data, client):
    """Test run_job2_endpoint function behavior when process_sales_data raises IOError."""

    # Setup test parameters
    test_raw_dir = "test/raw/dir"
    test_stg_dir = "test/stg/dir"
    test_input = {"raw_dir": test_raw_dir, "stg_dir": test_stg_dir}

    # Configure mock to raise IOError
    error_msg = "Disk full"
    mock_process_sales_data.side_effect = IOError(error_msg)

    # Call endpoint with test data
    response = client.post(
        "/", data=json.dumps(test_input), content_type="application/json"
    )

    # Assert response status code and error message
    assert response.status_code == 500
    response_data = json.loads(response.data)
    assert f"An error occurred while running job: {error_msg}" in response_data["error"]

    # Assert process_sales_data was called with correct parameters
    mock_process_sales_data.assert_called_once_with(
        raw_dir=test_raw_dir, stg_dir=test_stg_dir
    )


@mock.patch("lec02.hw.job2.main.process_sales_data")
def test_run_job2_endpoint_unexpected_error(mock_process_sales_data, client):
    """Test run_job2_endpoint function behavior when process_sales_data raises an unexpected exception."""

    # Setup test parameters
    test_raw_dir = "test/raw/dir"
    test_stg_dir = "test/stg/dir"
    test_input = {"raw_dir": test_raw_dir, "stg_dir": test_stg_dir}

    # Configure mock to raise an unexpected exception
    error_msg = "Unexpected error"
    mock_process_sales_data.side_effect = Exception(error_msg)

    # Call endpoint with test data
    response = client.post(
        "/", data=json.dumps(test_input), content_type="application/json"
    )

    # Assert response status code and error message
    assert response.status_code == 500
    response_data = json.loads(response.data)
    assert f"An unexpected error occurred: {error_msg}" in response_data["error"]

    # Assert process_sales_data was called with correct parameters
    mock_process_sales_data.assert_called_once_with(
        raw_dir=test_raw_dir, stg_dir=test_stg_dir
    )
