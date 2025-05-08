from unittest import mock
import pytest
import json
from flask import Flask

from lec02.hw.job1.main import app, run_job_endpoint


@pytest.fixture
def client():
    """Fixture to create a test client for the Flask application."""
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client


@mock.patch("lec02.hw.job1.main.save_sales_to_local_disk")
def test_run_job_endpoint_success(mock_save_sales_to_local_disk, client):
    """Test run_job_endpoint function behavior with valid input data."""

    # Setup test parameters
    test_date = "2024-05-07"
    test_dir = "test/raw/dir"
    test_input = {"date": test_date, "raw_dir": test_dir}

    # Call endpoint with test data
    response = client.post(
        "/",
        data=json.dumps(test_input),
        content_type="application/json"
    )

    # Assert response status code and content
    assert response.status_code == 201
    response_data = json.loads(response.data)
    assert response_data["message"] == "Job completed successfully."

    # Assert save_sales_to_local_disk was called with correct parameters
    mock_save_sales_to_local_disk.assert_called_once_with(
        date=test_date, raw_dir=test_dir
    )


def test_run_job_endpoint_no_input_data(client):
    """Test run_job_endpoint function behavior when no input data is provided."""

    # Call endpoint with empty JSON object
    response = client.post(
        "/",
        data=json.dumps({}),
        content_type="application/json"
    )

    # Assert response status code and error message
    assert response.status_code == 400
    response_data = json.loads(response.data)
    assert response_data["error"] == "No input data received."


def test_run_job_endpoint_missing_date(client):
    """Test run_job_endpoint function behavior when 'date' parameter is missing."""

    # Setup test input with missing date
    test_input = {"raw_dir": "test/raw/dir"}

    # Call endpoint with test data
    response = client.post(
        "/",
        data=json.dumps(test_input),
        content_type="application/json"
    )

    # Assert response status code and error message
    assert response.status_code == 400
    response_data = json.loads(response.data)
    assert response_data["error"] == "Missing 'date' parameter in input data."


def test_run_job_endpoint_missing_raw_dir(client):
    """Test run_job_endpoint function behavior when 'raw_dir' parameter is missing."""

    # Setup test input with missing raw_dir
    test_input = {"date": "2024-05-07"}

    # Call endpoint with test data
    response = client.post(
        "/",
        data=json.dumps(test_input),
        content_type="application/json"
    )

    # Assert response status code and error message
    assert response.status_code == 400
    response_data = json.loads(response.data)
    assert response_data["error"] == "Missing 'raw_dir' parameter in input data."


@mock.patch("lec02.hw.job1.main.save_sales_to_local_disk")
def test_run_job_endpoint_value_error(mock_save_sales_to_local_disk, client):
    """Test run_job_endpoint function behavior when save_sales_to_local_disk raises ValueError."""

    # Setup test parameters
    test_date = "2024-05-07"
    test_dir = "test/raw/dir"
    test_input = {"date": test_date, "raw_dir": test_dir}

    # Configure mock to raise ValueError
    error_msg = "Invalid date format"
    mock_save_sales_to_local_disk.side_effect = ValueError(error_msg)

    # Call endpoint with test data
    response = client.post(
        "/",
        data=json.dumps(test_input),
        content_type="application/json"
    )

    # Assert response status code and error message
    assert response.status_code == 500
    response_data = json.loads(response.data)
    assert f"An error occurred while running job: {error_msg}" in response_data["error"]

    # Assert save_sales_to_local_disk was called with correct parameters
    mock_save_sales_to_local_disk.assert_called_once_with(
        date=test_date, raw_dir=test_dir
    )


@mock.patch("lec02.hw.job1.main.save_sales_to_local_disk")
def test_run_job_endpoint_connection_error(mock_save_sales_to_local_disk, client):
    """Test run_job_endpoint function behavior when save_sales_to_local_disk raises ConnectionError."""

    # Setup test parameters
    test_date = "2024-05-07"
    test_dir = "test/raw/dir"
    test_input = {"date": test_date, "raw_dir": test_dir}

    # Configure mock to raise ConnectionError
    error_msg = "Failed to connect to API"
    mock_save_sales_to_local_disk.side_effect = ConnectionError(error_msg)

    # Call endpoint with test data
    response = client.post(
        "/",
        data=json.dumps(test_input),
        content_type="application/json"
    )

    # Assert response status code and error message
    assert response.status_code == 500
    response_data = json.loads(response.data)
    assert f"An error occurred while running job: {error_msg}" in response_data["error"]

    # Assert save_sales_to_local_disk was called with correct parameters
    mock_save_sales_to_local_disk.assert_called_once_with(
        date=test_date, raw_dir=test_dir
    )


@mock.patch("lec02.hw.job1.main.save_sales_to_local_disk")
def test_run_job_endpoint_os_error(mock_save_sales_to_local_disk, client):
    """Test run_job_endpoint function behavior when save_sales_to_local_disk raises OSError."""

    # Setup test parameters
    test_date = "2024-05-07"
    test_dir = "test/raw/dir"
    test_input = {"date": test_date, "raw_dir": test_dir}

    # Configure mock to raise OSError
    error_msg = "Permission denied"
    mock_save_sales_to_local_disk.side_effect = OSError(error_msg)

    # Call endpoint with test data
    response = client.post(
        "/",
        data=json.dumps(test_input),
        content_type="application/json"
    )

    # Assert response status code and error message
    assert response.status_code == 500
    response_data = json.loads(response.data)
    assert f"An error occurred while running job: {error_msg}" in response_data["error"]

    # Assert save_sales_to_local_disk was called with correct parameters
    mock_save_sales_to_local_disk.assert_called_once_with(
        date=test_date, raw_dir=test_dir
    )


@mock.patch("lec02.hw.job1.main.save_sales_to_local_disk")
def test_run_job_endpoint_unexpected_error(mock_save_sales_to_local_disk, client):
    """Test run_job_endpoint function behavior when save_sales_to_local_disk raises an unexpected exception."""

    # Setup test parameters
    test_date = "2024-05-07"
    test_dir = "test/raw/dir"
    test_input = {"date": test_date, "raw_dir": test_dir}

    # Configure mock to raise an unexpected exception
    error_msg = "Unexpected error"
    mock_save_sales_to_local_disk.side_effect = Exception(error_msg)

    # Call endpoint with test data
    response = client.post(
        "/",
        data=json.dumps(test_input),
        content_type="application/json"
    )

    # Assert response status code and error message
    assert response.status_code == 500
    response_data = json.loads(response.data)
    assert f"An unexpected error occurred: {error_msg}" in response_data["error"]

    # Assert save_sales_to_local_disk was called with correct parameters
    mock_save_sales_to_local_disk.assert_called_once_with(
        date=test_date, raw_dir=test_dir
    )
