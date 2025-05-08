from unittest import mock
import json
import pytest
import requests
import time

from lec02.hw.job1.dal.sales_api import (
    get_sales_per_page,
    API_URL,
    ERR_TOKEN_MISSING,
    MAX_RETRIES,
    RETRY_STATUS_CODES,
)


@mock.patch("lec02.hw.job1.dal.sales_api.AUTH_TOKEN", "test_token")  # Mock environment
# variable
@mock.patch("lec02.hw.job1.dal.sales_api.requests.get")  # Mock requests.get call
def test_get_sales_per_page_success(mock_requests_get):
    # Setup mock response
    mock_response = mock.Mock()
    mock_response.status_code = 200
    fake_page_data = [
        {
            "client": "Test Client 1",
            "purchase_date": "2024-05-07",
            "product": "Test Product A",
            "price": 150,
        },
        {
            "client": "Test Client 2",
            "purchase_date": "2024-05-07",
            "product": "Test Product B",
            "price": 250,
        },
    ]
    # Configure mock response behavior
    mock_response.json.return_value = fake_page_data
    mock_response.raise_for_status.return_value = None
    mock_requests_get.return_value = mock_response

    # Test parameters
    test_date = "2024-05-07"
    test_page = 1

    # Call function under test
    result_data = get_sales_per_page(date=test_date, page=test_page)

    # Assert response data matches expected data
    assert result_data == fake_page_data

    # Verify request was made exactly once
    mock_requests_get.assert_called_once()

    # Verify request was made with correct parameters
    expected_headers = {"Authorization": "test_token"}
    expected_params = {"page": str(test_page), "date": test_date}
    mock_requests_get.assert_called_with(
        API_URL, headers=expected_headers, params=expected_params, timeout=20
    )


@mock.patch("lec02.hw.job1.dal.sales_api.AUTH_TOKEN", "test_token")
@mock.patch("lec02.hw.job1.dal.sales_api.requests.get")
def test_get_sales_per_page_end_of_data_404(mock_requests_get):
    # Setup mock response for 404 error
    mock_response = mock.Mock()
    mock_response.status_code = 404
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        response=mock_response
    )
    mock_requests_get.return_value = mock_response

    # Test parameters
    test_date = "2024-05-07"
    test_page = 5

    # Call function under test
    result_data = get_sales_per_page(date=test_date, page=test_page)

    # Assert result is None when 404 is returned
    assert result_data is None

    # Verify request was made with correct parameters
    expected_headers = {"Authorization": "test_token"}
    expected_params = {"page": str(test_page), "date": test_date}
    mock_requests_get.assert_called_once_with(
        API_URL, headers=expected_headers, params=expected_params, timeout=20
    )


@mock.patch("lec02.hw.job1.dal.sales_api.AUTH_TOKEN", "test_token")
@mock.patch("lec02.hw.job1.dal.sales_api.requests.get")
def test_get_sales_per_page_end_of_data_empty_list(mock_requests_get):
    # Setup mock response
    mock_response = mock.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = []  # Return empty list to simulate end of data
    mock_response.raise_for_status.return_value = None
    mock_requests_get.return_value = mock_response

    # Test parameters
    test_date = "2024-05-07"
    test_page = 6

    # Call function under test
    result_data = get_sales_per_page(date=test_date, page=test_page)

    # Assert that empty list response returns None
    assert result_data is None

    # Verify request was made with correct parameters
    expected_headers = {"Authorization": "test_token"}
    expected_params = {"page": str(test_page), "date": test_date}
    mock_requests_get.assert_called_once_with(
        API_URL, headers=expected_headers, params=expected_params, timeout=20
    )


@mock.patch("lec02.hw.job1.dal.sales_api.AUTH_TOKEN", "test_token")
@mock.patch("lec02.hw.job1.dal.sales_api.requests.get")
def test_get_sales_per_page_unauthorized_401(mock_requests_get):
    # Setup mock response for 401 error
    mock_response = mock.Mock()
    mock_response.status_code = 401
    mock_response.reason = "Unauthorized"
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "401 Client Error: Unauthorized for url:", response=mock_response
    )
    mock_requests_get.return_value = mock_response

    # Test parameters
    test_date = "2024-05-07"
    test_page = 1

    # Test that the function raises ConnectionError for 401
    with pytest.raises(ConnectionError) as excinfo:
        get_sales_per_page(date=test_date, page=test_page)

    assert "Non-retryable HTTP error fetching page 1" in str(excinfo.value)

    mock_requests_get.assert_called_once()

    # Verify request was made with correct parameters
    expected_headers = {"Authorization": "test_token"}
    expected_params = {"page": str(test_page), "date": test_date}
    mock_requests_get.assert_called_once_with(
        API_URL, headers=expected_headers, params=expected_params, timeout=20
    )


@mock.patch("lec02.hw.job1.dal.sales_api.AUTH_TOKEN", "test_token")
@mock.patch("lec02.hw.job1.dal.sales_api.requests.get")
def test_get_sales_per_page_forbidden_403(mock_requests_get):
    # Setup mock response for 403 error
    mock_response = mock.Mock()
    mock_response.status_code = 403
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        response=mock_response
    )
    mock_requests_get.return_value = mock_response

    # Test parameters
    test_date = "2024-05-07"
    test_page = 1

    # Test that the function raises ConnectionError for 403
    with pytest.raises(ConnectionError) as excinfo:
        get_sales_per_page(date=test_date, page=test_page)

    assert "Non-retryable HTTP error fetching page 1" in str(excinfo.value)

    mock_requests_get.assert_called_once()

    # Verify request was made with correct parameters
    expected_headers = {"Authorization": "test_token"}
    expected_params = {"page": str(test_page), "date": test_date}
    mock_requests_get.assert_called_once_with(
        API_URL, headers=expected_headers, params=expected_params, timeout=20
    )


@mock.patch("lec02.hw.job1.dal.sales_api.AUTH_TOKEN", "test_token")
@mock.patch("lec02.hw.job1.dal.sales_api.requests.get")
def test_get_sales_per_page_invalid_json(mock_requests_get):
    # Setup mock response
    mock_response = mock.Mock()
    mock_response.status_code = 200
    mock_response.json.side_effect = json.JSONDecodeError(
        "Expecting value", "<html>Non-JSON content</html>", 0
    )
    mock_response.raise_for_status.return_value = None
    mock_requests_get.return_value = mock_response

    # Test parameters
    test_date = "2024-05-07"
    test_page = 1

    # Test that the function raises ValueError for invalid JSON
    with pytest.raises(ValueError) as excinfo:
        get_sales_per_page(date=test_date, page=test_page)

    assert "JSON decode error fetching page 1" in str(excinfo.value)

    # Verify request was made with correct parameters
    expected_headers = {"Authorization": "test_token"}
    expected_params = {"page": str(test_page), "date": test_date}
    mock_requests_get.assert_called_once_with(
        API_URL, headers=expected_headers, params=expected_params, timeout=20
    )


@mock.patch("lec02.hw.job1.dal.sales_api.AUTH_TOKEN", None)
def test_get_sales_per_page_missing_auth_token():
    """Test get_sales_per_page function behavior when AUTH_TOKEN is not set."""

    # Test parameters
    test_date = "2024-05-07"
    test_page = 1

    # Test that the function raises ValueError when AUTH_TOKEN is missing
    with pytest.raises(ValueError) as excinfo:
        get_sales_per_page(date=test_date, page=test_page)

    # Assert error message contains expected information
    assert ERR_TOKEN_MISSING in str(excinfo.value)


@mock.patch("lec02.hw.job1.dal.sales_api.AUTH_TOKEN", "test_token")
@mock.patch("lec02.hw.job1.dal.sales_api.requests.get")
def test_get_sales_per_page_response_not_list(mock_requests_get):
    """Test get_sales_per_page function behavior when API response is not a list."""

    # Setup mock response with non-list data
    mock_response = mock.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"error": "This is not a list"}
    mock_response.raise_for_status.return_value = None
    mock_requests_get.return_value = mock_response

    # Test parameters
    test_date = "2024-05-07"
    test_page = 1

    # Test that the function raises ValueError when response is not a list
    with pytest.raises(ValueError) as excinfo:
        get_sales_per_page(date=test_date, page=test_page)

    # Assert error message contains expected information
    assert f"Response is not a list for page {test_page}" in str(excinfo.value)

    # Verify request was made with correct parameters
    expected_headers = {"Authorization": "test_token"}
    expected_params = {"page": str(test_page), "date": test_date}
    mock_requests_get.assert_called_once_with(
        API_URL, headers=expected_headers, params=expected_params, timeout=20
    )


@mock.patch("lec02.hw.job1.dal.sales_api.AUTH_TOKEN", "test_token")
@mock.patch("lec02.hw.job1.dal.sales_api.requests.get")
@mock.patch("lec02.hw.job1.dal.sales_api.time.sleep")  # Mock sleep to avoid waiting
def test_get_sales_per_page_network_error_with_retry(mock_sleep, mock_requests_get):
    """Test get_sales_per_page function behavior with network errors and retry logic."""

    # Setup mock response with network error for first attempt, then success
    connection_error = requests.exceptions.ConnectionError("Connection refused")

    # Create a successful response for the second attempt
    mock_response_success = mock.Mock()
    mock_response_success.status_code = 200
    fake_page_data = [{"client": "Test Client", "purchase_date": "2024-05-07", "product": "Test Product", "price": 100}]
    mock_response_success.json.return_value = fake_page_data
    mock_response_success.raise_for_status.return_value = None

    # Configure mock to fail first, then succeed
    mock_requests_get.side_effect = [connection_error, mock_response_success]

    # Test parameters
    test_date = "2024-05-07"
    test_page = 1

    # Call function under test
    result_data = get_sales_per_page(date=test_date, page=test_page)

    # Assert response data matches expected data after retry
    assert result_data == fake_page_data

    # Verify request was made twice (initial failure + successful retry)
    assert mock_requests_get.call_count == 2

    # Verify sleep was called once for the retry
    mock_sleep.assert_called_once()

    # Verify both requests were made with correct parameters
    expected_headers = {"Authorization": "test_token"}
    expected_params = {"page": str(test_page), "date": test_date}
    mock_requests_get.assert_has_calls([
        mock.call(API_URL, headers=expected_headers, params=expected_params, timeout=20),
        mock.call(API_URL, headers=expected_headers, params=expected_params, timeout=20)
    ])


@mock.patch("lec02.hw.job1.dal.sales_api.AUTH_TOKEN", "test_token")
@mock.patch("lec02.hw.job1.dal.sales_api.requests.get")
@mock.patch("lec02.hw.job1.dal.sales_api.time.sleep")  # Mock sleep to avoid waiting
def test_get_sales_per_page_retryable_http_error(mock_sleep, mock_requests_get):
    """Test get_sales_per_page function behavior with retryable HTTP errors (5xx)."""

    # Setup mock responses - first with 503 error, then success
    mock_response_error = mock.Mock()
    mock_response_error.status_code = 503  # Service Unavailable (retryable)
    mock_response_error.raise_for_status.side_effect = requests.exceptions.HTTPError(
        response=mock_response_error
    )

    # Create a successful response for the second attempt
    mock_response_success = mock.Mock()
    mock_response_success.status_code = 200
    fake_page_data = [{"client": "Test Client", "purchase_date": "2024-05-07", "product": "Test Product", "price": 100}]
    mock_response_success.json.return_value = fake_page_data
    mock_response_success.raise_for_status.return_value = None

    # Configure mock to return error response first, then success
    mock_requests_get.side_effect = [mock_response_error, mock_response_success]

    # Test parameters
    test_date = "2024-05-07"
    test_page = 1

    # Call function under test
    result_data = get_sales_per_page(date=test_date, page=test_page)

    # Assert response data matches expected data after retry
    assert result_data == fake_page_data

    # Verify request was made twice (initial failure + successful retry)
    assert mock_requests_get.call_count == 2

    # Verify sleep was called once for the retry
    mock_sleep.assert_called_once()

    # Verify both requests were made with correct parameters
    expected_headers = {"Authorization": "test_token"}
    expected_params = {"page": str(test_page), "date": test_date}
    mock_requests_get.assert_has_calls([
        mock.call(API_URL, headers=expected_headers, params=expected_params, timeout=20),
        mock.call(API_URL, headers=expected_headers, params=expected_params, timeout=20)
    ])


@mock.patch("lec02.hw.job1.dal.sales_api.AUTH_TOKEN", "test_token")
@mock.patch("lec02.hw.job1.dal.sales_api.requests.get")
@mock.patch("lec02.hw.job1.dal.sales_api.time.sleep")  # Mock sleep to avoid waiting
def test_get_sales_per_page_max_retries_reached(mock_sleep, mock_requests_get):
    """Test get_sales_per_page function behavior when max retries are reached."""

    # Setup mock response with persistent network error
    connection_error = requests.exceptions.ConnectionError("Connection refused")

    # Configure mock to always fail with the same error
    mock_requests_get.side_effect = [connection_error] * MAX_RETRIES

    # Test parameters
    test_date = "2024-05-07"
    test_page = 1

    # Test that the function raises ConnectionError after max retries
    with pytest.raises(ConnectionError) as excinfo:
        get_sales_per_page(date=test_date, page=test_page)

    # Assert error message contains expected information
    assert f"Failed to fetch page {test_page} after {MAX_RETRIES} attempts" in str(excinfo.value)

    # Verify request was made MAX_RETRIES times
    assert mock_requests_get.call_count == MAX_RETRIES

    # Verify sleep was called MAX_RETRIES-1 times (no sleep after last attempt)
    assert mock_sleep.call_count == MAX_RETRIES - 1

    # Verify all requests were made with correct parameters
    expected_headers = {"Authorization": "test_token"}
    expected_params = {"page": str(test_page), "date": test_date}
    expected_calls = [
        mock.call(API_URL, headers=expected_headers, params=expected_params, timeout=20)
    ] * MAX_RETRIES
    mock_requests_get.assert_has_calls(expected_calls)
