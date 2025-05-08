from unittest import mock
import pytest
import json
import fastavro

from lec02.hw.job2.dal.file_io import read_json_file, write_avro_file, SALES_AVRO_SCHEMA


@mock.patch(
    "lec02.hw.job2.dal.file_io.open",
    mock.mock_open(read_data='[{"client": "Test Client", "price": 100}]'),
)
@mock.patch("lec02.hw.job2.dal.file_io.json.load")
@mock.patch("lec02.hw.job2.dal.file_io.logger.info")
def test_read_json_file_success(mock_logger_info, mock_json_load):
    """Test read_json_file function behavior when successfully reading a JSON file."""

    # Setup test data
    test_filepath = "test/file.json"
    test_data = [
        {
            "client": "Test Client",
            "purchase_date": "2024-05-07",
            "product": "Test Product",
            "price": 100,
        }
    ]

    # Configure mock behavior
    mock_json_load.return_value = test_data

    # Call function under test
    result = read_json_file(test_filepath)

    # Assert result matches expected data
    assert result == test_data

    # Assert json.load was called once
    mock_json_load.assert_called_once()

    # Assert logging messages
    mock_logger_info.assert_has_calls(
        [
            mock.call(f"Reading JSON file {test_filepath}..."),
            mock.call(
                f"JSON file {test_filepath} read successfully with {len(test_data)} records."
            ),
        ]
    )


@mock.patch("lec02.hw.job2.dal.file_io.open")
@mock.patch("lec02.hw.job2.dal.file_io.logger.info")
@mock.patch("lec02.hw.job2.dal.file_io.logger.error")
def test_read_json_file_file_not_found(mock_logger_error, mock_logger_info, mock_open):
    """Test read_json_file function behavior when the file is not found."""

    # Setup test data
    test_filepath = "test/nonexistent_file.json"

    # Configure mock behavior
    error_msg = "No such file or directory"
    mock_open.side_effect = FileNotFoundError(error_msg)

    # Test that the function raises FileNotFoundError
    with pytest.raises(FileNotFoundError) as excinfo:
        read_json_file(test_filepath)

    # Assert error message contains expected information
    assert f"File {test_filepath} not found" in str(excinfo.value)

    # Assert logging messages
    mock_logger_info.assert_called_once_with(f"Reading JSON file {test_filepath}...")
    mock_logger_error.assert_called_once_with(
        f"File {test_filepath} not found: {error_msg}"
    )


@mock.patch(
    "lec02.hw.job2.dal.file_io.open",
    mock.mock_open(read_data='{"invalid": "not a list"}'),
)
@mock.patch("lec02.hw.job2.dal.file_io.json.load")
@mock.patch("lec02.hw.job2.dal.file_io.logger.info")
@mock.patch("lec02.hw.job2.dal.file_io.logger.error")
def test_read_json_file_not_a_list(mock_logger_error, mock_logger_info, mock_json_load):
    """Test read_json_file function behavior when the JSON data is not a list."""

    # Setup test data
    test_filepath = "test/file.json"
    test_data = {"invalid": "not a list"}  # Not a list

    # Configure mock behavior
    mock_json_load.return_value = test_data

    # Test that the function raises Exception
    with pytest.raises(Exception) as excinfo:
        read_json_file(test_filepath)

    # Assert error message contains expected information
    assert f"An unexpected error occurred: Data is not a list: {test_data}" in str(
        excinfo.value
    )

    # Assert logging messages
    mock_logger_info.assert_called_once_with(f"Reading JSON file {test_filepath}...")
    # Check that both error logs were called
    mock_logger_error.assert_has_calls(
        [
            mock.call(f"Data is not a list: {test_data}"),
            mock.call(
                f"An unexpected error occurred: Data is not a list: {test_data}",
                exc_info=True,
            ),
        ]
    )


@mock.patch("lec02.hw.job2.dal.file_io.open", mock.mock_open(read_data="invalid json"))
@mock.patch("lec02.hw.job2.dal.file_io.json.load")
@mock.patch("lec02.hw.job2.dal.file_io.logger.info")
@mock.patch("lec02.hw.job2.dal.file_io.logger.error")
def test_read_json_file_json_decode_error(
    mock_logger_error, mock_logger_info, mock_json_load
):
    """Test read_json_file function behavior when the file contains invalid JSON."""

    # Setup test data
    test_filepath = "test/file.json"

    # Configure mock behavior
    error_msg = "Expecting value"
    mock_json_load.side_effect = json.JSONDecodeError(error_msg, "", 0)

    # Test that the function raises ValueError
    with pytest.raises(ValueError) as excinfo:
        read_json_file(test_filepath)

    # Assert error message contains expected information
    assert f"Error decoding JSON from file {test_filepath}" in str(excinfo.value)

    # Assert logging messages
    mock_logger_info.assert_called_once_with(f"Reading JSON file {test_filepath}...")
    # Check that error was logged (without checking exact message format)
    mock_logger_error.assert_called_once()
    # Verify the error message contains the expected information
    error_call_args = mock_logger_error.call_args[0][0]
    assert f"Error decoding JSON from file {test_filepath}" in error_call_args
    assert error_msg in error_call_args


@mock.patch("lec02.hw.job2.dal.file_io.open", mock.mock_open())
@mock.patch("lec02.hw.job2.dal.file_io.fastavro.writer")
@mock.patch("lec02.hw.job2.dal.file_io.logger.info")
def test_write_avro_file_success(mock_logger_info, mock_fastavro_writer):
    """Test write_avro_file function behavior when successfully writing an AVRO file."""

    # Setup test data
    test_filepath = "test/file.avro"
    test_data = [
        {
            "client": "Test Client",
            "purchase_date": "2024-05-07",
            "product": "Test Product",
            "price": 100,
        }
    ]

    # Call function under test
    write_avro_file(test_data, SALES_AVRO_SCHEMA, test_filepath)

    # Assert fastavro.writer was called once with correct parameters
    mock_fastavro_writer.assert_called_once()
    args, kwargs = mock_fastavro_writer.call_args
    assert args[1] == SALES_AVRO_SCHEMA
    assert args[2] == test_data

    # Assert logging messages
    mock_logger_info.assert_has_calls(
        [
            mock.call(f"Writing {len(test_data)} records to {test_filepath}..."),
            mock.call(
                f"{len(test_data)} records written to file Avro {test_filepath}."
            ),
        ]
    )


@mock.patch("lec02.hw.job2.dal.file_io.open")
@mock.patch("lec02.hw.job2.dal.file_io.logger.info")
@mock.patch("lec02.hw.job2.dal.file_io.logger.error")
def test_write_avro_file_io_error(mock_logger_error, mock_logger_info, mock_open):
    """Test write_avro_file function behavior when an IOError occurs during file writing."""

    # Setup test data
    test_filepath = "test/file.avro"
    test_data = [
        {
            "client": "Test Client",
            "purchase_date": "2024-05-07",
            "product": "Test Product",
            "price": 100,
        }
    ]

    # Configure mock behavior
    error_msg = "Permission denied"
    mock_open.side_effect = IOError(error_msg)

    # Test that the function raises IOError
    with pytest.raises(IOError) as excinfo:
        write_avro_file(test_data, SALES_AVRO_SCHEMA, test_filepath)

    # Assert error message contains expected information
    assert f"Error saving to file Avro {test_filepath}: {error_msg}" in str(
        excinfo.value
    )

    # Assert logging messages
    mock_logger_info.assert_called_once_with(
        f"Writing {len(test_data)} records to {test_filepath}..."
    )
    mock_logger_error.assert_called_once_with(
        f"Error saving to file Avro {test_filepath}: {error_msg}", exc_info=True
    )


@mock.patch("lec02.hw.job2.dal.file_io.open", mock.mock_open())
@mock.patch("lec02.hw.job2.dal.file_io.fastavro.writer")
@mock.patch("lec02.hw.job2.dal.file_io.logger.info")
@mock.patch("lec02.hw.job2.dal.file_io.logger.exception")
def test_write_avro_file_unexpected_error(
    mock_logger_exception, mock_logger_info, mock_fastavro_writer
):
    """Test write_avro_file function behavior when an unexpected exception occurs."""

    # Setup test data
    test_filepath = "test/file.avro"
    test_data = [
        {
            "client": "Test Client",
            "purchase_date": "2024-05-07",
            "product": "Test Product",
            "price": 100,
        }
    ]

    # Configure mock behavior
    error_msg = "Unexpected error"
    mock_fastavro_writer.side_effect = Exception(error_msg)

    # Test that the function raises Exception
    with pytest.raises(Exception) as excinfo:
        write_avro_file(test_data, SALES_AVRO_SCHEMA, test_filepath)

    # Assert error message contains expected information
    assert f"An unexpected error occurred: {error_msg}" in str(excinfo.value)

    # Assert logging messages
    mock_logger_info.assert_called_once_with(
        f"Writing {len(test_data)} records to {test_filepath}..."
    )
    mock_logger_exception.assert_called_once_with(
        f"An unexpected error occurred: {error_msg}"
    )
