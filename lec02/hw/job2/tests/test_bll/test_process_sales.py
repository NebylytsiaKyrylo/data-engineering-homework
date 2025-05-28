from unittest import mock
import pytest
import os

from lec02.hw.job2.bll.process_sales import process_sales_data


@mock.patch("lec02.hw.job2.bll.process_sales.os.path.isdir")
@mock.patch("lec02.hw.job2.bll.process_sales.os.makedirs")
@mock.patch("lec02.hw.job2.bll.process_sales.os.listdir")
@mock.patch("lec02.hw.job2.bll.process_sales.read_json_file")
@mock.patch("lec02.hw.job2.bll.process_sales.write_avro_file")
@mock.patch("lec02.hw.job2.bll.process_sales.logger.info")
def test_process_sales_data_success(
    mock_logger_info,
    mock_write_avro_file,
    mock_read_json_file,
    mock_os_listdir,
    mock_os_makedirs,
    mock_os_path_isdir,
):
    """Test process_sales_data function behavior when successfully processing files."""

    # Setup test parameters
    test_raw_dir = "test/raw/dir"
    test_stg_dir = "test/stg/dir"

    # Configure mock behavior
    mock_os_path_isdir.return_value = True
    mock_os_listdir.return_value = ["file1.json", "file2.json", "not_json.txt"]

    # Configure mock data for each JSON file
    file1_data = [
        {
            "client": "Client 1",
            "purchase_date": "2024-05-07",
            "product": "Product A",
            "price": 100,
        }
    ]
    file2_data = [
        {
            "client": "Client 2",
            "purchase_date": "2024-05-07",
            "product": "Product B",
            "price": 200,
        }
    ]

    # Configure read_json_file to return different data for each file
    mock_read_json_file.side_effect = [file1_data, file2_data]

    # Call function under test
    process_sales_data(raw_dir=test_raw_dir, stg_dir=test_stg_dir)

    # Assert directory existence was checked
    mock_os_path_isdir.assert_called_once_with(test_raw_dir)

    # Assert target directory was created
    mock_os_makedirs.assert_called_once_with(test_stg_dir, exist_ok=True)

    # Assert directory listing was called
    mock_os_listdir.assert_called_once_with(test_raw_dir)

    # Assert read_json_file was called for each JSON file
    assert mock_read_json_file.call_count == 2
    mock_read_json_file.assert_has_calls(
        [
            mock.call(os.path.join(test_raw_dir, "file1.json")),
            mock.call(os.path.join(test_raw_dir, "file2.json")),
        ]
    )

    # Assert write_avro_file was called for each JSON file
    assert mock_write_avro_file.call_count == 2
    mock_write_avro_file.assert_has_calls(
        [
            mock.call(
                page_data=file1_data,
                schema=mock.ANY,  # We don't need to test the exact schema here
                filepath=os.path.join(test_stg_dir, "file1.avro"),
            ),
            mock.call(
                page_data=file2_data,
                schema=mock.ANY,
                filepath=os.path.join(test_stg_dir, "file2.avro"),
            ),
        ]
    )

    # Assert appropriate log messages were recorded
    assert (
        mock_logger_info.call_count >= 7
    )  # At least 7 log messages should be recorded


@mock.patch("lec02.hw.job2.bll.process_sales.os.path.isdir")
@mock.patch("lec02.hw.job2.bll.process_sales.logger.error")
def test_process_sales_data_raw_dir_not_found(mock_logger_error, mock_os_path_isdir):
    """Test process_sales_data function behavior when raw_dir does not exist."""

    # Setup test parameters
    test_raw_dir = "test/nonexistent/dir"
    test_stg_dir = "test/stg/dir"

    # Configure mock behavior
    mock_os_path_isdir.return_value = False

    # Test that the function raises FileNotFoundError
    with pytest.raises(FileNotFoundError) as excinfo:
        process_sales_data(raw_dir=test_raw_dir, stg_dir=test_stg_dir)

    # Assert error message contains expected information
    assert f"Raw directory {test_raw_dir} does not exist" in str(excinfo.value)

    # Assert directory existence was checked
    mock_os_path_isdir.assert_called_once_with(test_raw_dir)

    # Assert error logging was called
    mock_logger_error.assert_called_once_with(
        f"Raw directory {test_raw_dir} does not exist or is not a directory."
    )


@mock.patch("lec02.hw.job2.bll.process_sales.os.path.isdir")
@mock.patch("lec02.hw.job2.bll.process_sales.os.makedirs")
@mock.patch("lec02.hw.job2.bll.process_sales.logger.info")
@mock.patch("lec02.hw.job2.bll.process_sales.logger.error")
def test_process_sales_data_makedirs_error(
    mock_logger_error, mock_logger_info, mock_os_makedirs, mock_os_path_isdir
):
    """Test process_sales_data function behavior when stg_dir cannot be created."""

    # Setup test parameters
    test_raw_dir = "test/raw/dir"
    test_stg_dir = "test/stg/dir"

    # Configure mock behavior
    mock_os_path_isdir.return_value = True
    error_msg = "Permission denied"
    mock_os_makedirs.side_effect = OSError(error_msg)

    # Test that the function raises OSError
    with pytest.raises(OSError) as excinfo:
        process_sales_data(raw_dir=test_raw_dir, stg_dir=test_stg_dir)

    # Assert error message contains expected information
    assert f"Error creating directory {test_stg_dir}: {error_msg}" in str(excinfo.value)

    # Assert directory existence was checked
    mock_os_path_isdir.assert_called_once_with(test_raw_dir)

    # Assert target directory creation was attempted
    mock_os_makedirs.assert_called_once_with(test_stg_dir, exist_ok=True)

    # Assert error logging was called
    mock_logger_error.assert_called_once_with(
        f"Error creating directory {test_stg_dir}: {error_msg}", exc_info=True
    )


@mock.patch("lec02.hw.job2.bll.process_sales.os.path.isdir")
@mock.patch("lec02.hw.job2.bll.process_sales.os.makedirs")
@mock.patch("lec02.hw.job2.bll.process_sales.os.listdir")
@mock.patch("lec02.hw.job2.bll.process_sales.read_json_file")
@mock.patch("lec02.hw.job2.bll.process_sales.logger.info")
@mock.patch("lec02.hw.job2.bll.process_sales.logger.error")
def test_process_sales_data_read_json_file_error(
    mock_logger_error,
    mock_logger_info,
    mock_read_json_file,
    mock_os_listdir,
    mock_os_makedirs,
    mock_os_path_isdir,
):
    """Test process_sales_data function behavior when read_json_file raises an error."""

    # Setup test parameters
    test_raw_dir = "test/raw/dir"
    test_stg_dir = "test/stg/dir"
    test_filename = "file1.json"
    test_filepath = os.path.join(test_raw_dir, test_filename)

    # Configure mock behavior
    mock_os_path_isdir.return_value = True
    mock_os_listdir.return_value = [test_filename]
    error_msg = "Invalid JSON format"
    mock_read_json_file.side_effect = ValueError(error_msg)

    # Test that the function raises Exception
    with pytest.raises(Exception) as excinfo:
        process_sales_data(raw_dir=test_raw_dir, stg_dir=test_stg_dir)

    # Assert error message contains expected information
    assert f"An unexpected error occurred: {error_msg}" in str(excinfo.value)

    # Assert directory existence was checked
    mock_os_path_isdir.assert_called_once_with(test_raw_dir)

    # Assert target directory was created
    mock_os_makedirs.assert_called_once_with(test_stg_dir, exist_ok=True)

    # Assert directory listing was called
    mock_os_listdir.assert_called_once_with(test_raw_dir)

    # Assert read_json_file was called
    mock_read_json_file.assert_called_once_with(test_filepath)

    # Assert error logging was called with both messages
    mock_logger_error.assert_has_calls(
        [
            mock.call(
                f"Error reading file {test_filepath}: {error_msg}", exc_info=True
            ),
            mock.call(f"An unexpected error occurred: {error_msg}", exc_info=True),
        ]
    )


@mock.patch("lec02.hw.job2.bll.process_sales.os.path.isdir")
@mock.patch("lec02.hw.job2.bll.process_sales.os.makedirs")
@mock.patch("lec02.hw.job2.bll.process_sales.os.listdir")
@mock.patch("lec02.hw.job2.bll.process_sales.read_json_file")
@mock.patch("lec02.hw.job2.bll.process_sales.write_avro_file")
@mock.patch("lec02.hw.job2.bll.process_sales.logger.info")
@mock.patch("lec02.hw.job2.bll.process_sales.logger.error")
def test_process_sales_data_write_avro_file_error(
    mock_logger_error,
    mock_logger_info,
    mock_write_avro_file,
    mock_read_json_file,
    mock_os_listdir,
    mock_os_makedirs,
    mock_os_path_isdir,
):
    """Test process_sales_data function behavior when write_avro_file raises an error."""

    # Setup test parameters
    test_raw_dir = "test/raw/dir"
    test_stg_dir = "test/stg/dir"
    test_filename = "file1.json"
    test_input_filepath = os.path.join(test_raw_dir, test_filename)
    test_output_filepath = os.path.join(test_stg_dir, "file1.avro")

    # Configure mock behavior
    mock_os_path_isdir.return_value = True
    mock_os_listdir.return_value = [test_filename]
    test_data = [
        {
            "client": "Client 1",
            "purchase_date": "2024-05-07",
            "product": "Product A",
            "price": 100,
        }
    ]
    mock_read_json_file.return_value = test_data
    error_msg = "Disk full"
    mock_write_avro_file.side_effect = IOError(error_msg)

    # Test that the function raises Exception
    with pytest.raises(Exception) as excinfo:
        process_sales_data(raw_dir=test_raw_dir, stg_dir=test_stg_dir)

    # Assert error message contains expected information
    assert f"An unexpected error occurred: {error_msg}" in str(excinfo.value)

    # Assert directory existence was checked
    mock_os_path_isdir.assert_called_once_with(test_raw_dir)

    # Assert target directory was created
    mock_os_makedirs.assert_called_once_with(test_stg_dir, exist_ok=True)

    # Assert directory listing was called
    mock_os_listdir.assert_called_once_with(test_raw_dir)

    # Assert read_json_file was called
    mock_read_json_file.assert_called_once_with(test_input_filepath)

    # Assert write_avro_file was called
    mock_write_avro_file.assert_called_once()

    # Assert error logging was called with both messages
    mock_logger_error.assert_has_calls(
        [
            mock.call(
                f"Error saving file {test_output_filepath}: {error_msg}", exc_info=True
            ),
            mock.call(f"An unexpected error occurred: {error_msg}", exc_info=True),
        ]
    )


@mock.patch("lec02.hw.job2.bll.process_sales.os.path.isdir")
@mock.patch("lec02.hw.job2.bll.process_sales.os.makedirs")
@mock.patch("lec02.hw.job2.bll.process_sales.os.listdir")
@mock.patch("lec02.hw.job2.bll.process_sales.logger.info")
@mock.patch("lec02.hw.job2.bll.process_sales.logger.exception")
def test_process_sales_data_unexpected_error(
    mock_logger_exception,
    mock_logger_info,
    mock_os_listdir,
    mock_os_makedirs,
    mock_os_path_isdir,
):
    """Test process_sales_data function behavior when an unexpected exception occurs."""

    # Setup test parameters
    test_raw_dir = "test/raw/dir"
    test_stg_dir = "test/stg/dir"

    # Configure mock behavior
    mock_os_path_isdir.return_value = True
    error_msg = "Unexpected error"
    mock_os_listdir.side_effect = Exception(error_msg)

    # Test that the function raises Exception
    with pytest.raises(Exception) as excinfo:
        process_sales_data(raw_dir=test_raw_dir, stg_dir=test_stg_dir)

    # Assert error message contains expected information
    assert f"An unexpected error occurred: {error_msg}" in str(excinfo.value)

    # Assert directory existence was checked
    mock_os_path_isdir.assert_called_once_with(test_raw_dir)

    # Assert target directory was created
    mock_os_makedirs.assert_called_once_with(test_stg_dir, exist_ok=True)

    # Assert directory listing was attempted
    mock_os_listdir.assert_called_once_with(test_raw_dir)

    # Assert exception logging was called
    mock_logger_exception.assert_called_once_with(
        f"An unexpected error occurred: {error_msg}"
    )
