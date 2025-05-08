from unittest import mock
from unittest.mock import call
import pytest
import json

from lec02.hw.job1.dal.local_disk import prepare_storage_dir, save_page_to_disk


@mock.patch("lec02.hw.job1.dal.local_disk.os.path.exists")
@mock.patch("lec02.hw.job1.dal.local_disk.shutil.rmtree")
@mock.patch("lec02.hw.job1.dal.local_disk.os.makedirs")
@mock.patch("lec02.hw.job1.dal.local_disk.logger.info")
def test_prepare_storage_dir_when_not_exists(
    mock_logger_info, mock_os_makedirs, mock_shutil_rmtree, mock_os_path_exists
):
    """Test prepare_storage_dir function behavior when directory does not exist."""

    # Test when the directory does not exist
    mock_os_path_exists.return_value = False
    test_directory = "test/directory/path"

    # Call function under test
    prepare_storage_dir(test_directory)

    # Assert directory existence check was called
    mock_os_path_exists.assert_called_once_with(test_directory)

    # Assert directory creation was not called
    mock_shutil_rmtree.assert_not_called()

    # Assert directory creation was called
    mock_os_makedirs.assert_called_once_with(test_directory)

    # Assert logging messages
    mock_logger_info.assert_has_calls(
        [
            call(f"Directory {test_directory} does not exist. Creating..."),
            call(f"Directory {test_directory} created successfully."),
        ]
    )


@mock.patch("lec02.hw.job1.dal.local_disk.os.path.exists")
@mock.patch("lec02.hw.job1.dal.local_disk.shutil.rmtree")
@mock.patch("lec02.hw.job1.dal.local_disk.os.makedirs")
@mock.patch("lec02.hw.job1.dal.local_disk.logger.info")
def test_prepare_storage_dir_when_exists(
    mock_logger_info, mock_os_makedirs, mock_shutil_rmtree, mock_os_path_exists
):
    """Test prepare_storage_dir function behavior when directory already exists."""
    # Test when the directory existes
    mock_os_path_exists.return_value = True
    test_directory = "test/directory/path"

    # Call function under test
    prepare_storage_dir(test_directory)

    # Assert directory existence check was called
    mock_os_path_exists.assert_called_once_with(test_directory)

    # Assert directory creation was called
    mock_shutil_rmtree.assert_called_once_with(test_directory)

    # Assert directory creation was called
    mock_os_makedirs.assert_called_once_with(test_directory)

    # Assert logging messages
    mock_logger_info.assert_has_calls(
        [
            call(f"Directory {test_directory} already exists. Removing..."),
            call(f"Directory {test_directory} created successfully."),
        ]
    )


@mock.patch("lec02.hw.job1.dal.local_disk.os.path.exists")
@mock.patch("lec02.hw.job1.dal.local_disk.shutil.rmtree")
@mock.patch("lec02.hw.job1.dal.local_disk.os.makedirs")
@mock.patch("lec02.hw.job1.dal.local_disk.logger.info")
@mock.patch("lec02.hw.job1.dal.local_disk.logger.error")
def test_prepare_storage_dir_when_rmtree_fails(
    mock_logger_error,
    mock_logger_info,
    mock_os_makedirs,
    mock_shutil_rmtree,
    mock_os_path_exists,
):
    """Test prepare_storage_dir function behavior when directory already
    exists but an error occurs during removal."""

    mock_os_path_exists.return_value = True
    test_directory = "test/directory/path"
    error_msg = f"Error during removal {test_directory}"
    mock_shutil_rmtree.side_effect = OSError(error_msg)

    with pytest.raises(OSError) as excinfo:
        prepare_storage_dir(test_directory)

    assert f"Error with directory {test_directory}: {error_msg}" in str(excinfo.value)

    # Assert directory existence check was called
    mock_os_path_exists.assert_called_once_with(test_directory)

    # Assert logging messages
    mock_logger_info.assert_called_once_with(
        f"Directory {test_directory} already " f"exists. Removing..."
    )

    # Assert directory removal was called
    mock_shutil_rmtree.assert_called_once_with(test_directory)

    # Assert error logging was called
    mock_logger_error.assert_called_once_with(
        f"Error with directory {test_directory}: {error_msg}", exc_info=True
    )

    # Assert directory creation was not called
    mock_os_makedirs.assert_not_called()


@mock.patch("lec02.hw.job1.dal.local_disk.os.path.exists")
@mock.patch("lec02.hw.job1.dal.local_disk.shutil.rmtree")
@mock.patch("lec02.hw.job1.dal.local_disk.os.makedirs")
@mock.patch("lec02.hw.job1.dal.local_disk.logger.info")
@mock.patch("lec02.hw.job1.dal.local_disk.logger.error")
def test_prepare_storage_dir_when_makedirs_fails(
    mock_logger_error,
    mock_logger_info,
    mock_os_makedirs,
    mock_shutil_rmtree,
    mock_os_path_exists,
):
    """Test prepare_storage_dir function behavior when directory not
    exists but en error occurs during creation."""

    mock_os_path_exists.return_value = False
    test_directory = "test/directory/path"
    error_msg = f"Error during creation {test_directory}"
    mock_os_makedirs.side_effect = OSError(error_msg)

    with pytest.raises(OSError) as excinfo:
        prepare_storage_dir(test_directory)

    assert f"Error with directory {test_directory}: {error_msg}" in str(excinfo.value)

    # Assert directory existence check was called
    mock_os_path_exists.assert_called_once_with(test_directory)

    # Assert logging messages
    mock_logger_info.assert_called_once_with(
        f"Directory {test_directory} does not exist. Creating..."
    )

    # Assert shutil.rmtree was not called
    mock_shutil_rmtree.assert_not_called()

    # Assert os.makedirs was called
    mock_os_makedirs.assert_called_once_with(test_directory)

    mock_logger_error.assert_called_once_with(
        f"Error with directory {test_directory}: {error_msg}", exc_info=True
    )


@mock.patch("lec02.hw.job1.dal.local_disk.open", mock.mock_open())
@mock.patch("lec02.hw.job1.dal.local_disk.json.dump")
@mock.patch("lec02.hw.job1.dal.local_disk.os.path.join")
@mock.patch("lec02.hw.job1.dal.local_disk.logger.info")
def test_save_page_to_disk_success(
    mock_logger_info, mock_os_path_join, mock_json_dump
):
    """Test save_page_to_disk function behavior when successfully saving data to disk."""

    # Setup test data
    test_data = [
        {"id": 1, "name": "Test Item 1", "price": 100},
        {"id": 2, "name": "Test Item 2", "price": 200},
    ]
    test_dir = "test/directory"
    test_filename = "test_file.json"
    test_filepath = f"{test_dir}/{test_filename}"

    # Configure mock behavior
    mock_os_path_join.return_value = test_filepath

    # Call function under test
    save_page_to_disk(test_data, test_dir, test_filename)

    # Assert path joining was called with correct parameters
    mock_os_path_join.assert_called_once_with(test_dir, test_filename)

    # Assert json.dump was called with correct parameters
    mock_json_dump.assert_called_once()
    args, kwargs = mock_json_dump.call_args
    assert args[0] == test_data  # First arg should be the data
    assert kwargs.get("ensure_ascii") is False
    assert kwargs.get("indent") == 4

    # Assert logging messages
    mock_logger_info.assert_has_calls(
        [
            call(f"Saving {len(test_data)} to {test_filepath}..."),
            call(f"{len(test_data)} records saved to {test_filepath}."),
        ]
    )


@mock.patch("lec02.hw.job1.dal.local_disk.open")
@mock.patch("lec02.hw.job1.dal.local_disk.os.path.join")
@mock.patch("lec02.hw.job1.dal.local_disk.logger.info")
@mock.patch("lec02.hw.job1.dal.local_disk.logger.error")
def test_save_page_to_disk_io_error(
    mock_logger_error, mock_logger_info, mock_os_path_join, mock_open
):
    """Test save_page_to_disk function behavior when an IOError occurs during file writing."""

    # Setup test data
    test_data = [{"id": 1, "name": "Test Item", "price": 100}]
    test_dir = "test/directory"
    test_filename = "test_file.json"
    test_filepath = f"{test_dir}/{test_filename}"

    # Configure mock behavior
    mock_os_path_join.return_value = test_filepath
    error_msg = "Permission denied"
    mock_open.side_effect = IOError(error_msg)

    # Test that the function raises IOError
    with pytest.raises(IOError) as excinfo:
        save_page_to_disk(test_data, test_dir, test_filename)

    # Assert error message contains expected information
    assert f"Error saving to {test_filepath}: {error_msg}" in str(excinfo.value)

    # Assert path joining was called with correct parameters
    mock_os_path_join.assert_called_once_with(test_dir, test_filename)

    # Assert logging messages
    mock_logger_info.assert_called_once_with(
        f"Saving {len(test_data)} to {test_filepath}..."
    )

    # Assert error logging was called
    mock_logger_error.assert_called_once_with(
        f"Error saving to {test_filepath}: {error_msg}", exc_info=True
    )


@mock.patch("lec02.hw.job1.dal.local_disk.open", mock.mock_open())
@mock.patch("lec02.hw.job1.dal.local_disk.json.dump")
@mock.patch("lec02.hw.job1.dal.local_disk.os.path.join")
@mock.patch("lec02.hw.job1.dal.local_disk.logger.info")
@mock.patch("lec02.hw.job1.dal.local_disk.logger.error")
def test_save_page_to_disk_type_error(
    mock_logger_error, mock_logger_info, mock_os_path_join, mock_json_dump
):
    """Test save_page_to_disk function behavior when a TypeError occurs during JSON serialization."""

    # Setup test data with a non-serializable object (a function)
    test_data = [{"id": 1, "function": lambda x: x}]  # Functions are not JSON serializable
    test_dir = "test/directory"
    test_filename = "test_file.json"
    test_filepath = f"{test_dir}/{test_filename}"

    # Configure mock behavior
    mock_os_path_join.return_value = test_filepath
    error_msg = "Object of type 'function' is not JSON serializable"
    mock_json_dump.side_effect = TypeError(error_msg)

    # Test that the function raises TypeError
    with pytest.raises(TypeError) as excinfo:
        save_page_to_disk(test_data, test_dir, test_filename)

    # Assert error message contains expected information
    assert f"Error saving to {test_filepath}: {error_msg}" in str(excinfo.value)

    # Assert path joining was called with correct parameters
    mock_os_path_join.assert_called_once_with(test_dir, test_filename)

    # Assert logging messages
    mock_logger_info.assert_called_once_with(
        f"Saving {len(test_data)} to {test_filepath}..."
    )

    # Assert error logging was called
    mock_logger_error.assert_called_once_with(
        f"Error saving to {test_filepath}: {error_msg}", exc_info=True
    )


@mock.patch("lec02.hw.job1.dal.local_disk.open", mock.mock_open())
@mock.patch("lec02.hw.job1.dal.local_disk.json.dump")
@mock.patch("lec02.hw.job1.dal.local_disk.os.path.join")
@mock.patch("lec02.hw.job1.dal.local_disk.logger.info")
@mock.patch("lec02.hw.job1.dal.local_disk.logger.exception")
def test_save_page_to_disk_unexpected_error(
    mock_logger_exception, mock_logger_info, mock_os_path_join, mock_json_dump
):
    """Test save_page_to_disk function behavior when an unexpected exception occurs."""

    # Setup test data
    test_data = [{"id": 1, "name": "Test Item", "price": 100}]
    test_dir = "test/directory"
    test_filename = "test_file.json"
    test_filepath = f"{test_dir}/{test_filename}"

    # Configure mock behavior
    mock_os_path_join.return_value = test_filepath
    error_msg = "Unexpected error occurred"
    mock_json_dump.side_effect = Exception(error_msg)

    # Test that the function raises Exception
    with pytest.raises(Exception) as excinfo:
        save_page_to_disk(test_data, test_dir, test_filename)

    # Assert error message contains expected information
    assert f"An unexpected error occurred: {error_msg}" in str(excinfo.value)

    # Assert path joining was called with correct parameters
    mock_os_path_join.assert_called_once_with(test_dir, test_filename)

    # Assert logging messages
    mock_logger_info.assert_called_once_with(
        f"Saving {len(test_data)} to {test_filepath}..."
    )

    # Assert exception logging was called
    mock_logger_exception.assert_called_once_with(
        f"An unexpected error occurred: {error_msg}"
    )
