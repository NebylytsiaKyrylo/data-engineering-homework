from unittest import mock
import pytest

from lec02.hw.job1.bll.sales_api import save_sales_to_local_disk


@mock.patch("lec02.hw.job1.bll.sales_api.local_disk.prepare_storage_dir")
@mock.patch("lec02.hw.job1.bll.sales_api.sales_api.get_sales_per_page")
@mock.patch("lec02.hw.job1.bll.sales_api.local_disk.save_page_to_disk")
@mock.patch("lec02.hw.job1.bll.sales_api.logger.info")
def test_save_sales_to_local_disk_success_multiple_pages(
    mock_logger_info, mock_save_page_to_disk, mock_get_sales_per_page, mock_prepare_storage_dir
):
    """Test save_sales_to_local_disk function behavior with multiple pages of data."""
    
    # Setup test parameters
    test_date = "2024-05-07"
    test_dir = "test/raw/dir"
    
    # Configure mock behavior for multiple pages of data
    page1_data = [
        {"client": "Test Client 1", "purchase_date": test_date, "product": "Product A", "price": 100},
        {"client": "Test Client 2", "purchase_date": test_date, "product": "Product B", "price": 200},
    ]
    page2_data = [
        {"client": "Test Client 3", "purchase_date": test_date, "product": "Product C", "price": 300},
    ]
    
    # Configure mock to return data for first two pages, then None (end of data)
    mock_get_sales_per_page.side_effect = [page1_data, page2_data, None]
    
    # Call function under test
    save_sales_to_local_disk(date=test_date, raw_dir=test_dir)
    
    # Assert prepare_storage_dir was called once with correct parameters
    mock_prepare_storage_dir.assert_called_once_with(dir_path=test_dir)
    
    # Assert get_sales_per_page was called for each page
    assert mock_get_sales_per_page.call_count == 3
    mock_get_sales_per_page.assert_has_calls([
        mock.call(date=test_date, page=1),
        mock.call(date=test_date, page=2),
        mock.call(date=test_date, page=3),
    ])
    
    # Assert save_page_to_disk was called for each page with data
    assert mock_save_page_to_disk.call_count == 2
    mock_save_page_to_disk.assert_has_calls([
        mock.call(page1_data, dir_path=test_dir, filename=f"sales_{test_date}_1.json"),
        mock.call(page2_data, dir_path=test_dir, filename=f"sales_{test_date}_2.json"),
    ])
    
    # Assert appropriate log messages were recorded
    assert mock_logger_info.call_count >= 8  # At least 8 log messages should be recorded


@mock.patch("lec02.hw.job1.bll.sales_api.local_disk.prepare_storage_dir")
@mock.patch("lec02.hw.job1.bll.sales_api.sales_api.get_sales_per_page")
@mock.patch("lec02.hw.job1.bll.sales_api.local_disk.save_page_to_disk")
@mock.patch("lec02.hw.job1.bll.sales_api.logger.info")
def test_save_sales_to_local_disk_no_data(
    mock_logger_info, mock_save_page_to_disk, mock_get_sales_per_page, mock_prepare_storage_dir
):
    """Test save_sales_to_local_disk function behavior when no data is available."""
    
    # Setup test parameters
    test_date = "2024-05-07"
    test_dir = "test/raw/dir"
    
    # Configure mock to return None (no data)
    mock_get_sales_per_page.return_value = None
    
    # Call function under test
    save_sales_to_local_disk(date=test_date, raw_dir=test_dir)
    
    # Assert prepare_storage_dir was called once with correct parameters
    mock_prepare_storage_dir.assert_called_once_with(dir_path=test_dir)
    
    # Assert get_sales_per_page was called once
    mock_get_sales_per_page.assert_called_once_with(date=test_date, page=1)
    
    # Assert save_page_to_disk was not called
    mock_save_page_to_disk.assert_not_called()
    
    # Assert appropriate log messages were recorded
    mock_logger_info.assert_any_call(f"Page 1 is empty, no more data to save.")
    mock_logger_info.assert_any_call("All pages processed. Saved 0 records.")


@mock.patch("lec02.hw.job1.bll.sales_api.local_disk.prepare_storage_dir")
@mock.patch("lec02.hw.job1.bll.sales_api.sales_api.get_sales_per_page")
@mock.patch("lec02.hw.job1.bll.sales_api.local_disk.save_page_to_disk")
@mock.patch("lec02.hw.job1.bll.sales_api.logger.info")
@mock.patch("lec02.hw.job1.bll.sales_api.logger.error")
def test_save_sales_to_local_disk_prepare_storage_dir_error(
    mock_logger_error, mock_logger_info, mock_save_page_to_disk, 
    mock_get_sales_per_page, mock_prepare_storage_dir
):
    """Test save_sales_to_local_disk function behavior when prepare_storage_dir raises an error."""
    
    # Setup test parameters
    test_date = "2024-05-07"
    test_dir = "test/raw/dir"
    
    # Configure mock to raise OSError
    error_msg = "Permission denied"
    mock_prepare_storage_dir.side_effect = OSError(error_msg)
    
    # Test that the function raises OSError
    with pytest.raises(OSError) as excinfo:
        save_sales_to_local_disk(date=test_date, raw_dir=test_dir)
    
    # Assert error message contains expected information
    assert error_msg in str(excinfo.value)
    
    # Assert prepare_storage_dir was called once with correct parameters
    mock_prepare_storage_dir.assert_called_once_with(dir_path=test_dir)
    
    # Assert get_sales_per_page was not called
    mock_get_sales_per_page.assert_not_called()
    
    # Assert save_page_to_disk was not called
    mock_save_page_to_disk.assert_not_called()
    
    # Assert error logging was called
    mock_logger_error.assert_called_once_with(f"An error occurred while saving data: {error_msg}")


@mock.patch("lec02.hw.job1.bll.sales_api.local_disk.prepare_storage_dir")
@mock.patch("lec02.hw.job1.bll.sales_api.sales_api.get_sales_per_page")
@mock.patch("lec02.hw.job1.bll.sales_api.local_disk.save_page_to_disk")
@mock.patch("lec02.hw.job1.bll.sales_api.logger.info")
@mock.patch("lec02.hw.job1.bll.sales_api.logger.error")
def test_save_sales_to_local_disk_get_sales_per_page_error(
    mock_logger_error, mock_logger_info, mock_save_page_to_disk, 
    mock_get_sales_per_page, mock_prepare_storage_dir
):
    """Test save_sales_to_local_disk function behavior when get_sales_per_page raises an error."""
    
    # Setup test parameters
    test_date = "2024-05-07"
    test_dir = "test/raw/dir"
    
    # Configure mock to raise ConnectionError
    error_msg = "Failed to connect to API"
    mock_get_sales_per_page.side_effect = ConnectionError(error_msg)
    
    # Test that the function raises ConnectionError
    with pytest.raises(ConnectionError) as excinfo:
        save_sales_to_local_disk(date=test_date, raw_dir=test_dir)
    
    # Assert error message contains expected information
    assert error_msg in str(excinfo.value)
    
    # Assert prepare_storage_dir was called once with correct parameters
    mock_prepare_storage_dir.assert_called_once_with(dir_path=test_dir)
    
    # Assert get_sales_per_page was called once
    mock_get_sales_per_page.assert_called_once_with(date=test_date, page=1)
    
    # Assert save_page_to_disk was not called
    mock_save_page_to_disk.assert_not_called()
    
    # Assert error logging was called
    mock_logger_error.assert_called_once_with(f"An error occurred while saving data: {error_msg}")


@mock.patch("lec02.hw.job1.bll.sales_api.local_disk.prepare_storage_dir")
@mock.patch("lec02.hw.job1.bll.sales_api.sales_api.get_sales_per_page")
@mock.patch("lec02.hw.job1.bll.sales_api.local_disk.save_page_to_disk")
@mock.patch("lec02.hw.job1.bll.sales_api.logger.info")
@mock.patch("lec02.hw.job1.bll.sales_api.logger.error")
def test_save_sales_to_local_disk_save_page_to_disk_error(
    mock_logger_error, mock_logger_info, mock_save_page_to_disk, 
    mock_get_sales_per_page, mock_prepare_storage_dir
):
    """Test save_sales_to_local_disk function behavior when save_page_to_disk raises an error."""
    
    # Setup test parameters
    test_date = "2024-05-07"
    test_dir = "test/raw/dir"
    
    # Configure mock behavior
    page_data = [
        {"client": "Test Client", "purchase_date": test_date, "product": "Product A", "price": 100},
    ]
    mock_get_sales_per_page.return_value = page_data
    
    # Configure mock to raise IOError
    error_msg = "Disk full"
    mock_save_page_to_disk.side_effect = IOError(error_msg)
    
    # Test that the function raises IOError
    with pytest.raises(IOError) as excinfo:
        save_sales_to_local_disk(date=test_date, raw_dir=test_dir)
    
    # Assert error message contains expected information
    assert error_msg in str(excinfo.value)
    
    # Assert prepare_storage_dir was called once with correct parameters
    mock_prepare_storage_dir.assert_called_once_with(dir_path=test_dir)
    
    # Assert get_sales_per_page was called once
    mock_get_sales_per_page.assert_called_once_with(date=test_date, page=1)
    
    # Assert save_page_to_disk was called once
    mock_save_page_to_disk.assert_called_once_with(
        page_data, dir_path=test_dir, filename=f"sales_{test_date}_1.json"
    )
    
    # Assert error logging was called
    mock_logger_error.assert_called_once_with(f"An error occurred while saving data: {error_msg}")


@mock.patch("lec02.hw.job1.bll.sales_api.local_disk.prepare_storage_dir")
@mock.patch("lec02.hw.job1.bll.sales_api.sales_api.get_sales_per_page")
@mock.patch("lec02.hw.job1.bll.sales_api.local_disk.save_page_to_disk")
@mock.patch("lec02.hw.job1.bll.sales_api.logger.info")
@mock.patch("lec02.hw.job1.bll.sales_api.logger.exception")
def test_save_sales_to_local_disk_unexpected_error(
    mock_logger_exception, mock_logger_info, mock_save_page_to_disk, 
    mock_get_sales_per_page, mock_prepare_storage_dir
):
    """Test save_sales_to_local_disk function behavior when an unexpected exception occurs."""
    
    # Setup test parameters
    test_date = "2024-05-07"
    test_dir = "test/raw/dir"
    
    # Configure mock to raise an unexpected exception
    error_msg = "Unexpected error"
    mock_prepare_storage_dir.side_effect = Exception(error_msg)
    
    # Test that the function raises Exception
    with pytest.raises(Exception) as excinfo:
        save_sales_to_local_disk(date=test_date, raw_dir=test_dir)
    
    # Assert error message contains expected information
    assert error_msg in str(excinfo.value)
    
    # Assert prepare_storage_dir was called once with correct parameters
    mock_prepare_storage_dir.assert_called_once_with(dir_path=test_dir)
    
    # Assert get_sales_per_page was not called
    mock_get_sales_per_page.assert_not_called()
    
    # Assert save_page_to_disk was not called
    mock_save_page_to_disk.assert_not_called()
    
    # Assert exception logging was called
    mock_logger_exception.assert_called_once_with(f"An unexpected error occurred: {error_msg}")