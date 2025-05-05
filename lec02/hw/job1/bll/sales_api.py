import logging

from lec02.hw.job1.dal import sales_api
from lec02.hw.job1.dal import local_disk


# Configure logging to output to the console (stdout/stderr)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    # filename="sales_api.log",
    # filemode="a",
    datefmt="%Y-%m-%d %H:%M:%S",
)


# Get a logger specific to this module
logger = logging.getLogger(__name__)


def save_sales_to_local_disk(date: str, raw_dir: str) -> None:
    """
    Save sales data for a specific date to local disk by fetching pages from API.

    Args:
        date (str): The date to fetch sales data for (format: YYYY-MM-DD)
        raw_dir (str): Directory path where files will be saved

    Raises:
        ValueError: If input parameters are invalid
        ConnectionError: If API communication fails
        OSError: If file/directory operations fail
        IOError: If writing to disk fails
        TypeError: If data types are incorrect
    """
    logger.info(f"Saving sales data for {date} to local disk.")

    try:
        # Create storage directory if it doesn't exist
        logger.info(f"Preparing storage directory {raw_dir}...")
        local_disk.prepare_storage_dir(dir_path=raw_dir)
        logger.info(f"Storage directory {raw_dir} created successfully.")

        page = 1
        total_records_saved = 0

        # Fetch and save pages until no more data
        while True:
            logger.info(f"Processing page {page}...")
            page_data = sales_api.get_sales_per_page(date=date, page=page)

            # Exit loop if no more data
            if page_data is None:
                logger.info(f"Page {page} is empty, no more data to save.")
                break

            # Generate filename for current page
            filename = f"sales_{date}_{page}.json"

            # Save page data to disk
            logger.info(f"Saving page {page} to {filename}...")
            local_disk.save_page_to_disk(page_data, dir_path=raw_dir, filename=filename)
            total_records_saved += len(page_data)
            logger.info(
                f"Page {page} with {total_records_saved} records "
                f"saved to {filename}."
            )
            page += 1

        logger.info(f"All pages processed. Saved {total_records_saved} records.")
    except (ValueError, ConnectionError, OSError, IOError, TypeError) as e:
        # Handle expected errors
        logger.error(f"An error occurred while saving data: {e}")
        raise
    except Exception as e:
        # Handle unexpected errors
        logger.exception(f"An unexpected error occurred: {e}")
        raise
