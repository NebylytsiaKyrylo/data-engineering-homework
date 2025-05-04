# Importing built-in modules
import json
import os
import logging
import time
import pprint

# Importing third party modules
from dotenv import load_dotenv
import requests
from typing import Any, Dict, List, Optional

# Load environment variables
load_dotenv()

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

# Get authentication token from environment variables
ENV_AUTH_TOKEN = "AUTH_TOKEN"
ERR_TOKEN_MISSING: str = "AUTH_TOKEN is not set"
AUTH_TOKEN = os.environ.get(ENV_AUTH_TOKEN)

# API URL Configuration
BASE_URL: str = "https://fake-api-vycpfa6oca-uc.a.run.app"  # Base API URL
ENDPOINT_SALES: str = "/sales"  # Sales data endpoint
API_URL: str = BASE_URL + ENDPOINT_SALES  # Complete API URL

# Retry configuration
MAX_RETRIES: int = 3
INITIAL_DELAY: float = 1.0
BACKOFF_FACTOR: float = 2.0
RETRY_STATUS_CODES: set[int] = {500, 502, 503, 504}


def get_sales_per_page(date: str, page: int) -> List[Dict[str, Any]] | None:
    """
    Fetches a single page of sales data from the API.

    :param date: The date for which to fetch data.
    :param page: The page number to fetch.
    :param headers: The request headers include authorization.
    :return: A list of sales data dictionaries for the page, or None if the page indicates the end of data.
    :raises ValueError: If the API response is not a list.
    :raises ConnectionError: For network-related errors or non-404 HTTP errors.
    """

    if not AUTH_TOKEN:
        logger.error(ERR_TOKEN_MISSING)
        raise ValueError(ERR_TOKEN_MISSING)

    headers: Dict[str, str] = {"Authorization": AUTH_TOKEN}
    params: Dict[str, str] = {"page": str(page), "date": date}
    last_exception: Exception | None = None

    for attempt in range(MAX_RETRIES):

        try:
            response = requests.get(API_URL, headers=headers, params=params, timeout=20)

            if response.status_code == 404:
                logger.warning(f"Page {page} not found, assuming end of data.")
                return None  # Signal end of data based on 404

            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            page_data: List[Dict[str, Any]] = response.json()

            if not isinstance(page_data, list):
                logger.error(f"Response is not a list for page {page}")
                raise ValueError(f"Response is not a list for page {page}")

            if not page_data:
                logger.info(f"Page {page} is empty, assuming end of data.")
                return None  # Signal end of data

            return page_data

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            last_exception = e  # Save exception for logging
            logger.warning(
                f"Network error on attempt {attempt + 1}/{MAX_RETRIES} for page {page}: {e}. Retrying..."
            )

        except requests.exceptions.HTTPError as e:
            last_exception = e  # Save exception for logging

            if response and response.status_code in RETRY_STATUS_CODES:
                logger.warning(
                    f"HTTP error {response.status_code} on attempt {attempt + 1}/{MAX_RETRIES} for page {page}. Retrying..."
                )
            else:
                # Non-retryable HTTP error (e.g., 400, 401, 403, 501)
                logger.error(
                    f"Non-retryable HTTP error occurred for page {page}: "
                    f"{response.status_code} {response.reason if response else 'N/A'}"
                )
                raise ConnectionError(
                    f"Non-retryable HTTP error fetching page {page}: {e}"
                ) from e

        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error occurred for page {page}: {e}")
            raise ValueError(f"JSON decode error fetching page {page}: {e}") from e
        except Exception as e:
            # Catch any other unexpected errors during page fetch
            logger.exception(f"Unexpected error occurred fetching page {page}: {e}")
            raise  # Re-raise unexpected errors

        if attempt < MAX_RETRIES - 1:
            delay = INITIAL_DELAY * (BACKOFF_FACTOR**attempt)
            logger.info(f"Waiting {delay:.2f} seconds before next retry...")
            time.sleep(delay)

    # If we reach this point, all retries have failed
    logger.error(f"Max retries ({MAX_RETRIES}) reached for page {page}. Failing.")
    # We raise a ConnectionError with the last exception to provide more context
    raise ConnectionError(
        f"Failed to fetch page {page} after {MAX_RETRIES} attempts. Last error: {last_exception}"
    ) from last_exception


def get_all_sales_aggregated(date: str) -> List[Dict[str, Any]]:
    """
    DEPRECATED for page-by-page saving. Kept for reference/learning.
    Fetches ALL sales data for a given date, aggregating results in memory.
    WARNING: Can consume a lot of memory for large datasets.

    :param date: The date of the sales data to retrieve (e.g., "YYYY-MM-DD").
    :return: A list of all sales records for the date.
    :raises ValueError: If AUTH_TOKEN is not set or the API response format is invalid.
    :raises ConnectionError: If network or unexpected API errors occur during fetching.
    """

    all_sales_data: List[Dict[str, Any]] = []
    page: int = 1

    while True:
        logger.info(f"Fetching page {page} for date {date}...")
        page_data = get_sales_per_page(date=date, page=page)

        if page_data is None:  # End of data signal (empty page or 404)
            break

        all_sales_data.extend(page_data)
        page += 1

    logger.info(f"Successfully fetched {len(all_sales_data)} records for date {date}.")
    return all_sales_data


# Main function for testing the API
if __name__ == "__main__":
    if not AUTH_TOKEN:
        logger.error(ERR_TOKEN_MISSING)
        raise ValueError(ERR_TOKEN_MISSING)
    else:
        try:
            # Set an example target date
            target_date = "2022-08-09"
            page = 4
            num_records_to_display = 50

            # Fetch sales data for the target date per page
            page_data = get_sales_per_page(date=target_date, page=page)

            if page_data is not None:
                print(
                    f"{min(num_records_to_display, len(page_data))} records for {target_date}:"
                )
                pprint.pprint(page_data[:num_records_to_display])
            else:
                print(f"There is no more data for {target_date} and page {page}")

            # Get sales data for the target date
            # all_sales_per_data = get_all_sales_aggregated(target_date)
            # Print the first 100 records (or fewer if less data)
            # print(
            #     f"First {min(num_records_to_display, len(all_sales_per_data))} records for {
            #     target_date}:"
            # )
            # pprint.pprint(all_sales_per_data[:num_records_to_display])

        except (ValueError, ConnectionError) as e:
            # Log expected errors during execution
            logger.error(f"Failed to get sales data: {e}")
        except Exception as e:
            # Log unexpected errors during execution
            logger.exception(f"An unexpected error occurred: {e}")
