# Importing built-in modules
import json
import os
import logging

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


def _fetch_page_data(
    date: str, page: int, headers: Dict[str, str]
) -> List[Dict[str, Any]] | None:
    """
    Fetches a single page of sales data from the API.

    :param date: The date for which to fetch data.
    :param page: The page number to fetch.
    :param headers: The request headers including authorization.
    :return: A list of sales data dictionaries for the page, or None if the page indicates the end of data.
    :raises ValueError: If the API response is not a list.
    :raises ConnectionError: For network-related errors or non-404 HTTP errors.
    """
    params: Dict[str, str] = {"page": str(page), "date": date}
    try:
        response = requests.get(API_URL, headers=headers, params=params)

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

    except requests.exceptions.HTTPError as e:
        logger.error(
            f"HTTP error occurred for page {page}: "
            f"{e.response.status_code} {e.response.reason}"
        )
        raise ConnectionError(f"HTTP error fetching page {page}: {e}") from e
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error occurred for page {page}: {e}")
        raise ConnectionError(f"Request error fetching page {page}: {e}") from e
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error occurred for page {page}: {e}")
        raise ValueError(f"JSON decode error fetching page {page}: {e}") from e
    except Exception as e:
        # Catch any other unexpected errors during page fetch
        logger.exception(f"Unexpected error occurred fetching page {page}: {e}")
        raise  # Re-raise unexpected errors


def get_sales(date: str) -> List[Dict[str, Any]]:
    """
    Fetches all sales data for a given date from the API, handling pagination.

    :param date: The date of the sales data to retrieve (e.g., "YYYY-MM-DD").
    :return: A list of dictionaries, each representing a sales record.
    :raises ValueError: If AUTH_TOKEN is not set or the API response format is invalid.
    :raises ConnectionError: If network or unexpected API errors occur during fetching.
    """
    if not AUTH_TOKEN:
        logger.error(ERR_TOKEN_MISSING)
        raise ValueError(ERR_TOKEN_MISSING)

    all_sales_data: List[Dict[str, Any]] = []
    page: int = 1
    headers: Dict[str, str] = {"Authorization": AUTH_TOKEN}

    while True:
        logger.info(f"Fetching page {page} for date {date}...")
        page_data = _fetch_page_data(date, page, headers)

        if page_data is None:  # End of data signal (empty page or 404)
            break

        all_sales_data.extend(page_data)
        page += 1

    logger.info(f"Successfully fetched {len(all_sales_data)} records for date {date}.")
    return all_sales_data


if __name__ == "__main__":
    try:
        # Set an example target date
        target_date = "2022-08-09"
        # Get sales data for the target date
        sales_data = get_sales(target_date)
        # Print first 100 records (or fewer if less data)
        print(f"First {min(100, len(sales_data))} records for {target_date}:")
        for record in sales_data[:100]:
            print(record)
    except (ValueError, ConnectionError) as e:
        # Log expected errors during execution
        logger.error(f"Failed to get sales data: {e}")
    except Exception as e:
        # Log unexpected errors during execution
        logger.exception(f"An unexpected error occurred: {e}")
