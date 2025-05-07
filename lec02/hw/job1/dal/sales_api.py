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

# Get a logger specific to this module
logger = logging.getLogger(__name__)

# Get authentication token from environment variables
ENV_AUTH_TOKEN = "AUTH_TOKEN"
ERR_TOKEN_MISSING: str = "AUTH_TOKEN is not set"
AUTH_TOKEN = os.environ.get(ENV_AUTH_TOKEN)

# API URL Configuration
BASE_URL: str = "https://fake-api-vycpfa6oca-uc.a.run.app"
ENDPOINT_SALES: str = "/sales"
API_URL: str = BASE_URL + ENDPOINT_SALES

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

        except (ValueError, ConnectionError) as e:
            # Log expected errors during execution
            logger.error(f"Failed to get sales data: {e}")
        except Exception as e:
            # Log unexpected errors during execution
            logger.exception(f"An unexpected error occurred: {e}")
