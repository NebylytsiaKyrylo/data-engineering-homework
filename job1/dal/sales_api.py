# Importing built-in modules
import json
import logging
import os
import time
from typing import Any, Dict, List

# Importing third party modules
from dotenv import load_dotenv
import requests

# Load environment variables
load_dotenv()

# Get a logger specific to this module
logger = logging.getLogger(__name__)

# Get authentication token from environment variables
ENV_AUTH_TOKEN = "AUTH_TOKEN"
ERR_TOKEN_MISSING: str = "AUTH_TOKEN is not set"
AUTH_TOKEN: str | None = os.environ.get(ENV_AUTH_TOKEN)

# API URL Configuration
BASE_URL: str = "https://fake-api-vycpfa6oca-uc.a.run.app"
ENDPOINT_SALES: str = "/sales"
API_URL: str = BASE_URL + ENDPOINT_SALES

# Retry configuration
MAX_RETRIES: int = 3
INITIAL_DELAY: float = 1.0
BACKOFF_FACTOR: float = 2.0
RETRY_STATUS_CODES: set[int] = {500, 502, 503, 504}


def _get_auth_token() -> str:
    """Get authentication token from environment, loading .env if needed."""
    load_dotenv()
    token = os.environ.get(ENV_AUTH_TOKEN)
    if not token:
        raise ValueError(ERR_TOKEN_MISSING)
    return token


def get_sales_per_page(date: str, page: int) -> List[Dict[str, Any]] | None:
    """
    Retrieves paginated sales data for a given date and page number by calling a remote API.

    This function fetches data from a remote API using the specified date and page number.
    It will attempt multiple retries in case of network-related errors or retryable HTTP
    errors. If the requested page is not found or contains no data, it signals the end of
    data by returning None.

    Args:
        date (str): The target date for which sales data is to be fetched (format: YYYY-MM-DD)
        page (int): The page number of the paginated sales data to fetch (must be > 0)

    Returns:
        List[Dict[str, Any]] | None: A list of sales data dictionaries if successful,
            or None if page is empty/not found (end of data)

    Raises:
        ValueError: If auth token is missing, date/page invalid, or response format is invalid
        ConnectionError: If all retry attempts fail or non-retryable HTTP error occurs
    """
    # Input validation
    if not isinstance(date, str) or not date.strip():
        raise ValueError(f"Invalid date: {date}")
    if not isinstance(page, int) or page < 1:
        raise ValueError(f"Invalid page: {page}")

    if not AUTH_TOKEN:
        logger.error(ERR_TOKEN_MISSING)
        raise ValueError(ERR_TOKEN_MISSING)

    headers: Dict[str, str] = {"Authorization": AUTH_TOKEN}
    params: Dict[str, str] = {"page": str(page), "date": date}
    last_exception: Exception | None = None
    response = None

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(API_URL, headers=headers, params=params, timeout=20)

            if response.status_code == 404:
                logger.warning(f"Page {page} not found, assuming end of data.")
                return None

            response.raise_for_status()
            page_data: List[Dict[str, Any]] = response.json()

            if not isinstance(page_data, list):
                logger.error(f"Response is not a list for page {page}")
                raise ValueError(f"Response is not a list for page {page}")

            if not page_data:
                logger.info(f"Page {page} is empty, assuming end of data.")
                return None

            return page_data

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            last_exception = e
            logger.warning(
                f"Network error on attempt {attempt + 1}/{MAX_RETRIES} for page {page}: {e}. Retrying..."
            )

        except requests.exceptions.HTTPError as e:
            last_exception = e
            
            # response est défini à ce stade
            if response.status_code in RETRY_STATUS_CODES:
                logger.warning(
                    f"HTTP error {response.status_code} on attempt {attempt + 1}/{MAX_RETRIES} "
                    f"for page {page}. Retrying..."
                )
            else:
                logger.error(
                    f"Non-retryable HTTP error for page {page}: "
                    f"{response.status_code} {response.reason}"
                )
                raise ConnectionError(
                    f"Non-retryable HTTP error {response.status_code} fetching page {page}: {e}"
                ) from e

        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error for page {page}: {e}")
            raise ValueError(f"JSON decode error fetching page {page}: {e}") from e

        except Exception as e:
            logger.exception(f"Unexpected error fetching page {page}: {e}")
            raise

        # Sleep before retrying (but not after the last attempt)
        if attempt < MAX_RETRIES - 1:
            delay = INITIAL_DELAY * (BACKOFF_FACTOR ** attempt)
            logger.info(f"Waiting {delay:.2f} seconds before next retry...")
            time.sleep(delay)

    # All retries exhausted
    logger.error(f"Max retries ({MAX_RETRIES}) reached for page {page}.")
    raise ConnectionError(
        f"Failed to fetch page {page} after {MAX_RETRIES} attempts. Last error: {last_exception}"
    ) from last_exception


if __name__ == "__main__":
    import pprint

    try:
        target_date = "2022-08-09"
        test_page = 4
        num_records_to_display = 50

        page_data = get_sales_per_page(date=target_date, page=test_page)

        if page_data is not None:
            print(f"{min(num_records_to_display, len(page_data))} records for {target_date}:")
            pprint.pprint(page_data[:num_records_to_display])
        else:
            print(f"No data for {target_date} page {test_page}")

    except (ValueError, ConnectionError) as e:
        logger.error(f"Failed to get sales data: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
