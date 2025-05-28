import json
import logging
from typing import Any, Dict, List
import fastavro


# Get a logger specific to this module
logger: logging.Logger = logging.getLogger(__name__)


# Define schema for AVRO file
SALES_AVRO_SCHEMA: Dict[str, Any] = {
    "type": "record",
    "name": "Sale",
    "namespace": "lec02.hw.job2.avro",
    "fields": [
        {"name": "client", "type": ["string", "null"]},
        {"name": "purchase_date", "type": ["string", "null"]},
        {"name": "product", "type": ["string", "null"]},
        {"name": "price", "type": ["int", "float", "null"]},
    ],
}


def read_json_file(filepath: str) -> List[Dict[str, Any]]:
    """
    Reads and parses a JSON file containing a list of dictionaries.

    Args:
        filepath (str): Path to the JSON file to read

    Returns:
        List[Dict[str, Any]]: List of dictionaries parsed from the JSON file

    Raises:
        FileNotFoundError: If the specified file does not exist
        ValueError: If the JSON data is not a list or cannot be decoded
        Exception: For any other unexpected errors
    """
    logger.info(f"Reading JSON file {filepath}...")

    try:
        # Open and read the JSON file with UTF-8 encoding
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Validate that the parsed data is a list
        if not isinstance(data, list):
            logger.error(f"Data is not a list: {data}")
            raise ValueError(f"Data is not a list: {data}")

        # Log success and return the data
        logger.info(f"JSON file {filepath} read successfully with {len(data)} records.")
        return data

    except FileNotFoundError as e:
        # Handle missing file error
        logger.error(f"File {filepath} not found: {e}")
        raise FileNotFoundError(f"File {filepath} not found: {e}") from e

    except json.JSONDecodeError as e:
        # Handle JSON parsing errors
        logger.error(f"Error decoding JSON from file {filepath}: {e}")
        raise ValueError(f"Error decoding JSON from file {filepath}: {e}") from e

    except Exception as e:
        # Handle any other unexpected errors
        logger.exception(f"An unexpected error occurred: {e}")
        raise Exception(f"An unexpected error occurred: {e}") from e


def write_avro_file(
    page_data: List[Dict[str, Any]], schema: Dict[str, Any], filepath: str
) -> None:
    logger.info(f"Writing {len(page_data)} records to {filepath}...")

    try:
        with open(filepath, "wb") as f:
            fastavro.writer(f, schema, page_data)

        logger.info(f"{len(page_data)} records written to file Avro" f" {filepath}.")

    except IOError as e:
        logger.error(f"Error saving to file Avro {filepath}: {e}", exc_info=True)
        raise IOError(f"Error saving to file Avro {filepath}: {e}") from e
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
        raise Exception(f"An unexpected error occurred: {e}") from e
