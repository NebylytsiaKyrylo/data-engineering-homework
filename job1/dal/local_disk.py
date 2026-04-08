import os
import shutil
import logging
import json
from typing import Any, Dict, List


# Get a logger specific to this module
logger = logging.getLogger(__name__)


def prepare_storage_dir(dir_path: str) -> None:
    """This function handles directory preparation for storing files:
    1. If the directory exists already:
       - Logs info message
       - Removes existing directory and all contents
    2. If the directory doesn't exist:
       - Logs info message
    3. Creates a new empty directory

    Args:
        dir_path (str): Path to the directory that needs to be prepared

    Returns:
        None

    Raises:
        ValueError: If dir_path is invalid
        OSError: If there are permission/filesystem issues
    """
    if not dir_path or not isinstance(dir_path, str):
        raise ValueError(f"Invalid directory path: {dir_path}")

    try:
        if os.path.exists(dir_path):
            logger.info(f"Directory {dir_path} already exists. Removing...")
            shutil.rmtree(dir_path)
        else:
            logger.info(f"Directory {dir_path} does not exist. Creating...")

        os.makedirs(dir_path)
        logger.info(f"Directory {dir_path} created successfully.")

    except OSError as e:
        logger.error(f"Error with directory {dir_path}: {e}", exc_info=True)
        raise OSError(f"Error with directory {dir_path}: {e}") from e


def save_page_to_disk(
    page_data: List[Dict[str, Any]], dir_path: str, filename: str
) -> None:
    """Function that saves page data to disk as a JSON file

    Args:
        page_data: List of dictionaries containing the page data to save
        dir_path: Directory path where to save the file
        filename: Name of the file to create

    Returns:
        None

    Raises:
        ValueError: If input parameters are invalid
        OSError: If there are I/O errors while saving the file
        TypeError: If the data is not serializable to JSON
    """
    if not isinstance(page_data, list):
        raise TypeError(f"page_data must be a list, got {type(page_data)}")
    
    if not dir_path or not isinstance(dir_path, str):
        raise ValueError(f"Invalid directory path: {dir_path}")
    
    if not filename or not isinstance(filename, str):
        raise ValueError(f"Invalid filename: {filename}")

    filepath = os.path.join(dir_path, filename)
    logger.info(f"Saving {len(page_data)} to {filepath}...")

    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(page_data, f, ensure_ascii=False, indent=4)
        logger.info(f"{len(page_data)} records saved to {filepath}.")

    except OSError as e:
        logger.error(f"Error saving to {filepath}: {e}", exc_info=True)
        raise OSError(f"Error saving to {filepath}: {e}") from e

    except TypeError as e:
        logger.error(f"Error saving to {filepath}: {e}", exc_info=True)
        raise TypeError(f"Error saving to {filepath}: {e}") from e

    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
        raise Exception(f"An unexpected error occurred: {e}") from e
