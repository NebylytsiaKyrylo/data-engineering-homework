import logging
import os

from lec02.hw.job2.dal.file_io import read_json_file, write_avro_file, SALES_AVRO_SCHEMA

# Get a logger specific to this module
logger = logging.getLogger(__name__)


# Process sales data function
def process_sales_data(raw_dir: str, stg_dir: str) -> None:
    """
    Process sales data files from JSON format to AVRO format.

    Args:
        raw_dir (str): Source directory containing JSON files
        stg_dir (str): Target directory for converted AVRO files

    Raises:
        FileNotFoundError: If raw_dir does not exist
        OSError: If stg_dir cannot be created
        Exception: For other unexpected errors
    """
    logger.info(f"Processing sales data from {raw_dir} and saving to {stg_dir}...")

    # Validate source directory exists
    if not os.path.isdir(raw_dir):
        logger.error(f"Raw directory {raw_dir} does not exist or is not a directory.")
        raise FileNotFoundError(
            f"Raw directory {raw_dir} does not exist or is not a " f"directory."
        )

    # Create target directory if needed
    try:
        os.makedirs(stg_dir, exist_ok=True)
        logger.info(f"Created directory {stg_dir} if it did not exist.")
    except OSError as e:
        logger.error(f"Error creating directory {stg_dir}: {e}", exc_info=True)
        raise OSError(f"Error creating directory {stg_dir}: {e}") from e

    # Initialize counters for processed files and records
    files_processed_count = 0
    total_records_processed = 0

    try:
        # Process each JSON file in source directory
        for filename in os.listdir(raw_dir):
            if filename.lower().endswith(".json"):
                input_filepath = os.path.join(raw_dir, filename)
                logger.info(f"Processing file {input_filepath}...")

                # Read JSON file content
                try:
                    page_data = read_json_file(input_filepath)
                    logger.info(f"File {input_filepath} read successfully.")
                except (FileNotFoundError, ValueError) as e:
                    logger.error(
                        f"Error reading file {input_filepath}: " f"{e}", exc_info=True
                    )
                    raise
                except Exception as e:
                    logger.exception(f"An unexpected error occurred: {e}")
                    raise Exception(f"An unexpected error occurred: {e}") from e

                # Generate output file path
                output_filename = filename.replace(".json", ".avro")
                output_filepath = os.path.join(stg_dir, output_filename)

                # Write data to AVRO file
                try:
                    write_avro_file(
                        page_data=page_data,
                        schema=SALES_AVRO_SCHEMA,
                        filepath=output_filepath,
                    )
                    logger.info(f"File {output_filepath} saved successfully.")
                    files_processed_count += 1
                    total_records_processed += len(page_data)
                    logger.info(
                        f"Processed {total_records_processed} records "
                        f"from file {input_filepath}."
                    )
                except (IOError, TypeError, Exception) as e:
                    logger.error(
                        f"Error saving file {output_filepath}: " f"{e}", exc_info=True
                    )
                    raise

            else:
                logger.info(f"Skipping file {filename} as it is not a JSON file.")

        # Log final processing statistics
        logger.info(
            f"Processed {files_processed_count} files with "
            f"{total_records_processed} records."
        )

    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
        raise Exception(f"An unexpected error occurred: {e}") from e
