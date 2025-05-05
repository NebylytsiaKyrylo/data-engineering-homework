# Import necessary modules
import logging
from typing import Any, Dict, Tuple
from flask import Flask, request
from dotenv import load_dotenv
from lec02.hw.job1.bll.sales_api import save_sales_to_local_disk

# Load environment variables from .env file
load_dotenv()

# Configure logging to output to the console (stdout/stderr)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    # filename="sales_api.log", # Uncomment to log to file
    # filemode="a", # Append mode for a log file
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Get a logger specific to this module
logger = logging.getLogger(__name__)

# Initialize Flask application
app = Flask(__name__)


# Define endpoint that accepts POST requests at root URL
@app.route("/", methods=["POST"])
def run_job_endpoint() -> Tuple[Dict[str, Any], int]:
    """
    Process POST request to run sales data collection job.

    Returns:
        Tuple containing response dict and HTTP status code
    """
    # Log receipt of request
    logger.info("Received request to run job.")

    # Get JSON data from request
    input_data = request.get_json()

    # Validate input data exists
    if not input_data:
        logger.error("No input data received.")
        return {"error": "No input data received."}, 400

    # Extract required parameters
    date = input_data.get("date")
    raw_dir = input_data.get("raw_dir")

    # Validate date parameter
    if not date:
        logger.error("Missing 'date' parameter in input data.")
        return {"error": "Missing 'date' parameter in input data."}, 400

    # Validate raw_dir parameter
    if not raw_dir:
        logger.error("Missing 'raw_dir' parameter in input data.")
        return {"error": "Missing 'raw_dir' parameter in input data."}, 400

    # Log job execution details
    logger.info(f"Running job for date {date} and saving to {raw_dir}.")

    try:
        # Execute job to save sales data
        logger.info(">>> Running job...")
        save_sales_to_local_disk(date=date, raw_dir=raw_dir)
        logger.info(">>> Job completed successfully.")
        return {"message": "Job completed successfully."}, 201

    # Handle expected errors
    except (ValueError, ConnectionError, OSError, IOError, TypeError) as e:
        logger.error(f"An error occurred while running job: {e}", exc_info=True)
        return {"error": f"An error occurred while running job: {e}"}, 500

    # Handle unexpected errors
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}", exc_info=True)
        return {"error": f"An unexpected error occurred: {e}"}, 500
