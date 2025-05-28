import logging
from flask import Flask, request
from dotenv import load_dotenv
from typing import Any, Dict, Tuple


# Load environment variables from .env file
load_dotenv()


# Try importing process_sales_data using an absolute path first
try:
    from lec02.hw.job2.bll.process_sales import process_sales_data
except ImportError:
    logging.error("ImportError: import absolute path to module")
    # Fallback to relative import if absolute import fails
    try:
        from bll.process_sales import process_sales_data
    except ImportError:
        logging.error("ImportError: import module from parent directory")
    # Use critical logger if available, otherwise use print
    log_func = (
        logging.getLogger(__name__).critical
        if logging.getLogger(__name__).hasHandlers()
        else print
    )
    log_func("Critical error: cannot import module")
    exit(1)


# Configure logging to output to the console (stdout/stderr)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    # filename=str(os.path.basename(__file__).replace(".py", ".log")),
    # filemode="a", # Append mode for a log file
    datefmt="%Y-%m-%d %H:%M:%S",
)


# Get a logger specific to this module
logger = logging.getLogger(__name__)

# Initialize Flask application
app = Flask(__name__)


@app.route("/", methods=["POST"])
def run_job2_endpoint() -> Tuple[Dict[str, Any], int] | None:
    """
    Process POST request to run sales data conversion job.

    Returns:
        Tuple containing response dict and HTTP status code
    """
    # Log receipt of request
    logger.info("Received request to run job.")
    input_data = request.get_json()

    # Validate input data exists
    if not input_data:
        logger.error("No input data received.")
        return {"error": "No input data received."}, 400

    # Extract required parameters
    raw_dir = input_data.get("raw_dir")
    stg_dir = input_data.get("stg_dir")

    # Validate raw_dir parameter
    if not raw_dir:
        logger.error("Missing 'raw_dir' parameter in input data.")
        return {"error": "Missing 'raw_dir' parameter in input data."}, 400

    # Validate stg_dir parameter
    if not stg_dir:
        logger.error("Missing 'stg_dir' parameter in input data.")
        return {"error": "Missing 'stg_dir' parameter in input data."}, 400

    # Log job execution details
    logger.info(f"Running job for raw_dir {raw_dir} and stg_dir {stg_dir}.")
    try:
        # Execute a job to process sales data
        process_sales_data(raw_dir=raw_dir, stg_dir=stg_dir)
        logger.info(">>> Job completed successfully.")
        return {"message": "Job completed successfully."}, 201

    # Handle expected errors
    except (
        FileNotFoundError,
        ValueError,
        ConnectionError,
        OSError,
        IOError,
        TypeError,
    ) as e:
        logger.error(f"An error occurred while running job: {e}", exc_info=True)
        return {"error": f"An error occurred while running job: {e}"}, 500

    # Handle unexpected errors
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}", exc_info=True)
        return {"error": f"An unexpected error occurred: {e}"}, 500


# Main function to start the Flask server
if __name__ == "__main__":
    logger.info("Flask server startup for Job 1...")
    app.run(debug=True, host="0.0.0.0", port=8082)
