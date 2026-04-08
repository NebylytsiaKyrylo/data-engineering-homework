import logging
import re
from typing import Any, Dict, Tuple
from flask import Flask, request
from dotenv import load_dotenv
from job1.bll.sales_api import save_sales_to_local_disk

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

# Initialize Flask application
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 1 * 1024 * 1024  # 1 MB max request size


def validate_date_format(date_str: str) -> bool:
    """Validate that date is in YYYY-MM-DD format."""
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", date_str))


def validate_request_input(input_data: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Validate incoming request data.
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not input_data:
        return False, "No input data received."
    
    date = input_data.get("date")
    raw_dir = input_data.get("raw_dir")
    
    if date is None:
        return False, "Missing 'date' parameter in input data."

    if not isinstance(date, str):
        return False, "Invalid 'date' parameter. Must be a string."

    if not validate_date_format(date):
        return False, "Invalid date format. Expected YYYY-MM-DD."

    if raw_dir is None:
        return False, "Missing 'raw_dir' parameter in input data."

    if not isinstance(raw_dir, str):
        return False, "Invalid 'raw_dir' parameter. Must be a string."
    
    if not raw_dir.strip():
        return False, "'raw_dir' cannot be empty or whitespace."
    
    return True, ""


@app.route("/", methods=["POST"])
def run_job_endpoint() -> Tuple[Dict[str, Any], int]:
    """
    Process POST request to run sales data collection job.
    
    Expected JSON payload:
        {
            "date": "YYYY-MM-DD",
            "raw_dir": "/path/to/directory"
        }
    
    Returns:
        Tuple of (response_dict, http_status_code)
    """
    logger.info("Received POST request to run job.")
    
    # Validate request has JSON content
    if not request.is_json:
        logger.warning("Request missing 'Content-Type: application/json' header.")
        return {"error": "Request must be JSON"}, 400
    
    # Get and validate input data
    input_data = request.get_json()
    is_valid, error_msg = validate_request_input(input_data)
    
    if not is_valid:
        logger.error(f"Invalid request: {error_msg}")
        return {"error": error_msg}, 400
    
    date = input_data["date"]
    raw_dir = input_data["raw_dir"]
    
    logger.info(f"Processing job for date={date}, raw_dir={raw_dir}")
    
    try:
        save_sales_to_local_disk(date=date, raw_dir=raw_dir)
        logger.info("Job completed successfully.")
        return {"message": "Job completed successfully."}, 201
    
    except (ValueError, ConnectionError, OSError, TypeError) as e:
        logger.error(f"An error occurred while running job: {e}", exc_info=True)
        return {"error": f"An error occurred while running job: {e}"}, 500

    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
        return {"error": f"An unexpected error occurred: {e}"}, 500


if __name__ == "__main__":
    # Use debug=False in production
    # Set environment variable FLASK_ENV=development for debug mode
    debug_mode = False
    
    logger.info(f"Starting Flask server on 0.0.0.0:8081 (debug={debug_mode})")
    app.run(debug=debug_mode, host="0.0.0.0", port=8081, use_reloader=False)
