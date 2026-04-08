# Job1: Sales Data Extraction

This directory contains the implementation of Job1, which is responsible for extracting sales data from an external API and saving it as JSON files.

## Purpose

Job1 serves as the extraction phase of the ETL pipeline. It:
1. Connects to an external sales API
2. Fetches sales data for a specific date
3. Processes the data page by page
4. Saves each page as a separate JSON file in a specified directory

## Components

### Main Layer (`main.py`)

The main layer is a Flask application that:
- Exposes an HTTP endpoint at the root URL (`/`)
- Accepts POST requests with JSON payload containing:
  - `date`: The date for which to fetch sales data (format: YYYY-MM-DD)
  - `raw_dir`: The directory where the JSON files will be saved
- Calls the business logic layer to process the request
- Returns appropriate HTTP responses and status codes

### Business Logic Layer (`bll/`)

The BLL contains the core business logic for processing sales data:

- `sales_api.py`: 
  - Contains the `save_sales_to_local_disk` function
  - Orchestrates the process of fetching data from the API and saving it to disk
  - Handles pagination by fetching data page by page until no more data is available

### Data Access Layer (`dal/`)

The DAL handles all data access operations:

- `sales_api.py`:
  - Contains the `get_sales_per_page` function
  - Makes HTTP requests to the external API
  - Implements retry logic for handling transient errors
  - Authenticates with the API using an auth token

- `local_disk.py`:
  - Contains functions for file system operations
  - `prepare_storage_dir`: Creates or cleans the storage directory
  - `save_page_to_disk`: Saves JSON data to a file

## API Interaction

The application interacts with an external API at `https://fake-api-vycpfa6oca-uc.a.run.app/sales` with the following parameters:
- `date`: The date for which to fetch sales data
- `page`: The page number to fetch

Authentication is done using an auth token provided via the `AUTH_TOKEN` environment variable.

## Running Job1

### Prerequisites

1. Set the required environment variables:
   ```bash
   export AUTH_TOKEN=your_api_auth_token
   ```

2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Starting the Flask Server

```bash
python main.py
```

This will start the Flask server on port 8081.

### Making a Request

You can make a request to the server using curl:

```bash
curl -X POST http://localhost:8081/ \
  -H "Content-Type: application/json" \
  -d '{"date": "2022-08-09", "raw_dir": "/path/to/raw/directory"}'
```

Or using Python requests:

```python
import requests

response = requests.post(
    "http://localhost:8081/",
    json={"date": "2022-08-09", "raw_dir": "/path/to/raw/directory"}
)
print(response.json())
```

## Testing

The job includes comprehensive unit tests for all layers. To run the tests:

```bash
pytest
```

This will run all tests in the `tests/` directory, including tests for the main, BLL, and DAL layers.