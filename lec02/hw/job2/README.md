# Job2: JSON to AVRO Transformation

This directory contains the implementation of Job2, which is responsible for transforming sales data from JSON format to AVRO format.

## Purpose

Job2 serves as the transformation phase of the ETL pipeline. It:
1. Reads JSON files created by Job1 from a specified directory
2. Transforms the data into AVRO format
3. Saves the transformed data as AVRO files in a specified staging directory

## Components

### Main Layer (`main.py`)

The main layer is a Flask application that:
- Exposes an HTTP endpoint at the root URL (`/`)
- Accepts POST requests with JSON payload containing:
  - `raw_dir`: The directory containing the JSON files to process
  - `stg_dir`: The directory where the AVRO files will be saved
- Calls the business logic layer to process the request
- Returns appropriate HTTP responses and status codes

### Business Logic Layer (`bll/`)

The BLL contains the core business logic for processing sales data:

- `process_sales.py`: 
  - Contains the `process_sales_data` function
  - Orchestrates the process of reading JSON files and converting them to AVRO
  - Processes each file in the raw directory
  - Handles directory creation and validation

### Data Access Layer (`dal/`)

The DAL handles all data access operations:

- `file_io.py`:
  - Contains functions for file I/O operations
  - `read_json_file`: Reads and parses JSON files
  - `write_avro_file`: Writes data to AVRO files
  - Defines the AVRO schema for sales data

## AVRO Schema

The application uses the following AVRO schema for sales data:

```json
{
    "type": "record",
    "name": "Sale",
    "namespace": "lec02.hw.job2.avro",
    "fields": [
        {"name": "client", "type": ["string", "null"]},
        {"name": "purchase_date", "type": ["string", "null"]},
        {"name": "product", "type": ["string", "null"]},
        {"name": "price", "type": ["int", "float", "null"]}
    ]
}
```

## Running Job2

### Prerequisites

1. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Ensure that Job1 has been run and has created JSON files in the raw directory.

### Starting the Flask Server

```bash
python main.py
```

This will start the Flask server on port 8082.

### Making a Request

You can make a request to the server using curl:

```bash
curl -X POST http://localhost:8082/ \
  -H "Content-Type: application/json" \
  -d '{"raw_dir": "/path/to/raw/directory", "stg_dir": "/path/to/staging/directory"}'
```

Or using Python requests:

```python
import requests

response = requests.post(
    "http://localhost:8082/",
    json={"raw_dir": "/path/to/raw/directory", "stg_dir": "/path/to/staging/directory"}
)
print(response.json())
```

## Testing

The job includes comprehensive unit tests for all layers. To run the tests:

```bash
pytest
```

This will run all tests in the `tests/` directory, including tests for the main, BLL, and DAL layers.

## Data Flow

1. Job2 reads all JSON files from the raw directory
2. For each JSON file:
   - The file is read and parsed into a list of dictionaries
   - The data is validated against the AVRO schema
   - The data is written to an AVRO file in the staging directory
   - The AVRO file has the same name as the JSON file but with a .avro extension
3. Job2 logs the number of files and records processed