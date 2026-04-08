# Data Engineering ETL Pipeline

A two-stage Extract-Transform-Load (ETL) pipeline for processing sales data using Python, Flask, and AVRO serialization.

## Pipeline Overview

The pipeline consists of two independent jobs that work together:

1. **Job1**: Extracts sales data from an external API and saves it as JSON files
2. **Job2**: Reads the JSON files created by Job1 and transforms them into AVRO format

## Architecture

Both jobs follow a **3-tier layered architecture**:

- **Main Layer** (`main.py`): Flask REST endpoints with request validation and error handling
- **BLL (Business Logic Layer)**: Core business logic and orchestration
- **DAL (Data Access Layer)**: External system integration (API calls, file I/O)

This separation of concerns ensures the code is testable, maintainable, and extensible.

## Project Structure

```
.
├── job1/                      # Extract & Load
│   ├── main.py               # Flask app (port 8081)
│   ├── bll/sales_api.py      # Core logic
│   ├── dal/                  # API client, file I/O
│   └── tests/                # Unit tests
├── job2/                      # Transform JSON to AVRO
│   ├── main.py               # Flask app (port 8082)
│   ├── bll/process_sales.py  # Core logic
│   ├── dal/file_io.py        # JSON/AVRO operations
│   └── tests/                # Unit tests
├── scripts/
│   └── check_jobs.py         # End-to-end pipeline runner
├── requirements.txt
├── .env.example
└── README.md
```

## Setup

### Requirements

- Python 3.13+
- pip

### Installation

1. Clone the repository
   ```bash
   git clone <repo-url>
   cd data-engineering-homework
   ```

2. Create a virtual environment
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. Install dependencies
   ```bash
   pip install -r requirements.txt
   ```

4. Configure environment variables
   ```bash
   cp .env.example .env
   # Edit .env and add your AUTH_TOKEN
   ```

## Running the Pipeline

### Job1: Extract Sales Data

```bash
# Start Job1 Flask app (runs on port 8081)
python job1/main.py
```

Example request:

```bash
curl -X POST http://localhost:8081/ \
  -H "Content-Type: application/json" \
  -d '{"date": "2022-08-09", "raw_dir": "/path/to/raw/data"}'
```

### Job2: Transform to AVRO

```bash
# Start Job2 Flask app (runs on port 8082)
python job2/main.py
```

Example request:

```bash
curl -X POST http://localhost:8082/ \
  -H "Content-Type: application/json" \
  -d '{"raw_dir": "/path/to/raw/data", "stg_dir": "/path/to/staging"}'
```

### Full Pipeline Execution

Run both jobs in sequence:

```bash
export BASE_DIR=/path/to/your/data/directory
python scripts/check_jobs.py
```

## Testing

Run the test suite:

```bash
# Test all jobs
pytest

# Test specific job
pytest job1/
pytest job2/

# Run with verbose output
pytest -v
```

## Key Features

- **Pagination Handling**: Automatic pagination through API responses
- **Retry Logic**: Exponential backoff for transient failures (3 retries)
- **Error Handling**: Comprehensive exception handling with meaningful HTTP responses
- **AVRO Serialization**: Schema-based data transformation for data consistency
- **Modular Design**: Easy to extend and modify individual components
- **Type Hints**: Modern Python type annotations throughout
- **Comprehensive Tests**: Full unit test coverage with mock-based testing

## API Responses

### Job1 Endpoints

**POST /**

- **Success**: `201 Created`
- **Error**: `400 Bad Request` (invalid parameters), `500 Internal Server Error` (processing failure)

### Job2 Endpoints

**POST /**

- **Success**: `201 Created`
- **Error**: `400 Bad Request` (invalid parameters), `500 Internal Server Error` (processing failure)

## Dependencies

- **Flask** 3.1.0 - Web framework
- **fastavro** 1.10.0 - AVRO serialization
- **requests** 2.32.3 - HTTP client
- **pytest** 8.3.5 - Testing framework
- **python-dotenv** 1.1.0 - Environment variable loading

See `requirements.txt` for complete list.

## Development

### Code Style

The project uses Black for code formatting. To format your code:

```bash
black .
```

### Environment Variables

- `AUTH_TOKEN`: API authentication token (required for Job1)
- `BASE_DIR`: Base directory for data storage (used by the pipeline runner)

## Data Flow

```
External API
    ↓
[Job1: Extract to JSON]
    ├─ Fetches paginated data
    └─ Saves as: $BASE_DIR/raw/sales/YYYY-MM-DD/*.json
    ↓
[Job2: Transform to AVRO]
    ├─ Reads all JSON files
    └─ Outputs: $BASE_DIR/stg/sales/YYYY-MM-DD/*.avro
    ↓
Ready for consumption (AVRO files)
```

## License

This project is provided as-is for educational purposes.
