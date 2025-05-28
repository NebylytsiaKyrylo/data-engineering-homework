# Lecture 02 Homework - ETL Pipeline

This directory contains the implementation of a two-stage ETL (Extract, Transform, Load) pipeline for processing sales data.

## Pipeline Overview

The pipeline consists of two jobs that work together to process sales data:

1. **Job1**: Extracts sales data from an external API and saves it as JSON files
2. **Job2**: Reads the JSON files created by Job1 and transforms them into AVRO format

## Directory Structure

- `bin/`: Contains utility scripts for running and testing the pipeline
  - `check_jobs.py`: Script to run both jobs in sequence
- `job1/`: Implementation of the first job (Extract & Load to JSON)
  - `main.py`: Flask application entry point
  - `bll/`: Business Logic Layer
  - `dal/`: Data Access Layer
  - `tests/`: Unit tests
- `job2/`: Implementation of the second job (Transform JSON to AVRO)
  - `main.py`: Flask application entry point
  - `bll/`: Business Logic Layer
  - `dal/`: Data Access Layer
  - `tests/`: Unit tests

## Data Flow

1. Job1 fetches sales data from an external API for a specific date
2. Job1 saves each page of data as a separate JSON file in the raw directory
3. Job2 reads all JSON files from the raw directory
4. Job2 converts each JSON file to AVRO format and saves it in the staging directory

## Architecture

Both jobs follow a layered architecture:

- **Main Layer**: Flask applications that handle HTTP requests and responses
- **BLL (Business Logic Layer)**: Contains the core business logic for processing data
- **DAL (Data Access Layer)**: Handles data access operations (API calls, file I/O)

This separation of concerns makes the code more maintainable, testable, and extensible.

## Running the Pipeline

See the README.md files in the job1 and job2 directories for specific instructions on running each job.

For running the complete pipeline, use the check_jobs.py script in the bin directory:

```bash
# Set the BASE_DIR environment variable
export BASE_DIR=/path/to/your/data/directory

# Run the pipeline
python bin/check_jobs.py
```

## Testing

Each job includes comprehensive unit tests for all layers. To run the tests:

```bash
# For job1
cd job1
pytest

# For job2
cd job2
pytest
```