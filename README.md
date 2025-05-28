# Data Engineering Homework

## Project Overview

This repository contains data engineering homework assignments, with a focus on building data pipelines using Python and Flask. The current implementation includes a two-stage ETL process in the `lec02` directory.

## Repository Structure

- `lec02/`: Contains the ETL pipeline implementation
  - `hw/`: Homework implementation with two jobs forming a data pipeline
    - `job1/`: Extracts sales data from an API and saves as JSON files
    - `job2/`: Transforms JSON files to AVRO format
    - `bin/`: Contains utility scripts to run the jobs

### Project Tree
```
.
├── README.md
├── lec02
│   ├── __init__.py
│   └── hw
│       ├── README.md
│       ├── __init__.py
│       ├── bin
│       │   ├── README.md
│       │   ├── __init__.py
│       │   └── check_jobs.py
│       ├── job1
│       │   ├── README.md
│       │   ├── __init__.py
│       │   ├── bll
│       │   │   ├── __init__.py
│       │   │   └── sales_api.py
│       │   ├── dal
│       │   │   ├── __init__.py
│       │   │   ├── local_disk.py
│       │   │   └── sales_api.py
│       │   ├── main.py
│       │   └── tests
│       │       ├── __init__.py
│       │       ├── test_bll
│       │       │   ├── __init__.py
│       │       │   └── test_sales_api.py
│       │       ├── test_dal
│       │       │   ├── __init__.py
│       │       │   ├── test_local_disk.py
│       │       │   └── test_sales_api.py
│       │       └── test_main.py
│       └── job2
│           ├── README.md
│           ├── __init__.py
│           ├── bll
│           │   ├── __init__.py
│           │   └── process_sales.py
│           ├── dal
│           │   ├── __init__.py
│           │   └── file_io.py
│           ├── main.py
│           └── tests
│               ├── __init__.py
│               ├── test_bll
│               │   ├── __init__.py
│               │   └── test_process_sales.py
│               ├── test_dal
│               │   ├── __init__.py
│               │   └── test_file_io.py
│               └── test_main.py
└── requirements.txt
```

## Requirements

- Python 3.8+
- Dependencies listed in `requirements.txt`

## Setup

1. Clone the repository
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Set up environment variables:
   ```
   export BASE_DIR=/path/to/your/data/directory
   export AUTH_TOKEN=your_api_auth_token
   ```

## Running the Pipeline

1. Start job1 (Flask server):
   ```
   cd lec02/hw/job1
   python main.py
   ```

2. Start job2 (Flask server):
   ```
   cd lec02/hw/job2
   python main.py
   ```

3. Run the complete pipeline using the check_jobs script:
   ```
   cd lec02/hw/bin
   python check_jobs.py
   ```

## Project Architecture

The project follows a layered architecture:

- **Main Layer**: Flask applications that handle HTTP requests
- **BLL (Business Logic Layer)**: Contains the core business logic
- **DAL (Data Access Layer)**: Handles data access operations (API calls, file I/O)

Each job is implemented as a separate Flask application that can be run independently or as part of the pipeline.

## License

This project is for educational purposes only.
