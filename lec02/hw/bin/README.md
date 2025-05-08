# Utility Scripts

This directory contains utility scripts for running and testing the ETL pipeline.

## Available Scripts

### check_jobs.py

This script runs both jobs in the ETL pipeline in sequence:

1. It first runs Job1 to extract sales data from the API and save it as JSON files
2. It then waits for 3 seconds to ensure Job1 has completed
3. Finally, it runs Job2 to transform the JSON files to AVRO format

#### Usage

Before running the script, you need to set the `BASE_DIR` environment variable:

```bash
export BASE_DIR=/path/to/your/data/directory
```

This directory will be used to create the following subdirectories:
- `$BASE_DIR/raw/sales/2022-08-09`: For storing the JSON files from Job1
- `$BASE_DIR/stg/sales/2022-08-09`: For storing the AVRO files from Job2

Then run the script:

```bash
python check_jobs.py
```

#### Prerequisites

1. Both Job1 and Job2 Flask servers must be running:
   - Job1 on port 8081
   - Job2 on port 8082

2. The `AUTH_TOKEN` environment variable must be set for Job1 to authenticate with the API.

#### How It Works

The script makes HTTP POST requests to the Flask servers:

1. For Job1:
   ```python
   requests.post(
       url="http://localhost:8081/",
       json={"date": "2022-08-09", "raw_dir": RAW_DIR}
   )
   ```

2. For Job2:
   ```python
   requests.post(
       url="http://localhost:8082/",
       json={"raw_dir": RAW_DIR, "stg_dir": STG_DIR}
   )
   ```

The script uses assertions to verify that both jobs complete successfully (HTTP status code 201).