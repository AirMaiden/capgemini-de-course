# NYC Airbnb ETL Pipeline

## Overview

This Apache Airflow DAG (`nyc_airbnb_etl.py`) automates the process of extracting, transforming, and loading data from the `AB_NYC_2019.csv` file (New York City Airbnb Open Data) into a PostgreSQL database. The pipeline is scheduled to run daily at midnight in your local timezone.

## Directory Structure

The directory structure for this project is as follows:
```
/airflow-docker/
├── raw/
│   └── AB_NYC_2019.csv
├── transformed/
├── processed/
├── dags/
│   └── nyc_airbnb_etl.py
├── logs/
│   └── airflow_failures.log
└── plugins/
```
- **`dags/`**: Contains the DAG script.
- **`raw_data/`**: Contains the raw CSV file.
- **`transformed_data/`**: Stores the transformed CSV file.
- **`logs/`**: Stores the failure logs.

## Prerequisites

- **Apache Airflow** installed and running as docker container.
- **PostgreSQL** installed and a database named `airflow_etl` created.
- The **`airbnb_listings` table** created in the `airflow_etl` database with the structure provided in the DAG script.

## Configuring Parameters

### Airflow Variables

Set the following Airflow variables in the UI or through the Airflow CLI:

- **`RAW_DATA_PATH`**: Path to the raw data file (e.g., `/opt/airflow/raw_data/AB_NYC_2019.csv`).
- **`TRANSFORMED_DATA_PATH`**: Path where the transformed data will be saved (e.g., `/opt/airflow/transformed_data/transformed_AB_NYC_2019.csv`).
- **`DB_CONN_ID`**: Connection ID for PostgreSQL (e.g., `postgres_airflow_etl`).
- **`TABLE_NAME`**: Table name in PostgreSQL DB (e.g., `airbnb_listings`).
- **`LOG_FILE_PATH`**: Path to the log files (e.g., `/opt/airflow/logs/airflow_failures.log`).

### Airflow Connections

Ensure that a PostgreSQL connection is configured in the Airflow UI with the connection ID provided above.

## Running the DAG

1. **Place the CSV file**: Ensure the `AB_NYC_2019.csv` file is placed in the `raw_data/` directory.
2. **Deploy the DAG**: Place the `nyc_airbnb_etl.py` script in your Airflow `dags/` directory. Airflow will automatically detect the new DAG.
3. **Trigger the DAG**: Go to the Airflow UI, locate the `nyc_airbnb_etl` DAG, and trigger it manually or wait for it to run at the scheduled time (the DAG is set to run daily at midnight by default).
4. **Monitor the DAG**: Use Airflow's UI to monitor the progress of each task.
5. **Check Logs**: If any task fails, refer to the `airflow_failures.log` file located in the `logs/` directory for detailed error messages.

### Task Details
1. **Ingest Data**: Reads the CSV file from the raw directory.
2. **Transform Data**: Filters out invalid rows, handles missing values, and saves the transformed data. After the DAG runs successfully, the transformed data will be saved in the `transformed_data/` directory as `transformed_AB_NYC_2019.csv`.
3. **Load Data**: Inserts the transformed data into the PostgreSQL database. The transformed data will be loaded into the `airbnb_listings` table in the `airflow_etl` database. **Note: current implementation include truncating the table before loading the records.**
4. **Data Quality Check**: Verifies that the loaded data meets certain quality standards. The DAG includes data quality checks to ensure that:
  - The number of records in the PostgreSQL table matches the expected number from the transformed CSV.
  - There are no `NULL` values in the `price`, `minimum_nights`, and `availability_365` columns.

If any of these checks fail, the workflow will log the error and stop further processing.

### Monitoring
- **Task Status**: Use the Airflow UI to monitor the status of each task in the DAG.
- **Logs**: Detailed logs are available for each task to troubleshoot issues. Review the `airflow_failures.log` file for any errors that occurred during the DAG execution.
  
## Optimization and Refactoring

The DAG is designed for:

- **Reduced I/O Operations**: Minimized unnecessary I/O operations by directly passing data between tasks via XCom or in-memory processing.
- **Code Readability**: The code is refactored to improve readability and maintainability, with detailed comments and documentation.

Areas for improvements: 

- **Parallel Execution**: Independent tasks can be parallelized if required. Current implementation doesn't have tasks that are independent and can be parallelized.

## Conclusion

This DAG provides an end-to-end ETL pipeline for ingesting, transforming, and loading NYC Airbnb data into a PostgreSQL database, with built-in data quality checks to ensure the integrity of the data.
