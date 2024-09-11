# PySpark ETL Pipeline

## Project Overview

This PySpark job is designed to perform an end-to-end ETL (Extract, Transform, Load) pipeline. It ingests data from CSV files, applies transformations and validations, runs queries on the transformed data, saves it as Parquet files, and logs all necessary information for troubleshooting and validation.

The job also tracks processed files to avoid reprocessing duplicates in future runs, ensuring only new data is processed.

### Key Features:
1. **Data Ingestion**: The pipeline reads new CSV files from a specified directory using PySpark's Structured Streaming.
2. **Transformation**: The job applies transformations, including filtering, date conversion, and categorization of price ranges.
3. **Data Validation**: The script performs validation checks, ensuring that the row counts match and critical columns contain no NULL values.
4. **SQL Queries**: PySpark queries are run on the transformed data, such as counting listings by neighborhood and calculating average prices by room type.
5. **Saving Processed Data**: The transformed data is saved as partitioned Parquet files.
6. **Logging**: The job logs errors and important events, saving them to a log file.
7. **File Tracking**: It tracks processed files to avoid duplication and prevent reprocessing.

---

## Prerequisites

Before running the PySpark ETL pipeline, ensure that you have the following installed:

1. **Apache Spark**: Follow [these steps](https://spark.apache.org/docs/latest/) to install Apache Spark.
2. **Python 3.x**: Make sure you have Python 3.x installed.
3. **PySpark**: You can install PySpark by running:
   ```
   bash
   pip install pyspark
   ```
4. **Java**: Apache Spark requires Java to be installed on your machine.

## Directory Structure

Here’s an overview of the project folder structure:
```
project-folder/
├── raw/                   # Directory containing raw CSV files
│   ├── AB_NYC_2019.csv
├── processed/             # Directory for processed Parquet files
├── logs/                  # Directory containing log files
│   ├── etl_pipeline.log
├── main_etl_script.py     # Main PySpark ETL job script
└── README.md              # This README file
```

Make sure to update the paths in the script as per your folder structure.

## Configuration

The following directories and files should be configured in the script before running:

   1. raw_folder: Path to the directory containing raw CSV files. Update the raw_folder variable to point to your raw directory.
   2. processed_folder: Path to the directory where processed Parquet files will be saved. Update the processed_folder variable.
   3. processed_files_log: File that tracks already processed files to prevent duplication. Set the path to the processed_files_log file.
   4. log_file_path: Path to the log file where all events and errors will be written.

Example:
```
raw_folder = "/path/to/your/raw_folder"
processed_folder = "/path/to/your/processed_folder"
processed_files_log = "/path/to/your/log/processed_files.log"
log_file_path = "/path/to/your/log/etl_pipeline.log"
```

## Running the PySpark Job

   1. Place the CSV files: Ensure that the CSV files are placed inside the raw/ folder.
   2. Execute the Script: To run the PySpark ETL job, navigate to the directory containing the script and run the following command:
   ```
   spark-submit main_etl_script.py
   ```
   This will start the PySpark job, continuously monitoring the raw/ folder for new files, processing them, and saving the results as Parquet files.

   3. Monitor Logs: You can check the log file located at logs/etl_pipeline.log to see the progress, errors, and validation checks.

## Interpretation of Results

   1. Processed Files: After each batch, the processed CSV files are saved as partitioned Parquet files in the processed/ directory.
   2. SQL Queries:
   •  Listings by Neighbourhood Group: Counts the number of listings per neighborhood group.
   •  Top 10 Most Expensive Listings: Shows the top 10 listings by price.
   •  Average Price by Room Type: Displays the average price per room type in each neighborhood group.
These results are displayed in the console as the job runs.
   3. Logs:
   •  Data Validation: After each batch, the row counts of the raw, transformed, and Parquet data are validated. Any NULL values in critical columns like price, minimum_nights, or availability_365 are logged.
   •  Error Handling: If any step of the ETL process fails (reading, transforming, saving), the error details are logged for troubleshooting.
   4. Processed Files Log: A log file (processed_files.log) tracks the files that have already been processed, preventing reprocessing in future runs.

## Error Handling

If an error occurs during the ETL process (e.g., file reading, transformations, saving), the error will be logged to the etl_pipeline.log file. The script is designed to handle the following scenarios:

   •  File Reading Issues: Errors during the file reading stage are logged and raised.
   •  Transformation Failures: If there’s an error during transformation, it’s logged, and the process stops for the current batch.
   •  Saving & Validation Failures: Errors during data saving or validation are logged.

To troubleshoot, check the log file for detailed information.

## Stopping the Job

The PySpark job runs continuously, monitoring for new files in the raw/ folder. To stop the job, press Ctrl+C in the terminal where the job is running.

## Notes

   •  Streaming Configuration: The PySpark job uses structured streaming, processing new files as they appear in the raw/ folder.
   •  File Tracking: The job keeps a log of processed files in the processed_files.log file to avoid duplication in future runs.
   •  Performance: You can adjust the processingTime trigger (currently set to 10 seconds) depending on the size and frequency of incoming files.
