# Databricks ETL Pipeline for Airbnb Data

This document provides instructions for setting up and running the Databricks pipeline that ingests, transforms, and loads the Airbnb dataset. The pipeline ingests raw CSV files into a Bronze Delta Table, applies transformations, and saves the cleaned data in a Silver Delta Table. The pipeline also includes data quality checks, error handling, and real-time monitoring.

## Requirements
- Databricks Workspace
- Databricks Cluster with Apache Spark and Delta Lake installed
- Airbnb dataset (`AB_NYC_2019.csv`) uploaded to DBFS (Databricks File System)

## Directory Structure

- **Raw Data Folder**: `/mnt/delta/raw/`
- **Bronze Table Path**: `/mnt/delta/bronze/airbnb_listings`
- **Silver Table Path**: `/mnt/delta/silver/airbnb_listings`
- **Checkpoint Folder for Bronze**: `/mnt/delta/checkpoints/bronze/`
- **Checkpoint Folder for Silver**: `/mnt/delta/checkpoints/silver/`
- **Log File**: `/dbfs/logs/etl_pipeline.log`


## Step-by-Step Guide

1. **Set Up the Environment**
- Databricks Cluster: Set up a Databricks cluster with Apache Spark and Delta Lake installed.
- Mount DBFS: Make sure the dataset is available in your DBFS storage. Use the AB_NYC_2019.csv file and place it in /mnt/delta/raw/.
```
dbutils.fs.cp("dbfs:/FileStore/AB_NYC_2019.csv", "dbfs:/mnt/delta/raw/AB_NYC_2019.csv")
```

2. **Databricks Auto Loader for Continuous Ingestion**

The script uses Auto Loader to monitor the /mnt/delta/raw/ folder and continuously ingest new CSV files into the Bronze Delta Table.
- Bronze Table: This stores the raw, untransformed data.
- Auto Loader will detect new files added to the raw folder and load them into the Bronze Table.

3. **Running the ETL Pipeline**

Follow these steps to run the full pipeline:

1. **Upload the Script**:
Upload the databricks_task.py script to Databricks.
2. **Create a Notebook**:
Create a new notebook in Databricks and attach it to your cluster.
3. **Run the Script**:
In the notebook, run the following commands:
```
%run /path/to/databricks_airbnb_etl_pipeline.py
```
This will execute the full ETL pipeline, continuously ingest new data, apply transformations, and save the cleaned data into the Silver Delta Table.

4. **Data Transformation**

Transformations applied to the data include:
- Filtering out rows where the price is 0 or negative.
- Converting last_review to a valid date format.
- Filling missing reviews_per_month with 0.
- Dropping rows with missing latitude or longitude.

5. **Data Quality Checks**

Data quality checks are implemented using Delta Lake’s constraint validation:
- Ensure no NULL values in critical fields like price, minimum_nights, and availability_365.

6. **Error Handling and Logging**
- The pipeline logs errors and data quality issues to /dbfs/logs/etl_pipeline.log.
- Logging is configured to handle any issues during ingestion, transformation, or saving of the data.

7. **Streaming and Monitoring**

The script continuously monitors the Bronze Table for changes and propagates them to the Silver Table using Structured Streaming.
- Use Databricks Jobs to schedule and automate this streaming job.
- Delta Live Tables (DLT) can also be used to simplify management of streaming jobs.

8. **Automating the Pipeline**
- Use the Databricks Job Scheduler to automate the ingestion and transformation processes daily or at any interval.
- Ensure that Delta Lake’s Time Travel is configured for the Bronze and Silver tables to enable recovery from accidental changes.

9. **Error Handling and Time Travel**

Delta Lake’s Time Travel feature allows you to query previous versions of data to recover from accidental changes.

To query a previous version of the Silver table:
```
SELECT * FROM delta.`/mnt/delta/silver/airbnb_listings` VERSION AS OF 3
```


Common Issues
1. **Table Not Found**:
Ensure the Bronze and Silver Delta tables are created successfully by checking the paths.
2. **File Not Found**:
Verify the raw files are placed correctly in the /mnt/delta/raw/ folder.
3. **Error Logs**:
If you encounter issues, check the logs in /dbfs/logs/etl_pipeline.log for details.


This pipeline handles end-to-end data processing for the Airbnb dataset, enabling real-time data transformations and monitoring using Databricks.
