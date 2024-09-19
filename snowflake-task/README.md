# Airbnb ETL Pipeline with Snowflake

## Overview
This project demonstrates an ETL pipeline using Snowflake to ingest, transform, and load Airbnb data from a CSV file stored in an AWS S3 bucket. The pipeline uses **Snowpipe** for continuous ingestion, SQL transformations for data cleaning, and **Snowflake Streams** for monitoring changes.

## Directory Structure
- **SQL Scripts**: All SQL scripts are provided in the `sql_scripts/` folder.
- **S3 Bucket**: Your data source files (CSV) are located in an AWS S3 bucket.
- **Snowflake**: Your Snowflake database will ingest, transform, and validate the data.

### 1. Set Up Snowflake and S3
- Create an S3 bucket for storing raw CSV files.
- Set up a Snowflake account and ensure you have the **SnowSQL** CLI installed for running SQL commands.

### 2. Create the Database and Warehouse in Snowflake
Before running any SQL scripts, log into SnowSQL and create the database and warehouse:

```sql
CREATE DATABASE nyc_airbnb;
CREATE WAREHOUSE nyc_airbnb_wh;
```

This ensures that all tables and stages will be created in the correct database and schema.

### 3. Create Raw and Transformed Tables

Run the SQL script to create the raw and transformed tables:

```bash
snowsql -a <SNOWFLAKE_ACCOUNT_NAME> -u <SNOWFLAKE_USERNAME> -f sql_scripts/create_tables.sql
```

This will create the RAW_AIRBNB_DATA and TRANSFORMED_AIRBNB_DATA tables in Snowflake.

### 4. Configure Snowpipe for Continuous Ingestion

To set up Snowpipe, run the Snowpipe configuration script:

```bash
snowsql -a <SNOWFLAKE_ACCOUNT_NAME> -u <SNOWFLAKE_USERNAME> -f sql_scripts/snowpipe_configuration.sql
```

This will create an external stage and set up Snowpipe for continuous ingestion from your AWS S3 bucket.

### 5. Ingest Data from S3

Once Snowpipe is set up, upload the CSV files to your S3 bucket. Snowpipe will automatically detect the new files and ingest them into the RAW_AIRBNB_DATA table. You can monitor the ingestion process using:

```bash
snowsql -a <SNOWFLAKE_ACCOUNT_NAME> -u <SNOWFLAKE_USERNAME> -q "SHOW PIPES;"
```

### 6. Perform Data Transformations

After data is ingested, transform the raw data using the following command:

```bash
snowsql -a <SNOWFLAKE_ACCOUNT_NAME> -u <SNOWFLAKE_USERNAME> -f sql_scripts/transform_data.sql
```

This will clean and load the data into the TRANSFORMED_AIRBNB_DATA table.

### 7. Perform Data Quality Checks

Once the transformation is complete, run data quality checks to validate the integrity of the data:

```bash
snowsql -a <SNOWFLAKE_ACCOUNT_NAME> -u <SNOWFLAKE_USERNAME> -f sql_scripts/data_quality_checks.sql
```

This will check for NULL values in critical columns and ensure the row counts match between the raw and transformed tables.

### 8. Set Up Snowflake Tasks for Automation

In this step, you will create a Snowflake Task that automates the daily transformation of the raw Airbnb data into the transformed table. This ensures that the data transformation happens automatically at scheduled intervals.

Run the automation_tasks.sql script using SnowSQL:

```bash
snowsql -a <SNOWFLAKE_ACCOUNT_NAME> -u <SNOWFLAKE_USERNAME> -f sql_scripts/automation_tasks.sql
```

This will:
- Create a scheduled task to transform the data in the raw_airbnb_data table daily at midnight.
- Create two streams: one for monitoring changes in the raw table (raw_airbnb_data_stream) and one for the transformed table (transformed_airbnb_data_stream).

After running the script, verify that the task has been created and enabled:

```bash
snowsql -a <SNOWFLAKE_ACCOUNT_NAME> -u <SNOWFLAKE_USERNAME> -q "SHOW TASKS;"
```

The output should show the task transform_airbnb_data_daily as started.

### 9. Error Handling and Monitoring

- **Error Handling**: Snowflake’s Time Travel feature allows you to recover historical data. You can query historical data using the following command:

```sql
SELECT * FROM TRANSFORMED_AIRBNB_DATA AT (OFFSET => -3600);
```

This retrieves the data as it was an hour ago.

- **Monitoring**: Use Snowflake Streams to monitor changes in your raw data:

```sql
CREATE OR REPLACE STREAM raw_airbnb_stream ON TABLE RAW_AIRBNB_DATA;
```

This stream will track changes made to the RAW_AIRBNB_DATA table.

### Conclusion

This setup ensures that your data ingestion, transformation, and loading process is automated and resilient. By using Snowflake Tasks, Streams, and Time Travel, you can monitor and recover from errors while ensuring continuous updates to your dataset.

### Note

Make sure to replace <SNOWFLAKE_ACCOUNT_NAME>, <SNOWFLAKE_USERNAME>, <AWS_BUCKET_NAME>, <AWS_ACCESS_KEY> and <AWS_SECRET_KEY> with your actual Snowflake and AWS credentials when running the commands. They can be stored in .env file (which must be added to .gitignore).

