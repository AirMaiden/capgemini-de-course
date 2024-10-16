# Azure ETL Pipeline for NYC Airbnb Data

## Overview

This project builds a fully automated ETL pipeline using **Azure Blob Storage**, **Azure Data Factory**, and **Azure Synapse Analytics**. The pipeline ingests, transforms, and loads NYC Airbnb Open Data, enabling structured analysis and data quality monitoring through Azure services.

## Prerequisites

- **Azure Account** with access to Blob Storage, Data Factory, and Synapse Analytics.
- **NYC Airbnb Dataset** downloaded from Kaggle and split into smaller test files if necessary.

## Project Structure

```plaintext
azure-etl-project/
│
├── README.md                                  # Project documentation
│
├── data/                                      # Data files for ingestion                    
│   ├── sample_data_part1.csv
│   └── sample_data_part2.csv
│
├── adf_pipeline/
│   ├── adf_pipeline.json                      # Azure Data Factory pipeline JSON
│
└── sql_scripts/
    ├── create_tables.sql                      # SQL script to create external tables
    └── data_quality_checks.sql                # SQL script to validate data quality
```

## Setup Instructions

### 1. Azure Blob Storage

1. Create a **Storage Account** in Azure and a **container** named `airbnb-data`.
2. Inside `airbnb-data`, create two folders:
   - `raw/`: for uploading raw input data files.
   - `processed/`: for storing transformed data.
3. Upload split data files to the `airbnb-data/raw/` directory.

### 2. Azure Synapse Analytics

1. Create a **Serverless SQL Pool** in Azure Synapse Analytics.
2. Run the `create_tables.sql` script (located in `sql_scripts/`) in Synapse to:
   - Set up an external data source pointing to Blob Storage.
   - Define external tables `RawTable` and `ProcessedTable`.

### 3. Azure Data Factory

1. Open **Azure Data Factory** and import the pipeline configuration file `adf_pipeline.json` (located in `adf_pipeline/`) to define the ETL workflow:
   - The pipeline includes activities to ingest data from Blob Storage, transform it, and load it into Synapse Analytics.
2. Set up an **Event-Based Trigger** in ADF to automatically start the pipeline when new data is uploaded to `airbnb-data/raw/`.

## Running the Pipeline

1. **Data Upload**: Add a new CSV file to the `airbnb-data/raw/` folder in Blob Storage. The event-based trigger in ADF will automatically initiate the pipeline.
2. **Pipeline Execution**: 
   - Go to Azure Data Factory’s **Monitor** tab to observe the pipeline’s execution status.
   - The pipeline will:
      - Ingest data from Blob Storage to the `RawTable` external table in Synapse.
      - Apply data transformations, including filtering invalid data and filling missing values.
      - Load the transformed data into the `ProcessedTable` external table in Synapse Analytics.

## Error Handling and Monitoring

- **ADF Monitoring**: Use ADF’s monitoring features to view any failed activities or retries. The pipeline has retry policies configured within the `adf_pipeline.json` file.
- **Azure Monitor**: Set up alerts in **Azure Monitor** to notify you of any failures in the pipeline. Recommended thresholds:
   - Alert on any failed activities or retries in ADF.
   - Log pipeline duration and activity latency to detect potential performance issues.
   
## Data Quality Checks

1. After the pipeline has processed data, run the **data quality checks** in Synapse by executing `data_quality_checks.sql` (located in `sql_scripts/`).
2. Key validation checks include:
   - Ensuring no null values in critical columns such as `price`, `latitude`, and `longitude`.
   - Validating that `price` is positive and `latitude`/`longitude` values are within expected geographic ranges.
   - Checking for consistent room type values.

## Cleanup

- Delete or disable the resources created (e.g., Blob Storage container, Synapse Analytics, and Data Factory pipelines) after completing the task to avoid unnecessary costs.

## Summary

This ETL pipeline provides a scalable and automated solution for processing NYC Airbnb data in Azure. The setup enables seamless data ingestion, transformation, validation, and storage for further analysis in Synapse Analytics. For troubleshooting or modifications, refer to the provided JSON and SQL configuration files.