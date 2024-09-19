-- snowpipe_configuration.sql

-- Step 1: Specify the database and schema
USE DATABASE nyc_airbnb;
USE SCHEMA public;

-- Step 2: Create the external stage pointing to the S3 bucket (don't forget to replace aws variables with real values for your bucket)
CREATE OR REPLACE STAGE nyc_airbnb_stage
URL = 's3://<AWS_BUCKET_NAME>/'
CREDENTIALS = (AWS_KEY_ID = '<AWS_ACCESS_KEY>' AWS_SECRET_KEY = '<AWS_SECRET_KEY>');

-- Step 3: Create Snowpipe for continuous ingestion
CREATE OR REPLACE PIPE nyc_airbnb_pipe AS
COPY INTO RAW_AIRBNB_DATA
FROM @nyc_airbnb_stage
FILE_FORMAT = (TYPE = 'CSV')
ON_ERROR = 'CONTINUE';