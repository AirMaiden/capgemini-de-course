-- Step 1: Specify the database and schema
USE DATABASE nyc_airbnb;
USE SCHEMA public;

-- Step 2: Perform data quality checks
-- Check if there are NULL values in critical columns
SELECT *
FROM TRANSFORMED_AIRBNB_DATA
WHERE price IS NULL
   OR minimum_nights IS NULL
   OR availability_365 IS NULL;

-- Check that the number of records matches between the raw and transformed tables
SELECT COUNT(*) AS raw_count FROM RAW_AIRBNB_DATA;
SELECT COUNT(*) AS transformed_count FROM TRANSFORMED_AIRBNB_DATA;