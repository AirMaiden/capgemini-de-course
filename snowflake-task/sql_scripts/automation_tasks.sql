-- Step 8: Create Snowflake Task for Daily Transformation

-- Create the task to automate transformation
CREATE OR REPLACE TASK transform_airbnb_data_daily
    WAREHOUSE = nyc_airbnb_wh
    SCHEDULE = 'USING CRON 0 0 * * * UTC' -- Daily at midnight
    COMMENT = 'Automated task to transform raw Airbnb data daily'
AS
    INSERT INTO transformed_airbnb_data
    SELECT
        id,
        name,
        host_id,
        host_name,
        neighbourhood_group,
        neighbourhood,
        latitude,
        longitude,
        room_type,
        price,
        minimum_nights,
        number_of_reviews,
        COALESCE(last_review, (SELECT MIN(last_review) FROM raw_airbnb_data)) AS last_review,
        COALESCE(reviews_per_month, 0) AS reviews_per_month,
        calculated_host_listings_count,
        availability_365
    FROM raw_airbnb_data
    WHERE price > 0
    AND latitude IS NOT NULL
    AND longitude IS NOT NULL;

-- Enable the task to run daily
ALTER TASK transform_airbnb_data_daily RESUME;

-- Step 9: Create Streams for Monitoring Changes

-- Create a stream on the raw table to track changes
CREATE OR REPLACE STREAM raw_airbnb_data_stream
    ON TABLE raw_airbnb_data
    SHOW_INITIAL_ROWS = TRUE;

-- Create a stream on the transformed table to track changes
CREATE OR REPLACE STREAM transformed_airbnb_data_stream
    ON TABLE transformed_airbnb_data
    SHOW_INITIAL_ROWS = TRUE;

-- Check Stream Data (Optional, just for verification purposes)
-- SELECT * FROM raw_airbnb_data_stream;
-- SELECT * FROM transformed_airbnb_data_stream;