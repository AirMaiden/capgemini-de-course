-- Step 1: Specify the database and schema
USE DATABASE nyc_airbnb;
USE SCHEMA public;

-- Step 2: Transform the data
INSERT INTO TRANSFORMED_AIRBNB_DATA
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
    TRY_TO_DATE(last_review, 'YYYY-MM-DD') AS last_review,
    COALESCE(reviews_per_month, 0) AS reviews_per_month,
    calculated_host_listings_count,
    availability_365
FROM RAW_AIRBNB_DATA
WHERE price > 0
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL;