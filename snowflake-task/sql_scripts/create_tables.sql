-- create_tables.sql

-- Step 1: Specify the database and schema
USE DATABASE nyc_airbnb;
USE SCHEMA public;

-- Step 2: Create Raw Table for Ingestion
CREATE OR REPLACE TABLE RAW_AIRBNB_DATA (
    id STRING,
    name STRING,
    host_id STRING,
    host_name STRING,
    neighbourhood_group STRING,
    neighbourhood STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    room_type STRING,
    price INT,
    minimum_nights INT,
    number_of_reviews INT,
    last_review STRING,
    reviews_per_month DOUBLE,
    calculated_host_listings_count INT,
    availability_365 INT
);

-- Step 3: Create Transformed Table for Processed Data
CREATE OR REPLACE TABLE TRANSFORMED_AIRBNB_DATA (
    id STRING,
    name STRING,
    host_id STRING,
    host_name STRING,
    neighbourhood_group STRING,
    neighbourhood STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    room_type STRING,
    price INT,
    minimum_nights INT,
    number_of_reviews INT,
    last_review DATE,
    reviews_per_month DOUBLE,
    calculated_host_listings_count INT,
    availability_365 INT
);