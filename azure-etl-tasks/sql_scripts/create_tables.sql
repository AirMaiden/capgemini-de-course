CREATE EXTERNAL DATA SOURCE MyBlobStorage
WITH (
    LOCATION = 'https://deblobstorageeus2.blob.core.windows.net/airbnb-data'
);
CREATE EXTERNAL FILE FORMAT MyCsvFormat
WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (FIELD_TERMINATOR = ',', STRING_DELIMITER = '"', FIRST_ROW = 2)
);

CREATE EXTERNAL TABLE [dbo].[RawTable] (
    id INT,
    name NVARCHAR(255),
    host_id INT,
    host_name NVARCHAR(255),
    neighbourhood_group NVARCHAR(255),
    neighbourhood NVARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    room_type NVARCHAR(255),
    price FLOAT,
    minimum_nights INT,
    number_of_reviews INT,
    last_review DATE,
    reviews_per_month FLOAT,
    calculated_host_listings_count INT,
    availability_365 INT
)
WITH (
    LOCATION = 'raw/sample_data_part1.csv',
    DATA_SOURCE = MyBlobStorage,
    FILE_FORMAT = MyCsvFormat
);


CREATE EXTERNAL TABLE [dbo].[ProcessedTable] (
    id INT,
    name NVARCHAR(255),
    host_id INT,
    host_name NVARCHAR(255),
    neighbourhood_group NVARCHAR(255),
    neighbourhood NVARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    room_type NVARCHAR(255),
    price FLOAT,
    minimum_nights INT,
    number_of_reviews INT,
    last_review DATE,
    reviews_per_month FLOAT,
    calculated_host_listings_count INT,
    availability_365 INT
)
WITH (
    LOCATION = 'processed/part-00000-40c145f7-1cd2-4caa-aafd-772e10a7bad8-c000.csv',
    DATA_SOURCE = MyBlobStorage,
    FILE_FORMAT = MyCsvFormat
);