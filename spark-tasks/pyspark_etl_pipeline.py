import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, avg, count, desc, min, max, input_file_name
import logging

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Airbnb PySpark ETL Pipeline") \
    .getOrCreate()

# Set up logging to output to a file
log_file_path = "/Users/evgeniyakulish/DE_course/projects/capgemini-de-course/spark-tasks/logs/etl_pipeline.log"
if not os.path.exists(os.path.dirname(log_file_path)):
    os.makedirs(os.path.dirname(log_file_path))

logging.basicConfig(
    filename=log_file_path,  # Set the log file path
    level=logging.INFO,  # Log level: INFO
    format='%(asctime)s - %(levelname)s - %(message)s',  # Format for log messages
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Define directories
raw_folder = "/Users/evgeniyakulish/DE_course/projects/capgemini-de-course/spark-tasks/raw/"
processed_folder = "/Users/evgeniyakulish/DE_course/projects/capgemini-de-course/spark-tasks/processed/"
processed_files_log = "/Users/evgeniyakulish/DE_course/projects/capgemini-de-course/spark-tasks/logs/processed_files.log"

# Define schema for CSV files
schema = "id INT, name STRING, host_id INT, host_name STRING, neighbourhood_group STRING, " \
         "neighbourhood STRING, latitude DECIMAL(10, 6), longitude DECIMAL(10, 6), " \
         "room_type STRING, price INT, minimum_nights INT, number_of_reviews INT, " \
         "last_review STRING, reviews_per_month DECIMAL(3, 2), calculated_host_listings_count INT, " \
         "availability_365 INT"

# Function to log processed files
def log_processed_file(file_name):
    with open(processed_files_log, 'a') as log_file:
        log_file.write(f"{file_name}\n")

# Function to check if a file has already been processed
def is_file_processed(file_name):
    if not os.path.exists(processed_files_log):
        return False  # Log file doesn't exist, so file hasn't been processed

    with open(processed_files_log, 'r') as log_file:
        processed_files = log_file.read().splitlines()

    return file_name in processed_files

# Function to validate saved data
def validate_saved_data(transformed_df, parquet_df):
    transformed_row_count = transformed_df.count()
    parquet_row_count = parquet_df.count()

    # Check row counts
    if transformed_row_count == parquet_row_count:
        logger.info("Row count validation passed: Row count matches: %d", parquet_row_count)
    else:
        logger.error(f"Row count validation failed: Transformed row count = {transformed_row_count}, Parquet row count = {parquet_row_count}")

    # Check for NULL values in critical columns
    null_checks = parquet_df.filter(col("price").isNull() | col("minimum_nights").isNull() | col("availability_365").isNull()).count()
    if null_checks == 0:
        logger.info("Null value check passed: no NULL values in critical columns (price, minimum_nights, availability_365).")
    else:
        logger.error(f"Null value check failed: found NULL values in critical columns. NULL count = {null_checks}")

    # Basic statistics check (optional)
    price_stats = parquet_df.agg(
        count("*").alias("total_count"),
        avg("price").alias("average_price"),
        min("price").alias("min_price"),
        max("price").alias("max_price")
    ).collect()[0]  # Collect the results as a Row object

    logger.info(f"Parquet file stats: Row count = {price_stats['total_count']}, "
                f"Average price = {price_stats['average_price']}, "
                f"Min price = {price_stats['min_price']}, "
                f"Max price = {price_stats['max_price']}")

# Function to process and transform the DataFrame
def transform_data(df):
    try:
        df_transformed = df.filter(col("price") > 0) \
            .withColumn("last_review", to_date(col("last_review"), "yyyy-MM-dd")) \
            .withColumn("last_review", when(col("last_review").isNull(), "2000-01-01").otherwise(col("last_review"))) \
            .fillna({'reviews_per_month': 0}) \
            .filter(col("latitude").isNotNull() & col("longitude").isNotNull()) \
            .withColumn("price_category", when(col("price") < 100, "budget")
                        .when((col("price") >= 100) & (col("price") < 300), "mid-range")
                        .otherwise("luxury"))

        logger.info("Transformation successful for current batch")
        return df_transformed

    except Exception as e:
        logger.error(f"Error during data transformation: {str(e)}")
        raise  # Re-raise the exception to stop the process in case of a transformation failure

# Function to save DataFrame as Parquet files
def save_data(df, epoch_id):
    try:
        # Save as Parquet file partitioned by neighbourhood_group
        df.write.mode("append").partitionBy("neighbourhood_group").parquet(processed_folder)
        logger.info(f"Data saved for batch {epoch_id}")

        # Load the saved Parquet file for validation
        parquet_df = spark.read.parquet(processed_folder)

        # Validate the saved data after each batch
        validate_saved_data(df, parquet_df)

    except Exception as e:
        logger.error(f"Error during saving or validation for batch {epoch_id}: {str(e)}")
        raise  # Re-raise the exception to stop the process if saving fails

# Function to perform PySpark DataFrame operations instead of SQL
def run_dataframe_queries(df):
    try:
        # Query 1: Listings by neighbourhood group
        listings_by_neighbourhood_group = df.groupBy("neighbourhood_group").agg(count("*").alias("listings_count")).orderBy(desc("listings_count"))
        listings_by_neighbourhood_group.show()

        # Query 2: Top 10 most expensive listings
        top_10_expensive = df.orderBy(desc("price")).limit(10)
        top_10_expensive.show()

        # Query 3: Average price by room type and neighbourhood group
        avg_price_by_room_type = df.groupBy("neighbourhood_group", "room_type").agg(avg("price").alias("avg_price"))
        avg_price_by_room_type.show()

        logger.info("SQL queries executed successfully")

    except Exception as e:
        logger.error(f"Error during SQL query execution: {str(e)}")
        raise  # Re-raise the exception if queries fail

# Function to read new CSV files using Structured Streaming
def read_streaming_data():
    try:
        raw_stream = spark.readStream \
            .schema(schema) \
            .option("header", "true") \
            .csv(raw_folder)

        logger.info("File reading initiated successfully")
        return raw_stream

    except Exception as e:
        logger.error(f"Error during file reading: {str(e)}")
        raise  # Re-raise the exception if file reading fails

# Function to process each file
def process_new_file(df, epoch_id):
    # Add a new column to capture the input file name
    df_with_file_name = df.withColumn("file_name", input_file_name())
    
    # Extract distinct file names (assuming you want one file per batch)
    file_name = df_with_file_name.select("file_name").distinct().collect()[0]["file_name"]
    
    # Check if file has been processed
    if is_file_processed(file_name):
        logger.info(f"File {file_name} already processed. Skipping.")
    else:
        logger.info(f"Processing new file: {file_name}")
        
        # Perform your transformation and saving logic
        transformed_df = transform_data(df_with_file_name)
        save_data(transformed_df, epoch_id)
        run_dataframe_queries(transformed_df)

        # Log the processed file
        log_processed_file(file_name)

# Main logic in foreachBatch processing
try:
    raw_stream = read_streaming_data()

    query = raw_stream.writeStream \
        .foreachBatch(lambda df, epoch_id: process_new_file(df, epoch_id)) \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

except Exception as e:
    logger.error(f"Error in main processing loop: {str(e)}")