from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, isnan, count
from pyspark.sql import DataFrame
from delta.tables import *
import logging

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Airbnb Delta Lake ETL Pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Set up logging
log_file_path = "/dbfs/logs/etl_pipeline.log"
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Paths
bronze_table_path = "/mnt/delta/bronze/airbnb_listings"
silver_table_path = "/mnt/delta/silver/airbnb_listings"
raw_data_path = "/mnt/delta/raw/"

# Delta Lake Configuration - Time Travel enabled by default
def setup_bronze_table():
    if not DeltaTable.isDeltaTable(spark, bronze_table_path):
        # Read raw CSV files using Auto Loader
        raw_stream = spark.readStream.format("cloudFiles") \
            .option("cloudFiles.format", "csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(raw_data_path)
        
        # Write to Bronze Delta Table
        raw_stream.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", "/mnt/delta/checkpoints/bronze/") \
            .start(bronze_table_path)
        logger.info("Bronze table created and Auto Loader started.")

# Data Transformation for Silver Table
def transform_bronze_to_silver(bronze_df: DataFrame):
    # Transformations: Filter rows, handle missing values, add price category
    silver_df = bronze_df.filter(col("price") > 0) \
        .withColumn("last_review", to_date(col("last_review"), "yyyy-MM-dd")) \
        .withColumn("last_review", when(col("last_review").isNull(), "2000-01-01").otherwise(col("last_review"))) \
        .fillna({'reviews_per_month': 0}) \
        .filter(col("latitude").isNotNull() & col("longitude").isNotNull()) \
        .withColumn("price_category", when(col("price") < 100, "budget")
                    .when((col("price") >= 100) & (col("price") < 300), "mid-range")
                    .otherwise("luxury"))
    return silver_df

# Save Silver Table
def save_silver_table(df: DataFrame):
    try:
        df.write.format("delta").mode("append").save(silver_table_path)
        logger.info("Silver table saved successfully.")
    except Exception as e:
        logger.error(f"Error saving Silver Table: {str(e)}")

# Data Quality Checks using Delta Lake
def data_quality_checks(silver_df: DataFrame):
    # Ensure no NULL values in critical fields
    critical_fields = ["price", "minimum_nights", "availability_365"]
    null_checks = silver_df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in critical_fields])
    null_counts = null_checks.collect()[0].asDict()
    
    if any(null_counts.values()):
        logger.error(f"Data Quality Check Failed: NULL values found in {null_counts}")
    else:
        logger.info("Data Quality Check Passed: No NULL values in critical fields.")

# Main streaming logic - Read from Bronze and transform to Silver
def bronze_to_silver_stream():
    bronze_stream = spark.readStream.format("delta").load(bronze_table_path)
    
    query = bronze_stream.writeStream \
        .foreachBatch(lambda df, _: (save_silver_table(transform_bronze_to_silver(df)), data_quality_checks(df))) \
        .option("checkpointLocation", "/mnt/delta/checkpoints/silver/") \
        .start()
    
    query.awaitTermination()

# Set up bronze table with Auto Loader and run the streaming logic
setup_bronze_table()
bronze_to_silver_stream()