from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import logging
import pendulum

# Set the timezone to EEST
local_tz = pendulum.timezone('Europe/Helsinki')

# Fetch variables from Airflow UI
RAW_DATA_PATH = Variable.get("RAW_DATA_PATH", default_var="/opt/airflow/raw_data/AB_NYC_2019.csv")
TRANSFORMED_DATA_PATH = Variable.get("TRANSFORMED_DATA_PATH", default_var="/opt/airflow/transformed_data/transformed_nyc_airbnb.csv")
DB_CONN_ID = Variable.get("DB_CONN_ID", default_var="postgres_airflow_etl")
TABLE_NAME = Variable.get("TABLE_NAME", default_var="airbnb_listings")
LOG_FILE_PATH = Variable.get("LOG_FILE_PATH", default_var="/opt/airflow/logs/airflow_failures.log")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'nyc_airbnb_etl',
    default_args=default_args,
    description='ETL pipeline for NYC Airbnb data',
    schedule_interval='0 0 * * *',  # Schedule the DAG to run daily at midnight
    start_date=datetime(2024, 8, 30, tzinfo=local_tz),
    catchup=False,
)

# Task 1: Ingest data from CSV file
def ingest_data(**kwargs):
    """Read data from the CSV file and store it in XCom."""
    try:
        df = pd.read_csv(RAW_DATA_PATH)
        kwargs['ti'].xcom_push(key='raw_data', value=df.to_dict())
    except FileNotFoundError:
        logging.error(f"File not found: {RAW_DATA_PATH}")
        raise

ingest_task = PythonOperator(
    task_id='ingest_data',
    python_callable=ingest_data,
    provide_context=True,
    dag=dag,
)

# Task 2: Transform the data
def transform_data(**kwargs):
    """Perform data transformation: filter, convert, and handle missing values."""
    raw_data = kwargs['ti'].xcom_pull(key='raw_data')
    if raw_data is None:
        logging.error("No data to transform")
        raise

    df = pd.DataFrame(raw_data)
    
    # Filter out rows where price is 0 or negative
    df = df[df['price'] > 0]
    
    # Convert last_review to datetime and handle missing values
    df['last_review'] = pd.to_datetime(df['last_review'], errors='coerce')
    df['last_review'].fillna(df['last_review'].min(), inplace=True)
    
    # Fill missing reviews_per_month with 0
    df['reviews_per_month'] = df['reviews_per_month'].fillna(0)
    
    # Drop rows with missing latitude or longitude
    df = df.dropna(subset=['latitude', 'longitude'])

    # Clip latitude and longitude to fit within DECIMAL(9,6)
    df['latitude'] = df['latitude'].clip(lower=-999.999999, upper=999.999999)
    df['longitude'] = df['longitude'].clip(lower=-999.999999, upper=999.999999)
    
    # Ensure that values are within the valid range for DECIMAL(3,2)
    df['reviews_per_month'] = df['reviews_per_month'].clip(-9.99, 9.99)

    # Ensure DataFrame doesn't have duplicates based on primary key column
    df = df.drop_duplicates(subset=['id'])
    
    # Save transformed data to CSV
    df.to_csv(TRANSFORMED_DATA_PATH, index=False)
    
    # Push transformed data to XCom
    transformed_data = pd.read_csv(TRANSFORMED_DATA_PATH)
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data.to_dict())

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

# Task 3: Load data into PostgreSQL
def load_data_to_postgres(**kwargs):
    """Load transformed data into the PostgreSQL table."""
    transformed_data = kwargs['ti'].xcom_pull(key='transformed_data')
    if transformed_data is None:
        logging.error("No data to load into PostgreSQL")
        raise

    df = pd.DataFrame(transformed_data)
    
    # Create PostgreSQL connection
    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
                        
    # Clear the table before insertion (use with caution)
    pg_hook.run(f"TRUNCATE TABLE {TABLE_NAME}")
                        
    # Insert records
    tuples = [tuple(x) for x in df.to_numpy()]
    pg_hook.insert_rows(TABLE_NAME, tuples, target_fields=None, commit_every=1000)

load_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    provide_context=True,
    dag=dag,
)

# Task 4: Data Quality Check
def data_quality_checks(**kwargs):
    """Perform data quality checks on the loaded data in PostgreSQL."""

    # Retrieve the actual row count from PostgreSQL table
    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    sql_query = f"SELECT COUNT(*) FROM {TABLE_NAME};"
    result = pg_hook.get_first(sql_query)
    table_count = result[0]

    # Retrieve the expected row count from XCom
    transformed_data = kwargs['ti'].xcom_pull(key='transformed_data')
    expected_row_count = len(pd.DataFrame(transformed_data))
 
    # Check for matching number of records
    if table_count != expected_row_count:
        logging.error("Record count does not match between the transformed data and PostgreSQL table.")
        return 'fail_task'

    # Validate no NULL values in specified columns
    sql_query = f"""
        SELECT COUNT(*)
        FROM {TABLE_NAME}
        WHERE price IS NULL OR minimum_nights IS NULL OR availability_365 IS NULL;
    """
    result = pg_hook.get_first(sql_query)
    null_count = result[0]

    if null_count > 0:
        logging.error("NULL values found in critical columns of the PostgreSQL table.")
        return 'fail_task'

    return 'success_task'

data_quality_task = BranchPythonOperator(
    task_id='data_quality_checks',
    python_callable=data_quality_checks,
    provide_context=True,
    dag=dag,
)

# Task 5: Handle success
def success_task(**kwargs):
    logging.info("Data quality checks passed. Pipeline succeeded.")

success_task = PythonOperator(
    task_id='success_task',
    python_callable=success_task,
    provide_context=True,
    dag=dag,
)

# Task 6: Handle failure
def fail_task(**kwargs):
    logging.error("Data quality checks failed. Stopping pipeline.")

fail_task = PythonOperator(
    task_id='fail_task',
    python_callable=fail_task,
    provide_context=True,
    dag=dag,
)

# Log failures
def log_failure(context):
    dag_id = context['dag_run'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    log_message = f"Task {task_id} in DAG {dag_id} failed on {execution_date}"
    logging.error(log_message)
    with open(LOG_FILE_PATH, 'a') as f:
        f.write(log_message + '\n')

# Set the failure callback for each task
for task in dag.tasks:
    task.on_failure_callback = log_failure

# Define task dependencies
ingest_task >> transform_task >> load_task >> data_quality_task
data_quality_task >> [success_task, fail_task]
