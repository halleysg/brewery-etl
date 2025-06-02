from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/spark/jobs')  # Add path to import from
# Import your function
from fetch_brewery_data import fetch_brewery_data
from brewery_medallion_etl import brewery_medallion_etl

# Default arguments
default_args = {
    "owner": "abi",
    "depends_on_past": False,
    "start_date": datetime(2025, 5, 27),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Create DAG
dag = DAG(
    "brewery_etl_pipeline",
    default_args=default_args,
    description="Fetch brewery data from API and process through medallion architecture",
    schedule_interval="@daily",
    catchup=False,
    tags=["brewery", "etl"]
)

# Task 1: Create directories
create_dirs_task = BashOperator(
    task_id="create_directories",
    bash_command="mkdir -p /spark/data/{raw,bronze,silver,gold}/breweries",
    dag=dag
)

# Task 2: Fetch data from API
fetch_api_data_task = PythonOperator(
    task_id='fetch_brewery_data',
    python_callable=fetch_brewery_data,
    dag=dag
)

# Task 3: Run medallion ETL pipeline
medallion_etl_task = PythonOperator(
    task_id="run_medallion_etl",
    python_callable=brewery_medallion_etl,
    dag=dag
)

# Define task dependencies
create_dirs_task >> fetch_api_data_task >> medallion_etl_task