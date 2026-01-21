"""
Airflow DAG: Flight Price Analysis Pipeline with Pandas
Processes Bangladesh flight price data using Pandas for data processing
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import subprocess
import sys

# Default arguments
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def log_start(**context):
    """Log pipeline start"""
    logging.info("=" * 60)
    logging.info("FLIGHT PRICE ANALYSIS PIPELINE STARTED")
    logging.info(f"Execution Date: {context['execution_date']}")
    logging.info("=" * 60)
    return "Pipeline started"

def run_ingestion(**context):
    """Run the ingestion script to load CSV data to MySQL"""
    logging.info("Starting CSV ingestion to MySQL...")
    
    result = subprocess.run(
        [
            sys.executable,
            '/opt/airflow/dags/jobs/Ingestion.py',
            '--input-path', '/opt/data/Flight_Price_Dataset_of_Bangladesh.csv',
            '--mysql-host', 'mysql-staging',
            '--mysql-port', '3306',
            '--mysql-database', 'flight_staging',
            '--mysql-user', 'airflow',
            '--mysql-password', 'airflow',
        ],
        capture_output=True,
        text=True
    )
    
    logging.info(result.stdout)
    if result.returncode != 0:
        logging.error(result.stderr)
        raise Exception(f"Ingestion failed: {result.stderr}")
    
    return "Ingestion completed"

def run_validation(**context):
    """Run the validation script to check data quality"""
    logging.info("Starting data validation...")
    
    result = subprocess.run(
        [
            sys.executable,
            '/opt/airflow/dags/jobs/Validation.py',
            '--mysql-host', 'mysql-staging',
            '--mysql-port', '3306',
            '--mysql-database', 'flight_staging',
            '--mysql-user', 'airflow',
            '--mysql-password', 'airflow',
        ],
        capture_output=True,
        text=True
    )
    
    logging.info(result.stdout)
    if result.returncode != 0:
        logging.error(result.stderr)
        raise Exception(f"Validation failed: {result.stderr}")
    
    return "Validation completed"

def run_transform_and_kpis(**context):
    """Run the transformation and KPI computation script"""
    logging.info("Starting transformation and KPI computation...")
    
    result = subprocess.run(
        [
            sys.executable,
            '/opt/airflow/dags/jobs/transform_and_compute_kpis.py',
            '--mysql-host', 'mysql-staging',
            '--mysql-port', '3306',
            '--mysql-database', 'flight_staging',
            '--mysql-user', 'airflow',
            '--mysql-password', 'airflow',
            '--postgres-host', 'postgres-analytics',
            '--postgres-port', '5432',
            '--postgres-database', 'flight_analytics',
            '--postgres-user', 'analytics',
            '--postgres-password', 'analytics',
        ],
        capture_output=True,
        text=True
    )
    
    logging.info(result.stdout)
    if result.returncode != 0:
        logging.error(result.stderr)
        raise Exception(f"Transformation failed: {result.stderr}")
    
    return "Transformation completed"

def log_completion(**context):
    """Generate final pipeline summary"""
    logging.info("=" * 60)
    logging.info("FLIGHT PRICE ANALYSIS PIPELINE COMPLETED")
    logging.info(f"Completion Date: {datetime.now()}")
    logging.info("=" * 60)
    
    summary = """
    Pipeline execution completed successfully.
    All Pandas jobs finished without errors.
    Data available in PostgreSQL analytics database.
    """
    logging.info(summary)
    return "Pipeline completed"


# Define DAG
with DAG(
    'flight_price_analysis',
    default_args=default_args,
    description='Pandas-based pipeline for Bangladesh flight price analysis',
    schedule_interval='@daily',
    catchup=False,
    tags=['flight', 'pandas', 'analysis', 'bangladesh'],
) as dag:
    
    # Task 0: Log start
    start_task = PythonOperator(
        task_id='log_pipeline_start',
        python_callable=log_start,
        provide_context=True,
    )
    
    # Task 1: Ingest CSV and load to MySQL
    ingest_task = PythonOperator(
        task_id='ingest_csv_to_mysql',
        python_callable=run_ingestion,
        provide_context=True,
    )
    
    # Task 2: Validate data quality
    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=run_validation,
        provide_context=True,
    )
    
    # Task 3: Transform and compute KPIs
    transform_task = PythonOperator(
        task_id='transform_and_compute_kpis',
        python_callable=run_transform_and_kpis,
        provide_context=True,
    )
    
    # Task 4: Log completion
    complete_task = PythonOperator(
        task_id='log_pipeline_completion',
        python_callable=log_completion,
        provide_context=True,
    )
    
    # Define task dependencies
    start_task >> ingest_task >> validate_task >> transform_task >> complete_task
