"""
Airflow DAG: Flight Price Analysis Pipeline with Apache Spark
Processes Bangladesh flight price data using Spark for distributed processing
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import logging

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

def log_completion(**context):
    """Generate final pipeline summary"""
    logging.info("=" * 60)
    logging.info("FLIGHT PRICE ANALYSIS PIPELINE COMPLETED")
    logging.info(f"Completion Date: {datetime.now()}")
    logging.info("=" * 60)
    
    # In production, you would pull metrics from XCom here
    summary = """
    Pipeline execution completed successfully.
    All Spark jobs finished without errors.
    Data available in PostgreSQL analytics database.
    """
    logging.info(summary)
    return "Pipeline completed"


# Define DAG
with DAG(
    'flight_price_analysis_spark',
    default_args=default_args,
    description='Spark-based pipeline for Bangladesh flight price analysis',
    schedule_interval='@daily',
    catchup=False,
    tags=['flight', 'spark', 'analysis', 'bangladesh'],
) as dag:
    
    # Task 0: Log start
    start_task = PythonOperator(
        task_id='log_pipeline_start',
        python_callable=log_start,
        provide_context=True,
    )
    
    # Task 1: Spark job to ingest CSV and load to MySQL
    ingest_task = SparkSubmitOperator(
        task_id='spark_ingest_csv_to_mysql',
        application='/opt/airflow/dags/spark_jobs/Ingestion.py',
        conn_id='spark_default',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.executor.memory': '2g',
            'spark.executor.cores': '2',
            'spark.driver.memory': '1g',
        },
        jars='/opt/airflow/spark/jars/mysql-connector-java-8.0.33.jar',
        application_args=[
            '--input-path', '/opt/data/Flight_Price_Dataset_of_Bangladesh.csv',
            '--mysql-host', 'mysql-staging',
            '--mysql-port', '3306',
            '--mysql-database', 'flight_staging',
            '--mysql-user', 'airflow',
            '--mysql-password', 'airflow',
        ],
        verbose=True,
    )
    
    # Task 2: Spark job for data validation
    validate_task = SparkSubmitOperator(
        task_id='spark_validate_data',
        application='/opt/airflow/dags/spark_jobs/Validation.py',
        conn_id='spark_default',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.executor.memory': '2g',
            'spark.executor.cores': '2',
            'spark.driver.memory': '1g',
        },
        jars='/opt/airflow/spark/jars/mysql-connector-java-8.0.33.jar',
        application_args=[
            '--mysql-host', 'mysql-staging',
            '--mysql-port', '3306',
            '--mysql-database', 'flight_staging',
            '--mysql-user', 'airflow',
            '--mysql-password', 'airflow',
        ],
        verbose=True,
    )
    
    # Task 3: Spark job for transformation and KPI computation
    transform_task = SparkSubmitOperator(
        task_id='spark_transform_and_compute_kpis',
        application='/opt/airflow/dags/spark_jobs/transform_and_compute_kpis.py',
        conn_id='spark_default',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.executor.memory': '2g',
            'spark.executor.cores': '2',
            'spark.driver.memory': '1g',
        },
        jars='/opt/airflow/spark/jars/mysql-connector-java-8.0.33.jar,/opt/airflow/spark/jars/postgresql-42.6.0.jar',
        application_args=[
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
        verbose=True,
    )
    
    # Task 4: Log completion
    complete_task = PythonOperator(
        task_id='log_pipeline_completion',
        python_callable=log_completion,
        provide_context=True,
    )
    
    # Define task dependencies
    start_task >> ingest_task >> validate_task >> transform_task >> complete_task