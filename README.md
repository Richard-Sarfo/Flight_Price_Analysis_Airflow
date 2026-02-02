# Flight Price Analysis Pipeline

This project is an Airflow-based data pipeline that analyzes flight price data from Bangladesh. It uses **Docker** to orchestrate the environment, **MySQL** for staging raw data, and **PostgreSQL** for storing analytical results.

## Project Overview

The pipeline performs the following steps:
1.  **Ingestion**: Reads a CSV file (`Flight_Price_Dataset_of_Bangladesh.csv`) and loads it into a MySQL staging database.
2.  **Validation**: Performs data quality checks on the raw data in MySQL.
3.  **Transformation & KPIs**: Cleans the data, calculates Key Performance Indicators (KPIs), and loads the results into a PostgreSQL analytics database.

## Prerequisites

*   Docker
*   Docker Compose

## Setup & Execution

1.  **Start the services**:
    Run the following command in the root directory of the project:
    ```bash
    docker-compose up -d
    ```
    This will start Airflow, MySQL, and PostgreSQL containers.

2.  **Access Airflow UI**:
    Open your browser and navigate to `http://localhost:8080`.
    *   **Username**: `airflow`
    *   **Password**: `airflow`

3.  **Trigger the DAG**:
    Enable and trigger the `flight_price_analysis` DAG to start the pipeline.

## Database Access

You can access the databases directly via the command line using Docker.

### 1. MySQL (Staging Database)
To access the staging database where raw data is loaded:
```bash
docker exec -it airflow-mysql-staging-1 mysql -u airflow -p
```
*   **Password**: `airflow`
*   (Or use user `root` with password `rootpassword`)

**Useful Commands:**
*   Show tables: `SHOW TABLES;`
*   View data: `SELECT * FROM flight_staging LIMIT 10;`
*   Exit: `exit`

### 2. PostgreSQL (Analytics Database)
To access the analytics database where final results are stored:
```bash
docker exec -it airflow-postgres-analytics-1 psql -U analytics -d flight_analytics
```
*   **Password**: `analytics`

**Useful Commands:**
*   Show tables: `\dt`
*   View data: `SELECT * FROM flight_analytics LIMIT 10;`
*   Exit: `\q`

## Project Structure

*   `dags/`: Contains the Airflow DAG (`flight_price_analysis_dag.py`) and Python scripts/jobs.
*   `data/`: Directory for input datasets (mapped to `/opt/data` in containers).
*   `docker-compose.yml`: Configuration for all Docker services.
*   `logs/`: Airflow logs.
