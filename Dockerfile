FROM apache/airflow:2.7.1-python3.9

USER airflow

# Install MySQL and Postgres providers with Pandas for data processing
RUN pip install --no-cache-dir \
    apache-airflow-providers-mysql \
    apache-airflow-providers-postgres \
    pandas \
    sqlalchemy \
    pymysql \
    psycopg2-binary
