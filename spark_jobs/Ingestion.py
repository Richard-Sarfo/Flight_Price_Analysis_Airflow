"""
Spark Job 1: Ingest CSV data and load to MySQL staging
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, coalesce, lit, current_timestamp,
    when, regexp_replace
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DecimalType, DateType, TimestampType
)
import argparse
import sys

def create_spark_session(app_name="FlightDataIngestion"):
    """Create and configure Spark session"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def clean_column_name(col_name):
    """Clean column names for database compatibility"""
    return col_name.strip().lower().replace(' ', '_').replace('&', 'and')

def ingest_csv_to_mysql(spark, args):
    """Main ingestion logic"""
    print("=" * 60)
    print("STARTING DATA INGESTION")
    print("=" * 60)
    
    # Define schema for better type inference
    schema = StructType([
        StructField("Airline", StringType(), True),
        StructField("Source", StringType(), True),
        StructField("Destination", StringType(), True),
        StructField("Base Fare", StringType(), True),
        StructField("Tax & Surcharge", StringType(), True),
        StructField("Total Fare", StringType(), True),
        StructField("Booking Date", StringType(), True),
    ])
    
    # Read CSV with Spark
    print(f"Reading CSV from: {args.input_path}")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .schema(schema) \
        .csv(args.input_path)
    
    initial_count = df.count()
    print(f"Initial record count: {initial_count}")
    
    # Clean column names
    for old_col in df.columns:
        new_col = clean_column_name(old_col)
        df = df.withColumnRenamed(old_col, new_col)
    
    # Data cleaning and transformation
    df_cleaned = df \
        .withColumn("airline", trim(coalesce(col("airline"), lit("Unknown")))) \
        .withColumn("source", trim(coalesce(col("source"), lit("Unknown")))) \
        .withColumn("destination", trim(coalesce(col("destination"), lit("Unknown")))) \
        .withColumn("base_fare", 
                   regexp_replace(col("base_fare"), "[^0-9.]", "").cast(DecimalType(10, 2))) \
        .withColumn("tax_and_surcharge", 
                   regexp_replace(col("tax_and_surcharge"), "[^0-9.]", "").cast(DecimalType(10, 2))) \
        .withColumn("total_fare", 
                   regexp_replace(col("total_fare"), "[^0-9.]", "").cast(DecimalType(10, 2)))
    
    # Calculate total fare if missing or incorrect
    df_cleaned = df_cleaned.withColumn(
        "total_fare",
        when(
            (col("total_fare").isNull()) | (col("total_fare") == 0),
            col("base_fare") + col("tax_and_surcharge")
        ).otherwise(col("total_fare"))
    )
    
    # Handle booking date
    if "booking_date" in df_cleaned.columns:
        df_cleaned = df_cleaned.withColumn(
            "booking_date",
            coalesce(col("booking_date").cast(DateType()), lit(None).cast(DateType()))
        )
    else:
        df_cleaned = df_cleaned.withColumn("booking_date", lit(None).cast(DateType()))
    
    # Add audit column
    df_cleaned = df_cleaned.withColumn("created_at", current_timestamp())
    
    # Remove rows with all null values in critical fields
    df_cleaned = df_cleaned.filter(
        col("airline").isNotNull() | 
        col("source").isNotNull() | 
        col("destination").isNotNull()
    )
    
    cleaned_count = df_cleaned.count()
    print(f"Cleaned record count: {cleaned_count}")
    print(f"Records removed during cleaning: {initial_count - cleaned_count}")
    
    # Show sample data
    print("\nSample of cleaned data:")
    df_cleaned.show(5, truncate=False)
    
    # Prepare MySQL connection properties
    mysql_url = f"jdbc:mysql://{args.mysql_host}:{args.mysql_port}/{args.mysql_database}"
    mysql_properties = {
        "user": args.mysql_user,
        "password": args.mysql_password,
        "driver": "com.mysql.cj.jdbc.Driver",
        "rewriteBatchedStatements": "true",
        "useSSL": "false",
        "allowPublicKeyRetrieval": "true"
    }
    
    # Write to MySQL
    print(f"\nWriting to MySQL: {mysql_url}")
    df_cleaned.write \
        .jdbc(
            url=mysql_url,
            table="flight_data_staging",
            mode="append",
            properties=mysql_properties
        )
    
    print(f"Successfully loaded {cleaned_count} records to MySQL staging")
    print("=" * 60)
    print("DATA INGESTION COMPLETED")
    print("=" * 60)
    
    return cleaned_count

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Ingest flight data CSV to MySQL')
    parser.add_argument('--input-path', required=True, help='Path to input CSV file')
    parser.add_argument('--mysql-host', required=True, help='MySQL host')
    parser.add_argument('--mysql-port', required=True, help='MySQL port')
    parser.add_argument('--mysql-database', required=True, help='MySQL database')
    parser.add_argument('--mysql-user', required=True, help='MySQL user')
    parser.add_argument('--mysql-password', required=True, help='MySQL password')
    
    args = parser.parse_args()
    
    spark = None
    try:
        spark = create_spark_session()
        records_loaded = ingest_csv_to_mysql(spark, args)
        print(f"\nFinal Status: SUCCESS - {records_loaded} records loaded")
        sys.exit(0)
    except Exception as e:
        print(f"\nERROR during ingestion: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()