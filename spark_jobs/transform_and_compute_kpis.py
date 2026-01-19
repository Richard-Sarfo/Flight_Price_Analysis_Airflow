"""
Spark Job 3: Transform data and compute KPIs, load to PostgreSQL
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, count, avg, round as spark_round, 
    when, month, dayofmonth, current_timestamp,
    row_number, desc, sum as spark_sum
)
import argparse
import sys

def create_spark_session(app_name="FlightDataTransformation"):
    """Create and configure Spark session"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def determine_peak_season(month_col, day_col):
    """
    Determine if a date falls in peak season for Bangladesh
    Peak seasons:
    - Eid-ul-Fitr: April (approximate)
    - Eid-ul-Adha: June-July (approximate)
    - Winter Holiday: December 15 - January 15
    - Summer Holiday: June 1 - July 15
    """
    return when(
        # Eid-ul-Fitr (April)
        (month_col == 4),
        "Eid_ul_Fitr"
    ).when(
        # Eid-ul-Adha / Summer Holiday (June-July)
        ((month_col == 6) | (month_col == 7)),
        "Eid_ul_Adha_Summer"
    ).when(
        # Winter Holiday (Dec 15 - Jan 15)
        ((month_col == 12) & (day_col >= 15)) | ((month_col == 1) & (day_col <= 15)),
        "Winter_Holiday"
    ).otherwise("Regular")

def is_peak_season_flag(season_col):
    """Flag whether season is peak or not"""
    return when(season_col != "Regular", True).otherwise(False)

def transform_and_compute_kpis(spark, args):
    """Main transformation and KPI computation logic"""
    print("=" * 60)
    print("STARTING TRANSFORMATION AND KPI COMPUTATION")
    print("=" * 60)
    
    # MySQL connection properties
    mysql_url = f"jdbc:mysql://{args.mysql_host}:{args.mysql_port}/{args.mysql_database}"
    mysql_properties = {
        "user": args.mysql_user,
        "password": args.mysql_password,
        "driver": "com.mysql.cj.jdbc.Driver",
        "useSSL": "false",
        "allowPublicKeyRetrieval": "true"
    }
    
    # PostgreSQL connection properties
    postgres_url = f"jdbc:postgresql://{args.postgres_host}:{args.postgres_port}/{args.postgres_database}"
    postgres_properties = {
        "user": args.postgres_user,
        "password": args.postgres_password,
        "driver": "org.postgresql.Driver"
    }
    
    # Read data from MySQL staging
    print(f"\nReading data from MySQL: {mysql_url}")
    df = spark.read \
        .jdbc(
            url=mysql_url,
            table="flight_data_staging",
            properties=mysql_properties
        )
    
    initial_count = df.count()
    print(f"Records loaded from staging: {initial_count}")
    
    # Filter valid records only
    df_valid = df.filter(
        (col("airline").isNotNull()) & 
        (col("source").isNotNull()) & 
        (col("destination").isNotNull()) & 
        (col("base_fare") >= 0) & 
        (col("total_fare") >= 0)
    )
    
    valid_count = df_valid.count()
    print(f"Valid records after filtering: {valid_count}")
    
    # Add season information
    print("\nAdding seasonal information...")
    df_transformed = df_valid.withColumn(
        "month_val", month(col("booking_date"))
    ).withColumn(
        "day_val", dayofmonth(col("booking_date"))
    ).withColumn(
        "season", determine_peak_season(col("month_val"), col("day_val"))
    ).withColumn(
        "is_peak_season", is_peak_season_flag(col("season"))
    ).withColumn(
        "processed_at", current_timestamp()
    ).drop("month_val", "day_val", "id", "created_at")
    
    # Show sample of transformed data
    print("\nSample of transformed data:")
    df_transformed.select(
        "airline", "source", "destination", "total_fare", "season", "is_peak_season"
    ).show(5, truncate=False)
    
    # Persist transformed data for multiple KPI computations
    df_transformed.cache()
    
    # Load main fact table to PostgreSQL
    print(f"\nLoading fact table to PostgreSQL: {postgres_url}")
    df_transformed.write \
        .jdbc(
            url=postgres_url,
            table="flight_data",
            mode="append",
            properties=postgres_properties
        )
    print(f"✓ Loaded {valid_count} records to flight_data table")
    
    # ========================================
    # KPI 1: Average Fare by Airline
    # ========================================
    print("\n" + "=" * 60)
    print("COMPUTING KPI 1: Average Fare by Airline")
    print("=" * 60)
    
    kpi_airline = df_transformed.groupBy("airline").agg(
        spark_round(avg("base_fare"), 2).alias("avg_base_fare"),
        spark_round(avg("total_fare"), 2).alias("avg_total_fare"),
        count("*").alias("booking_count")
    ).withColumn(
        "calculated_at", current_timestamp()
    ).orderBy(desc("booking_count"))
    
    airline_count = kpi_airline.count()
    print(f"Number of airlines analyzed: {airline_count}")
    
    print("\nTop 5 Airlines by Booking Volume:")
    kpi_airline.show(5, truncate=False)
    
    # Write to PostgreSQL
    kpi_airline.write \
        .jdbc(
            url=postgres_url,
            table="kpi_avg_fare_by_airline",
            mode="overwrite",
            properties=postgres_properties
        )
    print("✓ KPI 1 results loaded to PostgreSQL")
    
    # ========================================
    # KPI 2: Seasonal Fare Variation
    # ========================================
    print("\n" + "=" * 60)
    print("COMPUTING KPI 2: Seasonal Fare Variation")
    print("=" * 60)
    
    kpi_seasonal = df_transformed.groupBy("season", "is_peak_season").agg(
        spark_round(avg("total_fare"), 2).alias("avg_fare"),
        count("*").alias("booking_count")
    ).withColumn(
        "calculated_at", current_timestamp()
    ).orderBy(desc("booking_count"))
    
    print("\nSeasonal Analysis:")
    kpi_seasonal.show(truncate=False)
    
    # Calculate peak vs non-peak comparison
    peak_avg = kpi_seasonal.filter(col("is_peak_season") == True).agg(
        avg("avg_fare")
    ).collect()[0][0]
    
    non_peak_avg = kpi_seasonal.filter(col("is_peak_season") == False).agg(
        avg("avg_fare")
    ).collect()[0][0]
    
    if peak_avg and non_peak_avg:
        variation = ((peak_avg - non_peak_avg) / non_peak_avg) * 100
        print(f"\nPeak Season Average Fare: ${peak_avg:.2f}")
        print(f"Non-Peak Season Average Fare: ${non_peak_avg:.2f}")
        print(f"Price Variation: {variation:+.2f}%")
    
    # Write to PostgreSQL
    kpi_seasonal.write \
        .jdbc(
            url=postgres_url,
            table="kpi_seasonal_variation",
            mode="overwrite",
            properties=postgres_properties
        )
    print("✓ KPI 2 results loaded to PostgreSQL")
    
    # ========================================
    # KPI 3: Most Popular Routes
    # ========================================
    print("\n" + "=" * 60)
    print("COMPUTING KPI 3: Most Popular Routes")
    print("=" * 60)
    
    kpi_routes = df_transformed.groupBy("source", "destination").agg(
        count("*").alias("booking_count"),
        spark_round(avg("total_fare"), 2).alias("avg_fare")
    ).orderBy(desc("booking_count"))
    
    # Add ranking
    window_spec = Window.orderBy(desc("booking_count"))
    kpi_routes_ranked = kpi_routes.withColumn(
        "route_rank", row_number().over(window_spec)
    ).withColumn(
        "calculated_at", current_timestamp()
    )
    
    routes_count = kpi_routes_ranked.count()
    print(f"Total unique routes: {routes_count}")
    
    print("\nTop 10 Most Popular Routes:")
    kpi_routes_ranked.filter(col("route_rank") <= 10).show(10, truncate=False)
    
    # Write to PostgreSQL
    kpi_routes_ranked.write \
        .jdbc(
            url=postgres_url,
            table="kpi_popular_routes",
            mode="overwrite",
            properties=postgres_properties
        )
    print("✓ KPI 3 results loaded to PostgreSQL")
    
    # ========================================
    # Additional Analysis: Airline Market Share
    # ========================================
    print("\n" + "=" * 60)
    print("ADDITIONAL ANALYSIS: Airline Market Share")
    print("=" * 60)
    
    total_bookings = df_transformed.count()
    
    market_share = df_transformed.groupBy("airline").agg(
        count("*").alias("bookings")
    ).withColumn(
        "market_share_pct", spark_round((col("bookings") / total_bookings) * 100, 2)
    ).orderBy(desc("market_share_pct"))
    
    print("\nAirline Market Share:")
    market_share.show(10, truncate=False)
    
    # Unpersist cached dataframe
    df_transformed.unpersist()
    
    # Summary
    print("\n" + "=" * 60)
    print("TRANSFORMATION AND KPI COMPUTATION SUMMARY")
    print("=" * 60)
    print(f"Records Processed: {valid_count}")
    print(f"Airlines Analyzed: {airline_count}")
    print(f"Unique Routes: {routes_count}")
    print(f"Seasons Identified: {kpi_seasonal.count()}")
    print("=" * 60)
    print("All KPIs successfully computed and loaded to PostgreSQL")
    print("=" * 60)
    
    return {
        'records_processed': valid_count,
        'airlines_count': airline_count,
        'routes_count': routes_count
    }

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(
        description='Transform flight data and compute KPIs'
    )
    parser.add_argument('--mysql-host', required=True, help='MySQL host')
    parser.add_argument('--mysql-port', required=True, help='MySQL port')
    parser.add_argument('--mysql-database', required=True, help='MySQL database')
    parser.add_argument('--mysql-user', required=True, help='MySQL user')
    parser.add_argument('--mysql-password', required=True, help='MySQL password')
    parser.add_argument('--postgres-host', required=True, help='PostgreSQL host')
    parser.add_argument('--postgres-port', required=True, help='PostgreSQL port')
    parser.add_argument('--postgres-database', required=True, help='PostgreSQL database')
    parser.add_argument('--postgres-user', required=True, help='PostgreSQL user')
    parser.add_argument('--postgres-password', required=True, help='PostgreSQL password')
    
    args = parser.parse_args()
    
    spark = None
    try:
        spark = create_spark_session()
        results = transform_and_compute_kpis(spark, args)
        print(f"\nFinal Status: SUCCESS")
        print(f"Records Processed: {results['records_processed']}")
        sys.exit(0)
    except Exception as e:
        print(f"\nERROR during transformation: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()