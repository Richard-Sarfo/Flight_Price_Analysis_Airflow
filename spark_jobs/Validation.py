"""
Spark Job 2: Validate data quality in MySQL staging
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, sum as spark_sum
import argparse
import sys

def create_spark_session(app_name="FlightDataValidation"):
    """Create and configure Spark session"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def validate_data(spark, args):
    """Main validation logic"""
    print("=" * 60)
    print("STARTING DATA VALIDATION")
    print("=" * 60)
    
    # Prepare MySQL connection
    mysql_url = f"jdbc:mysql://{args.mysql_host}:{args.mysql_port}/{args.mysql_database}"
    mysql_properties = {
        "user": args.mysql_user,
        "password": args.mysql_password,
        "driver": "com.mysql.cj.jdbc.Driver",
        "useSSL": "false",
        "allowPublicKeyRetrieval": "true"
    }
    
    # Read data from MySQL
    print(f"Reading data from MySQL: {mysql_url}")
    df = spark.read \
        .jdbc(
            url=mysql_url,
            table="flight_data_staging",
            properties=mysql_properties
        )
    
    total_rows = df.count()
    print(f"\nTotal rows in staging: {total_rows}")
    
    if total_rows == 0:
        raise ValueError("No data found in staging table!")
    
    # Validation checks
    validation_results = {}
    
    # 1. Check for required columns
    required_columns = ['airline', 'source', 'destination', 'base_fare', 
                       'tax_and_surcharge', 'total_fare']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    print("\n✓ All required columns present")
    
    # 2. Check for null values in critical fields
    print("\nNull Value Analysis:")
    print("-" * 60)
    
    null_airline = df.filter(
        col("airline").isNull() | (col("airline") == "")
    ).count()
    print(f"Null/Empty Airlines: {null_airline} ({null_airline/total_rows*100:.2f}%)")
    validation_results['null_airlines'] = null_airline
    
    null_source = df.filter(
        col("source").isNull() | (col("source") == "")
    ).count()
    print(f"Null/Empty Source: {null_source} ({null_source/total_rows*100:.2f}%)")
    validation_results['null_source'] = null_source
    
    null_destination = df.filter(
        col("destination").isNull() | (col("destination") == "")
    ).count()
    print(f"Null/Empty Destination: {null_destination} ({null_destination/total_rows*100:.2f}%)")
    validation_results['null_destination'] = null_destination
    
    null_base_fare = df.filter(col("base_fare").isNull()).count()
    print(f"Null Base Fare: {null_base_fare} ({null_base_fare/total_rows*100:.2f}%)")
    validation_results['null_base_fare'] = null_base_fare
    
    null_total_fare = df.filter(col("total_fare").isNull()).count()
    print(f"Null Total Fare: {null_total_fare} ({null_total_fare/total_rows*100:.2f}%)")
    validation_results['null_total_fare'] = null_total_fare
    
    # 3. Check for negative values
    print("\nNegative Value Analysis:")
    print("-" * 60)
    
    negative_base_fare = df.filter(col("base_fare") < 0).count()
    print(f"Negative Base Fare: {negative_base_fare}")
    validation_results['negative_base_fare'] = negative_base_fare
    
    negative_tax = df.filter(col("tax_and_surcharge") < 0).count()
    print(f"Negative Tax & Surcharge: {negative_tax}")
    validation_results['negative_tax'] = negative_tax
    
    negative_total_fare = df.filter(col("total_fare") < 0).count()
    print(f"Negative Total Fare: {negative_total_fare}")
    validation_results['negative_total_fare'] = negative_total_fare
    
    # 4. Check for data consistency
    print("\nData Consistency Checks:")
    print("-" * 60)
    
    # Check if total_fare = base_fare + tax_and_surcharge (with tolerance)
    inconsistent_totals = df.filter(
        (col("base_fare").isNotNull()) & 
        (col("tax_and_surcharge").isNotNull()) & 
        (col("total_fare").isNotNull())
    ).filter(
        ~(
            (col("total_fare") >= (col("base_fare") + col("tax_and_surcharge") - 0.01)) & 
            (col("total_fare") <= (col("base_fare") + col("tax_and_surcharge") + 0.01))
        )
    ).count()
    
    print(f"Inconsistent Total Fare calculations: {inconsistent_totals}")
    validation_results['inconsistent_totals'] = inconsistent_totals
    
    # 5. Statistical summary
    print("\nStatistical Summary:")
    print("-" * 60)
    
    df.select(
        "base_fare", "tax_and_surcharge", "total_fare"
    ).describe().show()
    
    # 6. Check for duplicates
    duplicate_count = df.groupBy(
        "airline", "source", "destination", "base_fare", "total_fare"
    ).count().filter(col("count") > 1).count()
    
    print(f"Potential duplicate records: {duplicate_count}")
    validation_results['duplicates'] = duplicate_count
    
    # 7. Value distribution
    print("\nAirline Distribution:")
    print("-" * 60)
    df.groupBy("airline").count().orderBy(col("count").desc()).show(10)
    
    print("\nTop 10 Routes:")
    print("-" * 60)
    df.groupBy("source", "destination").count() \
        .orderBy(col("count").desc()).show(10)
    
    # Calculate overall quality score
    total_issues = (
        null_airline + null_source + null_destination + 
        null_base_fare + null_total_fare +
        negative_base_fare + negative_tax + negative_total_fare +
        inconsistent_totals
    )
    
    valid_rows = total_rows - total_issues
    quality_score = (valid_rows / total_rows) * 100
    
    validation_results['total_rows'] = total_rows
    validation_results['valid_rows'] = valid_rows
    validation_results['quality_score'] = quality_score
    
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)
    print(f"Total Rows: {total_rows}")
    print(f"Valid Rows: {valid_rows}")
    print(f"Invalid Rows: {total_issues}")
    print(f"Data Quality Score: {quality_score:.2f}%")
    print("=" * 60)
    
    # Fail if quality score is too low
    if quality_score < 90:
        print(f"\n⚠️  WARNING: Data quality score ({quality_score:.2f}%) is below threshold (90%)")
        print("Validation results:")
        for key, value in validation_results.items():
            print(f"  {key}: {value}")
        
        raise ValueError(f"Data quality score too low: {quality_score:.2f}%")
    
    print("\n✓ Data validation PASSED")
    print("=" * 60)
    
    return validation_results

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Validate flight data in MySQL')
    parser.add_argument('--mysql-host', required=True, help='MySQL host')
    parser.add_argument('--mysql-port', required=True, help='MySQL port')
    parser.add_argument('--mysql-database', required=True, help='MySQL database')
    parser.add_argument('--mysql-user', required=True, help='MySQL user')
    parser.add_argument('--mysql-password', required=True, help='MySQL password')
    
    args = parser.parse_args()
    
    spark = None
    try:
        spark = create_spark_session()
        results = validate_data(spark, args)
        print(f"\nFinal Status: SUCCESS - Quality Score: {results['quality_score']:.2f}%")
        sys.exit(0)
    except Exception as e:
        print(f"\nERROR during validation: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()