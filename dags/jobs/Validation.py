"""
Job 2: Validate data quality in MySQL staging (Pandas version)
"""
import pandas as pd
from sqlalchemy import create_engine
import argparse
import sys

def validate_data(args):
    """Main validation logic"""
    print("=" * 60)
    print("STARTING DATA VALIDATION")
    print("=" * 60)
    
    # Create MySQL connection
    mysql_url = f"mysql+pymysql://{args.mysql_user}:{args.mysql_password}@{args.mysql_host}:{args.mysql_port}/{args.mysql_database}"
    engine = create_engine(mysql_url)
    
    # Read data from MySQL
    print(f"Reading data from MySQL: {args.mysql_host}:{args.mysql_port}/{args.mysql_database}")
    df = pd.read_sql("SELECT * FROM flight_data_staging", con=engine)
    
    total_rows = len(df)
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
    
    null_airline = df['airline'].isna().sum() + (df['airline'] == '').sum()
    print(f"Null/Empty Airlines: {null_airline} ({null_airline/total_rows*100:.2f}%)")
    validation_results['null_airlines'] = int(null_airline)
    
    null_source = df['source'].isna().sum() + (df['source'] == '').sum()
    print(f"Null/Empty Source: {null_source} ({null_source/total_rows*100:.2f}%)")
    validation_results['null_source'] = int(null_source)
    
    null_destination = df['destination'].isna().sum() + (df['destination'] == '').sum()
    print(f"Null/Empty Destination: {null_destination} ({null_destination/total_rows*100:.2f}%)")
    validation_results['null_destination'] = int(null_destination)
    
    null_base_fare = df['base_fare'].isna().sum()
    print(f"Null Base Fare: {null_base_fare} ({null_base_fare/total_rows*100:.2f}%)")
    validation_results['null_base_fare'] = int(null_base_fare)
    
    null_total_fare = df['total_fare'].isna().sum()
    print(f"Null Total Fare: {null_total_fare} ({null_total_fare/total_rows*100:.2f}%)")
    validation_results['null_total_fare'] = int(null_total_fare)
    
    # 3. Check for negative values
    print("\nNegative Value Analysis:")
    print("-" * 60)
    
    negative_base_fare = (df['base_fare'] < 0).sum()
    print(f"Negative Base Fare: {negative_base_fare}")
    validation_results['negative_base_fare'] = int(negative_base_fare)
    
    negative_tax = (df['tax_and_surcharge'] < 0).sum()
    print(f"Negative Tax & Surcharge: {negative_tax}")
    validation_results['negative_tax'] = int(negative_tax)
    
    negative_total_fare = (df['total_fare'] < 0).sum()
    print(f"Negative Total Fare: {negative_total_fare}")
    validation_results['negative_total_fare'] = int(negative_total_fare)
    
    # 4. Check for data consistency
    print("\nData Consistency Checks:")
    print("-" * 60)
    
    # Check if total_fare = base_fare + tax_and_surcharge (with tolerance)
    df_check = df.dropna(subset=['base_fare', 'tax_and_surcharge', 'total_fare'])
    expected_total = df_check['base_fare'] + df_check['tax_and_surcharge']
    inconsistent_totals = ((df_check['total_fare'] - expected_total).abs() > 0.01).sum()
    
    print(f"Inconsistent Total Fare calculations: {inconsistent_totals}")
    validation_results['inconsistent_totals'] = int(inconsistent_totals)
    
    # 5. Statistical summary
    print("\nStatistical Summary:")
    print("-" * 60)
    print(df[['base_fare', 'tax_and_surcharge', 'total_fare']].describe())
    
    # 6. Check for duplicates
    duplicate_count = df.duplicated(
        subset=['airline', 'source', 'destination', 'base_fare', 'total_fare']
    ).sum()
    
    print(f"\nPotential duplicate records: {duplicate_count}")
    validation_results['duplicates'] = int(duplicate_count)
    
    # 7. Value distribution
    print("\nAirline Distribution:")
    print("-" * 60)
    print(df['airline'].value_counts().head(10))
    
    print("\nTop 10 Routes:")
    print("-" * 60)
    route_counts = df.groupby(['source', 'destination']).size().sort_values(ascending=False)
    print(route_counts.head(10))
    
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
    validation_results['valid_rows'] = int(valid_rows)
    validation_results['quality_score'] = float(quality_score)
    
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
    
    try:
        results = validate_data(args)
        print(f"\nFinal Status: SUCCESS - Quality Score: {results['quality_score']:.2f}%")
        sys.exit(0)
    except Exception as e:
        print(f"\nERROR during validation: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()