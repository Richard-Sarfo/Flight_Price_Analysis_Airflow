"""
Job 1: Ingest CSV data and load to MySQL staging (Pandas version)
"""
import pandas as pd
from sqlalchemy import create_engine
import argparse
import sys
import re

def clean_column_name(col_name):
    """Clean column names for database compatibility"""
    # Remove content in parentheses, clean spaces and special chars
    col = re.sub(r'\s*\([^)]*\)', '', col_name)  # Remove (BDT), (hrs), etc.
    return col.strip().lower().replace(' ', '_').replace('&', 'and')

def ingest_csv_to_mysql(args):
    """Main ingestion logic"""
    print("=" * 60)
    print("STARTING DATA INGESTION")
    print("=" * 60)
    
    # Read CSV with Pandas
    print(f"Reading CSV from: {args.input_path}")
    df = pd.read_csv(args.input_path)
    
    initial_count = len(df)
    print(f"Initial record count: {initial_count}")
    print(f"Original columns: {list(df.columns)}")
    
    # Clean column names
    df.columns = [clean_column_name(col) for col in df.columns]
    print(f"Cleaned columns: {list(df.columns)}")
    
    # Data cleaning and transformation
    # Fill null values for text columns
    if 'airline' in df.columns:
        df['airline'] = df['airline'].fillna('Unknown').astype(str).str.strip()
    if 'source' in df.columns:
        df['source'] = df['source'].fillna('Unknown').astype(str).str.strip()
    if 'destination' in df.columns:
        df['destination'] = df['destination'].fillna('Unknown').astype(str).str.strip()
    
    # Clean fare columns - remove non-numeric characters and convert to float
    fare_columns = ['base_fare', 'tax_and_surcharge', 'total_fare']
    for col in fare_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).apply(lambda x: re.sub(r'[^0-9.]', '', x))
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Calculate total fare if missing or incorrect
    if 'total_fare' in df.columns and 'base_fare' in df.columns and 'tax_and_surcharge' in df.columns:
        mask = df['total_fare'].isna() | (df['total_fare'] == 0)
        df.loc[mask, 'total_fare'] = df.loc[mask, 'base_fare'] + df.loc[mask, 'tax_and_surcharge']
    
    # Handle date columns
    date_columns = ['departure_date_and_time', 'arrival_date_and_time']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Add audit column
    df['created_at'] = pd.Timestamp.now()
    
    # Remove rows with all null values in critical fields
    critical_fields = [c for c in ['airline', 'source', 'destination'] if c in df.columns]
    if critical_fields:
        df = df.dropna(subset=critical_fields, how='all')
    
    cleaned_count = len(df)
    print(f"Cleaned record count: {cleaned_count}")
    print(f"Records removed during cleaning: {initial_count - cleaned_count}")
    
    # Show sample data
    print("\nSample of cleaned data:")
    print(df.head())
    
    # Create MySQL connection using SQLAlchemy
    mysql_url = f"mysql+pymysql://{args.mysql_user}:{args.mysql_password}@{args.mysql_host}:{args.mysql_port}/{args.mysql_database}"
    engine = create_engine(mysql_url)
    
    # Write to MySQL
    print(f"\nWriting to MySQL: {args.mysql_host}:{args.mysql_port}/{args.mysql_database}")
    df.to_sql(
        name='flight_data_staging',
        con=engine,
        if_exists='append',
        index=False
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
    
    try:
        records_loaded = ingest_csv_to_mysql(args)
        print(f"\nFinal Status: SUCCESS - {records_loaded} records loaded")
        sys.exit(0)
    except Exception as e:
        print(f"\nERROR during ingestion: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()