"""
Job 3: Transform data and compute KPIs, load to PostgreSQL
"""
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import argparse
import sys

def determine_peak_season(row, date_col):
    """
    Determine if a date falls in peak season for Bangladesh
    Peak seasons:
    - Eid-ul-Fitr: April (approximate)
    - Eid-ul-Adha: June-July (approximate)
    - Winter Holiday: December 15 - January 15
    - Summer Holiday: June 1 - July 15
    """
    date_value = row.get(date_col)
    if pd.isna(date_value):
        return "Unknown"
    
    try:
        month = date_value.month
        day = date_value.day
    except AttributeError:
        return "Unknown"
    
    if month == 4:
        return "Eid_ul_Fitr"
    elif month in [6, 7]:
        return "Eid_ul_Adha_Summer"
    elif (month == 12 and day >= 15) or (month == 1 and day <= 15):
        return "Winter_Holiday"
    else:
        return "Regular"

def transform_and_compute_kpis(args):
    """Main transformation and KPI computation logic"""
    print("=" * 60)
    print("STARTING TRANSFORMATION AND KPI COMPUTATION")
    print("=" * 60)
    
    # MySQL connection
    mysql_url = f"mysql+pymysql://{args.mysql_user}:{args.mysql_password}@{args.mysql_host}:{args.mysql_port}/{args.mysql_database}"
    mysql_engine = create_engine(mysql_url)
    
    # PostgreSQL connection
    postgres_url = f"postgresql+psycopg2://{args.postgres_user}:{args.postgres_password}@{args.postgres_host}:{args.postgres_port}/{args.postgres_database}"
    postgres_engine = create_engine(postgres_url)
    
    # Read data from MySQL staging
    print(f"\nReading data from MySQL: {args.mysql_host}:{args.mysql_port}/{args.mysql_database}")
    df = pd.read_sql("SELECT * FROM flight_data_staging", con=mysql_engine)
    
    initial_count = len(df)
    print(f"Records loaded from staging: {initial_count}")
    print(f"Available columns: {list(df.columns)}")
    
    # Filter valid records only
    df_valid = df[
        (df['airline'].notna()) & 
        (df['source'].notna()) & 
        (df['destination'].notna()) & 
        (df['base_fare'] >= 0) & 
        (df['total_fare'] >= 0)
    ].copy()
    
    valid_count = len(df_valid)
    print(f"Valid records after filtering: {valid_count}")
    
    # Determine date column for season calculation
    date_col = None
    for col in ['departure_date_and_time', 'booking_date', 'departure_date']:
        if col in df_valid.columns:
            date_col = col
            break
    
    # Add season information if date column exists
    print("\nAdding seasonal information...")
    if date_col:
        df_valid[date_col] = pd.to_datetime(df_valid[date_col], errors='coerce')
        # Use existing seasonality column if present, otherwise compute
        if 'seasonality' not in df_valid.columns or df_valid['seasonality'].isna().all():
            df_valid['season'] = df_valid.apply(lambda row: determine_peak_season(row, date_col), axis=1)
        else:
            df_valid['season'] = df_valid['seasonality'].fillna('Unknown')
    else:
        # Use existing seasonality column if present
        if 'seasonality' in df_valid.columns:
            df_valid['season'] = df_valid['seasonality'].fillna('Unknown')
        else:
            df_valid['season'] = 'Unknown'
    
    df_valid['is_peak_season'] = df_valid['season'].apply(
        lambda x: x not in ['Regular', 'Unknown', 'Off-Peak']
    )
    df_valid['processed_at'] = pd.Timestamp.now()
    
    # Drop unnecessary columns if they exist
    columns_to_drop = ['id', 'created_at']
    df_valid = df_valid.drop(columns=[c for c in columns_to_drop if c in df_valid.columns], errors='ignore')
    
    # Show sample of transformed data
    print("\nSample of transformed data:")
    display_cols = ['airline', 'source', 'destination', 'total_fare', 'season', 'is_peak_season']
    display_cols = [c for c in display_cols if c in df_valid.columns]
    print(df_valid[display_cols].head())
    
    # Load main fact table to PostgreSQL
    print(f"\nLoading fact table to PostgreSQL: {args.postgres_host}:{args.postgres_port}/{args.postgres_database}")
    df_valid.to_sql(
        name='flight_data',
        con=postgres_engine,
        if_exists='append',
        index=False
    )
    print(f"✓ Loaded {valid_count} records to flight_data table")
    
    # ========================================
    # KPI 1: Average Fare by Airline
    # ========================================
    print("\n" + "=" * 60)
    print("COMPUTING KPI 1: Average Fare by Airline")
    print("=" * 60)
    
    kpi_airline = df_valid.groupby('airline').agg(
        avg_base_fare=('base_fare', 'mean'),
        avg_total_fare=('total_fare', 'mean'),
        booking_count=('airline', 'count')
    ).reset_index()
    
    kpi_airline['avg_base_fare'] = kpi_airline['avg_base_fare'].round(2)
    kpi_airline['avg_total_fare'] = kpi_airline['avg_total_fare'].round(2)
    kpi_airline['calculated_at'] = pd.Timestamp.now()
    kpi_airline = kpi_airline.sort_values('booking_count', ascending=False)
    
    airline_count = len(kpi_airline)
    print(f"Number of airlines analyzed: {airline_count}")
    
    print("\nTop 5 Airlines by Booking Volume:")
    print(kpi_airline.head())
    
    # Write to PostgreSQL
    kpi_airline.to_sql(
        name='kpi_avg_fare_by_airline',
        con=postgres_engine,
        if_exists='replace',
        index=False
    )
    print("✓ KPI 1 results loaded to PostgreSQL")
    
    # ========================================
    # KPI 2: Seasonal Fare Variation
    # ========================================
    print("\n" + "=" * 60)
    print("COMPUTING KPI 2: Seasonal Fare Variation")
    print("=" * 60)
    
    kpi_seasonal = df_valid.groupby(['season', 'is_peak_season']).agg(
        avg_fare=('total_fare', 'mean'),
        booking_count=('season', 'count')
    ).reset_index()
    
    kpi_seasonal['avg_fare'] = kpi_seasonal['avg_fare'].round(2)
    kpi_seasonal['calculated_at'] = pd.Timestamp.now()
    kpi_seasonal = kpi_seasonal.sort_values('booking_count', ascending=False)
    
    print("\nSeasonal Analysis:")
    print(kpi_seasonal)
    
    # Calculate peak vs non-peak comparison
    peak_data = kpi_seasonal[kpi_seasonal['is_peak_season'] == True]
    non_peak_data = kpi_seasonal[kpi_seasonal['is_peak_season'] == False]
    
    if len(peak_data) > 0 and len(non_peak_data) > 0:
        peak_avg = peak_data['avg_fare'].mean()
        non_peak_avg = non_peak_data['avg_fare'].mean()
        if non_peak_avg > 0:
            variation = ((peak_avg - non_peak_avg) / non_peak_avg) * 100
            print(f"\nPeak Season Average Fare: ${peak_avg:.2f}")
            print(f"Non-Peak Season Average Fare: ${non_peak_avg:.2f}")
            print(f"Price Variation: {variation:+.2f}%")
    
    # Write to PostgreSQL
    kpi_seasonal.to_sql(
        name='kpi_seasonal_variation',
        con=postgres_engine,
        if_exists='replace',
        index=False
    )
    print("✓ KPI 2 results loaded to PostgreSQL")
    
    # ========================================
    # KPI 3: Most Popular Routes
    # ========================================
    print("\n" + "=" * 60)
    print("COMPUTING KPI 3: Most Popular Routes")
    print("=" * 60)
    
    kpi_routes = df_valid.groupby(['source', 'destination']).agg(
        booking_count=('source', 'count'),
        avg_fare=('total_fare', 'mean')
    ).reset_index()
    
    kpi_routes['avg_fare'] = kpi_routes['avg_fare'].round(2)
    kpi_routes = kpi_routes.sort_values('booking_count', ascending=False)
    
    # Add ranking
    kpi_routes['route_rank'] = range(1, len(kpi_routes) + 1)
    kpi_routes['calculated_at'] = pd.Timestamp.now()
    
    routes_count = len(kpi_routes)
    print(f"Total unique routes: {routes_count}")
    
    print("\nTop 10 Most Popular Routes:")
    print(kpi_routes.head(10))
    
    # Write to PostgreSQL
    kpi_routes.to_sql(
        name='kpi_popular_routes',
        con=postgres_engine,
        if_exists='replace',
        index=False
    )
    print("✓ KPI 3 results loaded to PostgreSQL")
    
    # ========================================
    # Additional Analysis: Airline Market Share
    # ========================================
    print("\n" + "=" * 60)
    print("ADDITIONAL ANALYSIS: Airline Market Share")
    print("=" * 60)
    
    total_bookings = len(df_valid)
    
    market_share = df_valid.groupby('airline').size().reset_index(name='bookings')
    market_share['market_share_pct'] = (market_share['bookings'] / total_bookings * 100).round(2)
    market_share = market_share.sort_values('market_share_pct', ascending=False)
    
    print("\nAirline Market Share:")
    print(market_share.head(10))
    
    # Summary
    print("\n" + "=" * 60)
    print("TRANSFORMATION AND KPI COMPUTATION SUMMARY")
    print("=" * 60)
    print(f"Records Processed: {valid_count}")
    print(f"Airlines Analyzed: {airline_count}")
    print(f"Unique Routes: {routes_count}")
    print(f"Seasons Identified: {len(kpi_seasonal)}")
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
    
    try:
        results = transform_and_compute_kpis(args)
        print(f"\nFinal Status: SUCCESS")
        print(f"Records Processed: {results['records_processed']}")
        sys.exit(0)
    except Exception as e:
        print(f"\nERROR during transformation: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()