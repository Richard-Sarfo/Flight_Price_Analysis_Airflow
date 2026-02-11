"""
Job 3A: Transform data and load fact table to PostgreSQL
"""

import pandas as pd
from sqlalchemy import create_engine
import argparse
import sys


def determine_peak_season(date_value):
    if pd.isna(date_value):
        return "Unknown"

    month = date_value.month
    day = date_value.day

    if month == 4:
        return "Eid_ul_Fitr"
    elif month in [6, 7]:
        return "Eid_ul_Adha_Summer"
    elif (month == 12 and day >= 15) or (month == 1 and day <= 15):
        return "Winter_Holiday"
    else:
        return "Regular"


def transform_data(args):

    print("=" * 60)
    print("STARTING TRANSFORMATION")
    print("=" * 60)

    mysql_url = f"mysql+pymysql://{args.mysql_user}:{args.mysql_password}@{args.mysql_host}:{args.mysql_port}/{args.mysql_database}"
    postgres_url = f"postgresql+psycopg2://{args.postgres_user}:{args.postgres_password}@{args.postgres_host}:{args.postgres_port}/{args.postgres_database}"

    mysql_engine = create_engine(mysql_url)
    postgres_engine = create_engine(postgres_url)

    df = pd.read_sql("SELECT * FROM flight_data_staging", con=mysql_engine)

    # Filter valid records
    df = df[
        (df['airline'].notna()) &
        (df['source'].notna()) &
        (df['destination'].notna()) &
        (df['base_fare'] >= 0) &
        (df['total_fare'] >= 0)
    ].copy()

    # Season logic
    if 'departure_date_and_time' in df.columns:
        df['departure_date_and_time'] = pd.to_datetime(df['departure_date_and_time'], errors='coerce')
        df['season'] = df['departure_date_and_time'].apply(determine_peak_season)
    else:
        df['season'] = 'Unknown'

    df['is_peak_season'] = df['season'].apply(
        lambda x: x not in ['Regular', 'Unknown']
    )

    df['processed_at'] = pd.Timestamp.now()

    df.to_sql(
        name='flight_data',
        con=postgres_engine,
        if_exists='append',
        index=False
    )

    print(f"Loaded {len(df)} records to flight_data")
    return len(df)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mysql-host', required=True)
    parser.add_argument('--mysql-port', required=True)
    parser.add_argument('--mysql-database', required=True)
    parser.add_argument('--mysql-user', required=True)
    parser.add_argument('--mysql-password', required=True)
    parser.add_argument('--postgres-host', required=True)
    parser.add_argument('--postgres-port', required=True)
    parser.add_argument('--postgres-database', required=True)
    parser.add_argument('--postgres-user', required=True)
    parser.add_argument('--postgres-password', required=True)

    args = parser.parse_args()

    try:
        transform_data(args)
        sys.exit(0)
    except Exception as e:
        print(str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
