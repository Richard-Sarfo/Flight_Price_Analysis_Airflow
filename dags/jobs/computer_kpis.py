"""
Job 3B: Compute KPIs from flight_data table
"""

import pandas as pd
from sqlalchemy import create_engine
import argparse
import sys


def compute_kpis(args):

    print("=" * 60)
    print("STARTING KPI COMPUTATION")
    print("=" * 60)

    postgres_url = f"postgresql+psycopg2://{args.postgres_user}:{args.postgres_password}@{args.postgres_host}:{args.postgres_port}/{args.postgres_database}"
    engine = create_engine(postgres_url)

    df = pd.read_sql("SELECT * FROM flight_data", con=engine)

    # KPI 1: Average Fare by Airline
    kpi_airline = df.groupby('airline').agg(
        avg_base_fare=('base_fare', 'mean'),
        avg_total_fare=('total_fare', 'mean'),
        booking_count=('airline', 'count')
    ).reset_index()

    kpi_airline.to_sql(
        name='kpi_avg_fare_by_airline',
        con=engine,
        if_exists='replace',
        index=False
    )

    # KPI 2: Seasonal Fare
    kpi_season = df.groupby(['season', 'is_peak_season']).agg(
        avg_fare=('total_fare', 'mean'),
        booking_count=('season', 'count')
    ).reset_index()

    kpi_season.to_sql(
        name='kpi_seasonal_variation',
        con=engine,
        if_exists='replace',
        index=False
    )

    # KPI 3: Popular Routes
    kpi_routes = df.groupby(['source', 'destination']).agg(
        booking_count=('source', 'count'),
        avg_fare=('total_fare', 'mean')
    ).reset_index()

    kpi_routes.to_sql(
        name='kpi_popular_routes',
        con=engine,
        if_exists='replace',
        index=False
    )

    print("All KPIs computed successfully.")
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--postgres-host', required=True)
    parser.add_argument('--postgres-port', required=True)
    parser.add_argument('--postgres-database', required=True)
    parser.add_argument('--postgres-user', required=True)
    parser.add_argument('--postgres-password', required=True)

    args = parser.parse_args()

    try:
        compute_kpis(args)
        sys.exit(0)
    except Exception as e:
        print(str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
