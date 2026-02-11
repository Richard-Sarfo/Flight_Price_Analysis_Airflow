"""
Job 4: Retrain Flight Price Prediction Model
Reads clean data from PostgreSQL analytics DB
"""

import pandas as pd
import joblib
from sqlalchemy import create_engine
import argparse
import sys
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error


def retrain_model(args):

    print("=" * 60)
    print("STARTING MODEL RETRAINING")
    print("=" * 60)

    # PostgreSQL connection
    postgres_url = f"postgresql+psycopg2://{args.postgres_user}:{args.postgres_password}@{args.postgres_host}:{args.postgres_port}/{args.postgres_database}"
    engine = create_engine(postgres_url)

    # Load clean fact table
    df = pd.read_sql("SELECT * FROM flight_data", con=engine)

    print(f"Records loaded for training: {len(df)}")

    # Basic feature selection
    features = ['airline', 'source', 'destination', 'base_fare', 'tax_and_surcharge', 'is_peak_season']
    target = 'total_fare'

    df = df.dropna(subset=features + [target])

    # Convert categorical variables
    df = pd.get_dummies(df, columns=['airline', 'source', 'destination'], drop_first=True)

    X = df.drop(columns=[target])
    y = df[target]

    # Train/Test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # Evaluate
    predictions = model.predict(X_test)
    mae = mean_absolute_error(y_test, predictions)

    print(f"Model MAE: {mae:.2f}")

    # Save model
    joblib.dump(model, "/opt/airflow/models/flight_price_model.pkl")

    print("Model successfully retrained and saved.")
    print("=" * 60)

    return mae


def main():
    parser = argparse.ArgumentParser(description='Retrain flight price model')

    parser.add_argument('--postgres-host', required=True)
    parser.add_argument('--postgres-port', required=True)
    parser.add_argument('--postgres-database', required=True)
    parser.add_argument('--postgres-user', required=True)
    parser.add_argument('--postgres-password', required=True)

    args = parser.parse_args()

    try:
        retrain_model(args)
        sys.exit(0)
    except Exception as e:
        print(f"ERROR during retraining: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
