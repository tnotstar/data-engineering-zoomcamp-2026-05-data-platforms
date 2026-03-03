"""@bruin

name: ingestion.trips

type: python
image: python3.12

depends:
  - ingestion.payment_lookup

connection: nyc-taxi-duckdb

materialization:
  type: table
  strategy: append
@bruin"""

import json
import os
from datetime import datetime

import pandas as pd


def materialize():
    """
    Ingests NYC Taxi trip data based on the run window and configured taxi types.
    """
    # 1. Retrieve configuration from Bruin environment
    start_date_str = os.getenv("BRUIN_START_DATE")
    end_date_str = os.getenv("BRUIN_END_DATE")
    bruin_vars_str = os.getenv("BRUIN_VARS", "{}")

    if not start_date_str or not end_date_str:
        raise ValueError("BRUIN_START_DATE and BRUIN_END_DATE must be set.")

    try:
        bruin_vars = json.loads(bruin_vars_str)
    except json.JSONDecodeError:
        print("Warning: Could not parse BRUIN_VARS, defaulting to empty dict.")
        bruin_vars = {}

    # Default to yellow if not specified
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])

    print(f"Ingestion Window: {start_date_str} to {end_date_str}")
    print(f"Taxi Types: {taxi_types}")

    # 2. Determine months to fetch
    start_date = datetime.strptime(start_date_str[:10], "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str[:10], "%Y-%m-%d")

    months_to_fetch = []
    current_date = start_date.replace(day=1)

    while current_date <= end_date:
        months_to_fetch.append((current_date.year, current_date.month))
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)

    # 3. Fetch Data
    dfs = []
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    for year, month in months_to_fetch:
        month_str = f"{month:02d}"
        for taxi_type in taxi_types:
            file_name = f"{taxi_type}_tripdata_{year}-{month_str}.parquet"
            url = f"{base_url}/{file_name}"

            print(f"Fetching: {url}")
            try:
                df = pd.read_parquet(url)
                df["source_url"] = url
                df["taxi_type"] = taxi_type
                df["filename_year"] = year
                df["filename_month"] = month
                dfs.append(df)
                print(f"  -> Success: {len(df)} rows")
            except Exception as e:
                print(f"  -> Skipped (Error): {e}")

    print(f"Data fetching complete: {len(dfs)} files fetched. Processing results...")
    # 4. Return Result
    if not dfs:
        print("No data found for the given window/configuration.")
        return pd.DataFrame()

    print("Concatenating dataframes...")
    final_df = pd.concat(dfs, ignore_index=True)
    print("Calculated extraction date...")
    final_df["extracted_at"] = datetime.now()

    print(f"Total ingested: {len(final_df)} rows.")
    return final_df
