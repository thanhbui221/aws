from __future__ import annotations

import pandas as pd


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Example ETL:
    - standardize column names
    - drop fully-empty rows
    - dedupe
    - add ingestion_date columns if a timestamp column exists
    """

    # 1) Clean column names
    df = df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # 2) Drop all-null rows
    df = df.dropna(how="all")

    # 3) Basic dedupe
    df = df.drop_duplicates()

    # 4) If you have a timestamp column, derive partitions
    # Change 'transaction_time' to your actual timestamp column name
    if "transaction_time" in df.columns:
        ts = pd.to_datetime(df["transaction_time"], errors="coerce", utc=True)
        df["year"] = ts.dt.year
        df["month"] = ts.dt.month
        df["day"] = ts.dt.day

    return df
