from __future__ import annotations

import argparse
import io
from datetime import datetime, timezone

import pandas as pd

from src.config import Config
from src.etl import transform
from src.s3_io import (
    get_boto3_session,
    join_s3_uri,
    write_s3_bytes,
    read_s3_dataframe,
)


def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def run(input_uri: str, output_uri: str, region: str, profile: str | None, partitioned: bool) -> None:
    session = get_boto3_session(region=region, profile=profile)

    print(f"Reading: {input_uri}")
    df = read_s3_dataframe(session, input_uri)
    print(f"Rows in: {len(df):,} | Cols: {len(df.columns):,}")

    df_out = transform(df)
    print(f"Rows out: {len(df_out):,} | Cols: {len(df_out.columns):,}")

    # Write strategy:
    # - If partitioned, require year/month/day columns and write one parquet per partition (simple & scalable-ish)
    # - Else write a single parquet file
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    if partitioned and {"year", "month", "day"}.issubset(df_out.columns):
        print("Writing partitioned parquet to S3...")
        for (y, m, d), g in df_out.groupby(["year", "month", "day"], dropna=False):
            key = f"year={int(y)}/month={int(m):02d}/day={int(d):02d}/part-{now}.parquet"
            s3_target = join_s3_uri(output_uri, key)
            write_s3_bytes(session, s3_target, df_to_parquet_bytes(g), content_type="application/octet-stream")
        print("Done.")
        return

    # Non-partitioned single file
    filename = f"output-{now}.parquet"
    s3_target = join_s3_uri(output_uri, filename)
    print(f"Writing: {s3_target}")
    write_s3_bytes(session, s3_target, df_to_parquet_bytes(df_out), content_type="application/octet-stream")
    print("Done.")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Simple S3 -> ETL -> S3 pipeline")
    p.add_argument("--input", dest="input_uri", help="S3 input URI (csv/parquet). Overrides env S3_INPUT_URI.")
    p.add_argument("--output", dest="output_uri", help="S3 output URI prefix. Overrides env S3_OUTPUT_URI.")
    p.add_argument("--region", help="AWS region (default from env AWS_REGION).")
    p.add_argument("--profile", help="AWS profile (default from env AWS_PROFILE).")
    p.add_argument("--partitioned", action="store_true", help="Write partitioned output if year/month/day exists.")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    cfg = Config()

    run(
        input_uri=args.input_uri or cfg.s3_input_uri,
        output_uri=args.output_uri or cfg.s3_output_uri,
        region=args.region or cfg.aws_region,
        profile=args.profile or cfg.aws_profile,
        partitioned=args.partitioned or cfg.output_partitioned,
    )
