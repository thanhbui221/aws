from __future__ import annotations

import io
import re
from dataclasses import dataclass
from urllib.parse import urlparse

import boto3
import pandas as pd


@dataclass(frozen=True)
class S3Path:
    bucket: str
    key: str


def parse_s3_uri(uri: str) -> S3Path:
    if not uri.startswith("s3://"):
        raise ValueError(f"Not an S3 uri: {uri}")
    parsed = urlparse(uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    return S3Path(bucket=bucket, key=key)


def get_boto3_session(region: str, profile: str | None = None) -> boto3.Session:
    # If profile is provided, use it; otherwise rely on environment/instance role
    if profile:
        return boto3.Session(profile_name=profile, region_name=region)
    return boto3.Session(region_name=region)


def read_s3_bytes(session: boto3.Session, s3_uri: str) -> bytes:
    s3 = session.client("s3")
    p = parse_s3_uri(s3_uri)
    obj = s3.get_object(Bucket=p.bucket, Key=p.key)
    return obj["Body"].read()


def write_s3_bytes(session: boto3.Session, s3_uri: str, data: bytes, content_type: str | None = None) -> None:
    s3 = session.client("s3")
    p = parse_s3_uri(s3_uri)
    extra = {}
    if content_type:
        extra["ContentType"] = content_type
    s3.put_object(Bucket=p.bucket, Key=p.key, Body=data, **extra)


def read_s3_dataframe(session: boto3.Session, s3_uri: str) -> pd.DataFrame:
    data = read_s3_bytes(session, s3_uri)
    lower = s3_uri.lower()

    if lower.endswith(".csv"):
        return pd.read_csv(io.BytesIO(data))

    if lower.endswith(".parquet"):
        return pd.read_parquet(io.BytesIO(data))

    raise ValueError("Unsupported input format. Use .csv or .parquet")


def join_s3_uri(prefix_uri: str, filename: str) -> str:
    # prefix_uri expected like s3://bucket/path/to/output/  (or without trailing slash)
    if not prefix_uri.startswith("s3://"):
        raise ValueError(f"Not an S3 uri: {prefix_uri}")

    prefix_uri = prefix_uri.rstrip("/") + "/"
    # Ensure no accidental double slashes in key
    return prefix_uri + re.sub(r"^/+", "", filename)
