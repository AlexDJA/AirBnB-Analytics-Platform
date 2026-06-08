"""
ingest.py
---------
M4 ingestion job: read the raw Airbnb Europe CSV from S3, cast columns to
proper types, serialize as Parquet (pyarrow), and write back to S3.

This replaces the M1 HDFS-based ingestion. The original two-step WebHDFS
PUT and the `hdfs_client` module are no longer used in the cloud
deployment — S3 is the single source of truth for raw and processed
data on AWS.

Design decisions (see REFLECTION.md):
  1. We cast types HERE (same as M1) so the Parquet on S3 carries a
     typed schema for Spark to read in M2.
  2. We use pandas with engine="python" and quoting=csv.QUOTE_ALL because
     the raw CSV has ~0.5% malformed lines (broken escaping in the
     `amenities` field). The C engine refuses to parse them.
  3. Boolean columns are stored as pandas nullable `boolean`, preserving
     <NA> for missing badges instead of falsely treating absence as False.
"""
from __future__ import annotations

import csv
import io
import logging
import os
import sys
from pathlib import Path

import boto3
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError


# ── configuration (env-driven, no hardcoded secrets) ──────────────────────
AWS_REGION       = os.environ.get("AWS_REGION", "us-east-2")
S3_BUCKET        = os.environ["S3_BUCKET"]
S3_CSV_KEY       = os.environ.get("S3_CSV_KEY",     "data/AirbnbEuropeMarket.csv")
S3_PARQUET_KEY   = os.environ.get("S3_PARQUET_KEY", "processed/airbnb_europe.parquet")

LOG_DIR  = Path("logs")
LOG_FILE = LOG_DIR / "ingest.log"


# ── column type spec (unchanged from M1) ──────────────────────────────────
STRING_COLS = [
    "listing_id", "listing_type", "room_type", "cover_photo_url",
    "host_id", "registration", "amenities", "cancellation_policy",
    "currency", "country", "state", "city",
]

BOOL_COLS = ["superhost", "instant_book", "professional_management"]


def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def s3_client() -> "boto3.client":
    """Single point where the boto3 S3 client is constructed.

    Credentials come from the standard chain: env vars first
    (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY for local dev), then the
    EC2 instance metadata service (the IAM role attached to the box in
    production). The code is identical in both cases.
    """
    return boto3.client("s3", region_name=AWS_REGION)


def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """
    Download the CSV from S3 into memory and parse it.
    We stream the body into a BytesIO so pandas can use its Python
    parser with the tolerant settings we need for the malformed rows.
    """
    logging.info("Downloading s3://%s/%s ...", bucket, key)
    try:
        obj = s3_client().get_object(Bucket=bucket, Key=key)
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(
            f"Could not read s3://{bucket}/{key}: {exc}"
        ) from exc

    raw_bytes = obj["Body"].read()
    logging.info("Downloaded %.2f MB from S3", len(raw_bytes) / 1024 / 1024)

    df = pd.read_csv(
        io.BytesIO(raw_bytes),
        engine="python",
        on_bad_lines="skip",
        quoting=csv.QUOTE_ALL,
    )
    logging.info("Parsed CSV: %d rows, %d columns", len(df), len(df.columns))
    return df


def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    """Cast columns to their proper types before writing Parquet."""
    for col in STRING_COLS:
        if col in df.columns:
            df[col] = df[col].astype("string")

    for col in BOOL_COLS:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype("string")
                .str.lower()
                .map({"true": True, "false": False})
                .astype("boolean")
            )

    logging.info("Post-cast dtype summary:")
    for col, dtype in df.dtypes.items():
        logging.info("  %-32s %s", col, dtype)
    return df


def write_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    """Serialize the DataFrame to Parquet in memory and upload to S3."""
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False, compression="snappy")
    payload = buf.getvalue()
    logging.info(
        "Serialized to Parquet: %d bytes (%.2f MB), compression=snappy",
        len(payload), len(payload) / 1024 / 1024,
    )

    try:
        s3_client().put_object(
            Bucket=bucket,
            Key=key,
            Body=payload,
            ContentType="application/octet-stream",
        )
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(
            f"Could not write s3://{bucket}/{key}: {exc}"
        ) from exc
    logging.info("Uploaded Parquet to s3://%s/%s", bucket, key)


def main() -> int:
    setup_logging()
    logging.info("=" * 60)
    logging.info("M4 ingestion job starting (S3 → cast → S3)")
    logging.info("=" * 60)

    try:
        df = read_csv_from_s3(S3_BUCKET, S3_CSV_KEY)
        df = cast_types(df)
        write_parquet_to_s3(df, S3_BUCKET, S3_PARQUET_KEY)
        logging.info("Ingestion complete.")
        return 0
    except Exception as exc:
        logging.exception("Ingestion failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())