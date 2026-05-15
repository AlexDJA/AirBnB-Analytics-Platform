"""
queries.py
----------
M1 README queries: prove end-to-end HDFS pipeline by reading the Parquet
file BACK from HDFS (not from local disk) and running 3 analytical queries.

Why read from HDFS and not from the local CSV?
  - Reading from CSV proves nothing about HDFS.
  - Reading the Parquet back via WebHDFS proves the full round-trip:
    CSV -> typed DataFrame -> Parquet -> HDFS upload -> HDFS download
    -> Parquet parse -> typed DataFrame.
  - It also proves the type casting survived Parquet serialization
    (boolean columns are real booleans, not "true"/"false" strings).

Run:
    docker compose exec ingest python src/queries.py
"""
from __future__ import annotations

import io
import logging
import sys

import pandas as pd

from hdfs_client import HDFSClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

HDFS_PATH = "/data/raw/airbnb_europe.parquet"


def load_from_hdfs() -> pd.DataFrame:
    log.info("Downloading Parquet from hdfs://%s ...", HDFS_PATH)
    client = HDFSClient()
    payload = client.download_bytes(HDFS_PATH)
    log.info("Downloaded %.2f MB", len(payload) / 1024 / 1024)
    df = pd.read_parquet(io.BytesIO(payload))
    log.info("Parsed Parquet: %d rows, %d columns", len(df), len(df.columns))
    return df


def header(title: str) -> None:
    print()
    print("=" * 70)
    print(title)
    print("=" * 70)


def query_1_top_cities(df: pd.DataFrame) -> None:
    """Q1: Volume — Top 10 European cities by number of listings."""
    header("Q1 — Top 10 cities by number of listings")
    result = (
        df.groupby("city", dropna=True)
        .size()
        .reset_index(name="listings_count")
        .sort_values("listings_count", ascending=False)
        .head(10)
        .reset_index(drop=True)
    )
    print(result.to_string(index=False))


def query_2_revenue_by_room_type(df: pd.DataFrame) -> None:
    """Q2: Aggregation — Avg trailing-12-month revenue (USD) by room type."""
    header("Q2 — Average TTM revenue (USD) by room_type")
    result = (
        df.dropna(subset=["ttm_revenue", "room_type"])
        .groupby("room_type")
        .agg(
            listings_count=("ttm_revenue", "size"),
            avg_ttm_revenue_usd=("ttm_revenue", "mean"),
            median_ttm_revenue_usd=("ttm_revenue", "median"),
        )
        .round(2)
        .sort_values("avg_ttm_revenue_usd", ascending=False)
        .reset_index()
    )
    print(result.to_string(index=False))


def query_3_top_superhosts(df: pd.DataFrame) -> None:
    """Q3: Filter + sort — Top 5 superhosts (rating >= 4.8) by TTM revenue."""
    header("Q3 — Top 5 superhosts (rating >= 4.8) by TTM revenue")
    mask = (
        (df["superhost"] == True)         # real boolean filter — proves the cast
        & (df["rating_overall"] >= 4.8)
        & df["ttm_revenue"].notna()
    )
    result = (
        df.loc[mask, ["listing_id", "city", "country", "room_type",
                      "rating_overall", "num_reviews", "ttm_revenue"]]
        .sort_values("ttm_revenue", ascending=False)
        .head(5)
        .reset_index(drop=True)
    )
    print(result.to_string(index=False))


def main() -> int:
    try:
        df = load_from_hdfs()
        query_1_top_cities(df)
        query_2_revenue_by_room_type(df)
        query_3_top_superhosts(df)
        print()
        log.info("All 3 queries completed successfully.")
        return 0
    except Exception as exc:
        log.exception("Queries failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())