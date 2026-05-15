"""
ingest.py
---------
M1 ingestion job: read the raw Airbnb Europe CSV, cast columns to proper
types, serialize as Parquet (pyarrow), and upload to HDFS via WebHDFS.
"""
from __future__ import annotations

import csv
import io
import logging
import os
import sys
from pathlib import Path

import pandas as pd

from hdfs_client import HDFSClient


# ── configuration ─────────────────────────────────────────────────────────
CSV_PATH = Path(os.environ.get("CSV_PATH", "data/AirbnbEuropeMarket.csv"))
HDFS_PATH = os.environ.get("HDFS_PATH", "/data/raw/airbnb_europe.parquet")
HDFS_DIR = "/data/raw"

LOG_DIR = Path("logs")
LOG_FILE = LOG_DIR / "ingest.log"


# ── column type spec ──────────────────────────────────────────────────────
# Columns kept as string (IDs, free text, categorical, URLs).
STRING_COLS = [
    "listing_id", "listing_type", "room_type", "cover_photo_url",
    "host_id", "registration", "amenities", "cancellation_policy",
    "currency", "country", "state", "city",
]

# Columns that should be real booleans. Stored as "true"/"false" strings in
# the CSV; cast via map() to handle nulls cleanly (rather than astype(bool)
# which would treat the literal string "false" as truthy).
BOOL_COLS = ["superhost", "instant_book", "professional_management"]

# All other numeric columns are float64 already after pandas inference.
# We leave them alone — pyarrow will preserve the dtype in Parquet.


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


def load_csv(path: Path) -> pd.DataFrame:
    """Robust CSV reader that tolerates the malformed lines in this dataset."""
    if not path.exists():
        raise FileNotFoundError(
            f"CSV not found at {path.resolve()}. "
            "Place AirbnbEuropeMarket.csv in the data/ directory."
        )

    logging.info("Reading CSV from %s", path)
    df = pd.read_csv(
        path,
        engine="python",
        on_bad_lines="skip",      # ~0.5% of rows have broken escaping
        quoting=csv.QUOTE_ALL,
    )
    logging.info("Loaded %d rows, %d columns", len(df), len(df.columns))
    return df


def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    """Cast columns to their proper types before writing Parquet."""
    # Ensure declared string columns are actually strings (no float NaN sneaking in).
    for col in STRING_COLS:
        if col in df.columns:
            df[col] = df[col].astype("string")

    # Boolean cast: map literal "true"/"false" -> True/False; anything else -> NA.
    for col in BOOL_COLS:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype("string")
                .str.lower()
                .map({"true": True, "false": False})
                .astype("boolean")  # pandas nullable bool — survives NA values
            )

    # Sanity log of post-cast dtypes
    logging.info("Post-cast dtype summary:")
    for col, dtype in df.dtypes.items():
        logging.info("  %-32s %s", col, dtype)

    return df


def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Serialize the DataFrame to Parquet (pyarrow) into an in-memory buffer."""
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False, compression="snappy")
    payload = buf.getvalue()
    logging.info(
        "Serialized to Parquet: %d bytes (%.2f MB), compression=snappy",
        len(payload), len(payload) / 1024 / 1024,
    )
    return payload


def main() -> int:
    setup_logging()
    logging.info("=" * 60)
    logging.info("M1 ingestion job starting")
    logging.info("=" * 60)

    try:
        df = load_csv(CSV_PATH)
        df = cast_types(df)
        parquet_bytes = to_parquet_bytes(df)

        client = HDFSClient()
        client.mkdirs(HDFS_DIR)
        client.upload_bytes(parquet_bytes, HDFS_PATH, overwrite=True)

        # Verify by reading file status back
        info = client.status(HDFS_PATH)
        if info is None:
            logging.error("Upload reported success but file not found in HDFS!")
            return 1

        logging.info(
            "HDFS confirmation: path=%s size=%d bytes owner=%s",
            HDFS_PATH, info["length"], info["owner"],
        )
        logging.info("Ingestion complete.")
        return 0

    except Exception as exc:
        logging.exception("Ingestion failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
