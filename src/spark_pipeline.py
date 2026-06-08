"""
spark_pipeline.py
-----------------
M2 Spark job. Reads the M1 Parquet file from HDFS, cleans it, computes 3
derived columns, and writes the result to MongoDB and ElasticSearch.

Pipeline stages:
  1. Read Parquet from HDFS via native HDFS protocol (port 8020).
  2. Clean:
       - drop rows missing critical identifiers (listing_id, city, country)
       - fill boolean nulls (superhost, instant_book) -> False
       - fill num_reviews null -> 0
       - dropDuplicates on listing_id
  3. Derive 3 columns:
       - revenue_per_booked_day   (ratio)
       - occupancy_tier           (binned category: Low/Medium/High)
       - is_premium               (boolean: superhost AND rating >= 4.8)
  4. Write to MongoDB collection `airbnb.listings` (mode=overwrite).
  5. Write to ElasticSearch index `airbnb_listings`   (mode=overwrite).

Run:
    docker compose run --rm spark_pipeline

Outputs:
    - logs/spark.log               (Python logging, structured)
    - stdout                       (log4j + Python logging, for screenshots)
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import requests
import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType


# ── configuration ─────────────────────────────────────────────────────────
# M4: read Parquet from S3 instead of HDFS.
S3_BUCKET       = os.environ.get("S3_BUCKET", "airbnb-m4-alexdja")
S3_PARQUET_KEY  = os.environ.get("S3_PARQUET_KEY", "processed/airbnb_europe.parquet")
PARQUET_URI     = f"s3a://{S3_BUCKET}/{S3_PARQUET_KEY}"

MONGO_URI = "mongodb://spark_writer:sparkpass@mongodb:27017/airbnb?authSource=airbnb"
MONGO_DB = "airbnb"
MONGO_COLLECTION = "listings"

ES_NODES = "elasticsearch"
ES_PORT = "9200"
ES_INDEX = "airbnb_listings"

LOG_DIR = Path("logs")
LOG_FILE = LOG_DIR / "spark.log"


# ── logging setup ─────────────────────────────────────────────────────────
def setup_logging() -> logging.Logger:
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
    return logging.getLogger("spark_pipeline")


# ── Spark session ─────────────────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    """
    Build a SparkSession configured with the Mongo and ES connectors.
    The packages themselves come from `spark-submit --packages` at launch
    time (see docker-compose.yml); here we only configure connection URIs.
    """
    spark = (
        SparkSession.builder
        .appName("M2-AirbnbPipeline")
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .config("spark.es.nodes", ES_NODES)
        .config("spark.es.port", ES_PORT)
        .config("spark.es.nodes.wan.only", "false")
        # Quiet down Spark's own log4j (still visible in stdout for screenshots)
        .config("spark.sql.adaptive.enabled", "true")
        # S3A connector configuration (M4)
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{os.environ.get('AWS_REGION', 'us-east-2')}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── pipeline stages ───────────────────────────────────────────────────────
def read_from_s3(spark: SparkSession, log: logging.Logger) -> DataFrame:
    log.info("Reading Parquet from %s", PARQUET_URI)
    df = spark.read.parquet(PARQUET_URI)
    count = df.count()
    log.info("Read %d records from HDFS (%d columns)", count, len(df.columns))
    return df


def clean_data(df: DataFrame, log: logging.Logger) -> DataFrame:
    """
    Three-step cleaning, with explicit before/after counts for the log.

      1. Drop rows missing critical identifiers (listing_id, city, country).
      2. Fill nulls where absence has a business-meaningful default.
      3. Drop duplicates on listing_id (same listing should not appear twice).

    Financial / rating columns keep their nulls deliberately — a missing
    revenue figure is *information*, not noise (the listing simply wasn't
    booked in that window), and silently filling with 0 would falsely lower
    aggregates downstream.
    """
    initial = df.count()
    log.info("Cleaning stage starting — initial record count: %d", initial)

    # Step 1: drop rows missing critical identifiers.
    after_critical = df.dropna(subset=["listing_id", "city", "country"])
    dropped_critical = initial - after_critical.count()
    log.info("Dropped %d rows missing critical fields (listing_id/city/country)",
             dropped_critical)

    # Step 2: targeted fills.
    superhost_nulls = after_critical.filter(F.col("superhost").isNull()).count()
    instant_book_nulls = after_critical.filter(F.col("instant_book").isNull()).count()
    num_reviews_nulls = after_critical.filter(F.col("num_reviews").isNull()).count()

    filled = (
        after_critical
        .fillna({"superhost": False, "instant_book": False})
        .fillna({"num_reviews": 0})
    )
    log.info("Filled %d nulls in superhost -> False", superhost_nulls)
    log.info("Filled %d nulls in instant_book -> False", instant_book_nulls)
    log.info("Filled %d nulls in num_reviews -> 0", num_reviews_nulls)

    # Step 3: drop duplicates on listing_id.
    before_dedup = filled.count()
    deduped = filled.dropDuplicates(["listing_id"])
    dropped_dups = before_dedup - deduped.count()
    log.info("Dropped %d duplicate rows on listing_id", dropped_dups)

    final = deduped.count()
    log.info("Cleaning stage complete — final record count: %d (lost %d total)",
             final, initial - final)
    return deduped


def add_derived_columns(df: DataFrame, log: logging.Logger) -> DataFrame:
    """
    Compute 3 business-meaningful derived columns.

    1. revenue_per_booked_day (ratio):
         ttm_revenue / ttm_reserved_days
       How much money the listing earned per night that was actually booked.
       Hosts can compare this to the local average daily rate to see whether
       they are pricing below market.

    2. occupancy_tier (binned category):
         < 0.30   -> "Low"
         < 0.60   -> "Medium"
         >= 0.60  -> "High"
       A categorical filter that powers fast ElasticSearch term queries
       without forcing every query to do a numeric range comparison.

    3. is_premium (boolean composite):
         superhost == true AND rating_overall >= 4.8
       Flags listings that combine host reputation with high guest ratings.
       Useful for marketing campaigns or for filtering high-end inventory.
    """
    log.info("Computing 3 derived columns: revenue_per_booked_day, "
             "occupancy_tier, is_premium")

    df = df.withColumn(
        "revenue_per_booked_day",
        F.when(
            (F.col("ttm_reserved_days").isNotNull())
            & (F.col("ttm_reserved_days") > 0)
            & (F.col("ttm_revenue").isNotNull()),
            F.round(F.col("ttm_revenue") / F.col("ttm_reserved_days"), 2),
        ).otherwise(F.lit(None)),
    )

    df = df.withColumn(
        "occupancy_tier",
        F.when(F.col("ttm_occupancy").isNull(), F.lit(None))
         .when(F.col("ttm_occupancy") < 0.30, F.lit("Low"))
         .when(F.col("ttm_occupancy") < 0.60, F.lit("Medium"))
         .otherwise(F.lit("High")),
    )

    df = df.withColumn(
        "is_premium",
        (
            (F.col("superhost") == True)
            & (F.col("rating_overall").isNotNull())
            & (F.col("rating_overall") >= 4.8)
        ).cast(BooleanType()),
    )

    # Diagnostic log lines (counts), so the screenshot of logs shows real activity
    premium_count = df.filter(F.col("is_premium") == True).count()
    log.info("is_premium == true on %d listings", premium_count)

    tier_dist = (
        df.groupBy("occupancy_tier")
          .count()
          .orderBy("occupancy_tier")
          .collect()
    )
    log.info("occupancy_tier distribution: %s",
             {r["occupancy_tier"]: r["count"] for r in tier_dist})

    return df


def write_to_mongo(df: DataFrame, log: logging.Logger) -> None:
    log.info("Writing %d records to MongoDB %s.%s (mode=overwrite)",
             df.count(), MONGO_DB, MONGO_COLLECTION)
    (
        df.write
        .format("mongodb")
        .mode("overwrite")
        .option("database", MONGO_DB)
        .option("collection", MONGO_COLLECTION)
        .save()
    )
    log.info("Wrote %d records to MongoDB", df.count())


def prepare_es_index(log: logging.Logger) -> None:
    """
    Delete + recreate the ES index with single-node-friendly settings before
    Spark writes to it.

    Why this is necessary:
      ElasticSearch's default index template asks for 1 primary shard + 1
      replica. With a single-node cluster (our case), the replica has nowhere
      to be placed and stays in `unassigned` state, which leaves the cluster
      stuck on `status: yellow`. The Spark ES connector probes the available
      shards before writing and is confused by the half-assigned state — that
      caused intermittent `Cannot determine write shards` task failures even
      though the writes themselves were succeeding.

      Pre-creating the index with `number_of_replicas: 0` removes the phantom
      replica entirely, the cluster goes to `green`, and the connector is
      happy.
    """
    base = f"http://{ES_NODES}:{ES_PORT}/{ES_INDEX}"
    try:
        # Delete any leftover index from previous runs. 404 is fine.
        requests.delete(base, timeout=10)
        # Recreate with single-node-friendly settings.
        resp = requests.put(
            base,
            json={"settings": {"number_of_shards": 1, "number_of_replicas": 0}},
            timeout=10,
        )
        resp.raise_for_status()
        log.info(
            "Pre-created ES index '%s' with 1 shard, 0 replicas (single-node setup)",
            ES_INDEX,
        )
    except requests.RequestException as exc:
        log.error("Failed to pre-create ES index '%s': %s", ES_INDEX, exc)
        raise


def write_to_elasticsearch(df: DataFrame, log: logging.Logger) -> None:
    log.info("Writing %d records to ElasticSearch index '%s' (mode=overwrite)",
             df.count(), ES_INDEX)
    (
        df.write
        .format("org.elasticsearch.spark.sql")
        .mode("overwrite")
        .option("es.resource", ES_INDEX)
        .option("es.nodes", ES_NODES)
        .option("es.port", ES_PORT)
        .option("es.nodes.wan.only", "false")
        # ES doesn't accept all Spark types natively for some null edge cases;
        # let the connector infer the mapping from data.
        .option("es.mapping.id", "listing_id")
        .save()
    )
    log.info("Wrote %d records to ElasticSearch", df.count())


# ── main ──────────────────────────────────────────────────────────────────
def main() -> int:
    log = setup_logging()
    log.info("=" * 60)
    log.info("M2 Spark pipeline starting")
    log.info("=" * 60)

    spark = create_spark_session()
    log.info("Spark session created (master=%s, version=%s)",
             spark.sparkContext.master, spark.version)

    try:
        df = read_from_s3(spark, log)
        df = clean_data(df, log)
        df = add_derived_columns(df, log)

        # Cache the DataFrame because we read it twice (Mongo + ES write).
        # Without cache, Spark would re-run cleaning and derivation for each
        # output sink — a real performance bug, not just a micro-optimization.
        df.cache()
        log.info("DataFrame cached before dual write (Mongo + ES)")

        write_to_mongo(df, log)
        prepare_es_index(log)
        write_to_elasticsearch(df, log)

        log.info("M2 pipeline complete.")
        return 0

    except Exception as exc:
        log.exception("M2 pipeline FAILED: %s", exc)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())