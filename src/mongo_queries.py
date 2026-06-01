"""
mongo_queries.py
----------------
M2 Mongo queries: three aggregation pipelines against the cleaned listings
collection populated by spark_pipeline.py.

Each query uses a DIFFERENT MongoDB operation, as required by the M2 brief:
    Q1 -> $group       (aggregate revenue and efficiency by occupancy tier)
    Q2 -> $sort+$limit (top 10 listings by revenue per booked day)
    Q3 -> $match       (multi-condition business filter on premium performers)

Run:
    docker compose exec ingest python src/mongo_queries.py
"""
from __future__ import annotations

import json
import sys
from typing import Any

from pymongo import MongoClient

# Inside the Docker network we reach Mongo by its service hostname.
MONGO_URI = "mongodb://spark_writer:sparkpass@mongodb:27017/airbnb?authSource=airbnb"
DB_NAME = "airbnb"
COLLECTION = "listings"


def header(title: str, op: str) -> None:
    print()
    print("=" * 76)
    print(f"{title}")
    print(f"Operation used: {op}")
    print("=" * 76)


def pretty(doc: Any) -> str:
    """JSON-encode a Mongo doc for printing, handling ObjectId and BSON quirks."""
    return json.dumps(doc, default=str, indent=2, ensure_ascii=False)


# ── Q1 ────────────────────────────────────────────────────────────────────
def query_1_group_by_occupancy_tier(coll) -> None:
    """
    Q1 — Revenue and pricing efficiency by occupancy tier.
    Business question: "Does higher occupancy translate into higher revenue
    per booked day, or do high-occupancy listings price themselves down?"

    Uses $group on the `occupancy_tier` derived column. The output lets us
    compare segment-level averages (mean revenue + mean efficiency) and
    spot whether `High` tier listings really are the cash-cows we expect.
    """
    header("Q1 — Revenue and efficiency by occupancy_tier",
           "$group  (with $match upfront to drop null tiers)")

    pipeline = [
        {"$match": {"occupancy_tier": {"$ne": None}}},
        {"$group": {
            "_id": "$occupancy_tier",
            "listings_count": {"$sum": 1},
            "avg_ttm_revenue_usd": {"$avg": "$ttm_revenue"},
            "avg_revenue_per_booked_day": {"$avg": "$revenue_per_booked_day"},
            "avg_occupancy": {"$avg": "$ttm_occupancy"},
        }},
        {"$project": {
            "_id": 0,
            "occupancy_tier": "$_id",
            "listings_count": 1,
            "avg_ttm_revenue_usd": {"$round": ["$avg_ttm_revenue_usd", 2]},
            "avg_revenue_per_booked_day": {"$round": ["$avg_revenue_per_booked_day", 2]},
            "avg_occupancy": {"$round": ["$avg_occupancy", 3]},
        }},
        {"$sort": {"avg_ttm_revenue_usd": -1}},
    ]

    results = list(coll.aggregate(pipeline))
    for row in results:
        print(pretty(row))


# ── Q2 ────────────────────────────────────────────────────────────────────
def query_2_top_efficient_listings(coll) -> None:
    """
    Q2 — Top 10 listings by revenue per booked day.
    Business question: "Which individual listings extract the most revenue
    out of every night they were actually booked? These are the pricing
    benchmarks for their local market."

    Uses $sort + $limit on the derived `revenue_per_booked_day` column.
    A small $match upfront removes nulls (listings with 0 reserved days)
    so the top 10 are real performers rather than NaN edge cases.
    """
    header("Q2 — Top 10 listings by revenue_per_booked_day",
           "$sort + $limit  (with $match upfront to drop nulls)")

    pipeline = [
        {"$match": {
            "revenue_per_booked_day": {"$ne": None, "$gt": 0},
            "ttm_reserved_days": {"$gt": 10},  # filter out 1-night flukes
        }},
        {"$sort": {"revenue_per_booked_day": -1}},
        {"$limit": 10},
        {"$project": {
            "_id": 0,
            "listing_id": 1,
            "city": 1,
            "country": 1,
            "room_type": 1,
            "ttm_reserved_days": 1,
            "ttm_revenue": 1,
            "revenue_per_booked_day": 1,
            "rating_overall": 1,
        }},
    ]

    results = list(coll.aggregate(pipeline))
    for row in results:
        print(pretty(row))


# ── Q3 ────────────────────────────────────────────────────────────────────
def query_3_high_performer_segment(coll) -> None:
    """
    Q3 — High-performer segment: high occupancy AND high pricing AND credible
    review count.
    Business question: "Which listings are simultaneously well-booked,
    well-priced, and well-reviewed? This is the segment to study when
    building a 'what makes a top performer?' model."

    Uses $match with a real composite business filter:
        - occupancy_tier == "High"   (derived column)
        - revenue_per_booked_day > 200 USD   (derived column)
        - num_reviews >= 50  (credibility threshold)

    Output sorted + capped at 5 docs so the result fits in a screenshot.
    """
    header("Q3 — High-performer segment (occupancy High + price > $200 + 50+ reviews)",
           "$match  (composite business filter on derived + native columns)")

    pipeline = [
        {"$match": {
            "occupancy_tier": "High",
            "revenue_per_booked_day": {"$gt": 200},
            "num_reviews": {"$gte": 50},
        }},
        {"$sort": {"revenue_per_booked_day": -1}},
        {"$limit": 5},
        {"$project": {
            "_id": 0,
            "listing_id": 1,
            "city": 1,
            "country": 1,
            "room_type": 1,
            "superhost": 1,
            "is_premium": 1,
            "rating_overall": 1,
            "num_reviews": 1,
            "ttm_occupancy": 1,
            "revenue_per_booked_day": 1,
        }},
    ]

    results = list(coll.aggregate(pipeline))

    # Also count the full segment size, not just the top-5 we printed
    full_count = coll.count_documents({
        "occupancy_tier": "High",
        "revenue_per_booked_day": {"$gt": 200},
        "num_reviews": {"$gte": 50},
    })
    print(f"Segment size: {full_count} listings match all 3 conditions")
    print(f"Showing top 5 by revenue_per_booked_day:")
    print()
    for row in results:
        print(pretty(row))


# ── main ──────────────────────────────────────────────────────────────────
def main() -> int:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    coll = client[DB_NAME][COLLECTION]

    total = coll.count_documents({})
    print(f"Connected to {DB_NAME}.{COLLECTION}: {total} documents")

    try:
        query_1_group_by_occupancy_tier(coll)
        query_2_top_efficient_listings(coll)
        query_3_high_performer_segment(coll)
        print()
        print("All 3 Mongo queries completed.")
        return 0
    except Exception as exc:
        print(f"Query failed: {exc}", file=sys.stderr)
        return 1
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())