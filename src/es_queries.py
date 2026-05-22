"""
es_queries.py
-------------
M2 ElasticSearch queries: three filter/search queries against the
`airbnb_listings` index populated by spark_pipeline.py.

Three different query types to show ES feature coverage:
    Q1 -> term filter         (exact match on keyword field)
    Q2 -> range + bool filter (numeric range combined with a category filter)
    Q3 -> match full-text     (real text search on the amenities field)

Run:
    docker compose exec ingest python src/es_queries.py
"""
from __future__ import annotations

import json
import sys

from elasticsearch import Elasticsearch

ES_URL = "http://elasticsearch:9200"
ES_INDEX = "airbnb_listings"


def header(title: str, query_type: str) -> None:
    print()
    print("=" * 76)
    print(f"{title}")
    print(f"Query type: {query_type}")
    print("=" * 76)


def pretty(obj) -> str:
    return json.dumps(obj, default=str, indent=2, ensure_ascii=False)


def print_query_and_results(es: Elasticsearch, body: dict, label: str) -> None:
    """Print the query JSON, then run it and show total + first hits."""
    print(f"\nQuery body sent to ES /{ES_INDEX}/_search :")
    print(pretty(body))

    resp = es.search(index=ES_INDEX, body=body)
    total = resp["hits"]["total"]["value"]
    hits = resp["hits"]["hits"]

    print(f"\nTotal matching documents: {total}")
    print(f"Showing first {len(hits)} hit(s):\n")
    for h in hits:
        print(pretty(h["_source"]))


# ── Q1 ────────────────────────────────────────────────────────────────────
def query_1_term_filter(es: Elasticsearch) -> None:
    """
    Q1 — Premium listings in France.
    Business question: "How many premium listings (superhost + rating >= 4.8)
    do we have in France, and what do the top ones look like?"

    Showcases ES `term` filtering — exact matches on keyword fields.
    Cheaper than a full `match` query because no analyzer / scoring runs.
    """
    header("Q1 — Premium listings in France",
           "bool.filter with two `term` clauses (exact keyword matches)")

    body = {
        "track_total_hits": True,
        "size": 3,
        "query": {
            "bool": {
                "filter": [
                    {"term": {"country.keyword": "France"}},
                    {"term": {"is_premium": True}},
                ]
            }
        },
        "sort": [{"ttm_revenue": "desc"}],
        "_source": [
            "listing_id", "city", "country", "room_type",
            "superhost", "rating_overall", "ttm_revenue", "is_premium",
        ],
    }
    print_query_and_results(es, body, "Q1")


# ── Q2 ────────────────────────────────────────────────────────────────────
def query_2_range_with_bool(es: Elasticsearch) -> None:
    """
    Q2 — High-revenue listings in the High occupancy tier.
    Business question: "Among well-booked listings (`occupancy_tier=High`),
    which ones cross the 100k USD trailing-12-month revenue mark?"

    Showcases ES `range` query combined with a `term` filter inside a
    `bool` — the canonical way to express "AND" semantics in ES.
    """
    header("Q2 — High-revenue listings (>= $100k) in occupancy tier 'High'",
           "bool.filter with a `range` and a `term` clause")

    body = {
        "track_total_hits": True,
        "size": 3,
        "query": {
            "bool": {
                "filter": [
                    {"range": {"ttm_revenue": {"gte": 100000}}},
                    {"term": {"occupancy_tier.keyword": "High"}},
                ]
            }
        },
        "sort": [{"ttm_revenue": "desc"}],
        "_source": [
            "listing_id", "city", "country", "room_type",
            "ttm_revenue", "ttm_occupancy", "occupancy_tier",
        ],
    }
    print_query_and_results(es, body, "Q2")


# ── Q3 ────────────────────────────────────────────────────────────────────
def query_3_match_full_text(es: Elasticsearch) -> None:
    """
    Q3 — Listings that mention "pool" in their amenities.
    Business question: "Pool is a major filter for travelers. How many
    listings in the index advertise one, and what are the top-rated ones?"

    Showcases ES `match` — actual text analysis (lowercasing, tokenization)
    on the `amenities` field. This is what makes ES a search engine rather
    than a key-value store.
    """
    header("Q3 — Listings with 'pool' in amenities (full-text search)",
           "match query (text analysis on the `amenities` field)")

    body = {
        "track_total_hits": True,
        "size": 3,
        "query": {
            "match": {
                "amenities": "pool",
            }
        },
        "sort": [
            {"rating_overall": "desc"},
            "_score",
        ],
        "_source": [
            "listing_id", "city", "country", "room_type",
            "rating_overall", "num_reviews",
        ],
    }
    print_query_and_results(es, body, "Q3")


# ── main ──────────────────────────────────────────────────────────────────
def main() -> int:
    es = Elasticsearch(ES_URL, request_timeout=10)
    if not es.ping():
        print("ES ping failed", file=sys.stderr)
        return 1

    total = es.count(index=ES_INDEX)["count"]
    print(f"Connected to ES index '{ES_INDEX}': {total} documents")

    try:
        query_1_term_filter(es)
        query_2_range_with_bool(es)
        query_3_match_full_text(es)
        print()
        print("All 3 ES queries completed.")
        return 0
    except Exception as exc:
        print(f"Query failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())