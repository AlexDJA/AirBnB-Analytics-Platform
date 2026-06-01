"""
query_executor.py
-----------------
Executes validated aggregation pipelines against MongoDB (primary) and
falls back to ElasticSearch if Mongo is unavailable. Implements Safeguard 4
of the M3 brief.

Fallback chain:
  1. MongoDB (agent_readonly user, role: read)
  2. ElasticSearch (best-effort translation of the most common stages)
  3. Last cached successful result (with a "stale" warning timestamp)
  4. Final descriptive error

Read-only enforcement (Safeguard 1) is provided by the Mongo URI itself:
the agent connects as `agent_readonly`. Any write attempt — even one that
bypasses our Python-level validator — is rejected by Mongo at the wire
protocol with `OperationFailure: not authorized`.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from elasticsearch import Elasticsearch
from elasticsearch import ConnectionError as ESConnectionError
from elasticsearch import TransportError
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure, ServerSelectionTimeoutError

log = logging.getLogger(__name__)

CACHE_DIR = Path("logs")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_FILE = CACHE_DIR / "last_report_cache.json"

# Read-only credentials — created by mongo-init/init-users.js.
MONGO_URI = os.environ.get(
    "AGENT_MONGO_URI",
    "mongodb://agent_readonly:agentpass@mongodb:27017/airbnb?authSource=airbnb",
)
DB_NAME = "airbnb"
DEFAULT_COLLECTION = "listings"

ES_URL = os.environ.get("ES_URL", "http://elasticsearch:9200")
ES_INDEX = "airbnb_listings"


class ExecutorError(RuntimeError):
    """All datastores failed and no usable cache was available."""


# ── Mongo (primary) ──────────────────────────────────────────────────────
def _try_mongo(pipeline: list, collection: str) -> list[dict] | None:
    """
    Try to run the aggregation against Mongo.

    Three possible outcomes:
      - SUCCESS    -> return the records list.
      - CONNECT    -> Mongo unreachable; return None so caller falls back to ES.
      - AUTH FAIL  -> the agent's read-only user tried a write; re-raise so the
                      caller surfaces "query rejected at driver level" (defense
                      in depth on top of the validator).
      - EXEC FAIL  -> the pipeline parsed and authorized, but the database
                      raised an executor error (e.g. $size on a string field).
                      We treat this as "no usable result" so the user gets a
                      clean message instead of a 500 — and we log it for the
                      operator. We don't fall back to ES because the pipeline
                      shape was Mongo-specific in the first place.
    """
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        coll = client[DB_NAME][collection]
        cursor = coll.aggregate(pipeline)
        records = list(cursor)
        log.info("Mongo executed pipeline OK — %d records returned", len(records))
        return records
    except (ConnectionFailure, ServerSelectionTimeoutError) as exc:
        log.warning("Mongo unreachable, will fall back: %s", exc)
        return None
    except OperationFailure as exc:
        # Mongo reports auth failures with code 13 (Unauthorized) or via the
        # error message starting with "not authorized". Anything else is an
        # execution-time error in the pipeline itself.
        code = exc.code if hasattr(exc, "code") else None
        msg = str(exc)
        is_auth = code == 13 or "not authorized" in msg.lower()
        if is_auth:
            log.error("Mongo rejected the operation (auth): %s", exc)
            raise
        # Execution failure -> log and return empty list so the agent answers
        # gracefully via the hallucination guard ("No data found").
        log.warning(
            "Mongo pipeline executed but raised at runtime "
            "(returning empty result so the agent answers cleanly): %s",
            msg[:300],
        )
        return []


# ── ES (fallback) ────────────────────────────────────────────────────────
def _pipeline_to_es_query(pipeline: list) -> dict | None:
    """
    Translate the subset of pipeline stages we can handle into an ES query.
    Only $match (-> bool/filter) and $limit (-> size) are supported here —
    enough to serve fallback during a Mongo outage for the most common
    summary/top-N questions. Returns None if the pipeline is too complex.
    """
    body: dict = {"size": 10, "query": {"match_all": {}}}
    filters: list = []
    has_aggregation = False

    for stage in pipeline:
        op, spec = next(iter(stage.items()))
        if op == "$match":
            for field, value in spec.items():
                if isinstance(value, (str, int, float, bool)):
                    filters.append({"term": {f"{field}.keyword" if isinstance(value, str) else field: value}})
                elif isinstance(value, dict):
                    range_clause = {}
                    for ranged_op, ranged_val in value.items():
                        if ranged_op in {"$gte", "$gt", "$lte", "$lt"}:
                            range_clause[ranged_op.lstrip("$")] = ranged_val
                    if range_clause:
                        filters.append({"range": {field: range_clause}})
        elif op == "$limit":
            body["size"] = int(spec)
        elif op in {"$group", "$bucket", "$facet"}:
            has_aggregation = True
            break

    if has_aggregation:
        # ES aggregations have a totally different shape; we don't try
        # to translate them. Caller will see None and skip ES fallback.
        return None

    if filters:
        body["query"] = {"bool": {"filter": filters}}
    return body


def _try_es(pipeline: list) -> list[dict] | None:
    """
    Try to translate-and-run the pipeline against ES. Returns the hits
    list, an empty list, or None if translation isn't possible / ES is
    unreachable.
    """
    body = _pipeline_to_es_query(pipeline)
    if body is None:
        log.warning("ES fallback skipped: pipeline too complex to translate")
        return None
    try:
        es = Elasticsearch(ES_URL, request_timeout=5)
        resp = es.search(index=ES_INDEX, body=body)
        hits = [h["_source"] for h in resp["hits"]["hits"]]
        log.info("ES fallback executed OK — %d hits returned", len(hits))
        return hits
    except (ESConnectionError, TransportError) as exc:
        log.warning("ES unreachable: %s", exc)
        return None


# ── Cache (3rd fallback level) ───────────────────────────────────────────
def _save_to_cache(question: str, records: list, source: str) -> None:
    """Save the last successful result so we can serve it stale if both DBs fail."""
    try:
        payload = {
            "question": question,
            "records": records,
            "source": source,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        CACHE_FILE.write_text(json.dumps(payload, default=str, indent=2))
    except OSError as exc:
        log.warning("Could not save report cache: %s", exc)


def _load_from_cache() -> dict | None:
    if not CACHE_FILE.exists():
        return None
    try:
        return json.loads(CACHE_FILE.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        log.warning("Could not load report cache: %s", exc)
        return None


# ── Public entry point ───────────────────────────────────────────────────
def execute_with_fallback(
    pipeline: list,
    collection: str,
    question: str,
) -> tuple[list[dict], str]:
    """
    Try Mongo, then ES, then cached result.
    Returns (records, source) where source is one of:
      "mongodb", "elasticsearch", "cache-stale"
    Raises ExecutorError if all three levels failed.
    """
    # Level 1: Mongo primary.
    mongo_result = _try_mongo(pipeline, collection)
    if mongo_result is not None:
        _save_to_cache(question, mongo_result, "mongodb")
        return mongo_result, "mongodb"

    log.warning("Falling back from Mongo to ElasticSearch (Safeguard 4 triggered)")

    # Level 2: ES fallback.
    es_result = _try_es(pipeline)
    if es_result is not None:
        _save_to_cache(question, es_result, "elasticsearch")
        return es_result, "elasticsearch"

    log.warning("Falling back from ES to cached result")

    # Level 3: stale cache.
    cached = _load_from_cache()
    if cached:
        log.warning("Serving stale cache from %s", cached["timestamp"])
        return cached["records"], f"cache-stale (since {cached['timestamp']})"

    raise ExecutorError(
        "All datastores are unavailable and no cached result exists."
    )