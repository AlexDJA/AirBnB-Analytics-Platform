"""
safeguards.py
-------------
Implements the 4 safeguards required by the M3 brief.

  Safeguard 1 — Read-only enforcement
      Provided at the *driver level* by connecting to MongoDB as the
      `agent_readonly` user (role: read). Any write attempt raises
      pymongo.errors.OperationFailure. This module exposes the helper
      that surfaces those errors cleanly.

  Safeguard 2 — Hallucination guard
      Provided by check_hallucination_guard(records). If the records
      list is empty, the agent must respond "No data found for this
      query" — never invent an answer.

  Safeguard 3 — Query validator
      Provided by validate_pipeline(pipeline, allowed_collections).
      Rejects any pipeline that:
        - is not a list of objects,
        - uses a forbidden stage ($out, $merge, $mongoExport, ...),
        - references a collection that doesn't exist (e.g. typos
          like "listing" instead of "listings").
      Every rejection is written to logs/safeguard.log so the brief's
      'rejection entry written to a file (not just printed to console)'
      requirement is met.

  Safeguard 4 — Failure handling
      Provided in query_executor.py (the Mongo->ES fallback chain).
      This module just defines the SafeguardError exception that the
      executor catches.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

# ── dedicated file logger for rejections (Safeguard 3 requirement) ───────
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

_safeguard_logger = logging.getLogger("safeguard")
_safeguard_logger.setLevel(logging.INFO)
# Don't double-print onto the root logger.
_safeguard_logger.propagate = False
if not _safeguard_logger.handlers:
    _handler = logging.FileHandler(LOG_DIR / "safeguard.log", encoding="utf-8")
    _handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    _safeguard_logger.addHandler(_handler)


# ── exceptions ───────────────────────────────────────────────────────────
class SafeguardError(Exception):
    """Raised by validate_pipeline when a query is rejected."""


# ── forbidden stages (write/admin operations) ────────────────────────────
FORBIDDEN_STAGES = {
    "$out", "$merge", "$function", "$accumulator",
    "$indexStats", "$collStats", "$listSessions", "$listLocalSessions",
    "$planCacheStats", "$currentOp",
}

# Stages we explicitly allow. Any stage not in this set is also rejected.
ALLOWED_STAGES = {
    "$match", "$group", "$sort", "$limit", "$skip", "$project",
    "$count", "$facet", "$bucket", "$bucketAuto", "$unwind",
    "$addFields", "$set", "$replaceRoot", "$sortByCount",
}


def _log_rejection(reason: str, pipeline: list, extra: dict | None = None) -> None:
    """Write a structured rejection record to logs/safeguard.log."""
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event": "query_rejected",
        "reason": reason,
        "pipeline_preview": json.dumps(pipeline, default=str)[:500],
    }
    if extra:
        record.update(extra)
    _safeguard_logger.info(json.dumps(record))


# ── Safeguard 3 : query validator ────────────────────────────────────────
def validate_pipeline(
    pipeline: list,
    collection: str,
    allowed_collections: Iterable[str],
) -> None:
    """
    Raise SafeguardError if the pipeline is unsafe. Returns None on success.

    Three classes of rejection:
      1. Structural — pipeline must be a list of single-key dicts.
      2. Operation — every stage must be in ALLOWED_STAGES (and never in
         FORBIDDEN_STAGES, defense in depth).
      3. Collection — the target collection must exist in the DB.
    """
    allowed = set(allowed_collections)

    # Rule 1 — structure
    if not isinstance(pipeline, list):
        _log_rejection("not_a_list", pipeline if isinstance(pipeline, list) else [],
                       extra={"got_type": type(pipeline).__name__})
        raise SafeguardError("Pipeline must be a JSON array.")

    for i, stage in enumerate(pipeline):
        if not isinstance(stage, dict) or len(stage) != 1:
            _log_rejection("malformed_stage", pipeline,
                           extra={"stage_index": i, "stage": str(stage)[:200]})
            raise SafeguardError(
                f"Stage {i} must be a single-key object (got: {stage!r})."
            )

        op = next(iter(stage))
        # Rule 2a — explicit deny list
        if op in FORBIDDEN_STAGES:
            _log_rejection("forbidden_stage", pipeline,
                           extra={"stage_index": i, "operator": op})
            raise SafeguardError(f"Stage {i} uses forbidden operator '{op}'.")

        # Rule 2b — unknown operator (not in our allow list)
        if op not in ALLOWED_STAGES:
            _log_rejection("unknown_stage", pipeline,
                           extra={"stage_index": i, "operator": op})
            raise SafeguardError(
                f"Stage {i} uses operator '{op}' which is not in the allow list."
            )

    # Rule 3 — target collection must exist
    if collection not in allowed:
        _log_rejection("invalid_collection", pipeline,
                       extra={"requested": collection,
                              "allowed": sorted(allowed)})
        raise SafeguardError(
            f"Collection '{collection}' does not exist. "
            f"Allowed: {sorted(allowed)}."
        )


# ── Safeguard 2 : hallucination guard ────────────────────────────────────
NO_DATA_MESSAGE = "No data found for this query."


def check_hallucination_guard(records: list) -> str | None:
    """
    Return the canned 'no data' message if records is empty, else None.
    The caller short-circuits the LLM reformatting step on a non-None
    return value, so the LLM is never asked to invent a story.
    """
    if not records:
        return NO_DATA_MESSAGE
    return None