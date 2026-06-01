"""
app.py
------
Minimal FastAPI application exposing the M3 agent over HTTP.

Endpoints:
  GET  /              — serves a one-page HTML chat interface
  POST /ask           — JSON {"question": "..."} -> JSON {"response": "..."}
  GET  /healthz       — simple liveness probe

The agent flow on /ask:
  1. Detect injection patterns. If found, return the locked role response
     and log injection_detected=true to logs/audit.log.
  2. Ask the LLM for a Mongo aggregation pipeline.
  3. Validate the pipeline (safeguards.validate_pipeline).
  4. Execute through the Mongo->ES->cache fallback chain.
  5. Hallucination guard: if 0 records, return canned 'no data' message.
  6. Otherwise, ask the LLM to format the records into a business report.
  7. Log everything to logs/agent.log and logs/audit.log.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

# Make `src.agent.*` importable when running this file via uvicorn.
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.agent.llm_client import (  # noqa: E402
    OpenRouterError,
    format_report,
    generate_pipeline,
)
from src.agent.prompts import (  # noqa: E402
    LOCKED_ROLE_RESPONSE,
    detect_injection,
    detect_small_talk,
    small_talk_response,
)
from src.agent.query_executor import (  # noqa: E402
    DEFAULT_COLLECTION,
    DB_NAME,
    ExecutorError,
    execute_with_fallback,
)
from src.agent.safeguards import (  # noqa: E402
    NO_DATA_MESSAGE,
    SafeguardError,
    check_hallucination_guard,
    validate_pipeline,
)


# ── logging setup ────────────────────────────────────────────────────────
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

# agent.log: regular operational log (queries, counts, errors).
agent_log = logging.getLogger("agent")
agent_log.setLevel(logging.INFO)
agent_log.propagate = False
if not agent_log.handlers:
    h = logging.FileHandler(LOG_DIR / "agent.log", encoding="utf-8")
    h.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    agent_log.addHandler(h)
    agent_log.addHandler(logging.StreamHandler(sys.stdout))

# audit.log: structured JSON, one line per exchange. Required for the
# M3 grading rubric (session_id, user_input, agent_response, injection_detected).
audit_log = logging.getLogger("audit")
audit_log.setLevel(logging.INFO)
audit_log.propagate = False
if not audit_log.handlers:
    h = logging.FileHandler(LOG_DIR / "audit.log", encoding="utf-8")
    # Audit log is pure JSON lines — no formatter prefix.
    h.setFormatter(logging.Formatter("%(message)s"))
    audit_log.addHandler(h)


def _audit(record: dict) -> None:
    """Append one JSON line to audit.log."""
    record.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
    audit_log.info(json.dumps(record, default=str))


# ── allowed collections (cached at startup) ──────────────────────────────
def _list_allowed_collections() -> list[str]:
    """Read the list of collections in the airbnb DB once at startup."""
    try:
        from pymongo import MongoClient
        client = MongoClient(
            os.environ.get(
                "AGENT_MONGO_URI",
                "mongodb://agent_readonly:agentpass@mongodb:27017/airbnb?authSource=airbnb",
            ),
            serverSelectionTimeoutMS=3000,
        )
        return client[DB_NAME].list_collection_names()
    except Exception as exc:
        agent_log.warning("Could not list Mongo collections at startup: %s", exc)
        return [DEFAULT_COLLECTION]


ALLOWED_COLLECTIONS = _list_allowed_collections()
agent_log.info("Agent starting. Allowed collections: %s", ALLOWED_COLLECTIONS)


# ── FastAPI app ──────────────────────────────────────────────────────────
app = FastAPI(title="M3 Airbnb Agent", version="1.0")


class AskRequest(BaseModel):
    question: str


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok", "allowed_collections": ALLOWED_COLLECTIONS}


class ValidateRequest(BaseModel):
    """Payload for the debug validator endpoint."""
    pipeline: list
    collection: str = DEFAULT_COLLECTION


@app.post("/debug/validate-pipeline")
def debug_validate(req: ValidateRequest) -> JSONResponse:
    """
    Debug-only endpoint to demonstrate Safeguard 3 (query validator).

    Posting a pipeline targeting a non-existent collection or using a
    forbidden stage returns a 422 with the rejection reason, and a
    structured entry is written to logs/safeguard.log.

    Example call (rejected — invalid collection):
        curl -X POST http://localhost:8000/debug/validate-pipeline \\
             -H "Content-Type: application/json" \\
             -d '{"pipeline":[{"$match":{}}], "collection":"fake_users"}'

    Example call (rejected — forbidden stage):
        curl -X POST http://localhost:8000/debug/validate-pipeline \\
             -H "Content-Type: application/json" \\
             -d '{"pipeline":[{"$out":"backup"}], "collection":"listings"}'
    """
    try:
        validate_pipeline(req.pipeline, req.collection, ALLOWED_COLLECTIONS)
    except SafeguardError as exc:
        agent_log.warning("Debug validator REJECTED: %s", exc)
        return JSONResponse(
            {"accepted": False, "rejection_reason": str(exc),
             "logged_to": "logs/safeguard.log"},
            status_code=422,
        )
    return JSONResponse({"accepted": True, "pipeline_stages": len(req.pipeline)})


@app.post("/ask")
def ask(req: AskRequest) -> JSONResponse:
    session_id = str(uuid.uuid4())[:8]
    question = req.question.strip()
    agent_log.info("[%s] User question: %s", session_id, question)

    # ── Safeguard: injection detection ─────────────────────────────
    injection = detect_injection(question)
    if injection:
        agent_log.warning(
            "[%s] Injection pattern detected (%s) — returning locked role",
            session_id, injection,
        )
        _audit({
            "session_id": session_id,
            "user_input": question,
            "agent_response": LOCKED_ROLE_RESPONSE,
            "injection_detected": True,
            "injection_pattern": injection,
        })
        return JSONResponse({
            "response": LOCKED_ROLE_RESPONSE,
            "source": "safeguard:injection",
        })

    # ── Small-talk short-circuit ───────────────────────────────────
    # Greetings, thanks and "who are you" don't need a database trip.
    # Catching them locally saves two LLM calls and ~5s of latency.
    # We distinguish "greeting" (long welcome with suggestions) from
    # "thanks" (short acknowledgement) so the reply feels natural.
    small_talk_kind = detect_small_talk(question)
    if small_talk_kind:
        response_text = small_talk_response(small_talk_kind)
        agent_log.info("[%s] Small-talk detected (%s) — returning canned reply",
                       session_id, small_talk_kind)
        _audit({
            "session_id": session_id,
            "user_input": question,
            "agent_response": response_text,
            "injection_detected": False,
            "small_talk": small_talk_kind,
        })
        return JSONResponse({
            "response": response_text,
            "source": f"small-talk:{small_talk_kind}",
        })

    # ── LLM call 1: generate the pipeline ──────────────────────────
    try:
        pipeline = generate_pipeline(question)
    except OpenRouterError as exc:
        agent_log.error("[%s] OpenRouter failed at pipeline generation: %s",
                        session_id, exc)
        msg = ("The language model is temporarily unavailable. "
               "Please try again in a moment.")
        _audit({"session_id": session_id, "user_input": question,
                "agent_response": msg, "injection_detected": False,
                "error": str(exc)})
        return JSONResponse({"response": msg, "source": "error:openrouter"},
                            status_code=503)

    agent_log.info("[%s] Pipeline generated (%d stages): %s",
                   session_id, len(pipeline),
                   json.dumps(pipeline, default=str)[:300])

    # ── Safeguard 3: validate the pipeline ─────────────────────────
    try:
        validate_pipeline(pipeline, DEFAULT_COLLECTION, ALLOWED_COLLECTIONS)
    except SafeguardError as exc:
        agent_log.warning("[%s] Pipeline rejected by validator: %s",
                          session_id, exc)
        msg = f"I couldn't run that query safely: {exc}"
        _audit({"session_id": session_id, "user_input": question,
                "agent_response": msg, "injection_detected": False,
                "validator_rejection": str(exc)})
        return JSONResponse({"response": msg, "source": "safeguard:validator"})

    # ── Safeguard 4: execute with fallback ─────────────────────────
    try:
        records, source = execute_with_fallback(pipeline, DEFAULT_COLLECTION, question)
    except ExecutorError as exc:
        agent_log.error("[%s] All datastores failed: %s", session_id, exc)
        msg = ("All data backends are currently unavailable and no "
               "cached result exists. Please try again later.")
        _audit({"session_id": session_id, "user_input": question,
                "agent_response": msg, "injection_detected": False,
                "error": "all_backends_down"})
        return JSONResponse({"response": msg, "source": "error:all_backends_down"},
                            status_code=503)
    except Exception as exc:
        # Last-resort net so the frontend always gets clean JSON, never HTML.
        # Auth failures (read-only user attempted a write) land here — that's
        # Safeguard 1 firing at the driver level.
        from pymongo.errors import OperationFailure
        is_auth = isinstance(exc, OperationFailure) and (
            getattr(exc, "code", None) == 13 or "not authorized" in str(exc).lower()
        )
        if is_auth:
            agent_log.warning("[%s] Mongo refused write at driver level: %s",
                              session_id, exc)
            msg = ("The generated query attempted a write operation, which is "
                   "blocked at the database driver level. Please rephrase as "
                   "a read-only question.")
            _audit({"session_id": session_id, "user_input": question,
                    "agent_response": msg, "injection_detected": False,
                    "driver_auth_failure": str(exc)[:300]})
            return JSONResponse({"response": msg, "source": "safeguard:readonly"})
        agent_log.exception("[%s] Unexpected executor error: %s", session_id, exc)
        msg = ("An unexpected error occurred while running the query. "
               "Please try rephrasing or pick a simpler question.")
        _audit({"session_id": session_id, "user_input": question,
                "agent_response": msg, "injection_detected": False,
                "error": str(exc)[:300]})
        return JSONResponse({"response": msg, "source": "error:unexpected"},
                            status_code=500)

    agent_log.info("[%s] Executed via %s — %d records",
                   session_id, source, len(records))

    # ── Safeguard 2: hallucination guard ───────────────────────────
    no_data = check_hallucination_guard(records)
    if no_data:
        agent_log.info("[%s] Hallucination guard triggered — empty result", session_id)
        _audit({"session_id": session_id, "user_input": question,
                "agent_response": no_data, "injection_detected": False,
                "records_returned": 0, "source": source})
        return JSONResponse({"response": no_data, "source": f"{source}:no-data"})

    # ── LLM call 2: format the records into a business report ──────
    try:
        report = format_report(question, records[:50])  # cap to keep token cost down
    except OpenRouterError as exc:
        agent_log.error("[%s] OpenRouter failed at report formatting: %s",
                        session_id, exc)
        # Graceful degradation: dump the raw records instead of crashing.
        report = (
            f"Query returned {len(records)} records but the language model "
            f"is temporarily unavailable for formatting. Sample of the data:\n"
            f"{json.dumps(records[:5], default=str, indent=2)}"
        )

    _audit({"session_id": session_id, "user_input": question,
            "agent_response": report, "injection_detected": False,
            "records_returned": len(records), "source": source})

    return JSONResponse({"response": report, "source": source,
                         "records_returned": len(records)})


# ── Single-page HTML interface ───────────────────────────────────────────
_CHAT_HTML_PATH = Path(__file__).parent / "chat.html"


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    """Serve the single-page Vue chat UI from chat.html."""
    if not _CHAT_HTML_PATH.exists():
        return "<h1>chat.html missing</h1>"
    return _CHAT_HTML_PATH.read_text(encoding="utf-8")