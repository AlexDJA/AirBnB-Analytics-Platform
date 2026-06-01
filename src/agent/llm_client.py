"""
llm_client.py
-------------
Thin wrapper around the OpenRouter Chat Completions API.

Two public functions:
  generate_pipeline(question)  -> list of Mongo aggregation stages
  format_report(question, records) -> plain-English summary string

Both functions wrap one HTTP POST to OpenRouter. We keep this tiny and
synchronous so the rest of the agent reads like a script.
"""
from __future__ import annotations

import json
import logging
import os
import re

import requests

from .prompts import QUERY_GEN_SYSTEM, REPORT_FMT_SYSTEM

log = logging.getLogger(__name__)

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"


class OpenRouterError(RuntimeError):
    """Raised when the OpenRouter API fails or returns an unusable response."""


def _api_key() -> str:
    key = os.environ.get("OPENROUTER_API_KEY")
    if not key:
        raise OpenRouterError(
            "OPENROUTER_API_KEY is not set. Add it to .env."
        )
    return key


def _model() -> str:
    return os.environ.get(
        "OPENROUTER_MODEL", "mistralai/ministral-14b-2512"
    )


def _chat(messages: list[dict], temperature: float = 0.0, max_tokens: int = 800) -> str:
    """
    One blocking call to OpenRouter. Returns the assistant's text content.
    Temperature is 0 by default — we want determinism for query generation.
    max_tokens caps the response length, which is the single biggest lever
    on latency (a model that would otherwise babble for 3000 tokens stops
    at the cap and returns in a fraction of the time).
    """
    payload = {
        "model": _model(),
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    try:
        resp = requests.post(
            OPENROUTER_URL,
            headers={
                "Authorization": f"Bearer {_api_key()}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=60,
        )
    except requests.RequestException as exc:
        raise OpenRouterError(f"OpenRouter network error: {exc}") from exc

    if resp.status_code != 200:
        raise OpenRouterError(
            f"OpenRouter returned HTTP {resp.status_code}: {resp.text[:300]}"
        )

    body = resp.json()
    try:
        return body["choices"][0]["message"]["content"]
    except (KeyError, IndexError) as exc:
        raise OpenRouterError(
            f"Unexpected OpenRouter response shape: {body}"
        ) from exc


# ── JSON extraction ───────────────────────────────────────────────────────
# LLMs love wrapping JSON in markdown fences ```json ... ``` even when told
# not to. We strip them defensively.
_FENCE_RE = re.compile(
    r"^\s*```(?:json)?\s*(.*?)\s*```\s*$",
    re.DOTALL | re.IGNORECASE,
)


def _extract_json_array(raw: str) -> list:
    """
    Pull a JSON array out of an LLM response, even if it's wrapped in
    markdown fences or prefixed with "Here is the pipeline:".
    """
    cleaned = raw.strip()

    # Strip ```json ... ``` or ``` ... ``` wrappers if present.
    fence_match = _FENCE_RE.match(cleaned)
    if fence_match:
        cleaned = fence_match.group(1).strip()

    # If the model still added prose before the array, slice from the first [.
    if not cleaned.startswith("["):
        bracket = cleaned.find("[")
        if bracket == -1:
            raise OpenRouterError(
                f"LLM did not return a JSON array. Raw output: {raw[:300]}"
            )
        cleaned = cleaned[bracket:]

    # Same on the trailing side.
    if not cleaned.endswith("]"):
        last_bracket = cleaned.rfind("]")
        if last_bracket == -1:
            raise OpenRouterError(
                f"LLM output has no closing bracket. Raw: {raw[:300]}"
            )
        cleaned = cleaned[: last_bracket + 1]

    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise OpenRouterError(
            f"LLM returned invalid JSON: {exc.msg}. Raw: {raw[:300]}"
        ) from exc

    if not isinstance(parsed, list):
        raise OpenRouterError(
            f"LLM returned JSON but not an array. Type was {type(parsed).__name__}."
        )

    return parsed


# ── Public API ────────────────────────────────────────────────────────────
def generate_pipeline(question: str) -> list:
    """Translate a natural-language question into a Mongo aggregation pipeline."""
    raw = _chat(
        [
            {"role": "system", "content": QUERY_GEN_SYSTEM},
            {"role": "user", "content": question},
        ],
        temperature=0.0,
        max_tokens=2500,  # large enough to fit any pipeline without truncation
    )
    log.info("LLM raw pipeline output (%d chars): %s",
             len(raw), raw[:200].replace("\n", " "))
    return _extract_json_array(raw)


def format_report(question: str, records: list[dict]) -> str:
    """Turn raw query results into a plain-English business answer."""
    user_msg = (
        f"Question: {question}\n\n"
        f"Records returned ({len(records)} total):\n{json.dumps(records, default=str, indent=2)}"
    )
    return _chat(
        [
            {"role": "system", "content": REPORT_FMT_SYSTEM},
            {"role": "user", "content": user_msg},
        ],
        temperature=0.3,  # a touch of variation in phrasing, but still grounded
        max_tokens=400,   # business reports should be 2-4 sentences, not essays
    ).strip()