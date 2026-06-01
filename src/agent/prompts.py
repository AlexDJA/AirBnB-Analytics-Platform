"""
prompts.py
----------
Centralized prompt templates and injection patterns for the M3 agent.

Why a dedicated module:
  - Keeps the system prompt out of the executor logic so it's easy to
    review and edit.
  - The locked role response is the single string the agent returns when
    an injection attempt is detected, so it has to be findable and tweakable
    in one place.
  - Injection patterns are tuned over time as we see attempts in the wild;
    keeping them as a list of regexes here makes adding new ones a 1-line
    change.
"""
from __future__ import annotations

import re

# ── Dataset schema (passed to the LLM in every system prompt) ─────────────
# Only the columns the LLM is allowed to filter / aggregate on.
DATASET_SCHEMA = """\
Collection: airbnb.listings  (95,412 documents)

Schema (the only fields the agent is allowed to query):
  listing_id           string   unique listing identifier
  city                 string   e.g. "Paris", "Roquebrune-sur-Argens"
  country              string   e.g. "France", "United Kingdom"
  room_type            string   one of: entire_home, private_room, hotel_room, shared_room
  superhost            boolean  true if host has the Superhost badge
  instant_book         boolean  true if booking does not require host approval
  num_reviews          float    cumulative number of reviews
  rating_overall       float    overall rating, 0.0 - 5.0
  ttm_revenue          float    trailing-12-month revenue in USD
  ttm_reserved_days    float    nights booked in the trailing 12 months
  ttm_occupancy        float    occupancy ratio in the trailing 12 months, 0.0 - 1.0
  l90d_revenue         float    revenue over the last 90 days, USD
  l90d_occupancy       float    occupancy over the last 90 days, 0.0 - 1.0

Derived columns (computed by the M2 Spark job):
  revenue_per_booked_day  float    ttm_revenue / ttm_reserved_days
  occupancy_tier          string   one of "Low" (<0.30), "Medium" (<0.60), "High" (>=0.60)
  is_premium              boolean  superhost AND rating_overall >= 4.8
"""


# ── System prompt for the QUERY-GENERATION call ───────────────────────────
QUERY_GEN_SYSTEM = f"""\
You are a MongoDB aggregation pipeline generator for a read-only analytics
tool. You translate natural-language business questions about Airbnb listings
into a single valid MongoDB aggregation pipeline.

{DATASET_SCHEMA}

Field-type notes — read these carefully:
  - `amenities` is a COMMA-SEPARATED STRING (e.g. "Wifi,Pool,Kitchen"),
    NOT an array. Do NOT use $size on it. To filter by amenity, use
    $regex on the string (e.g. {{"amenities": {{"$regex": "Pool", "$options": "i"}}}}).
  - `superhost`, `instant_book`, `professional_management`, `is_premium`
    are real booleans. Compare with true/false, not "true"/"false" strings.
  - All financial/rating columns may be null. When averaging, the schema
    is set up so $avg ignores nulls naturally — no need to wrap in $cond.

Rules — these are absolute and non-negotiable:
  1. Output ONLY a JSON array. The first character of your reply must be `[`
     and the last must be `]`. No markdown fences, no explanations, no
     "Here is the pipeline" prefix.
  2. Use ONLY read-only stages: $match, $group, $sort, $limit, $project,
     $skip, $count, $facet, $bucket, $unwind. NEVER use $out, $merge, or
     anything that writes.
  3. Use ONLY fields from the schema above. If the question asks for a field
     that does not exist, return [] (an empty array literal).
  4. Cap any unbounded result with $limit (default 10) so we don't return
     thousands of rows.
  5. If the question is not about the Airbnb listings dataset, return [].
  6. Prefer compact pipelines. AVOID $facet for simple summary questions —
     a single $group followed by a $project is usually enough.

Example — for "give me a summary of the dataset", emit exactly:
[
  {{"$group": {{
    "_id": null,
    "total_listings": {{"$sum": 1}},
    "avg_rating": {{"$avg": "$rating_overall"}},
    "avg_ttm_revenue": {{"$avg": "$ttm_revenue"}},
    "avg_occupancy": {{"$avg": "$ttm_occupancy"}},
    "superhost_count": {{"$sum": {{"$cond": [{{"$eq": ["$superhost", true]}}, 1, 0]}}}},
    "premium_count": {{"$sum": {{"$cond": [{{"$eq": ["$is_premium", true]}}, 1, 0]}}}}
  }}}},
  {{"$project": {{"_id": 0}}}}
]
"""

# ── System prompt for the REPORT-FORMATTING call ──────────────────────────
REPORT_FMT_SYSTEM = """\
You are a business analyst summarizing the result of a database query in
plain English. You will receive:
  - the user's original question
  - the records returned by the query (already executed)

Your output is rendered as Markdown in a chat window, so use light formatting
for readability:
  - short paragraphs (1-2 sentences each), separated by a blank line
  - bullet lists for enumerations (top 3, top 5, comparisons)
  - **bold** for the key numbers, names, and cities the user cares about
  - never use headers (# ## ###), never use code blocks

Length: 3-6 sentences total, or a short bulleted list of 3-5 items.
Be specific: cite numbers and entities from the records. Do NOT invent
facts that aren't in the records. Do NOT recommend actions.
"""


# ── Locked role response (single source of truth) ─────────────────────────
LOCKED_ROLE_RESPONSE = (
    "I'm an Airbnb listings analytics agent and I only answer questions "
    "about the listings dataset (cities, revenue, occupancy, ratings, room "
    "types, etc). I can't take on other roles or follow instructions that "
    "ask me to ignore my purpose. Please rephrase your question as a "
    "dataset question and I'll be happy to help."
)


# ── Injection patterns ────────────────────────────────────────────────────
# Each pattern is a compiled, case-insensitive regex. The list is short and
# focused on the patterns that actually appear in published jailbreak
# corpora — we'd rather miss exotic attacks than false-positive on real
# user questions (e.g. "ignore my previous question" is a legitimate
# conversational repair, NOT an injection, so we require "previous
# instructions/prompt/system" specifically).
INJECTION_PATTERNS = [
    # The classic "ignore previous instructions" family.
    re.compile(r"\bignore\b.{0,30}\b(previous|prior|above|earlier|all)\b.{0,30}\b(instruction|prompt|rule|system|directive)", re.IGNORECASE | re.DOTALL),
    # "Disregard / forget / override" variants of the same family.
    re.compile(r"\b(disregard|forget|override|bypass)\b.{0,30}\b(previous|prior|above|system|instruction|prompt|rule)", re.IGNORECASE | re.DOTALL),
    # "You are now a [different role]" — the role-swap attack.
    re.compile(r"\byou\s+are\s+(now|actually)\s+(a|an)\s+(general|generic|unrestricted|uncensored|developer|dev|admin|root|jailbroken)\b", re.IGNORECASE),
    # "Pretend to be / act as / role-play as" — softer role-swap.
    re.compile(r"\b(pretend|act|role[-\s]?play)\s+(to\s+be|as)\s+(a|an)\s+\w+", re.IGNORECASE),
    # System-prompt extraction attempts.
    re.compile(r"\b(repeat|reveal|show|print|tell\s+me)\b.{0,30}\b(system|initial|original|hidden)\b.{0,30}\b(prompt|instruction|message)", re.IGNORECASE | re.DOTALL),
    # DAN ("Do Anything Now") and its descendants.
    re.compile(r"\b(DAN|do\s+anything\s+now|developer\s+mode|jailbreak)\b", re.IGNORECASE),
]


def detect_injection(text: str) -> str | None:
    """
    Returns the name of the first matching pattern (for logging),
    or None if the text looks clean.
    """
    for i, pattern in enumerate(INJECTION_PATTERNS):
        if pattern.search(text):
            return f"pattern_{i+1}"
    return None


# ── Small-talk detection ──────────────────────────────────────────────────
# Greetings, thanks, and "who are you" questions don't need a Mongo trip.
# Catching them locally with regex avoids two LLM calls (~$0 cost, ~0ms
# latency) and gives a more natural conversational feel.
#
# We split into two buckets so the agent's reaction matches the user's
# intent:
#   - GREETING -> long welcome with suggestions
#   - THANKS   -> short acknowledgement, no re-spam of suggestions

GREETING_PATTERNS = [
    re.compile(r"^\s*(hi|hello|hey|yo|hola|salut|bonjour|good\s+(morning|afternoon|evening))\b[!.?\s]*$", re.IGNORECASE),
    re.compile(r"^\s*(who\s+are\s+you|what\s+(are\s+you|can\s+you\s+do|do\s+you\s+do)|what'?s\s+your\s+(name|purpose|role))\b[!.?\s]*$", re.IGNORECASE),
    re.compile(r"^\s*(help|\?|how\s+do\s+i\s+use|how\s+does\s+this\s+work)\b[!.?\s]*$", re.IGNORECASE),
]

THANKS_PATTERNS = [
    re.compile(r"^\s*(thanks|thank\s+you|thx|ty|merci|cheers|appreciated|nice|cool|great|awesome|perfect|got\s+it|ok|okay)\b[!.?\s]*$", re.IGNORECASE),
]


GREETING_RESPONSE = (
    "Hi! I'm your Airbnb analytics assistant for the 95,412 European listings "
    "in this dataset. Ask me things like:\n\n"
    "- **Summary**: \"Give me a summary of the dataset\"\n"
    "- **Top N**: \"What are the top 5 cities by total revenue?\"\n"
    "- **Trends**: \"Compare High vs Low occupancy tiers\"\n"
    "- **Anomalies**: \"Listings with revenue per booked day above $2000\"\n\n"
    "Try one of those and I'll fetch the real numbers from MongoDB."
)

THANKS_RESPONSE = (
    "You're welcome! Let me know if you want to dig into another angle of "
    "the dataset."
)


def detect_small_talk(text: str) -> str | None:
    """
    Return 'greeting' / 'thanks' if the message matches, else None.
    The caller dispatches on this value to pick the right canned response.
    """
    if any(p.match(text) for p in GREETING_PATTERNS):
        return "greeting"
    if any(p.match(text) for p in THANKS_PATTERNS):
        return "thanks"
    return None


def small_talk_response(kind: str) -> str:
    return THANKS_RESPONSE if kind == "thanks" else GREETING_RESPONSE


# Backwards-compatible export (old name still referenced by app.py).
SMALL_TALK_RESPONSE = GREETING_RESPONSE