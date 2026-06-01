---

# Milestone 2 — Spark Processing

**Name:** *Alexandre DJADJAGLO* — Student ID *40243644*

This milestone adds a PySpark job that reads the raw Parquet file from HDFS
(produced in M1), cleans it, computes three derived columns, and writes the
result to both MongoDB and ElasticSearch.

## 7. M2 architecture

```
HDFS (raw Parquet, 95,415 rows)        ← M1
       │
       ▼
   Spark job  (apache/spark:3.5.3, local[*])
       │
       ├── clean (nulls, fills, dropDuplicates)
       ├── derive 3 columns
       │
       ├──────────► MongoDB  airbnb.listings   (95,412 docs)
       └──────────► ElasticSearch  airbnb_listings   (95,412 docs)
```

A new `spark_pipeline` service was added to `docker-compose.yml` with
`profiles: [manual]`, so it does **not** start automatically with
`docker compose up -d` — it must be launched explicitly after the M1
ingest job has finished.

## 8. Running the Spark job

```bash
# M1 must be done first (HDFS contains the Parquet file)
docker compose up -d
docker compose logs -f ingest    # wait for "Ingestion complete."

# Then run the M2 pipeline
docker compose run --rm spark_pipeline
```

First run: 2–5 minutes (downloads the Mongo + ES Spark connectors from
Maven Central). Subsequent runs: ~1 minute thanks to the local Ivy cache.

Output goes to **stdout** (log4j + structured Python logging) and to
`logs/spark.log` (Python logging only — easier to grep for the M2
deliverables).

## 9. Cleaning pipeline

The job applies a 3-step cleaning before deriving new columns:

1. **Drop rows missing critical identifiers**
   `listing_id`, `city`, or `country` null → dropped.
   In practice 0 rows were dropped at this step (M1 already filtered them).

2. **Targeted null fills** (where absence has a business-meaningful default)
   - `superhost` null → `False` — no badge = not a superhost (569 rows)
   - `instant_book` null → `False` — no opt-in = manual booking (76,121 rows)
   - `num_reviews` null → `0` — no reviews = 0 reviews (652 rows)

   Financial and rating columns keep their nulls deliberately: a missing
   `ttm_revenue` is *information* (the listing wasn't booked in that
   window), not noise. Filling with 0 would falsely lower aggregates.

3. **De-duplicate on `listing_id`** — 3 rows dropped.

Final count: **95,412 rows** entering the derived-column stage.

## 10. Derived columns

| Name | Type | Computation | Why it's useful |
|---|---|---|---|
| `revenue_per_booked_day` | float (ratio) | `ttm_revenue / ttm_reserved_days` when both > 0, else null | Pricing efficiency. A host can compare it to the local average daily rate to know whether they're under- or over-priced relative to market. |
| `occupancy_tier` | string (binned) | `Low` (< 0.30), `Medium` (< 0.60), `High` (>= 0.60) from `ttm_occupancy`; null if occupancy is null | A categorical filter that powers fast ElasticSearch `term` queries without forcing every search to do a numeric range comparison. |
| `is_premium` | boolean (composite) | `superhost == true AND rating_overall >= 4.8` | Flags listings combining host reputation and high guest ratings — the segment to target for premium marketing campaigns. |

## 11. MongoDB queries

All three queries are in `src/mongo_queries.py`. Run with:

```bash
docker compose exec ingest python src/mongo_queries.py
```

### Q1 — Revenue and efficiency by occupancy tier (`$group`)

**Business question:** does higher occupancy translate into higher revenue
per booked day, or do high-occupancy listings price themselves down?

```python
pipeline = [
    {"$match": {"occupancy_tier": {"$ne": None}}},
    {"$group": {
        "_id": "$occupancy_tier",
        "listings_count": {"$sum": 1},
        "avg_ttm_revenue_usd": {"$avg": "$ttm_revenue"},
        "avg_revenue_per_booked_day": {"$avg": "$revenue_per_booked_day"},
        "avg_occupancy": {"$avg": "$ttm_occupancy"},
    }},
    {"$sort": {"avg_ttm_revenue_usd": -1}},
]
```

**Output:**

```
{ "occupancy_tier": "High",   "listings_count": 12599, "avg_ttm_revenue_usd": 38618.22, "avg_revenue_per_booked_day": 147.26, "avg_occupancy": 0.722 }
{ "occupancy_tier": "Medium", "listings_count": 29942, "avg_ttm_revenue_usd": 26250.98, "avg_revenue_per_booked_day": 166.30, "avg_occupancy": 0.435 }
{ "occupancy_tier": "Low",    "listings_count": 52304, "avg_ttm_revenue_usd":  9540.90, "avg_revenue_per_booked_day": 183.24, "avg_occupancy": 0.144 }
```

Interesting result: **Low**-tier listings have the *highest* average
revenue per booked day (\$183) — they're premium properties that rent
expensively but rarely. **High**-tier wins on absolute volume (\$38k avg
revenue) but with a lower per-night price.

### Q2 — Top 10 listings by revenue per booked day (`$sort + $limit`)

**Business question:** which individual listings extract the most revenue
out of every night they were actually booked?

```python
pipeline = [
    {"$match": {
        "revenue_per_booked_day": {"$ne": None, "$gt": 0},
        "ttm_reserved_days": {"$gt": 10},   # filter out 1-night flukes
    }},
    {"$sort": {"revenue_per_booked_day": -1}},
    {"$limit": 10},
    {"$project": {...}},
]
```

**Output (top 5 of 10):**

```
1. listing_id 15787184 — Derbyshire Dales (UK)    — $4582.75/day, 20 booked days
2. listing_id 20471461 — Saint Martin (FR)        — $4382.67/day, 39 booked days
3. listing_id 10713126 — Derbyshire Dales (UK)    — $4362.51/day, 59 booked days
4. listing_id  7466111 — Kenmare (IE)             — $4289.86/day, 14 booked days
5. listing_id 19466040 — Palma (ES)               — $4183.15/day, 186 booked days
```

The `ttm_reserved_days > 10` threshold was a design choice — without it,
listings with a single \$5,000 night would dominate the leaderboard.

### Q3 — High-performer segment (`$match`)

**Business question:** which listings are simultaneously well-booked,
well-priced, *and* well-reviewed? This is the segment to study when
building a "what makes a top performer?" model.

```python
pipeline = [
    {"$match": {
        "occupancy_tier": "High",
        "revenue_per_booked_day": {"$gt": 200},
        "num_reviews": {"$gte": 50},
    }},
    {"$sort": {"revenue_per_booked_day": -1}},
    {"$limit": 5},
]
```

**Output:**

```
Segment size: 2063 listings match all 3 conditions.
Top 5 by revenue_per_booked_day:
  1. 1864876   — Roquebrune-sur-Argens (FR)  $2082.70/day  occupancy 0.668  is_premium=true
  2. 12005090  — Funchal (PT)                $1797.38/day  occupancy 0.762  is_premium=true
  3. 6668012   — Canterbury (UK)             $1692.09/day  occupancy 0.638  is_premium=true
  4. 15835941  — Roquebrune-sur-Argens (FR)  $1668.68/day  occupancy 0.849  is_premium=true
  5. 14492279  — Funchal (PT)                $1658.22/day  occupancy 0.819  is_premium=false
```

The segment of 2,063 listings is large enough to be statistically useful
yet small enough to be actionable (2.2% of the catalog). Notable: not all
top performers are superhosts — listing 14492279 is a high performer
without the badge.

## 12. ElasticSearch queries

All three queries are in `src/es_queries.py`. Run with:

```bash
docker compose exec ingest python src/es_queries.py
```

### Q1 — Premium listings in France (`term` filter)

```json
{
  "query": {
    "bool": {
      "filter": [
        { "term": { "country.keyword": "France" } },
        { "term": { "is_premium": true } }
      ]
    }
  },
  "sort": [ { "ttm_revenue": "desc" } ]
}
```

**Total matching:** 6,615 documents. Top hit:
`listing_id 15835941` at Roquebrune-sur-Argens, rating 5.0, \$517,291 TTM revenue.

### Q2 — High-revenue listings (>= \$100k) in occupancy tier "High" (`range + bool`)

```json
{
  "query": {
    "bool": {
      "filter": [
        { "range": { "ttm_revenue": { "gte": 100000 } } },
        { "term":  { "occupancy_tier.keyword": "High" } }
      ]
    }
  },
  "sort": [ { "ttm_revenue": "desc" } ]
}
```

**Total matching:** 514 documents. Top hit is the same `15835941`
listing (\$517k revenue, 0.849 occupancy).

### Q3 — Listings with "pool" in amenities (`match` full-text)

```json
{
  "query": {
    "match": { "amenities": "pool" }
  },
  "sort": [
    { "rating_overall": "desc" },
    "_score"
  ]
}
```

**Total matching:** 17,049 documents (about 18% of the index — consistent
with how common pools are for European whole-home rentals).

This is the only one of the three queries that exercises ES's actual
text-search machinery (tokenization, lowercasing, scoring). Q1 and Q2 are
fast filter-only queries that any key-value store could in principle
serve; Q3 is the one that needs a search engine.

## 13. M2 deliverables checklist

- [x] `docker-compose.yml` — added `spark_pipeline` service (manual profile)
- [x] `src/spark_pipeline.py` — reads HDFS Parquet, cleans, derives 3 columns, writes Mongo + ES
- [x] 3 derived columns: `revenue_per_booked_day`, `occupancy_tier`, `is_premium`
- [x] 3 MongoDB aggregation queries (different operations: `$group`, `$sort+$limit`, `$match`)
- [x] 3 ElasticSearch filter queries (different types: `term`, `range+bool`, `match`)
- [x] README updated with M2 sections + full name + student ID
- [x] REFLECTION.md updated with M2 design decisions

---

# Milestone 3 — Agent & Safeguards

**Submitted by:** Alexandre DJADJAGLO — Student ID *40243644*

This milestone adds a natural-language agent on top of the M2 datastore. It
turns plain-English business questions into MongoDB aggregation pipelines,
executes them safely, and answers in plain English — all behind a layer of
four mandatory safeguards plus prompt-injection detection.

## 14. M3 architecture

```
                 ┌─────────────────────┐
   User input ─▶│  Injection detector │ ─── matched → locked role reply
                 └──────────┬──────────┘
                            ▼
                 ┌─────────────────────┐
                 │ Small-talk detector │ ─── matched → canned chat reply
                 └──────────┬──────────┘
                            ▼
                 ┌─────────────────────┐
                 │   LLM call 1        │
                 │ (OpenRouter)        │
                 │   → JSON pipeline   │
                 └──────────┬──────────┘
                            ▼
                 ┌─────────────────────┐
                 │  Query validator    │ ─── rejected → log + error reply
                 │  (Safeguard 3)      │
                 └──────────┬──────────┘
                            ▼
                 ┌─────────────────────┐
                 │  Mongo (read-only)  │ ─── unreachable
                 │  agent_readonly     │      │
                 │  (Safeguard 1)      │      ▼
                 └──────────┬──────────┘   ┌─────────────────┐
                            │              │ ElasticSearch   │ ─── unreachable
                            │              │ (Safeguard 4)   │      │
                            │              └────────┬────────┘      ▼
                            │                       │           ┌──────────────┐
                            ▼                       ▼           │ Stale cache  │
                 ┌─────────────────────────────────────┐        │ (Safeguard 4)│
                 │  Hallucination guard (Safeguard 2)  │        └──────────────┘
                 │  empty result → "No data found"     │
                 └──────────┬──────────────────────────┘
                            ▼
                 ┌─────────────────────┐
                 │   LLM call 2        │
                 │ (format records     │
                 │  into Markdown)     │
                 └──────────┬──────────┘
                            ▼
                       User reply
```

A new `agent` service was added to `docker-compose.yml` exposing port 8000.
It serves a Vue.js single-page interface and a `POST /ask` JSON API.

## 15. Running the agent

```bash
docker compose up -d
# Wait ~30 seconds for the agent to install fastapi/uvicorn and start.
# Then open http://localhost:8000 in a browser.
```

The agent uses OpenRouter for both the query-generation and report-formatting
LLM calls. Set `OPENROUTER_API_KEY` and `OPENROUTER_MODEL` in `.env` —
`meta-llama/llama-3.3-70b-instruct:free` and `openai/gpt-oss-20b:free` are
both known to work; the more compact the JSON the model emits, the better.

## 16. The four safeguards

### Safeguard 1 — Read-only enforcement (driver level)

The agent connects to MongoDB as the `agent_readonly` user, created by
`mongo-init/init-users.js` with the built-in `read` role on the `airbnb`
database. Any insert/update/delete sent over the wire — even one that
bypasses our Python-level validator — is rejected by Mongo itself with
`MongoServerError: not authorized on airbnb to execute command`.

This is **defense in depth**: the Python validator catches write ops in the
LLM output, *and* the database refuses to execute them even if they slipped
through.

### Safeguard 2 — Hallucination guard

After execution, `check_hallucination_guard(records)` checks whether the
record list is empty. If so, the agent returns the canonical message
`"No data found for this query."` **without ever asking the LLM** to
summarize an empty result. The LLM is therefore structurally unable to
invent a story when there's nothing to summarize.

### Safeguard 3 — Query validator

Every LLM-generated pipeline is passed through `validate_pipeline()` before
being sent to MongoDB. Rejections happen on three classes of issue:

1. **Structural** — pipeline is not a list of single-key objects.
2. **Operation** — uses a forbidden stage (`$out`, `$merge`, `$function`...)
   or a stage outside the allow list.
3. **Collection** — targets a collection that doesn't exist in the `airbnb`
   database.

Every rejection writes a JSON line to `logs/safeguard.log` with the reason,
the offending stage, and the timestamp.

### Safeguard 4 — Failure handling (Mongo → ES → cache)

`execute_with_fallback()` tries the three backends in order:
1. MongoDB primary (the typed read-only connection),
2. ElasticSearch, with a best-effort translation of `$match`/`$limit` stages,
3. A local JSON cache of the last successful result (with a stale timestamp).

If all three fail, the agent returns a descriptive error message — it never
crashes the request. Every fallback step is logged to `logs/agent.log`.

## 17. Injection protection

In addition to the 4 safeguards above, `detect_injection()` scans the user
input against 6 regex patterns covering the classic jailbreak families:
"ignore previous instructions", "disregard / override / forget", role swap
("you are now a..."), "pretend to be / act as", system-prompt exfiltration
("repeat your system prompt"), and DAN-style attacks.

On match, the agent returns a **locked role response** and writes an audit
entry to `logs/audit.log` with `"injection_detected": true` and the pattern
that matched. The LLM is never called for those requests.

## 18. M3 deliverables checklist

- [x] Agent connected to OpenRouter, handles natural-language questions
- [x] Generates Mongo aggregation pipelines, executes them safely
- [x] Returns plain-English Markdown business reports
- [x] Handles summary / top-N / trend / anomaly questions
- [x] Safeguard 1: Read-only enforcement at driver level
- [x] Safeguard 2: Hallucination guard ("No data found")
- [x] Safeguard 3: Query validator (writes to `logs/safeguard.log`)
- [x] Safeguard 4: Mongo → ES → cache fallback chain
- [x] Injection detection on every user message
- [x] `logs/agent.log` + `logs/audit.log` + `logs/safeguard.log`
- [x] Web UI at http://localhost:8000