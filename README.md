# Milestone 1 — Infrastructure & Storage

**Course:** CEBD 1261 — Big Data Infrastructure
**Dataset:** Airbnb Market Data: Europe 373 Cities (Kaggle)
**Stack:** Docker Compose · Hadoop HDFS · MongoDB · ElasticSearch · Python 3.11

This repository sets up the storage layer for the course project. A single
command brings up HDFS (NameNode + DataNode), MongoDB, ElasticSearch, and a
Python container that ingests the raw Airbnb dataset into HDFS as a Parquet
file. Following the M1 brief, MongoDB and ElasticSearch are launched and
verified as reachable but are **not** populated in M1 — they will be loaded
in M2 after Spark processing.

---

## 1. Dataset

- **Name:** Airbnb Market Data: Europe 373 Cities
- **Source:** Kaggle — https://www.kaggle.com/datasets/jasonairroi/airbnb-market-data-europe?resource=download&select=listings.csv
- **Raw size:** ~79 MB CSV, 95,898 rows × 61 columns
- **After cleaning:** 95,415 rows (~0.5% dropped due to malformed quoting in `amenities`)
- **Coverage:** 373 European cities across 22 countries, with listing metadata
  (room type, capacity, amenities), host attributes (`superhost`,
  `professional_management`), pricing (`cleaning_fee`, `extra_guest_fee`),
  ratings, and trailing-12-month / last-90-day revenue and occupancy metrics.
- **Stored in HDFS as:** Apache Parquet (snappy compression) — 19.37 MB,
  giving ~4× compression vs raw CSV.

---

## 2. Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                       Docker network: pipeline-net               │
│                                                                  │
│   ┌──────────┐    ┌──────────┐    ┌──────────┐   ┌────────────┐  │
│   │ namenode │◄──►│ datanode │    │ mongodb  │   │   elastic  │  │
│   │  :9870   │    │  :9864   │    │  :27017  │   │   :9200    │  │
│   └────┬─────┘    └────┬─────┘    └────┬─────┘   └─────┬──────┘  │
│        │               │               │               │         │
│        └───────────┬───┴───────────────┴───────────────┘         │
│                    │                                             │
│                    ▼                                             │
│              ┌───────────┐                                       │
│              │  ingest   │  reads data/*.csv → WebHDFS upload    │
│              │  (python) │  also runs smoke tests + queries      │
│              └───────────┘                                       │
└──────────────────────────────────────────────────────────────────┘
        ▲ host ports: 9870, 9864, 27015→27017, 9200
```

### Services

| Service | Image | Host port | Role |
|---|---|---|---|
| `namenode` | `apache/hadoop:3.3.6` | `9870`, `8020` | HDFS master, WebHDFS REST API |
| `datanode` | `apache/hadoop:3.3.6` | `9864` | HDFS worker, holds data blocks |
| `mongodb` | `mongo:7` | `27015→27017` | Document store (no data in M1) |
| `elasticsearch` | `elasticsearch:8.13.0` | `9200` | Search layer (no data in M1) |
| `ingest` | `python:3.11-slim` | — | One-shot job: CSV → Parquet → HDFS |

---

## 3. Setup

### Prerequisites

- Docker Desktop (or Docker Engine + Compose v2)
- ~4 GB of free RAM (ElasticSearch is capped at 1 GB; Hadoop ~1 GB; the rest is overhead)
- The Kaggle CSV placed at `data/AirbnbEuropeMarket.csv`

### Repository layout

```
.
├── docker-compose.yml
├── requirements.txt
├── README.md
├── REFLECTION.md
├── hadoop/
│   ├── hadoop.env
│   ├── core-site.xml
│   └── hdfs-site.xml
├── src/
│   ├── ingest.py         # CSV → Parquet → HDFS
│   ├── hdfs_client.py    # WebHDFS client (upload, download, mkdirs)
│   ├── queries.py        # 3 README queries (reads Parquet from HDFS)
│   ├── check_mongo.py    # MongoDB connectivity smoke test
│   └── check_es.py       # ElasticSearch connectivity smoke test
├── data/
│   └── AirbnbEuropeMarket.csv
└── logs/
    └── ingest.log        # produced at runtime
```

### Start everything

```bash
docker compose up -d
```

This pulls the four service images, formats HDFS on first boot, starts all
containers, and runs `src/ingest.py` automatically once HDFS reports
healthy. The `ingest` container keeps itself alive after the job (via
`tail -f /dev/null`) so you can `docker compose exec` into it.

### Watch the ingestion in real time

```bash
docker compose logs -f ingest
```

You should see `Ingestion complete.` after ~30–60 seconds (most of it is `pip install`).

### Verify HDFS contents

- Browser: open <http://localhost:9870> → **Utilities → Browse the file system**
  → navigate to `/data/raw/` and confirm `airbnb_europe.parquet` is listed
  (~19 MB).
- CLI: `docker compose exec namenode hdfs dfs -ls /data/raw`

### Run smoke tests

```bash
docker compose exec ingest python src/check_mongo.py
docker compose exec ingest python src/check_es.py
```

Both should print `... smoke test PASSED`.

### Tear down

```bash
docker compose down -v   # -v also removes named volumes (clean slate)
```

---

## 4. Example queries (real output)

All three queries read the Parquet file **back from HDFS** (not from local
disk), which exercises the full round-trip: CSV → typed DataFrame → Parquet
serialization → WebHDFS upload → WebHDFS download → Parquet parse → query.
Run them with:

```bash
docker compose exec ingest python src/queries.py
```

### Q1 — Top 10 cities by number of listings

```python
df.groupby("city", dropna=True)
  .size()
  .reset_index(name="listings_count")
  .sort_values("listings_count", ascending=False)
  .head(10)
```

**Output:**

```
    city  listings_count
Bordeaux             303
Arcachon             303
   Olbia             303
 Fethiye             303
  Dublin             302
  Bodrum             302
Cagliari             302
   Dijon             302
 Antibes             302
    Agde             302
```

The near-uniform `302–303` count strongly suggests the source data was
sampled with a per-city cap.

### Q2 — Average TTM revenue (USD) by room type

```python
(df.dropna(subset=["ttm_revenue", "room_type"])
   .groupby("room_type")
   .agg(listings_count=("ttm_revenue", "size"),
        avg_ttm_revenue_usd=("ttm_revenue", "mean"),
        median_ttm_revenue_usd=("ttm_revenue", "median"))
   .round(2)
   .sort_values("avg_ttm_revenue_usd", ascending=False))
```

**Output:**

```
   room_type  listings_count  avg_ttm_revenue_usd  median_ttm_revenue_usd
 entire_home           81709             20240.75                 13303.0
private_room           12548              9018.54                  6517.5
  hotel_room             460              8844.35                  4859.5
 shared_room             129              3894.15                  2461.0
```

`entire_home` dominates both volume (~86% of listings) and revenue (~2.2×
the next category). The median being well below the mean in every category
indicates a right-skewed distribution (a few high-revenue outliers pull
the average up).

### Q3 — Top 5 superhosts (rating ≥ 4.8) by TTM revenue

```python
mask = ((df["superhost"] == True)
        & (df["rating_overall"] >= 4.8)
        & df["ttm_revenue"].notna())
df.loc[mask, ["listing_id", "city", "country", "room_type",
              "rating_overall", "num_reviews", "ttm_revenue"]]
  .sort_values("ttm_revenue", ascending=False)
  .head(5)
```

**Output:**

```
listing_id                  city        country   room_type  rating_overall  num_reviews  ttm_revenue
  15835941 Roquebrune-sur-Argens         France entire_home            5.00         91.0     517291.0
   1864876 Roquebrune-sur-Argens         France entire_home            4.98        125.0     508180.0
  12005090               Funchal       Portugal entire_home            4.84         83.0     499672.0
   2220578     City of Edinburgh United Kingdom entire_home            4.97        155.0     499616.0
   6668012            Canterbury United Kingdom entire_home            4.91        227.0     394257.0
```

This query exercises a real boolean filter on `superhost` — proof that the
M1 type cast survived Parquet serialization (booleans are stored as 1-bit
values, not as the strings `"true"`/`"false"` they were in the CSV).

---

## 5. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `python: can't open file '/app/src/ingest.py'` | Local folder is named differently than `src/` | Make sure the local folder name matches what `docker-compose.yml` expects |
| `WebHDFS CREATE step 2 ... Connection refused` | DataNode not yet reachable | Wait 30 s and `docker compose restart ingest` |
| `404` on WebHDFS | `dfs.webhdfs.enabled` not set | Add the property in `hadoop/hdfs-site.xml` |
| ElasticSearch container restarts in a loop | Insufficient RAM | Raise Docker Desktop's RAM limit or lower `ES_JAVA_OPTS` |
| HDFS browser shows empty `/` after `up` | Ingest container failed | `docker compose logs ingest` — usually a missing CSV or import error |

---

## 6. M1 deliverables checklist

- [x] `docker-compose.yml` — all services start with `docker compose up -d`
- [x] Python ingestion script — CSV → Parquet (pyarrow) → HDFS
- [x] MongoDB & ElasticSearch services running and reachable (no data load required for M1)
- [x] `requirements.txt` with `pandas`, `pyarrow`, `requests`, `pymongo`, `elasticsearch`
- [x] Kaggle dataset (≥ 50 000 records — actual: **95 415**)
- [x] README.md with setup + 3 example queries with real output
- [x] REFLECTION.md (see file)
- [ ] GitHub release tagged `M1`