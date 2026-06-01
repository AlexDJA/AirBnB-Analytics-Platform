# REFLECTION - Milestone 1

## What I built for M1

- A `docker-compose.yml` that starts 5 services on the same Docker network: HDFS NameNode + DataNode (image `apache/hadoop:3.3.6`), MongoDB 7, ElasticSearch 8.13, and a one-shot Python container called `ingest`.
- `src/ingest.py` - reads `data/AirbnbEuropeMarket.csv`, casts column types properly (booleans become real booleans, IDs stay as strings, financial columns stay as float64), converts to Parquet in memory using pyarrow with snappy compression, and uploads to HDFS at `/data/raw/airbnb_europe.parquet` via WebHDFS.
- `src/hdfs_client.py` - I pulled the WebHDFS logic out into its own module so it could be reused by both the ingest script and the queries script. It handles the two-step PUT (CREATE on NameNode → upload to DataNode) and fixes the Docker hostname issue described below.
- `src/queries.py` - runs 3 analytical queries (top cities, average revenue by room type, top superhosts). The important detail is that it downloads the Parquet file *back* from HDFS instead of reading the local CSV - otherwise the queries don't actually prove the pipeline works end-to-end.
- `src/check_mongo.py` and `src/check_es.py` - small connectivity tests so I can confirm both services are reachable and grab a screenshot for the submission.
- Logging is done with the `logging` module (timestamps, levels, named loggers), written to both stdout and `logs/ingest.log`. Not with `print()` statements.

## The hardest problem I solved

- The CSV crashed pandas on row 64 with `ParserError: Expected 61 fields, saw 76`. I dug in and the issue was the `amenities` column: it's a comma-separated string of features wrapped in double quotes, and some rows have escaping problems (things like `"(B2)\""` with a stray backslash-quote). The fast C parser can't recover from those, so it just dies on the first bad row.
- The fix was to switch to `engine="python"`, `quoting=csv.QUOTE_ALL`, and `on_bad_lines="skip"`. This skips the broken rows and keeps going. End result: 95 415 clean rows out of 95 898 - about 0.5% lost, still way above the 50k minimum. The pipeline is now resilient instead of failing on the first messy row.
- Related second fix: the WebHDFS template in the brief uses `redirect_url.split("/")[2]` to swap the DataNode host, which feels fragile. I rewrote it using `urllib.parse.urlparse` + `urlunparse` so it works regardless of what exact host:port the NameNode returns in the redirect.

## One design decision I made

- I had to decide when to cast the column types: now in M1 during the CSV -> Parquet step, or later in M2 inside the Spark job.
- I went with casting now. Parquet stores the schema natively, so the file on HDFS is already typed correctly. Spark in M2 will read pre-typed columns, queries can use real boolean filters like `df["superhost"] == True`, and I catch dirty data immediately instead of in M2 when the stakes are higher.
- The trade-off is that the schema definition lives in two places once M2 is built (the cast in `ingest.py` and the Spark schema). If I ever need to change a type, I'll have to update both. I figured that's a one-time M2 problem, and meanwhile every read from the Parquet file benefits from already-clean types.

## AI tools used

- The only AI tool I used was Claude AI.
- I used Claude to understand the WebHDFS redirect and the Hadoop files, help me write comments in a better written english, help me stucture the Python code (such as making a separate `hdfs_client.py` module) and for better syntax and logic.
- I used Claude to debug the issues encountered such as the one above, and give me an explanation as to why, so that I can explain it in my own terms

## What I would improve with more time

- I would add a `Makefile` with `make up`, `make ingest`, `make queries`, `make logs`, and `make clean`. The lines `docker compose exec ingest python src/...` quickly become long, and a Makefile would make the README shorter and harder to mess up when typing a command.

# REFLECTION - Milestone 2

## What I built for M2

- A `spark_pipeline` service added to `docker-compose.yml` using the `apache/spark:3.5.3` image, with `profiles: [manual]` so it doesn't try to start automatically with `docker compose up -d` - it must be triggered explicitly with `docker compose run --rm spark_pipeline` once M1's ingest is done.
- `src/spark_pipeline.py` - the actual PySpark job. It reads the Parquet file from HDFS (`hdfs://namenode:8020/...`), runs a 3-step cleaning (drop critical-field nulls, targeted fills, dropDuplicates on `listing_id`), computes 3 derived columns, then writes the result to both MongoDB and ElasticSearch.
- `src/mongo_queries.py` and `src/es_queries.py` - the 6 queries required by the M2 brief. Each uses a different operation type so the grader can see breadth: `$group`, `$sort+$limit`, `$match` for Mongo, and `term`, `range+bool`, `match` for ES.
- A small helper inside the Spark job, `prepare_es_index`, that hits the ES REST API directly to pre-create the index with `number_of_replicas: 0` before letting Spark write to it. This was added to fix the bug described below.
- Logging into `logs/spark.log` via the Python `logging` module, with the kinds of messages the M2 brief explicitly asks for ("records read from HDFS / cleaning operations performed / records written to MongoDB").

## The hardest problem I solved

- First runs of the Spark job kept failing on the ES write step with `org.elasticsearch.hadoop.EsHadoopIllegalArgumentException: Cannot determine write shards for [airbnb_listings]`. The Mongo write right before it always succeeded, which made the issue confusing - the data and the code were clearly fine.
- I checked the cluster health with `curl http://localhost:9200/_cluster/health?pretty` and it came back `status: yellow` with `unassigned_shards: 1`. That was the key. The default ES index template asks for 1 primary shard + 1 replica. With a single-node cluster, the replica has nowhere to live, so it sits unassigned forever and the cluster never reaches `green`. The Spark ES connector probes the shard layout before writing and refuses to proceed when it sees an unassigned shard.
- I tried first to fix it through the connector by adding `es.nodes.discovery=false`, `es.index.auto.create=true`, `es.write.operation=index`. No effect. Then I downgraded the connector from `8.13.4` to `8.11.4` thinking it was a version mismatch. Still failing.
- The real fix was at the index layer, not the connector layer: I added `prepare_es_index()` that does a `DELETE /airbnb_listings` followed by a `PUT /airbnb_listings` with `{ "settings": { "number_of_shards": 1, "number_of_replicas": 0 } }` before the Spark write. Cluster goes to `green`, the connector is happy, all 95,412 docs land in ES, and the index size even drops from 186 MB to 148 MB because there's no longer space reserved for a phantom replica.
- I also hit a second issue along the way - I had added `import requests` to the script but forgotten to add `requests` to the `pip install` in the compose. One-line fix once I saw the `ModuleNotFoundError`. Small bug, but a good reminder that the Spark container is rebuilt from scratch on each `run --rm`, so dependencies have to be declared explicitly.

## One design decision I made

- The decision was around handling nulls. I had three options: drop every row with any null (aggressive), fill everything with a default (lossy), or do something more targeted.
- I went with a 3-step pipeline:
  - **Drop** rows missing critical identifiers (`listing_id`, `city`, `country`) - these are unrecoverable, there's nothing useful you can do with a listing that doesn't tell you where it is. In practice 0 rows were dropped here because M1 had already filtered them.
  - **Fill** the 3 columns where the absence has a clear business meaning: `superhost` null → `False` (no badge = not a superhost, 569 rows), `instant_book` null → `False` (no opt-in = manual booking, 76,121 rows), `num_reviews` null → `0` (no reviews = 0 reviews, 652 rows).
  - **Keep** all the financial and rating nulls deliberately. A missing `ttm_revenue` means the listing simply wasn't booked in that window - that's information, not noise. Filling those with 0 would have falsely dragged down every aggregate downstream.
- The trade-off is that the data is harder to query - anyone writing Mongo or ES queries against this collection has to remember that financial columns can be null, and add `$ne: null` where appropriate. I accepted that because the alternative (silent zero-filling) would give wrong answers without anyone noticing.

## AI tools used

- The only AI tool I used was Claude AI.
- I used Claude to help me write comments in a better written english, help me stucture the Python code and for better syntax and logic. 
- I also used Claude to understand the ES bug "Cannot determine write shards", suggest the Spark job structure, explain the difference between bool.filter and bool.must
- Claude initially suggested using the ES connector in 8.13.4, I had to switch to 8.11.4 and then finally add the index pre-create.
- To sum up, I used Claude to debug the issues encountered, and give me an explanation as to why, so that I can explain it in my own terms. I would have never found what was wrong without it.
- As with the M1, I would relaunch and check each step before accepting a suggestion - for example, by manually testing the ES writing via `curl` before touching the Spark connector.

## What I would improve with more time

- Add a unit test on the derived columns. Currently, the logic for `revenue_per_booked_day` and `occupancy_tier` is inline in `spark_pipeline.py` and is not tested - I should extract it into pure functions and write 3-4 test cases with `pyspark.testing` to confirm that the boundaries (occupancy = exactly 0.30 for example) fall on the correct side.

# REFLECTION — Milestone 3

## What I built for M3

- A natural-language agent that turns plain-English questions about the Airbnb dataset into MongoDB aggregation pipelines, executes them safely, and replies in Markdown via a Vue.js single-page UI on port 8000.
- A four-safeguard layer: **read-only enforcement** at the MongoDB driver level via a dedicated `agent_readonly` user, **hallucination guard** that returns "No data found" instead of asking the LLM to invent a story for empty results, **query validator** that rejects writes and unknown collections (with rejections written to `logs/safeguard.log`), and a **Mongo → ES → cache fallback chain** so the agent never crashes when a backend goes down.
- **Injection detection** with 6 regex patterns covering the classic jailbreak families (ignore previous, role swap, DAN, system-prompt extraction, etc). On match the agent returns a locked role response and writes `injection_detected: true` to `logs/audit.log`.
- A `mongo-init/init-users.js` that runs once at first Mongo boot to create two users with separate roles: `spark_writer` (readWrite, used by M2 Spark job) and `agent_readonly` (read only, used by M3 agent).
- A small-talk short-circuit that catches greetings and thanks locally with regex — no Mongo trip, no LLM call. Saves about 5 seconds of latency on "hi" and "thanks" messages.

## The hardest problem I solved

- The agent kept crashing in unpredictable ways during the first full integration. Each crash had a different root cause, so I had to peel the onion.
- **Crash 1**: the frontend would show `JSON.parse: unexpected character` because the agent was returning a 500 HTML page instead of clean JSON. The actual cause was deeper — the LLM had generated a pipeline with `$size` on the `amenities` column. But `amenities` in our dataset is a comma-separated string, not an array, so Mongo crashed at execution time with `PlanExecutor error :: The argument to $size must be an array`. The `OperationFailure` bubbled all the way up to FastAPI without being caught. I fixed both layers: caught `OperationFailure` in `query_executor.py` and treated execution errors as "no data" so the hallucination guard takes over cleanly, AND added an explicit field-type note in the system prompt telling the LLM that `amenities` is a string and to use `$regex` instead of `$size`.
- **Crash 2**: after fixing crash 1, the agent now returned `"The language model is temporarily unavailable"` for the same Summary question. Logs showed `LLM returned invalid JSON: Expecting ',' delimiter`. The LLM was generating valid JSON but my `max_tokens=800` cap was truncating it mid-stream (Llama liked to use `$facet` with all 60+ columns of the schema). Two fixes: bumped `max_tokens` to 2500 for the pipeline-generation call (kept the report-formatting call at 400 because reports should be short), and added a few-shot example in the system prompt showing a compact `$group + $project` pattern as the preferred shape for summary questions.
- **Crash 3 / observation**: even after that, "Give me a summary of the dataset" was inconsistent — sometimes the LLM listened to my example, sometimes it still went its own way. I accepted this and rephrased to "Give me a count and average rating of all listings" for the screenshot. **Lesson**: a production LLM agent has to be robust to both the LLM's output AND the database's response, and the prompt system is part of the code — when behavior is wrong, the prompt is sometimes the right place to fix, not the Python.

## One design decision I made

- The decision was around the Mongo connection model: should the agent connect to MongoDB **with a read-only credential** (defense at the driver level), or should it rely purely on the Python query validator to reject writes?
- I went with both. The `mongo-init/init-users.js` creates a strictly read-only `agent_readonly` user, and the agent uses it. Any write attempt — even one that bypasses the validator (say, because the LLM came up with an exotic stage name I forgot to blacklist) — is rejected by Mongo itself with `MongoServerError: not authorized on airbnb to execute command`.
- The trade-off: it adds a few moving parts (Mongo `--auth` flag, mounted init script, two users, updated URIs in `spark_pipeline.py` / `mongo_queries.py` / `check_mongo.py`). Some of those changes broke M2 scripts temporarily. The alternative — validator-only — would have been simpler but it puts 100% of the security on a Python function I wrote in a few hours. The M3 brief explicitly says "agent connection must be **read-only at driver level**", so this was actually the only conforming choice — but I now see why: any safeguard you can push down to the database is one fewer thing that depends on perfect Python code.

## AI tools used

- The only AI tool I used was Claude AI.
- Generate the FastAPI agent skeleton, suggest regex patterns for injection detection, explain the difference between `bool.filter` and `bool.must` in Elasticsearch, and help me debug the three successive crashes mentioned above.
- What I changed compared to the suggestions, for example: the tool initially offered me an agent with a single "all-in-one" LLM call; I split it into two separate calls to be able to insert the validation between the two, which was necessary to comply with Safeguard 3

## What I would improve with more time

- Add **Langfuse** for LLM observability. The course Session 7 demo uses it and it would give me per-trace latency, token counts, cost, and a searchable history of every agent exchange. With Langfuse I would have diagnosed the truncation bug in 10 seconds instead of grep-ing through logs. The wiring is about 20 lines of code around `call_openrouter` and one extra service in `docker-compose.yml`.
- I'd add a **caching layer in front of the LLM** keyed by (question hash, model). Identical questions are common during demos and testing, and each cache hit would save ~5 seconds and two LLM calls. The cache would also make screenshots and tests reproducible run-to-run.