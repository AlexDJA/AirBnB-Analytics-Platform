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
- To sum  up, I used Claude to debug the issues encountered, and give me an explanation as to why, so that I can explain it in my own terms. I would have never found what was wrong without it.
- As with the M1, I would relaunch and check each step before accepting a suggestion - for example, by manually testing the ES writing via `curl` before touching the Spark connector.

## What I would improve with more time

- Add a unit test on the derived columns. Currently, the logic for `revenue_per_booked_day` and `occupancy_tier` is inline in `spark_pipeline.py` and is not tested - I should extract it into pure functions and write 3-4 test cases with `pyspark.testing` to confirm that the boundaries (occupancy = exactly 0.30 for example) fall on the correct side.