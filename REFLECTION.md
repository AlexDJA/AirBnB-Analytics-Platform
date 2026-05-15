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