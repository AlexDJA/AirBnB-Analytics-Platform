"""
check_mongo.py
--------------
M1 smoke test: confirm the MongoDB service is reachable from this container.
No data is loaded in M1 — that happens in M2 after Spark processing.

Run:
    docker compose exec ingest python src/check_mongo.py
"""
from __future__ import annotations

import logging
import sys

from pymongo import MongoClient
from pymongo.errors import PyMongoError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Inside the Docker network we reach MongoDB by its service hostname on its
# internal port 27017 (the 27015 mapping is only for host->container).
MONGO_URI = "mongodb://mongodb:27017"


def main() -> int:
    log.info("Connecting to %s ...", MONGO_URI)
    try:
        # serverSelectionTimeoutMS fails fast instead of hanging 30s if unreachable
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

        # ping is the canonical Mongo health check
        ping_result = client.admin.command("ping")
        log.info("Ping OK: %s", ping_result)

        server_info = client.server_info()
        log.info("MongoDB version: %s", server_info["version"])

        databases = client.list_database_names()
        log.info("Existing databases: %s", databases)
        log.info("(empty user-data is expected in M1 — populated in M2)")

        log.info("MongoDB smoke test PASSED")
        return 0

    except PyMongoError as exc:
        log.error("MongoDB smoke test FAILED: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())