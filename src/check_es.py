"""
check_es.py
-----------
M1 smoke test: confirm the ElasticSearch service is reachable from this
container. No index is populated in M1 — that happens in M2.

Run:
    docker compose exec ingest python src/check_es.py
"""
from __future__ import annotations

import logging
import sys

from elasticsearch import Elasticsearch
from elasticsearch import ConnectionError as ESConnectionError
from elasticsearch import TransportError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Inside the Docker network, reach ES by its service hostname on default port.
ES_URL = "http://elasticsearch:9200"


def main() -> int:
    log.info("Connecting to %s ...", ES_URL)
    try:
        es = Elasticsearch(ES_URL, request_timeout=5)

        if not es.ping():
            log.error("ES ping failed (no exception, but ping returned False)")
            return 1
        log.info("Ping OK")

        info = es.info()
        log.info("Cluster name: %s", info["cluster_name"])
        log.info("ElasticSearch version: %s", info["version"]["number"])

        health = es.cluster.health()
        log.info(
            "Cluster status: %s (nodes=%d, active_shards=%d)",
            health["status"], health["number_of_nodes"], health["active_shards"],
        )

        indices = list(es.indices.get_alias(index="*").keys())
        user_indices = [i for i in indices if not i.startswith(".")]
        log.info("User indices: %s", user_indices or "(none)")

        log.info("ElasticSearch smoke test PASSED")
        return 0

    except (ESConnectionError, TransportError) as exc:
        log.error("ES smoke test FAILED: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())