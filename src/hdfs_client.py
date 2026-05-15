"""
hdfs_client.py
--------------
Minimal WebHDFS client wrapping the two-step PUT redirect dance.
---------------
"""

from __future__ import annotations

import io
import logging
from typing import Iterable
from urllib.parse import urlparse, urlunparse

import requests

logger = logging.getLogger(__name__)


class HDFSClient:
    def __init__(
        self,
        namenode_host: str = "namenode",
        namenode_port: int = 9870,
        datanode_host: str = "datanode",
        datanode_port: int = 9864,
        user: str = "root",
        timeout: int = 60,
    ) -> None:
        self.namenode = f"http://{namenode_host}:{namenode_port}"
        self.datanode_netloc = f"{datanode_host}:{datanode_port}"
        self.user = user
        self.timeout = timeout

    # ---- internal helpers --------------------------------------------------

    def _webhdfs_url(self, path: str, op: str, **params: str) -> str:
        """Build a WebHDFS URL: http://namenode:9870/webhdfs/v1<path>?op=<OP>&..."""
        if not path.startswith("/"):
            path = "/" + path
        query = f"op={op}&user.name={self.user}"
        for key, value in params.items():
            query += f"&{key}={value}"
        return f"{self.namenode}/webhdfs/v1{path}?{query}"

    def _fix_datanode_redirect(self, redirect_url: str) -> str:
        """
        Replace the host:port the NameNode returned (typically the DataNode's
        internal Docker IP) with the DataNode's Docker hostname so requests
        from another container can actually reach it.
        """
        parsed = urlparse(redirect_url)
        fixed = parsed._replace(netloc=self.datanode_netloc)
        return urlunparse(fixed)

    # ---- public API --------------------------------------------------------

    def upload_bytes(self, data: bytes, hdfs_path: str, overwrite: bool = True) -> None:
        """
        Two-step WebHDFS PUT:
          1. PUT to NameNode (no body) -> 307 redirect to DataNode
          2. PUT to DataNode with the actual bytes
        """
        create_url = self._webhdfs_url(
            hdfs_path,
            op="CREATE",
            overwrite=str(overwrite).lower(),
        )
        logger.debug("WebHDFS CREATE step 1: %s", create_url)

        try:
            step1 = requests.put(create_url, allow_redirects=False, timeout=self.timeout)
        except requests.RequestException as exc:
            logger.error("WebHDFS CREATE step 1 network error: %s", exc)
            raise

        if step1.status_code not in (307, 308):
            logger.error(
                "Expected 307 redirect from NameNode, got %s: %s",
                step1.status_code, step1.text[:300],
            )
            step1.raise_for_status()

        redirect_url = step1.headers.get("Location")
        if not redirect_url:
            raise RuntimeError("NameNode 307 response missing Location header")

        fixed_url = self._fix_datanode_redirect(redirect_url)
        logger.debug("WebHDFS CREATE step 2: %s", fixed_url)

        try:
            step2 = requests.put(
                fixed_url,
                data=data,
                headers={"Content-Type": "application/octet-stream"},
                timeout=self.timeout,
            )
            step2.raise_for_status()
        except requests.RequestException as exc:
            logger.error("WebHDFS CREATE step 2 upload error: %s", exc)
            raise

        logger.info("Uploaded %d bytes to hdfs://%s", len(data), hdfs_path)

    def download_bytes(self, hdfs_path: str) -> bytes:
        """
        Two-step WebHDFS OPEN to download a file as raw bytes.
        Same redirect dance as upload.
        """
        open_url = self._webhdfs_url(hdfs_path, op="OPEN")
        logger.debug("WebHDFS OPEN step 1: %s", open_url)

        step1 = requests.get(open_url, allow_redirects=False, timeout=self.timeout)
        if step1.status_code not in (307, 308):
            step1.raise_for_status()

        redirect_url = step1.headers.get("Location")
        if not redirect_url:
            raise RuntimeError("NameNode 307 response missing Location header")

        fixed_url = self._fix_datanode_redirect(redirect_url)
        logger.debug("WebHDFS OPEN step 2: %s", fixed_url)

        step2 = requests.get(fixed_url, timeout=self.timeout)
        step2.raise_for_status()
        return step2.content

    def mkdirs(self, hdfs_path: str) -> None:
        """Create an HDFS directory (and parents). Idempotent."""
        url = self._webhdfs_url(hdfs_path, op="MKDIRS")
        resp = requests.put(url, timeout=self.timeout)
        resp.raise_for_status()
        logger.info("Ensured HDFS directory exists: %s", hdfs_path)

    def list_dir(self, hdfs_path: str) -> list[dict]:
        """List files in an HDFS directory. Returns list of FileStatus dicts."""
        url = self._webhdfs_url(hdfs_path, op="LISTSTATUS")
        resp = requests.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()["FileStatuses"]["FileStatus"]

    def status(self, hdfs_path: str) -> dict | None:
        """Return file metadata (size, owner, modification time) or None if missing."""
        url = self._webhdfs_url(hdfs_path, op="GETFILESTATUS")
        resp = requests.get(url, timeout=self.timeout)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()["FileStatus"]