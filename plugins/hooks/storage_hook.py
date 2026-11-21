"""Object Storage Hook (S3 / SeaweedFS / Local Staging) for Data Lake and Landing Zones."""

from __future__ import annotations

import logging
import os
import shutil
from typing import Any, Dict, List, Optional
from airflow.hooks.base import BaseHook  # type: ignore

logger = logging.getLogger(__name__)


class StorageHook(BaseHook):
    """Hook for interacting with S3-compatible Object Storage (SeaweedFS, AWS S3, GCS).
    
    Provides upload, download, and listing methods with automated fallback to
    local simulated storage directory for offline development and testing.
    """

    conn_name_attr = "conn_id"
    default_conn_name = "storage_default"
    conn_type = "s3"
    hook_name = "Object Storage Hook"

    def __init__(self, conn_id: str = "storage_default"):
        super().__init__()
        self.conn_id = conn_id
        self._connection = None

    @property
    def connection(self):
        if self._connection is None:
            self._connection = self.get_connection(self.conn_id)
        return self._connection

    def upload_file(self, local_path: str, remote_key: str, bucket: str = "data-landing") -> str:
        """Upload a local file to object storage bucket."""
        logger.info("Storage Upload: %s -> %s/%s (conn: %s)", local_path, bucket, remote_key, self.conn_id)
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local file does not exist: {local_path}")

        # Try boto3 S3 client if available
        try:
            import boto3  # type: ignore
            conn = self.connection
            extra = getattr(conn, "extra_dejson", {}) or {}
            endpoint = extra.get("endpoint_url", conn.host)
            s3_client = boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=conn.login,
                aws_secret_access_key=conn.password,
                region_name=extra.get("region_name", "us-east-1"),
            )
            s3_client.upload_file(local_path, bucket, remote_key)
            return f"s3://{bucket}/{remote_key}"
        except (ImportError, Exception) as exc:
            logger.debug("Falling back to local simulated storage: %s", exc)

        # Local storage fallback
        simulated_root = os.path.join(os.getcwd(), "staging", "storage_remote", bucket)
        target = os.path.join(simulated_root, remote_key.lstrip("/"))
        os.makedirs(os.path.dirname(target), exist_ok=True)
        shutil.copyfile(local_path, target)
        logger.info("Simulated storage upload saved to %s", target)
        return target

    def download_file(self, remote_key: str, local_path: str, bucket: str = "data-landing") -> str:
        """Download remote object storage file to local path."""
        logger.info("Storage Download: %s/%s -> %s (conn: %s)", bucket, remote_key, local_path, self.conn_id)
        try:
            import boto3  # type: ignore
            conn = self.connection
            extra = getattr(conn, "extra_dejson", {}) or {}
            endpoint = extra.get("endpoint_url", conn.host)
            s3_client = boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=conn.login,
                aws_secret_access_key=conn.password,
                region_name=extra.get("region_name", "us-east-1"),
            )
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            s3_client.download_file(bucket, remote_key, local_path)
            return local_path
        except (ImportError, Exception) as exc:
            logger.debug("Falling back to local simulated storage: %s", exc)

        simulated_root = os.path.join(os.getcwd(), "staging", "storage_remote", bucket)
        source = os.path.join(simulated_root, remote_key.lstrip("/"))
        if not os.path.exists(source):
            raise FileNotFoundError(f"File not found in simulated storage: {source}")
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        shutil.copyfile(source, local_path)
        return local_path

    def list_files(self, prefix: str = "", bucket: str = "data-landing") -> List[str]:
        """List object keys under prefix in bucket."""
        try:
            import boto3  # type: ignore
            conn = self.connection
            extra = getattr(conn, "extra_dejson", {}) or {}
            s3_client = boto3.client(
                "s3",
                endpoint_url=extra.get("endpoint_url", conn.host),
                aws_access_key_id=conn.login,
                aws_secret_access_key=conn.password,
            )
            resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            return [obj["Key"] for obj in resp.get("Contents", [])]
        except (ImportError, Exception) as exc:
            logger.debug("Listing from local simulated storage: %s", exc)

        simulated_root = os.path.join(os.getcwd(), "staging", "storage_remote", bucket)
        if not os.path.exists(simulated_root):
            return []
        keys = []
        for root, _, files in os.walk(simulated_root):
            for f in files:
                full = os.path.join(root, f)
                rel = os.path.relpath(full, simulated_root)
                if rel.startswith(prefix):
                    keys.append(rel)
        return sorted(keys)
