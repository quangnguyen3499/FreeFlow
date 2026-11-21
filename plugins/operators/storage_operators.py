"""Object Storage operators for uploading and downloading batch artifacts."""

from __future__ import annotations

import logging
from typing import Any, Dict, List
from airflow.models import BaseOperator  # type: ignore
from plugins.hooks.storage_hook import StorageHook

logger = logging.getLogger(__name__)


class UploadToStorageOperator(BaseOperator):
    """Uploads local files or batch datasets to S3/SeaweedFS bucket."""

    template_fields = ("local_files", "remote_prefix", "bucket")

    def __init__(
        self,
        *,
        task_id: str,
        storage_conn_id: str = "storage_default",
        bucket: str = "data-landing",
        local_files: List[str],
        remote_prefix: str = "raw",
        **kwargs: Any,
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.storage_conn_id = storage_conn_id
        self.bucket = bucket
        self.local_files = local_files
        self.remote_prefix = remote_prefix

    def execute(self, context: Dict[str, Any]) -> List[str]:
        hook = StorageHook(self.storage_conn_id)
        uploaded = []
        for file_path in self.local_files:
            file_name = file_path.split("/")[-1]
            remote_key = f"{self.remote_prefix.rstrip('/')}/{file_name}"
            dest = hook.upload_file(local_path=file_path, remote_key=remote_key, bucket=self.bucket)
            uploaded.append(dest)
        return uploaded


class DownloadFromStorageOperator(BaseOperator):
    """Downloads artifacts or raw files from S3/SeaweedFS bucket to local directory."""

    template_fields = ("remote_keys", "local_dir", "bucket")

    def __init__(
        self,
        *,
        task_id: str,
        storage_conn_id: str = "storage_default",
        bucket: str = "data-landing",
        remote_keys: List[str],
        local_dir: str = "staging/downloads",
        **kwargs: Any,
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.storage_conn_id = storage_conn_id
        self.bucket = bucket
        self.remote_keys = remote_keys
        self.local_dir = local_dir

    def execute(self, context: Dict[str, Any]) -> List[str]:
        hook = StorageHook(self.storage_conn_id)
        downloaded = []
        for key in self.remote_keys:
            file_name = key.split("/")[-1]
            target = f"{self.local_dir.rstrip('/')}/{file_name}"
            dest = hook.download_file(remote_key=key, local_path=target, bucket=self.bucket)
            downloaded.append(dest)
        return downloaded
