"""
Reusable Google Cloud Storage source connector.

This connector centralizes bucket authentication and object IO helpers so
ingestion developers only implement source-specific logic.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any, BinaryIO, Dict, List, Optional

log = logging.getLogger(__name__)


class GCSSourceConnector:
    """
    Thin wrapper around google-cloud-storage client for reusable source access.
    """

    def __init__(
        self,
        bucket_name: str,
        project_id: Optional[str] = None,
        credentials_path: Optional[str] = None,
    ):
        if not bucket_name:
            raise ValueError("bucket_name is required")

        try:
            from google.cloud import storage
        except ImportError as exc:
            raise ImportError(
                "google-cloud-storage is required for GCSSourceConnector. "
                "Install with `pip install google-cloud-storage`."
            ) from exc

        self._storage = storage
        self.bucket_name = bucket_name

        if credentials_path:
            self.client = storage.Client.from_service_account_json(
                credentials_path,
                project=project_id,
            )
        else:
            self.client = storage.Client(project=project_id)

        self.bucket = self.client.bucket(bucket_name)

    def upload_bytes(
        self,
        object_path: str,
        payload: bytes,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, str]] = None,
    ) -> None:
        blob = self.bucket.blob(object_path)
        if metadata:
            blob.metadata = metadata
        blob.upload_from_string(payload, content_type=content_type)

    def upload_stream(
        self,
        object_path: str,
        file_stream: BinaryIO,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, str]] = None,
    ) -> None:
        blob = self.bucket.blob(object_path)
        if metadata:
            blob.metadata = metadata
        blob.upload_from_file(file_stream, rewind=True, content_type=content_type)

    def download_bytes(self, object_path: str) -> bytes:
        blob = self.bucket.blob(object_path)
        return blob.download_as_bytes()

    def exists(self, object_path: str) -> bool:
        blob = self.bucket.blob(object_path)
        return blob.exists()

    def delete(self, object_path: str) -> bool:
        blob = self.bucket.blob(object_path)
        try:
            blob.delete()
            return True
        except Exception as exc:
            log.warning("Failed to delete object '%s' from GCS: %s", object_path, exc)
            return False

    def list_objects(self, prefix: str = "") -> List[Dict[str, Any]]:
        blobs = self.bucket.list_blobs(prefix=prefix)
        return [
            {
                "name": blob.name,
                "size": blob.size,
                "content_type": blob.content_type,
                "updated": blob.updated,
                "generation": blob.generation,
            }
            for blob in blobs
        ]

    def generate_signed_url(
        self,
        object_path: str,
        expiration_seconds: int = 3600,
        method: str = "GET",
    ) -> str:
        blob = self.bucket.blob(object_path)
        return blob.generate_signed_url(
            version="v4",
            expiration=timedelta(seconds=expiration_seconds),
            method=method,
        )
