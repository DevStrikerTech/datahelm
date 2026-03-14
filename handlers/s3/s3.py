"""
Reusable Amazon S3 source connector.

Centralizes S3 connection and object IO helpers so ingestion flows can focus on
source-specific parsing/transformation logic.
"""

from __future__ import annotations

import logging
from io import BytesIO
from typing import Any, BinaryIO, Dict, List, Optional

log = logging.getLogger(__name__)


class S3SourceConnector:
    """
    Thin wrapper around boto3 S3 client for reusable ingestion source access.
    """

    def __init__(
        self,
        bucket_name: str,
        region_name: str = "us-east-1",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
    ):
        if not bucket_name:
            raise ValueError("bucket_name is required")

        try:
            import boto3
        except ImportError as exc:
            raise ImportError(
                "boto3 is required for S3SourceConnector. Install with `pip install boto3`."
            ) from exc

        self.bucket_name = bucket_name
        self.client = boto3.client(
            "s3",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )

    def upload_bytes(
        self,
        object_key: str,
        payload: bytes,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, str]] = None,
    ) -> None:
        extra_args: Dict[str, Any] = {"ContentType": content_type}
        if metadata:
            extra_args["Metadata"] = metadata

        self.client.put_object(
            Bucket=self.bucket_name,
            Key=object_key,
            Body=payload,
            **extra_args,
        )

    def upload_stream(
        self,
        object_key: str,
        file_stream: BinaryIO,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, str]] = None,
    ) -> None:
        payload = file_stream.read()
        self.upload_bytes(
            object_key=object_key,
            payload=payload,
            content_type=content_type,
            metadata=metadata,
        )

    def download_bytes(self, object_key: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket_name, Key=object_key)
        body = response["Body"]
        if hasattr(body, "read"):
            return body.read()
        return bytes(body)

    def exists(self, object_key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=object_key)
            return True
        except Exception:
            return False

    def delete(self, object_key: str) -> bool:
        try:
            self.client.delete_object(Bucket=self.bucket_name, Key=object_key)
            return True
        except Exception as exc:
            log.warning("Failed to delete s3://%s/%s: %s", self.bucket_name, object_key, exc)
            return False

    def list_objects(self, prefix: str = "", max_keys: int = 1000) -> List[Dict[str, Any]]:
        response = self.client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=prefix,
            MaxKeys=max_keys,
        )
        contents = response.get("Contents", [])
        return [
            {
                "key": item.get("Key"),
                "size": item.get("Size"),
                "last_modified": item.get("LastModified"),
                "etag": item.get("ETag"),
                "storage_class": item.get("StorageClass"),
            }
            for item in contents
        ]

    def generate_presigned_url(
        self,
        object_key: str,
        expiration_seconds: int = 3600,
        operation: str = "get_object",
    ) -> str:
        params = {"Bucket": self.bucket_name, "Key": object_key}
        return self.client.generate_presigned_url(
            ClientMethod=operation,
            Params=params,
            ExpiresIn=expiration_seconds,
        )

    @staticmethod
    def bytes_stream(payload: bytes) -> BytesIO:
        return BytesIO(payload)
