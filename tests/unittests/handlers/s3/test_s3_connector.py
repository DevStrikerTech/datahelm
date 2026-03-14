import io
import sys
import types
from datetime import datetime

import pytest

from handlers.s3 import S3SourceConnector


class _FakeS3Client:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.store = {}

    def put_object(self, Bucket, Key, Body, **extra):
        self.store[(Bucket, Key)] = {"Body": Body, **extra}

    def get_object(self, Bucket, Key):
        obj = self.store[(Bucket, Key)]
        return {"Body": io.BytesIO(obj["Body"])}

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.store:
            raise RuntimeError("not found")
        return {}

    def delete_object(self, Bucket, Key):
        self.store.pop((Bucket, Key), None)
        return {}

    def list_objects_v2(self, Bucket, Prefix="", MaxKeys=1000):
        contents = []
        for (bucket, key), value in self.store.items():
            if bucket != Bucket or not key.startswith(Prefix):
                continue
            contents.append(
                {
                    "Key": key,
                    "Size": len(value["Body"]),
                    "LastModified": datetime(2026, 1, 1),
                    "ETag": "etag",
                    "StorageClass": "STANDARD",
                }
            )
        return {"Contents": contents[:MaxKeys]}

    def generate_presigned_url(self, ClientMethod, Params, ExpiresIn):
        return f"presigned://{ClientMethod}/{Params['Bucket']}/{Params['Key']}?exp={ExpiresIn}"


def _patch_fake_boto3(monkeypatch):
    fake_client = _FakeS3Client
    boto3_module = types.SimpleNamespace(client=lambda *_args, **kwargs: fake_client(**kwargs))
    monkeypatch.setitem(sys.modules, "boto3", boto3_module)


def test_s3_connector_requires_bucket_name():
    with pytest.raises(ValueError, match="bucket_name is required"):
        S3SourceConnector(bucket_name="")


def test_s3_connector_upload_download_list_exists_delete(monkeypatch):
    _patch_fake_boto3(monkeypatch)

    connector = S3SourceConnector(bucket_name="demo-bucket", region_name="us-east-1")
    connector.upload_bytes("folder/a.txt", b"hello", content_type="text/plain")

    assert connector.exists("folder/a.txt") is True
    assert connector.download_bytes("folder/a.txt") == b"hello"

    listed = connector.list_objects(prefix="folder/")
    assert len(listed) == 1
    assert listed[0]["key"] == "folder/a.txt"

    url = connector.generate_presigned_url("folder/a.txt", expiration_seconds=120)
    assert "presigned://get_object/demo-bucket/folder/a.txt?exp=120" == url

    assert connector.delete("folder/a.txt") is True
    assert connector.exists("folder/a.txt") is False


def test_s3_upload_stream_uses_stream_content(monkeypatch):
    _patch_fake_boto3(monkeypatch)

    connector = S3SourceConnector(bucket_name="demo-bucket")
    connector.upload_stream("folder/b.txt", io.BytesIO(b"stream-data"), content_type="text/plain")

    assert connector.download_bytes("folder/b.txt") == b"stream-data"
