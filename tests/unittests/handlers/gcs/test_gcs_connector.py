import sys
import types
from datetime import datetime

import pytest

from handlers.gcs import GCSSourceConnector


class _FakeBlob:
    def __init__(self, name: str):
        self.name = name
        self.size = 3
        self.content_type = "text/plain"
        self.updated = datetime(2026, 1, 1)
        self.generation = "1"
        self.metadata = None
        self._content = b"abc"
        self._exists = True

    def upload_from_string(self, payload, content_type="application/octet-stream"):
        self._content = payload
        self.content_type = content_type
        self.size = len(payload)

    def upload_from_file(self, file_stream, rewind=True, content_type="application/octet-stream"):
        if rewind:
            file_stream.seek(0)
        payload = file_stream.read()
        self._content = payload
        self.content_type = content_type
        self.size = len(payload)

    def download_as_bytes(self):
        return self._content

    def exists(self):
        return self._exists

    def delete(self):
        self._exists = False

    def generate_signed_url(self, version, expiration, method):
        return f"signed://{self.name}?method={method}&exp={int(expiration.total_seconds())}"


class _FakeBucket:
    def __init__(self):
        self._blobs = {}

    def blob(self, object_path):
        self._blobs.setdefault(object_path, _FakeBlob(object_path))
        return self._blobs[object_path]

    def list_blobs(self, prefix=""):
        return [blob for name, blob in self._blobs.items() if name.startswith(prefix)]


class _FakeStorageClient:
    def __init__(self, project=None):
        self.project = project
        self._bucket = _FakeBucket()

    @classmethod
    def from_service_account_json(cls, credentials_path, project=None):
        return cls(project=project)

    def bucket(self, bucket_name):
        return self._bucket


def _patch_fake_google_storage(monkeypatch):
    storage_module = types.SimpleNamespace(Client=_FakeStorageClient)
    cloud_module = types.SimpleNamespace(storage=storage_module)
    google_module = types.SimpleNamespace(cloud=cloud_module)

    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_module)
    monkeypatch.setitem(sys.modules, "google.cloud.storage", storage_module)


def test_gcs_connector_requires_bucket_name():
    with pytest.raises(ValueError, match="bucket_name is required"):
        GCSSourceConnector(bucket_name="")


def test_gcs_connector_upload_download_list_exists_and_delete(monkeypatch):
    _patch_fake_google_storage(monkeypatch)
    connector = GCSSourceConnector(bucket_name="demo-bucket")

    connector.upload_bytes("folder/file.txt", b"hello", content_type="text/plain")
    assert connector.download_bytes("folder/file.txt") == b"hello"
    assert connector.exists("folder/file.txt") is True

    listed = connector.list_objects(prefix="folder/")
    assert len(listed) == 1
    assert listed[0]["name"] == "folder/file.txt"

    signed_url = connector.generate_signed_url("folder/file.txt", expiration_seconds=120, method="GET")
    assert "signed://folder/file.txt" in signed_url

    assert connector.delete("folder/file.txt") is True
    assert connector.exists("folder/file.txt") is False
