import sys
import types

import pandas as pd

from handlers.bigquery import BigQuerySourceConnector


class _FakeBQRow(dict):
    def items(self):
        return super().items()


class _FakeBQField:
    def __init__(self, name, field_type, mode):
        self.name = name
        self.field_type = field_type
        self.mode = mode


class _FakeBQTable:
    def __init__(self):
        self.schema = [_FakeBQField("id", "STRING", "REQUIRED")]


class _FakeBQQueryJob:
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return self._rows


class _FakeBQLoadJob:
    def result(self):
        return None


class _FakeBQClient:
    def __init__(self, project=None, location=None):
        self.project = project
        self.location = location
        self._table_exists = True

    @classmethod
    def from_service_account_json(cls, credentials_path, project=None, location=None):
        return cls(project=project, location=location)

    def query(self, sql, job_config=None, location=None):
        return _FakeBQQueryJob([_FakeBQRow({"id": "1", "name": "alice"})])

    def list_rows(self, table_id, max_results=None):
        return [_FakeBQRow({"id": "2", "name": "bob"})]

    def load_table_from_dataframe(self, dataframe, table_id, job_config=None):
        return _FakeBQLoadJob()

    def get_table(self, table_id):
        if not self._table_exists:
            raise RuntimeError("not found")
        return _FakeBQTable()


def _patch_fake_bigquery(monkeypatch):
    fake_bigquery_module = types.SimpleNamespace(
        Client=_FakeBQClient,
        LoadJobConfig=lambda write_disposition: types.SimpleNamespace(write_disposition=write_disposition),
    )
    cloud_module = types.SimpleNamespace(bigquery=fake_bigquery_module)
    google_module = types.SimpleNamespace(cloud=cloud_module)

    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_module)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", fake_bigquery_module)


def test_bigquery_connector_query_table_and_schema(monkeypatch):
    _patch_fake_bigquery(monkeypatch)
    connector = BigQuerySourceConnector(project_id="proj", location="US")

    query_rows = connector.run_query("select 1")
    table_rows = connector.fetch_table_rows("proj.ds.tbl", max_results=10)
    schema = connector.get_table_schema("proj.ds.tbl")

    assert query_rows == [{"id": "1", "name": "alice"}]
    assert table_rows == [{"id": "2", "name": "bob"}]
    assert schema == [{"name": "id", "field_type": "STRING", "mode": "REQUIRED"}]


def test_bigquery_connector_load_dataframe_and_table_exists(monkeypatch):
    _patch_fake_bigquery(monkeypatch)
    connector = BigQuerySourceConnector(project_id="proj")

    connector.load_dataframe(
        dataframe=pd.DataFrame([{"id": "1"}]),
        table_id="proj.ds.tbl",
        write_disposition="WRITE_TRUNCATE",
    )
    assert connector.table_exists("proj.ds.tbl") is True

    connector.client._table_exists = False
    assert connector.table_exists("proj.ds.tbl") is False
