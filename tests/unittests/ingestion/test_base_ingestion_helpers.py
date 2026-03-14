import os

# Ensure required DB env vars exist before importing ingestion modules.
os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "postgres")
os.environ.setdefault("DB_NAME", "postgres")

from ingestion.native_ingestions.base_ingestion import BaseIngestion


class _FakeCursor:
    def __init__(self, fetchone_values):
        self.fetchone_values = list(fetchone_values)
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchone(self):
        if self.fetchone_values:
            return self.fetchone_values.pop(0)
        return None


class _FakeConnection:
    def __init__(self, fetchone_values):
        self._cursor = _FakeCursor(fetchone_values)

    def cursor(self):
        return self._cursor


def test_ensure_schema_exists_creates_when_missing():
    conn = _FakeConnection(fetchone_values=[None])
    ingestion = BaseIngestion()

    ingestion._ensure_schema_exists(conn, "clashofclans")

    # 1st query checks existence, 2nd creates schema.
    assert len(conn._cursor.executed) == 2


def test_ensure_schema_exists_skips_when_present():
    conn = _FakeConnection(fetchone_values=[("clashofclans",)])
    ingestion = BaseIngestion()

    ingestion._ensure_schema_exists(conn, "clashofclans")

    # Only existence query should run.
    assert len(conn._cursor.executed) == 1


def test_ensure_table_exists_skips_when_present():
    conn = _FakeConnection(fetchone_values=[(True,)])
    ingestion = BaseIngestion()

    ingestion._ensure_table_exists(
        conn=conn,
        schema_name="clashofclans",
        table_name="clashofclans_stats",
        columns_def=[{"name": "id", "source_key": "tag"}, {"name": "name"}],
    )

    # Only existence query should run.
    assert len(conn._cursor.executed) == 1


def test_ensure_table_exists_creates_when_missing():
    conn = _FakeConnection(fetchone_values=[(False,)])
    ingestion = BaseIngestion()

    ingestion._ensure_table_exists(
        conn=conn,
        schema_name="clashofclans",
        table_name="clashofclans_stats",
        columns_def=[{"name": "id", "source_key": "tag"}, {"name": "name"}],
    )

    # 1st query checks existence, 2nd creates table.
    assert len(conn._cursor.executed) == 2


def test_publish_to_postgres_returns_early_when_schema_or_table_missing(monkeypatch):
    ingestion = BaseIngestion()
    connect_called = {"value": False}

    def fake_connect(*_args, **_kwargs):
        connect_called["value"] = True
        raise AssertionError("connect should not be called")

    monkeypatch.setattr("ingestion.native_ingestions.base_ingestion.psycopg2.connect", fake_connect)

    ingestion._publish_to_postgres(
        table_params={"target_schema": "", "target_table": "clashofclans_stats", "columns": [{"name": "id"}]},
        record_iter=iter([{"id": "ABC"}]),
    )

    assert connect_called["value"] is False


def test_publish_to_postgres_returns_early_when_columns_missing(monkeypatch):
    ingestion = BaseIngestion()
    connect_called = {"value": False}

    def fake_connect(*_args, **_kwargs):
        connect_called["value"] = True
        raise AssertionError("connect should not be called")

    monkeypatch.setattr("ingestion.native_ingestions.base_ingestion.psycopg2.connect", fake_connect)

    ingestion._publish_to_postgres(
        table_params={"target_schema": "clashofclans", "target_table": "clashofclans_stats", "columns": []},
        record_iter=iter([{"id": "ABC"}]),
    )

    assert connect_called["value"] is False
