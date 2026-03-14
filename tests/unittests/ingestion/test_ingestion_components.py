import os

import pytest

# Ensure required DB env vars exist before importing ingestion modules.
os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "postgres")
os.environ.setdefault("DB_NAME", "postgres")

from ingestion.ingestion_factory import IngestionFactory
from ingestion.native_ingestions.clashofclans_ingestion import ClashOfClansIngestion


def test_ingestion_factory_loads_known_ingestion_type(tmp_path):
    config_dir = tmp_path / "api"
    config_dir.mkdir()
    (config_dir / "clash.yaml").write_text(
        """
CLASHOFCLANS_PLAYER_STATS:
  ingest_type: clashofclans
  extract_init:
    token_env_var: CLASHOFCLANS_API_TOKEN
  extract_params:
    player_tag: "#ABC"
    player_achievements: achievements
    player_troops: troops
    player_heroes: heroes
  target_table: clashofclans_stats
  columns:
    - name: id
      source_key: tag
""".strip(),
        encoding="utf-8",
    )

    factory = IngestionFactory(config_dir=str(config_dir))

    ingestion = factory.create_ingestion("CLASHOFCLANS_PLAYER_STATS")
    assert isinstance(ingestion, ClashOfClansIngestion)
    assert "CLASHOFCLANS_PLAYER_STATS" in factory.ingestions_map


def test_ingestion_factory_skips_unknown_ingest_type(tmp_path):
    config_dir = tmp_path / "api"
    config_dir.mkdir()
    (config_dir / "unknown.yaml").write_text(
        """
UNKNOWN_INGESTION:
  ingest_type: unknown_type
""".strip(),
        encoding="utf-8",
    )

    factory = IngestionFactory(config_dir=str(config_dir))
    assert factory.ingestions_map == {}


def test_ingestion_factory_raises_for_missing_ingestion_name(tmp_path):
    config_dir = tmp_path / "api"
    config_dir.mkdir()
    (config_dir / "empty.yaml").write_text("{}", encoding="utf-8")

    factory = IngestionFactory(config_dir=str(config_dir))

    with pytest.raises(KeyError, match="Ingestion 'MISSING' not found"):
        factory.create_ingestion("MISSING")


def test_clash_ingestion_validates_required_extract_params(monkeypatch):
    config = {
        "extract_init": {"token_env_var": "CLASHOFCLANS_API_TOKEN"},
        "extract_params": {
            "player_achievements": "achievements",
            "player_troops": "troops",
            "player_heroes": "heroes",
        },
        "target_table": "clashofclans_stats",
        "columns": [{"name": "id", "source_key": "tag"}],
    }
    ingestion = ClashOfClansIngestion("CLASHOFCLANS_PLAYER_STATS", config)
    monkeypatch.setenv("CLASHOFCLANS_API_TOKEN", "token")

    with pytest.raises(ValueError, match="player_tag"):
        ingestion.run()


def test_clash_ingestion_runs_and_publishes_records(monkeypatch):
    config = {
        "extract_init": {"token_env_var": "CLASHOFCLANS_API_TOKEN"},
        "extract_params": {
            "player_tag": "#ABC",
            "player_achievements": "achievements",
            "player_troops": "troops",
            "player_heroes": "heroes",
        },
        "publish_params": {
            "target_db": "postgres",
            "target_schema": "clashofclans",
        },
        "target_table": "clashofclans_stats",
        "columns": [{"name": "id", "source_key": "tag"}],
    }

    class FakeHandler:
        def __init__(self, token_env_var):
            self.token_env_var = token_env_var

        def get_data_iter(self, **_kwargs):
            yield [{"id": "ABC"}]

    captured = {}

    def fake_publish(table_params, record_iter):
        captured["table_params"] = table_params
        captured["records"] = list(record_iter)

    monkeypatch.setenv("CLASHOFCLANS_API_TOKEN", "token")
    ingestion = ClashOfClansIngestion("CLASHOFCLANS_PLAYER_STATS", config)
    monkeypatch.setattr(ingestion, "handler_class", FakeHandler)
    monkeypatch.setattr(ingestion, "_publish_to_postgres", fake_publish)

    ingestion.run()

    assert captured["table_params"]["target_table"] == "clashofclans_stats"
    assert captured["table_params"]["target_schema"] == "clashofclans"
    assert captured["records"] == [{"id": "ABC"}]
