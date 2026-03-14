import json

import pytest

from analytics.nl_query.semantic_catalog import SemanticCatalog
from analytics.nl_query.service import NLQueryService


class _FakeLLM:
    def __init__(self, response_text: str):
        self.response_text = response_text
        self.calls = []

    def generate(self, prompt: str, system_prompt: str = "") -> str:
        self.calls.append({"prompt": prompt, "system_prompt": system_prompt})
        return self.response_text


def _sample_catalog() -> SemanticCatalog:
    return SemanticCatalog(
        warehouse="postgres",
        global_rules=["read only"],
        datasets={
            "clashofclans_player_stats": {
                "table": "clashofclans_analytics.fct_clashofclans_player_stats",
                "dimensions": ["id", "name", "last_mtime"],
                "metrics": ["attackwins", "defensewins"],
            }
        },
    )


def test_service_generates_guarded_sql_and_dataset():
    llm_output = json.dumps(
        {
            "dataset": "clashofclans_player_stats",
            "sql": "SELECT name, attackwins FROM clashofclans_analytics.fct_clashofclans_player_stats ORDER BY attackwins DESC LIMIT 25",
            "rationale": "Top players by attack wins.",
        }
    )
    service = NLQueryService(catalog=_sample_catalog(), llm_client=_FakeLLM(llm_output), max_limit=1000)

    result = service.generate_sql("top players by attack wins")

    assert result.dataset == "clashofclans_player_stats"
    assert "LIMIT 25" in result.sql
    assert "Top players" in result.rationale


def test_service_rejects_unknown_dataset():
    llm_output = json.dumps(
        {
            "dataset": "unknown_dataset",
            "sql": "SELECT 1 LIMIT 1",
            "rationale": "test",
        }
    )
    service = NLQueryService(catalog=_sample_catalog(), llm_client=_FakeLLM(llm_output))

    with pytest.raises(ValueError, match="unknown dataset"):
        service.generate_sql("anything")


def test_service_rejects_invalid_json():
    service = NLQueryService(catalog=_sample_catalog(), llm_client=_FakeLLM("not-json"))

    with pytest.raises(ValueError, match="valid JSON"):
        service.generate_sql("anything")
