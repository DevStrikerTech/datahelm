from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict

from analytics.nl_query.semantic_catalog import SemanticCatalog
from analytics.nl_query.sql_guard import ensure_safe_readonly_sql


@dataclass(frozen=True)
class NLQueryResult:
    question: str
    sql: str
    rationale: str
    dataset: str


class NLQueryService:
    """
    Orchestrates NL question -> LLM JSON -> guarded SQL.
    """

    def __init__(self, catalog: SemanticCatalog, llm_client: Any, max_limit: int = 1000):
        self.catalog = catalog
        self.llm_client = llm_client
        self.max_limit = max_limit

    def generate_sql(self, question: str) -> NLQueryResult:
        prompt = self._build_prompt(question)
        system_prompt = (
            "You are a strict analytics SQL assistant. "
            "Return JSON only with keys: dataset, sql, rationale."
        )
        raw = self.llm_client.generate(prompt=prompt, system_prompt=system_prompt)
        parsed = self._parse_llm_json(raw)

        sql = ensure_safe_readonly_sql(parsed["sql"], max_limit=self.max_limit)
        dataset = str(parsed["dataset"]).strip()
        if dataset not in self.catalog.datasets:
            raise ValueError(f"LLM selected unknown dataset '{dataset}'.")

        return NLQueryResult(
            question=question,
            sql=sql,
            rationale=str(parsed.get("rationale", "")).strip(),
            dataset=dataset,
        )

    def _build_prompt(self, question: str) -> str:
        catalog_json = json.dumps(
            {
                "warehouse": self.catalog.warehouse,
                "global_rules": self.catalog.global_rules,
                "datasets": self.catalog.datasets,
                "constraints": {
                    "readonly_sql": True,
                    "must_include_limit": True,
                    "max_limit": self.max_limit,
                },
            },
            indent=2,
        )
        return (
            "Convert the analytics question into a safe PostgreSQL query.\n"
            "Use only the provided catalog metadata.\n"
            "Output JSON only.\n\n"
            f"QUESTION:\n{question}\n\n"
            f"CATALOG:\n{catalog_json}\n"
        )

    @staticmethod
    def _parse_llm_json(raw_text: str) -> Dict[str, Any]:
        try:
            parsed = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            raise ValueError("LLM output was not valid JSON.") from exc

        if not isinstance(parsed, dict):
            raise ValueError("LLM output must be a JSON object.")
        if "dataset" not in parsed or "sql" not in parsed:
            raise ValueError("LLM output missing required keys: dataset/sql.")
        return parsed
