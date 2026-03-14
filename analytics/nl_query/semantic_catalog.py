from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import yaml


@dataclass(frozen=True)
class SemanticCatalog:
    """
    Lightweight semantic metadata used by NL query generation.
    """

    warehouse: str
    datasets: Dict[str, Dict[str, Any]]
    global_rules: List[str]


def load_semantic_catalog(path: str) -> SemanticCatalog:
    """
    Load catalog YAML into a strongly-typed container.

    Expected shape:
      warehouse: postgres
      global_rules: []
      datasets:
        some_dataset:
          table: schema.table
          description: ...
          dimensions: []
          metrics: []
          synonyms: []
    """
    with open(path, "r", encoding="utf-8") as file:
        raw = yaml.safe_load(file) or {}

    warehouse = str(raw.get("warehouse", "postgres")).strip().lower()
    datasets = raw.get("datasets", {})
    global_rules = raw.get("global_rules", [])

    if not isinstance(datasets, dict):
        raise ValueError("Invalid semantic catalog: 'datasets' must be a dictionary.")
    if not isinstance(global_rules, list):
        raise ValueError("Invalid semantic catalog: 'global_rules' must be a list.")

    return SemanticCatalog(
        warehouse=warehouse,
        datasets=datasets,
        global_rules=[str(rule) for rule in global_rules],
    )
