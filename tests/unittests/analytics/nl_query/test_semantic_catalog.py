import pytest

from analytics.nl_query.semantic_catalog import load_semantic_catalog


def test_load_semantic_catalog_reads_expected_shape(tmp_path):
    catalog_path = tmp_path / "catalog.yaml"
    catalog_path.write_text(
        """
warehouse: postgres
global_rules:
  - read only
datasets:
  sample:
    table: schema.table
""".strip(),
        encoding="utf-8",
    )

    catalog = load_semantic_catalog(str(catalog_path))

    assert catalog.warehouse == "postgres"
    assert catalog.global_rules == ["read only"]
    assert "sample" in catalog.datasets


def test_load_semantic_catalog_validates_datasets_type(tmp_path):
    catalog_path = tmp_path / "catalog.yaml"
    catalog_path.write_text(
        """
warehouse: postgres
datasets: []
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="datasets"):
        load_semantic_catalog(str(catalog_path))
