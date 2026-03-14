"""
Reusable Google BigQuery source connector.

Centralizes BigQuery client setup and common read/write helpers for ingestion
pipelines so developers can focus on source logic.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


class BigQuerySourceConnector:
    """
    Thin wrapper around google-cloud-bigquery client for reusable ingestion IO.
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        credentials_path: Optional[str] = None,
        location: Optional[str] = None,
    ):
        try:
            from google.cloud import bigquery
        except ImportError as exc:
            raise ImportError(
                "google-cloud-bigquery is required for BigQuerySourceConnector. "
                "Install with `pip install google-cloud-bigquery`."
            ) from exc

        self._bigquery = bigquery
        self.location = location

        if credentials_path:
            self.client = bigquery.Client.from_service_account_json(
                credentials_path,
                project=project_id,
                location=location,
            )
        else:
            self.client = bigquery.Client(project=project_id, location=location)

    def run_query(self, sql: str, job_config: Any = None) -> List[Dict[str, Any]]:
        job = self.client.query(sql, job_config=job_config, location=self.location)
        rows = job.result()
        return [dict(row.items()) for row in rows]

    def fetch_table_rows(self, table_id: str, max_results: Optional[int] = None) -> List[Dict[str, Any]]:
        rows = self.client.list_rows(table_id, max_results=max_results)
        return [dict(row.items()) for row in rows]

    def load_dataframe(
        self,
        dataframe: Any,
        table_id: str,
        write_disposition: str = "WRITE_APPEND",
    ) -> None:
        job_config = self._bigquery.LoadJobConfig(write_disposition=write_disposition)
        job = self.client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
        job.result()

    def table_exists(self, table_id: str) -> bool:
        try:
            self.client.get_table(table_id)
            return True
        except Exception:
            return False

    def get_table_schema(self, table_id: str) -> List[Dict[str, str]]:
        table = self.client.get_table(table_id)
        return [{"name": field.name, "field_type": field.field_type, "mode": field.mode} for field in table.schema]
