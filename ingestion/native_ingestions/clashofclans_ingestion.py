import os
from typing import Any, Dict, Iterator, List

from ingestion.native_ingestions.base_ingestion import BaseIngestion
from handlers.api.clashofclans import ClashOfClansHandler
from utils.ingestion_utils import ExtractOutputType

class ClashOfClansIngestion(BaseIngestion):
    """
    Native ingestion for the Clash of Clans API.

    This class implements the extract and publish steps for a Clash of Clans ingestion.
    It uses the ClashOfClansHandler to call the API and the BaseIngestion’s
    _publish_to_postgres() method to upsert the data into PostgreSQL.

    Attributes:
        handler_class (Type): The API handler class to use.
        handler_extract_method (str): The name of the method to call on the handler.
        extract_method_watermark_col (str): Field name used for watermarking.
        extract_output_type (ExtractOutputType): Expected type of the extracted output.
        source_hash_cols (List[str]): Columns used for calculating a source hash.
        ingest_type (str): Identifier for this ingestion type.
    """

    # Class-level attributes for shared ingestion behavior.
    handler_class = ClashOfClansHandler
    handler_extract_method = "get_data_iter"
    extract_method_watermark_col = "last_mtime"
    extract_output_type = ExtractOutputType.list_of_dicts
    source_hash_cols = ["player_tag"]
    ingest_type = "clashofclans"

    def __init__(self, ingestion_name: str, config: Dict[str, Any]) -> None:
        """
        Initialize the ClashOfClansIngestion instance.

        :param ingestion_name: The name of the ingestion (e.g. "CLASHOFCLANS_PLAYER_STATS").
        :param config: The YAML configuration dictionary for this ingestion.
        """
        self.ingestion_name = ingestion_name
        self.config = config
        super().__init__()

    @property
    def schedules(self) -> List[Dict[str, Any]]:
        """
        Returns the schedule configuration for this ingestion as defined in the YAML.

        :return: A list of schedule dictionaries. If not defined in the config,
                 an empty list is returned.
        """
        return self.config.get("schedules", [])

    def run(self) -> None:
        """
        Execute the ingestion process: extract data from the API and publish it to PostgreSQL.
        """
        # Read extraction parameters and API credentials
        extract_params: Dict[str, Any] = self.config.get("extract_params", {})
        player_tag: str = extract_params.get("player_tag")
        player_achievements: str = extract_params.get("player_achievements")
        player_troops: str = extract_params.get("player_troops")
        player_heroes: str = extract_params.get("player_heroes")

        if not player_tag:
            raise ValueError("Missing required parameter 'player_tag' in extract_params.")

        if not player_achievements:
            raise ValueError("Missing required parameter 'player_achievements' in extract_params.")

        if not player_troops:
            raise ValueError("Missing required parameter 'player_troops' in extract_params.")

        if not player_heroes:
            raise ValueError("Missing required parameter 'player_heroes' in extract_params.")

        extract_init: Dict[str, Any] = self.config.get("extract_init", {})
        token_env_var: str = extract_init.get("token_env_var")

        if not token_env_var:
            raise ValueError("Missing required key 'token_env_var' in extract_init configuration.")

        api_token: str = os.getenv(token_env_var)

        if not api_token:
            raise EnvironmentError(
                f"Missing Clash of Clans API token: Environment variable '{token_env_var}' is not set or empty."
            )

        # Initialize the API handler
        self.handler = self.handler_class(token_env_var)

        # Extract data from the API
        data_iter: Iterator[List[Dict[str, Any]]] = self.handler.get_data_iter(player_tag=player_tag,
                                                                               player_achievements=player_achievements,
                                                                               player_troops=player_troops,
                                                                               player_heroes=player_heroes)

        # Accumulate all records from the iterator.
        all_records: List[Dict[str, Any]] = []

        for chunk in data_iter:
            all_records.extend(chunk)

        # Publish data to PostgreSQL
        publish_params: Dict[str, Any] = self.config.get("publish_params", {})
        target_table: str = self.config.get("target_table")

        if not target_table:
            raise ValueError("Missing required key 'target_table' in configuration.")

        # Build table parameters from publish_params and YAML columns.
        table_params: Dict[str, Any] = {
            "target_db": publish_params.get("target_db"),
            "target_schema": publish_params.get("target_schema"),
            "target_table": target_table,
            "columns": self.config.get("columns", []),
        }

        # Call the shared method to upsert the records into Postgres.
        self._publish_to_postgres(table_params, iter(all_records))
