import os
import logging
from pathlib import Path
from typing import Dict, Any

from omegaconf import OmegaConf

from ingestion.native_ingestions.base_ingestion import BaseIngestion
from ingestion.native_ingestions.clashofclans_ingestion import ClashOfClansIngestion


log = logging.getLogger(__name__)

# Mapping of ingestion type names to corresponding ingestion classes
INGESTION_CLASSES: Dict[str, Any] = {
    "clashofclans": ClashOfClansIngestion,
    # Add new ingestion types as needed
}


class IngestionFactory:
    """
    Factory for dynamically loading ingestion configurations from YAML files
    and instantiating ingestion objects.
    """

    def __init__(self, config_dir: str):
        """
        Initialize the factory with a config directory.

        :param config_dir: Path to the ingestion YAML configurations.
        """
        self.config_dir = config_dir

        if not os.path.exists(self.config_dir):
            raise FileNotFoundError(f"Configuration directory {self.config_dir} does not exist.")

        # Load and instantiate ingestions
        self.ingestions_map = self._load_ingestions()

    def _load_ingestions(self) -> Dict[str, BaseIngestion]:
        """
        Load all ingestion definitions from YAML files in the config directory,
        instantiate them as Python objects.

        :return: Dictionary mapping ingestion names to instantiated ingestion objects.
        """
        ingestions_map: Dict[str, BaseIngestion] = {}

        yaml_files = Path(self.config_dir).glob("*.yaml")

        for yaml_file in yaml_files:
            with open(yaml_file, "r", encoding="utf-8") as file:
                raw_conf = OmegaConf.load(file)

            # Resolve YAML interpolations and convert to plain Python containers
            # so downstream code can safely use isinstance(..., dict/list).
            OmegaConf.resolve(raw_conf)
            conf = OmegaConf.to_container(raw_conf, resolve=True)

            if not isinstance(conf, dict):
                log.warning("Top-level YAML content in '%s' is not a dict. Skipping file.", yaml_file.name)
                continue

            log.debug("Resolved configuration for %s", yaml_file.name)

            # Iterate over top-level ingestion names in YAML
            for ingestion_name, ingestion_config in conf.items():
                if not isinstance(ingestion_config, dict):
                    log.warning(
                        "Ingestion block '%s' in '%s' is not a dict. Skipping.",
                        ingestion_name,
                        yaml_file.name,
                    )
                    continue

                ingest_type_raw = ingestion_config.get("ingest_type")
                ingest_type = str(ingest_type_raw).lower() if ingest_type_raw else ""

                # Shared/template blocks may intentionally omit ingest_type.
                if not ingest_type:
                    log.debug("Skipping config block '%s' without 'ingest_type'.", ingestion_name)
                    continue

                # Get corresponding ingestion class
                ingestion_class = INGESTION_CLASSES.get(ingest_type)

                if not ingestion_class:
                    log.warning(f"Unknown ingest_type '{ingest_type}' for '{ingestion_name}'. Skipping.")
                    continue

                try:
                    # Instantiate ingestion object and store in map
                    ingestion_obj = ingestion_class(ingestion_name, ingestion_config)
                    ingestions_map[ingestion_name] = ingestion_obj
                    log.info(f"Loaded ingestion '{ingestion_name}' of type '{ingest_type}'")
                except Exception as e:
                    log.error(f"Failed to instantiate ingestion '{ingestion_name}': {e}")

        return ingestions_map

    def create_ingestion(self, ingestion_name: str) -> BaseIngestion:
        """
        Retrieve the ingestion object for the given ingestion name.

        Args:
            ingestion_name (str): The key corresponding to the ingestion configuration.

        Returns:
            BaseIngestion: The instantiated ingestion object.

        Raises:
            KeyError: If the ingestion name is not found.
        """
        ingestion_obj = self.ingestions_map.get(ingestion_name)

        if not ingestion_obj:
            raise KeyError(f"Ingestion '{ingestion_name}' not found in configuration.")

        return ingestion_obj
