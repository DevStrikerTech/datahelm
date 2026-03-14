import os
from pathlib import Path
from typing import Any, Dict

from omegaconf import OmegaConf


class DashboardFactory:
    """
    Loads dashboard unit definitions from YAML config files.
    """

    def __init__(self, config_dir: str):
        self.config_dir = config_dir
        if not os.path.exists(self.config_dir):
            raise FileNotFoundError(f"Dashboard config directory {self.config_dir} does not exist.")
        self.units_map = self._load_units()

    def _load_units(self) -> Dict[str, Dict[str, Any]]:
        units_map: Dict[str, Dict[str, Any]] = {}
        yaml_files = Path(self.config_dir).glob("*.yaml")

        for yaml_file in yaml_files:
            raw_conf = OmegaConf.load(yaml_file)
            OmegaConf.resolve(raw_conf)
            conf = OmegaConf.to_container(raw_conf, resolve=True)

            if not isinstance(conf, dict):
                continue

            for source_name, source_config in conf.items():
                if not isinstance(source_config, dict):
                    continue

                notebook_path = source_config.get("notebook_path")
                units = source_config.get("units")
                source_is_active = bool(source_config.get("is_active", True))

                # Shared/template blocks are allowed.
                if not notebook_path or not isinstance(units, dict):
                    continue

                source_defaults: Dict[str, Any] = {
                    "notebook_path": str(notebook_path),
                    "schedules": list(source_config.get("schedules", [])),
                }

                for unit_name, unit_config in units.items():
                    if not isinstance(unit_config, dict):
                        continue

                    unit_key = f"{source_name}_{unit_name}".upper()
                    is_active = source_is_active and bool(unit_config.get("is_active", True))

                    units_map[unit_key] = {
                        "source_name": source_name,
                        "unit_name": unit_name,
                        "is_active": is_active,
                        "notebook_path": str(unit_config.get("notebook_path", source_defaults["notebook_path"])),
                        "db_schema": str(unit_config.get("db_schema", "")),
                        "db_table": str(unit_config.get("db_table", "")),
                        "chart_x_col": str(unit_config.get("chart_x_col", "")),
                        "chart_y_col": str(unit_config.get("chart_y_col", "")),
                        "row_limit": int(unit_config.get("row_limit", 25)),
                        "schedules": list(unit_config.get("schedules", source_defaults["schedules"])),
                    }

        return units_map

    def get_unit(self, unit_key: str) -> Dict[str, Any]:
        unit = self.units_map.get(unit_key)
        if not unit:
            raise KeyError(f"Dashboard unit '{unit_key}' not found in configuration.")
        return unit
