import os
import re
from pathlib import Path
from typing import Any, Dict

from omegaconf import OmegaConf


def _default_target_schema(source_name: str) -> str:
    normalized = source_name.strip().lower()
    normalized = re.sub(r"[^a-z0-9_]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")

    if normalized.endswith("_source"):
        normalized = normalized[: -len("_source")]

    if not normalized:
        normalized = "source"

    return f"{normalized}_analytics"


class DbtProjectFactory:
    """
    Loads dbt source/unit definitions from YAML config files.
    """

    def __init__(self, config_dir: str):
        self.config_dir = config_dir
        if not os.path.exists(self.config_dir):
            raise FileNotFoundError(f"DBT config directory {self.config_dir} does not exist.")
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

                project_dir = source_config.get("project_dir")
                units = source_config.get("units")
                source_is_active = bool(source_config.get("is_active", True))

                # Shared/template blocks are allowed (no project_dir/units).
                if not project_dir or not isinstance(units, dict):
                    continue

                source_defaults: Dict[str, Any] = {
                    "project_dir": str(project_dir),
                    "profile_name": str(source_config.get("profile_name", "datahelm")),
                    "target": str(source_config.get("target", "dev")),
                    "profiles_dir": str(source_config.get("profiles_dir", "analytics/dbt_profiles")),
                    "dbt_command": str(source_config.get("dbt_command", "build")),
                    "select": list(source_config.get("select", [])),
                    "exclude": list(source_config.get("exclude", [])),
                    "vars": dict(source_config.get("vars", {})),
                    "schedules": list(source_config.get("schedules", [])),
                }

                for unit_name, unit_config in units.items():
                    if not isinstance(unit_config, dict):
                        continue

                    is_active = source_is_active and bool(unit_config.get("is_active", True))
                    unit_key = f"{source_name}_{unit_name}".upper()

                    merged_vars = dict(source_defaults["vars"])
                    merged_vars.update(dict(unit_config.get("vars", {})))
                    if "target_schema" not in merged_vars:
                        merged_vars["target_schema"] = _default_target_schema(source_name)

                    merged_select = (
                        list(unit_config.get("select", []))
                        if unit_config.get("select") is not None
                        else list(source_defaults["select"])
                    )
                    merged_exclude = (
                        list(unit_config.get("exclude", []))
                        if unit_config.get("exclude") is not None
                        else list(source_defaults["exclude"])
                    )
                    merged_schedules = (
                        list(unit_config.get("schedules", []))
                        if unit_config.get("schedules") is not None
                        else list(source_defaults["schedules"])
                    )

                    units_map[unit_key] = {
                        "source_name": source_name,
                        "unit_name": unit_name,
                        "is_active": is_active,
                        "project_dir": source_defaults["project_dir"],
                        "profile_name": str(unit_config.get("profile_name", source_defaults["profile_name"])),
                        "target": str(unit_config.get("target", source_defaults["target"])),
                        "profiles_dir": str(unit_config.get("profiles_dir", source_defaults["profiles_dir"])),
                        "dbt_command": str(unit_config.get("dbt_command", source_defaults["dbt_command"])),
                        "select": merged_select,
                        "exclude": merged_exclude,
                        "vars": merged_vars,
                        "schedules": merged_schedules,
                    }

        return units_map

    def get_unit(self, unit_key: str) -> Dict[str, Any]:
        unit = self.units_map.get(unit_key)
        if not unit:
            raise KeyError(f"DBT unit '{unit_key}' not found in configuration.")
        return unit
