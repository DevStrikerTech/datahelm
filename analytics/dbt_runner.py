import json
import subprocess
from pathlib import Path
from typing import Any, Dict, List

from dagster import get_dagster_logger

log = get_dagster_logger()


def _append_multi_value_flag(command: List[str], flag: str, values: List[str]) -> None:
    for value in values:
        command.extend([flag, value])


def run_dbt_command(project_name: str, project_config: Dict[str, Any], command_name: str) -> None:
    """
    Execute a dbt CLI command for a configured project.
    """
    project_dir = Path(project_config["project_dir"]).resolve()
    profiles_dir = Path(project_config["profiles_dir"]).resolve()

    command: List[str] = [
        "dbt",
        command_name,
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
        "--profile",
        str(project_config["profile_name"]),
        "--target",
        str(project_config["target"]),
    ]

    select_values = project_config.get("select", [])
    exclude_values = project_config.get("exclude", [])
    vars_payload = project_config.get("vars", {})

    _append_multi_value_flag(command, "--select", select_values)
    _append_multi_value_flag(command, "--exclude", exclude_values)

    if vars_payload:
        command.extend(["--vars", json.dumps(vars_payload)])

    log.info("Running dbt for project '%s': %s", project_name, " ".join(command))

    completed = subprocess.run(
        command,
        check=False,
        text=True,
        capture_output=True,
    )

    if completed.stdout:
        log.info("[dbt stdout] %s", completed.stdout)
    if completed.stderr:
        log.warning("[dbt stderr] %s", completed.stderr)

    if completed.returncode != 0:
        raise RuntimeError(
            f"dbt command failed for project '{project_name}' with exit code {completed.returncode}"
        )
