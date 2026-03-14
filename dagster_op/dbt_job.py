from typing import Dict, List, Optional

from dagster import ConfigMapping, JobDefinition, get_dagster_logger, job, op

from analytics.dbt_factory import DbtProjectFactory
from analytics.dbt_runner import run_dbt_command

log = get_dagster_logger()


@op(config_schema={"unit_key": str, "config_dir": str, "dbt_command": str})
def op_run_dbt_project(context) -> None:
    unit_key: str = context.op_config["unit_key"]
    config_dir: str = context.op_config["config_dir"]
    dbt_command: str = context.op_config["dbt_command"]

    factory = DbtProjectFactory(config_dir=config_dir)
    unit_config = factory.get_unit(unit_key)

    log.info("[op_run_dbt_project] Running dbt unit '%s' with command '%s'.", unit_key, dbt_command)
    run_dbt_command(project_name=unit_key, project_config=unit_config, command_name=dbt_command)
    log.info("[op_run_dbt_project] Completed dbt unit '%s'.", unit_key)


def _create_single_dbt_job(unit_key: str, config_dir: str, dbt_command: str) -> JobDefinition:
    def config_fn(_job_config: dict) -> dict:
        return {
            "ops": {
                "op_run_dbt_project": {
                    "config": {
                        "unit_key": unit_key,
                        "config_dir": config_dir,
                        "dbt_command": dbt_command,
                    }
                }
            }
        }

    config_mapping = ConfigMapping(
        config_schema={},
        receive_processed_config_values=False,
        config_fn=config_fn,
    )

    @job(
        name=f"{unit_key}_dbt_job",
        config=config_mapping,
        description=f"Job that runs dbt transformation unit '{unit_key}'",
    )
    def _dbt_job():
        op_run_dbt_project()

    return _dbt_job


class DbtJobCreator:
    def __init__(self, config_dir: str, target_filter: Optional[str] = None):
        self.config_dir = config_dir
        self.target_filter = target_filter

    def create_jobs(self) -> Dict[str, JobDefinition]:
        factory = DbtProjectFactory(config_dir=self.config_dir)
        jobs_dict: Dict[str, JobDefinition] = {}

        for unit_key, unit_config in factory.units_map.items():
            if not unit_config.get("is_active", True):
                continue
            if self.target_filter and self.target_filter not in unit_key:
                continue

            dbt_command = str(unit_config.get("dbt_command", "build"))
            job_def = _create_single_dbt_job(
                unit_key=unit_key,
                config_dir=self.config_dir,
                dbt_command=dbt_command,
            )
            jobs_dict[job_def.name] = job_def

        return jobs_dict


def create_all_dbt_jobs(config_dir: str, target_filter: Optional[str] = None) -> List[JobDefinition]:
    creator = DbtJobCreator(config_dir=config_dir, target_filter=target_filter)
    return list(creator.create_jobs().values())
