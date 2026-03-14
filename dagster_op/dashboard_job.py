from typing import Dict, List, Optional

from dagster import ConfigMapping, JobDefinition, get_dagster_logger, job
from dagstermill import define_dagstermill_op, local_output_notebook_io_manager

from analytics.dashboard_factory import DashboardFactory

log = get_dagster_logger()


def _create_single_dashboard_job(unit_key: str, config_dir: str, unit_config: Dict[str, object]) -> JobDefinition:
    op_name = f"{unit_key.lower()}_dashboard_op"
    notebook_path = str(unit_config["notebook_path"])

    dashboard_op = define_dagstermill_op(
        name=op_name,
        notebook_path=notebook_path,
        output_notebook_name=f"{unit_key.lower()}_executed",
        config_schema={
            "unit_key": str,
            "db_schema": str,
            "db_table": str,
            "chart_x_col": str,
            "chart_y_col": str,
            "row_limit": int,
        },
        description=f"Dagstermill dashboard notebook op for '{unit_key}'.",
    )

    def config_fn(_job_config: dict) -> dict:
        return {
            "ops": {
                op_name: {
                    "config": {
                        "unit_key": unit_key,
                        "db_schema": str(unit_config["db_schema"]),
                        "db_table": str(unit_config["db_table"]),
                        "chart_x_col": str(unit_config["chart_x_col"]),
                        "chart_y_col": str(unit_config["chart_y_col"]),
                        "row_limit": int(unit_config["row_limit"]),
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
        name=f"{unit_key}_dashboard_job",
        config=config_mapping,
        description=f"Job that renders dashboard notebook for '{unit_key}'.",
        resource_defs={"output_notebook_io_manager": local_output_notebook_io_manager},
    )
    def _dashboard_job():
        dashboard_op()

    return _dashboard_job


class DashboardJobCreator:
    def __init__(self, config_dir: str, target_filter: Optional[str] = None):
        self.config_dir = config_dir
        self.target_filter = target_filter

    def create_jobs(self) -> Dict[str, JobDefinition]:
        factory = DashboardFactory(config_dir=self.config_dir)
        jobs_dict: Dict[str, JobDefinition] = {}

        for unit_key, unit_config in factory.units_map.items():
            if not unit_config.get("is_active", True):
                continue
            if self.target_filter and self.target_filter not in unit_key:
                continue

            job_def = _create_single_dashboard_job(
                unit_key=unit_key,
                config_dir=self.config_dir,
                unit_config=unit_config,
            )
            jobs_dict[job_def.name] = job_def
            log.info("Registered dashboard job: %s", job_def.name)

        return jobs_dict


def create_all_dashboard_jobs(config_dir: str, target_filter: Optional[str] = None) -> List[JobDefinition]:
    creator = DashboardJobCreator(config_dir=config_dir, target_filter=target_filter)
    return list(creator.create_jobs().values())
