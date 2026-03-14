import importlib.util
from typing import Union, List, Dict, Any

from dagster import repository, get_dagster_logger

from dagster_op.ingestion_job import create_all_ingestion_jobs
from dagster_op.ingestion_schedule import IngestionScheduleCreator
from dagster_op.ingestion_sensor import ingestion_sensor
from dagster_op.dbt_job import create_all_dbt_jobs
from dagster_op.dbt_schedule import DbtScheduleCreator
from dagster_op.dashboard_job import create_all_dashboard_jobs
from dagster_op.dashboard_schedule import DashboardScheduleCreator

log = get_dagster_logger()


class LocalPathRepositoryLoader:
    """
    Helper class to dynamically load local Dagster repository modules.
    """

    def __init__(self, repository_paths: Dict[str, str]):
        """
        :param repository_paths: Dict where keys are module names and values are file paths.
        """
        self.repository_paths = repository_paths

    def load_repositories(self) -> Dict[str, Any]:
        """
        Loads all specified repositories dynamically.
        """
        loaded_repos = {}

        for name, path in self.repository_paths.items():
            log.info(f"Loading repository {name} from {path}")

            try:
                loaded_repos[name] = self.import_path(name, path)
            except Exception as e:
                log.info(f"Failed to load {name}: {e}")

        return loaded_repos

    @staticmethod
    def import_path(name: str, path: str):
        """
        Import a module dynamically from a given file path.
        """
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        return module


# Define our repository
@repository
def datahelm_eng_core_repo() -> List[Union[...]]:
    """
    Dagster repository definition for the DataHelm ingestion framework.
    """
    # Create ingestion jobs (one per YAML sub-block, e.g. “FORTNITE_STATS”)
    ingestion_jobs = create_all_ingestion_jobs(config_dir="config/api")

    # Build ingestion schedules using IngestionScheduleCreator
    schedule_creator = IngestionScheduleCreator(ingestion_config_dir="config/api")
    ingestion_schedules = schedule_creator.get_dagster_schedules()

    # Include sensors (e.g. an ingestion_sensor that checks for new data)
    ingestion_sensors = [ingestion_sensor]

    # Build dbt jobs/schedules from config for transformation layer
    dbt_jobs = create_all_dbt_jobs(config_dir="config/dbt")
    dbt_schedule_creator = DbtScheduleCreator(dbt_config_dir="config/dbt")
    dbt_schedules = dbt_schedule_creator.get_dagster_schedules()

    # Build dashboard jobs/schedules from config for notebook dashboards
    dashboard_jobs = create_all_dashboard_jobs(config_dir="config/dashboard")
    dashboard_schedule_creator = DashboardScheduleCreator(dashboard_config_dir="config/dashboard")
    dashboard_schedules = dashboard_schedule_creator.get_dagster_schedules()

    # Return them all in a single list for Dagster to register
    return [
        *ingestion_jobs,
        *ingestion_schedules,
        *ingestion_sensors,
        *dbt_jobs,
        *dbt_schedules,
        *dashboard_jobs,
        *dashboard_schedules,
    ]
