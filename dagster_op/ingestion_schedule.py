from typing import List

from dagster import (
    ScheduleDefinition,
    DefaultScheduleStatus,
    get_dagster_logger,
)
from ingestion.ingestion_factory import IngestionFactory

log = get_dagster_logger()


class IngestionScheduleCreator:
    """
    Class that scans ingestion YAML configurations (using IngestionFactory)
    and creates Dagster ScheduleDefinition objects for each ingestion that has
    a defined schedule.
    """

    def __init__(self, ingestion_config_dir: str, debug: bool = False):
        """
        Initialize the schedule creator.

        Args:
            ingestion_config_dir (str): The directory containing ingestion YAML files (e.g. "config/api").
            debug (bool, optional): If True, enables debug logging. Defaults to False.
        """
        self.ingestion_config_dir = ingestion_config_dir
        self.debug = debug
        self.schedules: List[ScheduleDefinition] = []
        self._factory = IngestionFactory(
            config_dir=self.ingestion_config_dir
        )
        self._build_schedules()

    def _build_schedules(self) -> None:
        """
        Build Dagster schedules from ingestion YAML configurations.
        Each ingestion configuration may define one or more schedule blocks.
        If a schedule block is a list (from YAML substitution), the first dictionary
        in the list is used.
        """
        # Retrieve the map of ingestion objects (each keyed by ingestion name)
        ingestion_map = self._factory.ingestions_map

        for ingestion_name, ingestion_obj in ingestion_map.items():
            schedules_config = ingestion_obj.schedules

            if not schedules_config:
                if self.debug:
                    log.debug("No schedules defined for ingestion: %s", ingestion_name)
                continue

            for idx, schedule_block in enumerate(schedules_config, start=1):
                # If schedule_block is a list, use the first element
                if isinstance(schedule_block, list):
                    if schedule_block:
                        schedule_block = schedule_block[0]
                    else:
                        log.warning("Empty schedule block for ingestion: %s", ingestion_name)
                        continue

                # Validate that schedule_block is a dict
                if not isinstance(schedule_block, dict):
                    log.warning("Invalid schedule block type for ingestion: %s", ingestion_name)
                    continue

                schedule_name = f"{ingestion_name}_schedule_{idx}"
                cron_expr = schedule_block.get("cron_schedule")
                if not cron_expr:
                    log.warning("Skipping schedule block for [%s] at index [%d]: missing cron_schedule", ingestion_name, idx)
                    continue

                # Validate cron_expr: Dagster expects a 5-field cron string.
                # (Any substitution errors should be caught by the caller.)
                execution_tz = schedule_block.get("execution_timezone", "UTC")
                default_status_str = schedule_block.get("default_status")

                if default_status_str:
                    try:
                        default_status = DefaultScheduleStatus[default_status_str.upper()]
                    except KeyError:
                        log.warning(
                            "Invalid default_status '%s' for ingestion: %s; defaulting to RUNNING",
                            default_status_str, ingestion_name
                        )
                        default_status = DefaultScheduleStatus.RUNNING
                else:
                    default_status = DefaultScheduleStatus.RUNNING

                # Bind each schedule directly to its matching ingestion job.
                schedule_def = ScheduleDefinition(
                    name=schedule_name,
                    cron_schedule=cron_expr,
                    job_name=f"{ingestion_name}_job",
                    execution_timezone=execution_tz,
                    default_status=default_status,
                    run_config={},
                    tags={
                        "ingestion_name": ingestion_name,
                        "automated": "true",
                    },
                )

                self.schedules.append(schedule_def)

                if self.debug:
                    log.debug(
                        "Created schedule [%s] with cron expression [%s] for ingestion [%s]",
                        schedule_name, cron_expr, ingestion_name
                    )

    def get_dagster_schedules(self) -> List[ScheduleDefinition]:
        """
        Retrieve the list of built Dagster ScheduleDefinition objects.
        Returns:
            List[ScheduleDefinition]: A list of schedule definitions.
        """
        return self.schedules
