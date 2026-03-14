from typing import List

from dagster import DefaultScheduleStatus, ScheduleDefinition, get_dagster_logger

from analytics.dbt_factory import DbtProjectFactory

log = get_dagster_logger()


class DbtScheduleCreator:
    def __init__(self, dbt_config_dir: str, debug: bool = False):
        self.dbt_config_dir = dbt_config_dir
        self.debug = debug
        self.schedules: List[ScheduleDefinition] = []
        self._factory = DbtProjectFactory(config_dir=self.dbt_config_dir)
        self._build_schedules()

    def _build_schedules(self) -> None:
        for unit_key, unit_config in self._factory.units_map.items():
            if not unit_config.get("is_active", True):
                continue

            schedules = unit_config.get("schedules", [])
            if not schedules:
                continue

            for idx, schedule_block in enumerate(schedules, start=1):
                if isinstance(schedule_block, list):
                    schedule_block = schedule_block[0] if schedule_block else None
                if not isinstance(schedule_block, dict):
                    log.warning("Invalid dbt schedule block for unit: %s", unit_key)
                    continue

                cron_expr = schedule_block.get("cron_schedule")
                if not cron_expr:
                    log.warning("Skipping dbt schedule for %s due to missing cron_schedule", unit_key)
                    continue

                execution_tz = schedule_block.get("execution_timezone", "UTC")
                default_status_raw = str(schedule_block.get("default_status", "RUNNING")).upper()
                default_status = (
                    DefaultScheduleStatus.RUNNING
                    if default_status_raw != "STOPPED"
                    else DefaultScheduleStatus.STOPPED
                )

                schedule_name = f"{unit_key}_dbt_schedule_{idx}"
                dbt_command = str(schedule_block.get("dbt_command", unit_config.get("dbt_command", "build")))

                schedule_def = ScheduleDefinition(
                    name=schedule_name,
                    cron_schedule=cron_expr,
                    job_name=f"{unit_key}_dbt_job",
                    execution_timezone=execution_tz,
                    default_status=default_status,
                    run_config={
                        "ops": {
                            "op_run_dbt_project": {
                                "config": {
                                    "unit_key": unit_key,
                                    "config_dir": self.dbt_config_dir,
                                    "dbt_command": dbt_command,
                                }
                            }
                        }
                    },
                    tags={
                        "pipeline_layer": "dbt",
                        "dbt_unit": unit_key,
                        "dbt_source": str(unit_config.get("source_name", "")),
                    },
                )
                self.schedules.append(schedule_def)

                if self.debug:
                    log.info("Created dbt schedule '%s' for unit '%s'", schedule_name, unit_key)

    def get_dagster_schedules(self) -> List[ScheduleDefinition]:
        return self.schedules
