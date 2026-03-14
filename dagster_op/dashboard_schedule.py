from typing import List

from dagster import DefaultScheduleStatus, ScheduleDefinition, get_dagster_logger

from analytics.dashboard_factory import DashboardFactory

log = get_dagster_logger()


class DashboardScheduleCreator:
    def __init__(self, dashboard_config_dir: str, debug: bool = False):
        self.dashboard_config_dir = dashboard_config_dir
        self.debug = debug
        self.schedules: List[ScheduleDefinition] = []
        self._factory = DashboardFactory(config_dir=self.dashboard_config_dir)
        self._build_schedules()

    def _build_schedules(self) -> None:
        for unit_key, unit_config in self._factory.units_map.items():
            if not unit_config.get("is_active", True):
                continue

            schedules = unit_config.get("schedules", [])
            if not schedules:
                continue

            op_name = f"{unit_key.lower()}_dashboard_op"

            for idx, schedule_block in enumerate(schedules, start=1):
                if isinstance(schedule_block, list):
                    schedule_block = schedule_block[0] if schedule_block else None
                if not isinstance(schedule_block, dict):
                    log.warning("Invalid dashboard schedule block for unit: %s", unit_key)
                    continue

                cron_expr = schedule_block.get("cron_schedule")
                if not cron_expr:
                    log.warning("Skipping dashboard schedule for %s due to missing cron_schedule", unit_key)
                    continue

                execution_tz = schedule_block.get("execution_timezone", "UTC")
                default_status_raw = str(schedule_block.get("default_status", "RUNNING")).upper()
                default_status = (
                    DefaultScheduleStatus.RUNNING
                    if default_status_raw != "STOPPED"
                    else DefaultScheduleStatus.STOPPED
                )

                schedule_name = f"{unit_key}_dashboard_schedule_{idx}"

                schedule_def = ScheduleDefinition(
                    name=schedule_name,
                    cron_schedule=cron_expr,
                    job_name=f"{unit_key}_dashboard_job",
                    execution_timezone=execution_tz,
                    default_status=default_status,
                    run_config={
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
                    },
                    tags={
                        "pipeline_layer": "dashboard",
                        "dashboard_unit": unit_key,
                    },
                )
                self.schedules.append(schedule_def)

    def get_dagster_schedules(self) -> List[ScheduleDefinition]:
        return self.schedules
