from typing import Optional

from dagster import (
    sensor,
    SensorEvaluationContext,
    RunRequest,
    SkipReason,
    DefaultSensorStatus,
    get_dagster_logger,
)

from ingestion.ingestion_factory import IngestionFactory

log = get_dagster_logger()
INGESTION_TARGET = "CLASHOFCLANS_PLAYER_STATS"


@sensor(
    name="ingestion_sensor",
    job_name=f"{INGESTION_TARGET}_job",
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=300,
)
def ingestion_sensor(context: SensorEvaluationContext) -> Optional[RunRequest]:
    """
    Sensor that evaluates whether new data is available for an ingestion job.

    This sensor loads the ingestion configuration for a specific ingestion target
    (in this example, "CLASH_OF_CLANS_PLAYER") using the IngestionFactory. It then
    calls the ingestion’s extract() method in incremental mode. If data is returned,
    a RunRequest is yielded to trigger the corresponding Dagster job; otherwise, a
    SkipReason is returned.

    Returns:
        RunRequest if new data is available; otherwise, SkipReason.
    """
    # Define the ingestion target key as defined in your YAML config.
    ingestion_target = INGESTION_TARGET  # Must match the YAML sub-block key

    # Create the ingestion factory instance. The factory reads YAML config files
    # from the specified directory (here, "config/api"). The factory automatically
    # resolves environment variables.
    try:
        factory = IngestionFactory(
            config_dir="config/api"
        )
        ingestion = factory.create_ingestion(ingestion_name=ingestion_target)
    except Exception as e:
        log.error("Error creating ingestion for target %s: %s", ingestion_target, e)
        return SkipReason(f"Error creating ingestion: {e}")

    # Retrieve the last watermark (last_mtime) from persistent storage.
    # In a production system, this should be read from the database.
    # For this example, we set it to None.
    last_mtime = None

    try:
        # Prefer ingestion.extract() when available; otherwise use the native handler.
        if hasattr(ingestion, "extract"):
            data_generator = ingestion.extract(
                extract_mode="INCR",
                last_mtime_override=last_mtime,
            )
        else:
            config = getattr(ingestion, "config", {})
            extract_init = config.get("extract_init", {})
            token_env_var = extract_init.get("token_env_var")
            extract_params = config.get("extract_params", {})
            handler_class = getattr(ingestion, "handler_class", None)

            if not token_env_var or handler_class is None:
                return SkipReason(
                    f"Ingestion '{ingestion_target}' does not expose sensor-check extract support."
                )

            handler = handler_class(token_env_var)
            data_generator = handler.get_data_iter(
                last_mtime=last_mtime,
                **extract_params,
            )
        # Retrieve the first batch from the generator (if any).
        first_batch = next(data_generator, None)

        if first_batch:
            log.info("New data detected for ingestion target: %s", ingestion_target)
            return RunRequest(
                run_key=None,  # Let Dagster auto-generate a unique run key.
                run_config={},  # Additional run config can be passed here.
                job_name=f"{ingestion_target}_job",
                tags={
                    "ingestion_target": ingestion_target,
                    "new_data": "true",
                },
            )
        else:
            log.info("No new data available for ingestion target: %s", ingestion_target)
            return SkipReason("No new data available")
    except Exception as ex:
        log.error("Error during sensor evaluation for %s: %s", ingestion_target, ex)
        return SkipReason(f"Sensor error: {ex}")
