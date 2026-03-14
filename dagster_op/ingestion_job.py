from typing import Optional, Dict, List

from dagster import op, job, ConfigMapping, get_dagster_logger, JobDefinition
from ingestion.ingestion_factory import IngestionFactory

log = get_dagster_logger()


@op(config_schema={"ingestion_name": str, "config_dir": str})
def op_run_ingestion(context) -> None:
    """
    Dagster op that runs a single ingestion based on its configuration.

    The op expects its configuration to include:
      - ingestion_name: The key corresponding to the ingestion YAML sub-block.
      - config_dir: The directory where the ingestion YAML files are located.

    It creates an ingestion object via the IngestionFactory and calls its run() method.
    """
    ingestion_name: str = context.op_config["ingestion_name"]
    config_dir: str = context.op_config["config_dir"]

    log.info(f"[op_run_ingestion] Starting ingestion '{ingestion_name}' using config '{config_dir}'...")
    factory = IngestionFactory(config_dir=config_dir)
    ingestion_obj = factory.create_ingestion(ingestion_name)
    ingestion_obj.run()
    log.info(f"[op_run_ingestion] Completed ingestion '{ingestion_name}'.")


def _create_single_ingestion_job(ingestion_name: str, config_dir: str) -> JobDefinition:
    """
    Creates a Dagster JobDefinition for a single ingestion.

    The job consists of the op_run_ingestion op with a config mapping that injects
    the ingestion name and config directory.

    Args:
        ingestion_name (str): The name (key) of the ingestion as defined in YAML.
        config_dir (str): The directory containing ingestion YAML config files.

    Returns:
        JobDefinition: A Dagster job that runs the specified ingestion.
    """
    def config_fn(_job_config: dict) -> dict:
        return {
            "ops": {
                "op_run_ingestion": {
                    "config": {
                        "ingestion_name": ingestion_name,
                        "config_dir": config_dir,
                    }
                }
            }
        }

    config_mapping = ConfigMapping(
        config_schema={},  # No additional user-supplied config required.
        receive_processed_config_values=False,
        config_fn=config_fn,
    )

    @job(
        name=f"{ingestion_name}_job",
        config=config_mapping,
        description=f"Job that runs ingestion '{ingestion_name}' from YAML config in '{config_dir}'."
    )
    def _ingestion_job():
        op_run_ingestion()

    return _ingestion_job


class IngestionJobCreator:
    """
    Creates multiple ingestion jobs based on ingestion YAML configurations.
    The IngestionFactory reads all sub-blocks from the provided configuration directory,
    and this class builds a Dagster job for each ingestion.
    """

    def __init__(self, config_dir: str, target_filter: Optional[str] = None):
        """
        Initialize the job creator.

        Args:
            config_dir (str): Path to the YAML ingestion configuration directory (e.g. "config/api").
            target_filter (Optional[str]): An optional string to filter ingestion names.
        """
        self.config_dir = config_dir
        self.target_filter = target_filter

    def create_jobs(self) -> Dict[str, JobDefinition]:
        """
        Creates a dictionary of ingestion jobs.

        Returns:
            Dict[str, JobDefinition]: A mapping from job name to JobDefinition.
        """
        factory = IngestionFactory(config_dir=self.config_dir)
        # The factory is expected to populate an attribute `ingestions_map`
        # where keys are ingestion names (e.g., "CLASHOFCLANS_PLAYER_STATS") and
        # values are ingestion objects.
        ingestion_map = factory.ingestions_map
        jobs_dict: Dict[str, JobDefinition] = {}

        for ingestion_name in ingestion_map.keys():
            if self.target_filter and self.target_filter not in ingestion_name:
                continue

            job_def = _create_single_ingestion_job(ingestion_name, self.config_dir)
            jobs_dict[job_def.name] = job_def

        return jobs_dict


def create_all_ingestion_jobs(config_dir: str, target_filter: Optional[str] = None) -> List[JobDefinition]:
    """
    Convenience function to create all ingestion jobs.

    Args:
        config_dir (str): The directory where ingestion YAML files are located.
        target_filter (Optional[str]): Optional filter to include only certain ingestion names.

    Returns:
        List[JobDefinition]: A list of Dagster JobDefinition objects.
    """
    job_creator = IngestionJobCreator(config_dir=config_dir, target_filter=target_filter)
    jobs_dict = job_creator.create_jobs()

    return list(jobs_dict.values())
