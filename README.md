# datahelm_eng_core

DataHelm engineering core for ingestion, transformation, and lightweight dashboard orchestration.

## What this repo contains

- Config-driven API ingestion flows
- Dagster jobs, schedules, and sensors
- dbt project + runners for transformation units
- Dagstermill dashboard notebook execution

## Quick start

1. Create and activate a virtual environment.
2. Install the package in editable mode:
   `pip install -e .`
3. Create `.env` from `.env.example` and fill credentials.
4. Start Dagster locally:
   `python scripts/run_dagster_dev.py`

## Project layout

- `config/` for ingestion, dbt, and dashboard unit configuration
- `ingestion/` for ingestion factory and source implementations
- `dagster_op/` for jobs, schedules, repository wiring
- `analytics/` for dbt assets, notebook assets, and analytics helpers
