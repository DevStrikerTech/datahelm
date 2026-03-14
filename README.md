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

## Branching and CI/CD

- `dev` is the integration branch for day-to-day development.
- `master` is the production branch.
- Pull requests into `dev` and `master` run CI tests.
- Pull requests into `master` are expected to come from `dev`.
- Pushes to `master` trigger Docker image builds and publish to GHCR.

## Container image

- Dockerfile is included for production-style packaging.
- Default container command starts Dagster gRPC API:
  `python -m dagster api grpc -m dagster_op.repository`
