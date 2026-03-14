# Local Development

## Prerequisites

- Python 3.12+
- PostgreSQL reachable from your machine
- dbt and Dagster dependencies installed via `pip install -e .`

## Environment

Copy `.env.example` to `.env` and set all values before running jobs.

## Run Dagster

Use the helper script to ensure a stable `DAGSTER_HOME`:

`python scripts/run_dagster_dev.py`

## Common checks

- Run tests: `pytest`
- Validate imports and packaging: `python -m pip check`
