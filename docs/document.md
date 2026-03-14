# DataHelm Detailed Documentation

This document is the single reference for local development, architecture, configuration, CI/CD, and operations for this repository.

## 1) Purpose

DataHelm provides a configurable data platform skeleton for:

- source ingestion orchestration
- dbt-based transformations
- notebook-driven dashboard jobs
- reusable source connector handlers
- optional local-LLM analytics query capabilities

The design goal is rapid onboarding of new data sources while reusing shared orchestration and connector patterns.

## 2) Core Architecture

The project has four primary layers:

- `ingestion/` - source extraction and publish logic
- `analytics/` - dbt orchestration, dashboard helpers, and optional NL-to-SQL module
- `dagster_op/` - jobs, schedules, sensors, and repository registration
- `config/` - YAML-driven source/dbt/dashboard/runtime metadata

Execution is orchestrated by Dagster. Ingestion writes raw data to Postgres, dbt builds transformed models, and dashboard jobs generate notebook outputs.

## 3) Directory Guide

- `config/api/` - ingestion source configs
- `config/dbt/` - dbt project and unit config
- `config/dashboard/` - dashboard unit config
- `config/analytics/` - semantic catalog for optional NL query workflows
- `handlers/` - provider/source handlers (`api`, `sharepoint`, `gcs`, `s3`, `bigquery`)
- `ingestion/native_ingestions/` - ingestion implementations
- `analytics/dbt_projects/` - dbt project definitions
- `analytics/notebooks/` - Dagstermill notebooks
- `scripts/` - local utility scripts
- `tests/` - unit test suite
- `docs/` - documentation (this file)

## 4) Local Development Setup

### Prerequisites

- Python 3.12+
- PostgreSQL instance reachable from your machine
- Optional: dbt CLI, Docker, and local Ollama runtime

### Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .
```

### Environment Variables

Create a `.env` file in repo root with at least:

- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASSWORD`
- `DB_NAME`
- `CLASHOFCLANS_API_TOKEN` (for current API example)

Optional examples:

- `DAGSTER_HOME`
- `DAGSTER_HOME_DIR`
- `DBT_TARGET`
- `DBT_SCHEMA`

### Run Dagster Locally

```bash
python scripts/run_dagster_dev.py
```

Use `--print-only` to inspect resolved paths/command without launching:

```bash
python scripts/run_dagster_dev.py --print-only
```

## 5) Configuration Model

YAML configs are designed for shared defaults and per-unit overrides using OmegaConf interpolation.

### Ingestion (`config/api/*.yaml`)

Defines:

- `ingest_type`
- extraction init parameters
- extraction runtime params
- publish target and table mapping
- schedules

### dbt (`config/dbt/projects.yaml`)

Defines:

- source/project directory mappings
- profile/target settings
- unit-level select/exclude/vars
- schedules

### Dashboard (`config/dashboard/projects.yaml`)

Defines:

- notebook path
- source table metadata
- chart fields and row limits
- schedules

### Semantic Catalog (`config/analytics/semantic_catalog.yaml`)

Defines metadata for optional NL-to-SQL generation:

- dataset registry
- table names
- dimensions/metrics
- business synonyms
- global query rules

## 6) Reusable Source Connectors

The project includes reusable connectors so new sources avoid repeated auth and IO boilerplate.

- `handlers/sharepoint/sharepoint.py`
  - Graph auth, site resolution, file download, folder listing
- `handlers/gcs/gcs.py`
  - upload/download/list/delete/signed-url helpers
- `handlers/s3/s3.py`
  - upload/download/list/delete/presigned-url helpers
- `handlers/bigquery/bigquery.py`
  - query execution, table reads, dataframe load, schema helper

These connectors are intentionally generic so ingestion implementations can focus on parsing and data contracts.

## 7) Optional Local LLM Analytics Module

`analytics/nl_query/` provides an isolated local-Ollama NL-to-SQL scaffold.

Includes:

- semantic catalog loader
- SQL safety guardrails (read-only and bounded queries)
- minimal Ollama client
- NL query orchestration service

Important:

- this module is optional and not wired into existing ingestion flow by default
- no production behavior changes unless explicitly integrated

## 8) Testing

Run full test suite:

```bash
.venv/bin/python -m pytest -q
```

Coverage currently includes:

- handler logic and edge cases
- ingestion factory and native run paths
- base ingestion helper branches
- analytics dbt runner and factory behavior
- script/bootstrap behavior
- connector modules (SharePoint, GCS, S3, BigQuery)
- NL query scaffold modules

## 9) CI/CD and Branch Strategy

Branching:

- `dev` = integration branch
- `master` = release/prod branch

Workflows:

- `CI` validates tests for development flow
- `Docker Release` builds/pushes image on `master`
- `Deploy Release` supports auto/manual deployment paths

Deployment uses SSH + GHCR pull model and gracefully skips remote deployment when secrets are not yet configured.

## 10) Required Secrets for Deployment

Shared:

- `GHCR_USERNAME`
- `GHCR_READ_TOKEN`

Staging:

- `STAGING_SSH_HOST`
- `STAGING_SSH_USER`
- `STAGING_SSH_KEY`
- `STAGING_APP_ENV_FILE`

Production:

- `PROD_SSH_HOST`
- `PROD_SSH_USER`
- `PROD_SSH_KEY`
- `PROD_APP_ENV_FILE`

## 11) Common Operations

### Run tests

```bash
.venv/bin/python -m pytest -q
```

### Start local Dagster

```bash
python scripts/run_dagster_dev.py
```

### Build Docker image locally

```bash
docker build -t datahelm:local .
```
