# DataHelm

DataHelm is a data engineering framework focused on:

- Source ingestion and orchestration
- dbt transformation workflows
- Notebook-based dashboard execution
- Reusable provider connectors (SharePoint, GCS, S3, BigQuery)
- Optional local LLM analytics query scaffolding

## Table of Contents


- [Core Capabilities](#core-capabilities)
- [High-Level Architecture](#high-level-architecture)
- [Repository Structure](#repository-structure)
- [Local Setup](#local-setup)
- [Configuration Model](#configuration-model)
- [Reusable Connectors](#reusable-connectors)
- [Local LLM Analytics Module](#local-llm-analytics-module)
- [Testing](#testing)
- [CI/CD and Branching](#cicd-and-branching)
- [Containerization](#containerization)
- [Deployment](#deployment)
- [Contributing and Governance](#contributing-and-governance)
- [Detailed Technical Documentation](#detailed-technical-documentation)

## Core Capabilities


- **Config-driven ingestion** using YAML in `config/api/`
- **Dagster orchestration** for jobs, schedules, and sensors
- **dbt project execution** through `analytics/dbt_runner.py` and dbt configs
- **Dashboard generation** with Dagstermill notebooks
- **Reusable handlers/connectors** for multiple external providers
- **Optional NL-to-SQL module** (`analytics/nl_query/`) for local Ollama-based analytics workflows

## High-Level Architecture


The repository follows layered responsibilities:

- `handlers/`: provider-specific source connectors and API handlers
- `ingestion/`: ingestion factory and native ingestion implementations
- `analytics/`: dbt, dashboard, and optional NL-query modules
- `dagster_op/`: orchestration objects (jobs, schedules, repository)
- `config/`: all runtime configuration (API, dbt, dashboard, analytics metadata)
- `tests/`: unit tests for handlers, ingestion, analytics, and scripts

![alt text](https://github.com/DevStrikerTech/datahelm/blob/master/docs/architecture.png?raw=true)

## Repository Structure


```text
dagster_op/
ingestion/
tests/
scripts/
docs/
config/
  api/
  dbt/
  dashboard/
  analytics/
analytics/
  dbt_projects/
  notebooks/
  nl_query/
dagster_op/
handlers/
  api/
  sharepoint/
  gcs/
  s3/
  bigquery/
ingestion/
tests/
scripts/
docs/
```

## Local Setup


### Prerequisites

- Python 3.12+
- PostgreSQL (accessible from the local environment)
- Optional: Docker, local Ollama, dbt CLI

### Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .
```

### Environment Variables

Create a `.env` file in the repository root with required values, for example:

```env
DB_HOST=${DB_HOST}
DB_PORT=${DB_PORT}
DB_USER=${DB_USER}
DB_PASSWORD=${DB_PASSWORD}
DB_NAME=${DB_NAME}
CLASHOFCLANS_API_TOKEN=${CLASHOFCLANS_API_TOKEN}
```

### Run Dagster Locally

```bash
python scripts/run_dagster_dev.py
```

Useful option:

```bash
python scripts/run_dagster_dev.py --print-only
```

## Configuration Model


### Ingestion Config (`config/api/*.yaml`)

Defines source-level extraction, publish targets, schedules, and column mapping.

Example currently included:

- `CLASHOFCLANS_PLAYER_STATS`

### dbt Config (`config/dbt/projects.yaml`)

Defines dbt units, selection/exclusion rules, variables, and schedules.

### Dashboard Config (`config/dashboard/projects.yaml`)

Defines notebook path, source table mapping, chart columns, and cadence.

### Analytics Semantic Config (`config/analytics/semantic_catalog.yaml`)

Defines dataset metadata for the isolated NL-to-SQL module.

## Reusable Connectors


The repository includes reusable connector classes under `handlers/`:

- `handlers/sharepoint/sharepoint.py`
  - Microsoft Graph authentication and site/file access helpers
- `handlers/gcs/gcs.py`
  - Upload/download/list/delete/signed URL helpers
- `handlers/s3/s3.py`
  - Upload/download/list/delete/presigned URL helpers
- `handlers/bigquery/bigquery.py`
  - Query, row fetch, dataframe load, schema helpers

## Local LLM Analytics Module


`analytics/nl_query/` is an isolated module for natural-language-to-SQL generation using local Ollama:

- Semantic catalog loader
- SQL read-only safety guard
- Ollama client wrapper
- Orchestration service

## Testing


Run all tests:

```bash
.venv/bin/python -m pytest -q
```

The current test suite covers:

- Ingestion and handler behavior
- Analytics factory and runner logic
- Connector modules (SharePoint, GCS, S3, BigQuery)
- Script behavior
- NL-query safety and service paths

## CI/CD and Branching


- `dev`: integration branch
- `master`: release/production branch

Workflows:

- **CI**: tests on development and PR flows
- **Docker Release**: image build/publish on `master`
- **Deploy Release**: workflow_run/manual deployment orchestration

## Containerization


The container image is defined via `Dockerfile`.

The default runtime command starts Dagster gRPC:

```bash
python -m dagster api grpc -m dagster_op.repository
```

## Deployment


Deployment flow is workflow-based:

- Production auto-path after successful Docker release
- Manual staging/production dispatch path

## Contributing and Governance


- Contribution guide: [`CONTRIBUTING.md`](CONTRIBUTING.md)
- Code of conduct: [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md)
- Security reporting: [`SECURITY.md`](SECURITY.md)

## Detailed Technical Documentation


For complete, long-form project documentation (operations, architecture, and runbook-style details), see:

- [`docs/document.md`](docs/document.md)
