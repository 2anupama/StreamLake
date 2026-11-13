# StreamLake

StreamLake is the foundation for a local-first, production-style data ingestion
and analytics platform. It is being built incrementally; this initial version
provides project configuration, structured logging, and a runnable application
entry point without requiring cloud services.

## Requirements

- macOS
- Python 3.11
- Git

Docker Desktop will be used later for local PostgreSQL, Kafka, Debezium, and
Airflow services. Cloud integrations will remain optional.

## Setup

From the project root, create a virtual environment only if `.venv` does not
already exist:

```zsh
python3.11 -m venv .venv
```

Activate it and install the pinned foundation and development dependencies:

```zsh
source .venv/bin/activate
python --version
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Copy `.env.example` to `.env` and adjust local settings as needed. `.env` is
ignored by Git. Leave cloud settings blank for local development.

```zsh
cp .env.example .env
```

## Run and test

Run the initial test suite and start the application from the project root:

```zsh
python -m pytest
python -m streamlake
```

The application logs to the console and, by default, to `logs/streamlake.log`.
Set `ENABLE_FILE_LOGGING=false` to use console logging only.

### Verified results

Verified on 2026-11-13 from the project root using the project's `.venv`:

- `.venv/bin/python -m pytest`: **5 passed**.
- `.venv/bin/python -m streamlake`: exited successfully and logged startup in
  the `local` environment.
- An invalid file-log target falls back to console logging, and repeated
  logging configuration does not add duplicate handlers.

This phase does not yet include a data pipeline. Batch and streaming ingestion,
processing, storage, and quality modules are placeholders, so there is no
pipeline output to validate or pipeline-level idempotency/error handling to
claim. The executed application currently verifies startup only.

## Optional dependency groups

The default install intentionally avoids large platform dependencies. Install
only the groups needed for a development step:

```zsh
python -m pip install -e '.[batch,postgres]'
python -m pip install -e '.[kafka,spark,analytics]'
python -m pip install -e '.[gcp,dbt]'
```

Airflow is planned as a Docker service because installing it directly into a
general-purpose Python environment requires version-specific constraints.
GCP credentials are not stored in this repository; use Application Default
Credentials when cloud work is introduced.

## Project layout

```text
config/                  Deployment and application configuration
data/                    Local landing zone and medallion layer locations
  incoming/              Batch source files awaiting ingestion
  bronze/                Raw, append-oriented data
  silver/                Validated and conformed data
  gold/                  Analytics-ready datasets
  quarantine/            Rejected records for inspection
src/streamlake/          Installable application package
  ingestion/batch/       CSV, Excel, and REST ingestion adapters (future)
  ingestion/streaming/   Kafka and CDC ingestion adapters (future)
  processing/            Spark and transformation logic (future)
  storage/               Local and cloud storage adapters (future)
  quality/               Data validation checks (future)
  monitoring/            Metrics and health reporting (future)
  common/                Shared settings and logging
airflow/dags/             Workflow definitions
dbt/                      Analytical transformation project
spark/                    Spark jobs and configuration
scripts/                  Developer and operations utilities
tests/unit/               Fast isolated tests
tests/integration/        Service-backed tests
docker/                   Local service definitions and images
docs/                     Architecture and operational documentation
logs/                      Local application logs (ignored by Git)
```

## Configuration

`src/streamlake/common/settings.py` loads typed settings from environment
variables and an optional `.env` file. No credentials or paid cloud services
are needed for the foundation. The GCS and BigQuery settings are optional and
remain empty unless explicitly configured.