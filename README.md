# StreamLake

StreamLake is a local-first employee benefits data platform. Phase 1 implements
synthetic source generation and a chunked batch pipeline that writes raw Bronze,
validated Silver, business-ready Gold, and rejected Quarantine Parquet data.
DuckDB maintains batch history and the current employee/dependent state. Local
execution does not require Docker, a cloud account, or GCP credentials.

## Requirements

- macOS
- Python 3.11
- Git (optional for running the project)

## Setup

From the project root, create and activate a virtual environment, then install
the pinned local, batch, analytics, and test dependencies:

```zsh
python3.11 -m venv .venv
source .venv/bin/activate
python --version
python -m pip install -r requirements.txt
```

Optionally copy `.env.example` to `.env` and adjust local paths or chunk size.
The file is ignored by Git; cloud settings can remain blank.

```zsh
cp .env.example .env
```

## Architecture

```text
CSV/XLS/XLSX files in data/incoming
  -> chunked reader + schema checks + source fingerprint
  -> Bronze Parquet (source values plus ingestion metadata)
  -> cleaning and validation
       -> Silver event Parquet + DuckDB Silver state
       -> Quarantine Parquet for invalid or malformed rows
  -> Gold Parquet snapshots + DuckDB batch history
```

`data/bronze/`, `data/silver/`, `data/gold/`, and `data/quarantine/` use local
Parquet. Bronze and event/quarantine files are partitioned by
`ingestion_date`, `source_type`, and `batch_id`. Current Silver state and batch
history are in `data/streamlake.duckdb`; Gold tables are refreshed from that
state and exported to Parquet after each successful file.

The standard additions and updates schema contains `employee_id`,
`employee_name`, `email`, `mobile_number`, `gender`, `date_of_birth`,
`joining_date`, `company_id`, `company_name`, `subsidiary`, `department`,
`designation`, `sum_insured`, `policy_id`, `relationship`, `record_status`,
`created_at`, and `updated_at`. Deletion files require `employee_id`; dependent
files require `employee_id`, `dependent_id`, `dependent_name`, `gender`,
`date_of_birth`, `relationship`, and `record_status`.

## Run

Generate at least 10,000 unique employee inputs plus deliberately invalid and
duplicate rows, then run all recognized files in additions, updates,
dependents, deletions order:

```zsh
python -m streamlake generate-data --employees 10000 --seed 42
python -m streamlake validate
python -m streamlake run-batch
python -m streamlake report
```

The requested `python -m src.main ...` form is also supported from the project
root. For example, ingest a single source file or validate an alternate folder:

```zsh
python -m src.main ingest --file data/incoming/employees_additions.csv
python -m src.main validate --input-dir data/incoming
```

`generate-data` accepts `--output-dir`, `--employees`, and `--seed`. `ingest`
accepts `--file`, optional `--source-type` (`additions`, `updates`,
`deletions`, or `dependents`), and optional `--source-system`. `run-batch` and
`validate` accept `--input-dir`. The source type is otherwise inferred from
conventional filenames. `BATCH_CHUNK_SIZE` defaults to 2,000 rows. `validate`
is read-only and returns a nonzero exit code if any row or schema is invalid.

## Data Quality And Idempotency

- Silver trims text, lowercases and checks email, normalizes US/international
  phone numbers, converts dates to `YYYY-MM-DD`, and validates mandatory fields,
  company IDs, employee/dependent references, positive whole-number coverage,
  source duplicate IDs, and record status.
- Bronze keeps source business values unchanged and records
  `ingestion_timestamp`, `batch_id`, `source_file`, `source_system`,
  `source_type`, and `ingestion_status`.
- Quarantine retains the original parsed row as JSON, its error reason, batch
  and source metadata, and `processing_timestamp`. Malformed over-wide CSV rows
  are preserved in both Bronze malformed output and Quarantine.
- A SHA-256 fingerprint of file bytes, source type, and source system prevents
  completed content from being loaded twice. A replay is recorded as
  `skipped_duplicate` with a `duplicate_of` batch ID; the DuckDB batch history
  and JSON Lines audit log are retained.
- Local storage and DuckDB are the executed path. Optional GCS and BigQuery
  adapters are available via the `gcp` extra; they are not initialized during
  local runs.

## Outputs

- `gold/gold_employee_master.parquet`
- `gold/gold_employee_dependents.parquet`
- `gold/gold_company_employee_summary.parquet`
- `gold/gold_policy_summary.parquet`
- `logs/ingestion_audit.jsonl`
- `data/streamlake.duckdb` (`batch_history`, Silver state, and Gold tables)

## Test And Verify

Run the full test suite and check dependency consistency:

```zsh
python -m pytest
python -m pip check
```

## Full Sample Run

Executed with Python 3.11.15 using `--employees 10000 --seed 42`:

| Source file | Rows ingested | Processed/loaded | Rejected |
| --- | ---: | ---: | ---: |
| `employees_additions.csv` | 10,005 | 10,000 | 5 |
| `employees_updates.csv` | 500 | 500 | 0 |
| `employee_dependents.csv` | 3,002 | 3,000 | 2 |
| `employees_deletions.csv` | 100 | 100 | 0 |
| **Total** | **13,607** | **13,600** | **7** |

The seven quarantined rows were one duplicate employee ID, one missing email,
one invalid birth date, one missing company ID, one invalid sum-insured value,
one duplicate dependent ID, and one dependent referencing an unknown employee.
The output checks found 9,900 unique active Gold employees, 2,970 Gold
dependents linked to active employees, five company summary rows, 20 policy
summary rows, and seven quarantine records. All Gold employee emails were
lowercase.

Representative values from the generated Parquet outputs:

| Layer | Example |
| --- | --- |
| Bronze | `EMP-0006001`; source `employees_additions.csv`; status `ingested`; business values retained as received. |
| Silver | `EMP-0000021`; email `corrected.21@c002.example`; coverage `500000`; policy `POL-200`. |
| Gold employee | `EMP-0000021`; active, with standardized email/date/phone values. |
| Gold dependent | `DEP-00000001`; linked to active employee `EMP-0000001`; relationship `Parent`. |
| Quarantine | `EMP-0000001`; reason `Duplicate employee_id in source file`; original row retained as JSON. |

Re-running `run-batch` on the same files created four `skipped_duplicate`
history entries, linked to the original batches, and loaded **0** additional
rows. `python -m pytest` completed with **13 passed**; `python -m pip check`
reported no broken requirements.

## Layout

```text
src/streamlake/ingestion/batch/  Generator, readers, validation, CLI, pipeline
src/streamlake/storage/         Local Parquet and optional cloud adapters
data/incoming/                  Generated or client-provided source files
data/bronze/                    Raw source chunks and ingestion metadata
data/silver/                    Validated events and current DuckDB state
data/gold/                      Employee, dependent, company, policy outputs
data/quarantine/                Invalid and malformed rows with error reasons
logs/                           Application log and JSONL ingestion audit
tests/unit/                     Validation and layer behavior tests
```

## Phase 5: GCP Storage And BigQuery

Local remains the default and continues to use filesystem Parquet and DuckDB.
The GCP adapters are created only when selected. Batch Parquet output uses the
storage adapter; the warehouse module exposes the managed BigQuery datasets,
table schemas, load, incremental MERGE, and query operations for cloud batch
jobs. No cloud service is started or accessed by the default local workflow.

Install the optional client libraries:

```zsh
python -m pip install -e '.[gcp]'
```

### Create a GCP project and resources

Create or select your own project in the Google Cloud Console, enable billing
for that project, and set its ID and a supported region. The commands below do
not assume a project or bucket already exists:

```zsh
export GCP_PROJECT_ID="your-project-id"
export GCP_REGION="us-central1"
export GCS_BUCKET="your-globally-unique-streamlake-bucket"
gcloud config set project "$GCP_PROJECT_ID"
gcloud services enable storage.googleapis.com bigquery.googleapis.com --project "$GCP_PROJECT_ID"
gcloud storage buckets create "gs://$GCS_BUCKET" \
  --project="$GCP_PROJECT_ID" --location="$GCP_REGION" \
  --uniform-bucket-level-access
```

The runtime identity needs `roles/storage.objectAdmin` on the bucket and
`roles/bigquery.dataEditor` plus `roles/bigquery.jobUser` on the project. The
identity that provisions datasets also needs `roles/bigquery.user` or equivalent
dataset creation permission. Prefer user ADC for local development:

```zsh
gcloud auth application-default login
gcloud auth application-default set-quota-project "$GCP_PROJECT_ID"
```

For a service account, set `GOOGLE_APPLICATION_CREDENTIALS` to a local key file
path only when ADC is not available. Never commit the key; `.env` is ignored by
Git. Workload identity or attached service accounts are preferred outside local
development.

### Select cloud backends

Copy `.env.example` to `.env` and set:

```dotenv
STORAGE_BACKEND=gcp
WAREHOUSE_BACKEND=bigquery
GCP_PROJECT_ID=your-project-id
GCP_REGION=us-central1
GCS_BUCKET=your-globally-unique-streamlake-bucket
BIGQUERY_DATASET=streamlake_silver
GOOGLE_APPLICATION_CREDENTIALS=
```

The GCS layout uses date and batch partitions where applicable:

```text
gs://<bucket>/bronze/{employees,dependents,policies}/ingestion_date=YYYY-MM-DD/...
gs://<bucket>/silver/{employees,dependents,policies}/ingestion_date=YYYY-MM-DD/...
gs://<bucket>/gold/{dimensions,facts,marts}/...
gs://<bucket>/{quarantine,checkpoints,audit}/...
```

The local batch pipeline creates the storage adapter from `STORAGE_BACKEND` and
retains DuckDB for its transaction processing. BigQuery operations are available
through `streamlake.warehouse.bigquery.BigQueryWarehouse`; create datasets and
managed Bronze/Silver/Gold/Audit tables on demand:

```python
from streamlake.common.settings import Settings
from streamlake.warehouse import create_warehouse

settings = Settings()
warehouse = create_warehouse(settings)  # BigQueryWarehouse when configured
warehouse.ensure_model_tables()
warehouse.load_file(
    "gs://your-bucket/bronze/employees/ingestion_date=2026-01-01/part.parquet",
    "streamlake_bronze", "raw_employees", file_format="PARQUET",
)
```

`load_file` supports local CSV/Parquet and GCS URI inputs. `load_incremental`
loads a bounded file to a staging table and runs a key-based BigQuery `MERGE`.
Use `query` with BigQuery query parameters for values; identifiers are validated
and are not interpolated as values. Jobs are awaited and return job ID, state,
row count, and errors; `get_job` can inspect a known job ID. BigQuery tables are
date-partitioned and clustered where the model has an event/ingestion date and
high-use keys.

Example analytical query using the SDK:

```python
from google.cloud import bigquery
from streamlake.common.settings import Settings
from streamlake.warehouse import create_warehouse

warehouse = create_warehouse(Settings())
rows = warehouse.query(
    "SELECT company_id, employee_count FROM `your-project-id.streamlake_gold.company_employee_summary` WHERE as_of_date >= @cutoff",
    [bigquery.ScalarQueryParameter("cutoff", "DATE", "2026-01-01")],
)
```

### Cost and cleanup

GCS storage, operations, and network egress can incur charges. BigQuery storage,
load jobs, and bytes scanned by queries can incur charges. Use a development
bucket, bounded loads, date filters on partitioned tables, and dry-run/query
cost checks. No always-running cloud compute is required. Inspect resources and
remove only the resources created for this project when finished:

```zsh
gcloud storage rm --recursive "gs://$GCS_BUCKET/**"
gcloud storage buckets delete "gs://$GCS_BUCKET"
for dataset in streamlake_bronze streamlake_silver streamlake_gold streamlake_audit; do
  bq --project_id="$GCP_PROJECT_ID" rm -r -f "$dataset"
done
```

Cloud integration tests are opt-in and require real ADC, project APIs, IAM
permissions, and a test bucket/project. Unit tests use SDK fakes and run without
GCP authentication. No authenticated GCP integration test has been run, so this
documentation and the mocked adapter tests do not claim live cloud verification.

## Phase 2: PostgreSQL CDC Streaming

Phase 2 adds PostgreSQL logical replication, Debezium PostgreSQL CDC, Kafka,
and a separate PySpark Structured Streaming consumer. The local stack is
optional; Phase 1 remains usable without Docker or Spark.

### Verified Version Matrix

| Component | Pin | Compatibility evidence |
| --- | --- | --- |
| PostgreSQL | 16.4 | Debezium 3.2 tested with PostgreSQL 12 through 17 and `pgoutput`. |
| Kafka broker | Apache Kafka 4.0.1 | One-node KRaft configuration; replication factor 1. |
| Kafka Connect / Debezium | `quay.io/debezium/connect:3.2` pinned by OCI digest in Compose; live connector reported **3.2.4.Final** | Debezium 3.2 supports Kafka Connect 3.1+; the live Debezium source metadata reported 3.2.4.Final. |
| Spark | Apache Spark 4.0.1 | Requires Java 17 and is built for Scala 2.13. |
| Spark Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1` | Exact artifact/version from Spark 4.0.1’s Structured Streaming Kafka guide. |

Spark Kafka integration is deliberately not substituted with a Python Kafka
consumer. The `streaming` extra pins PySpark 4.0.1 and psycopg 3.2.10 for
host-side execution; the optional Compose `streaming` profile supplies Spark
4.0.1 in a container so local use does not need the large PySpark wheel.

Debezium PostgreSQL uses `pgoutput`, a stable replication slot and a
pre-created publication. PostgreSQL schema history is not stored in a separate
history topic for this connector; event schemas are carried in the JSON value
converter envelope. The connector uses `snapshot.mode=initial`, includes
schema-bearing values, preserves before/after/source/transaction fields, and
routes table topics to `employee_changes`, `dependent_changes`,
`policy_changes`, and `company_changes`. Delete events are followed by Kafka
tombstones. The consumer keeps the delete operation as a Silver
`record_status=deleted` state rather than treating the tombstone as the delete.

### Data Flow And Recovery

```text
PostgreSQL WAL (logical, pgoutput)
  -> Debezium PostgreSQL connector / Kafka Connect offsets
  -> employee_changes, dependent_changes, policy_changes, company_changes
  -> Spark Structured Streaming (Kafka source + durable checkpoint)
       -> Bronze Parquet: untouched key/value envelope, schema, metadata, offsets
       -> Kafka employee_dead_letter + Quarantine Parquet: malformed/schema errors
       -> DuckDB event ledger: topic/partition/offset event ID dedupe
       -> Silver employee_current: LSN, event timestamp, transaction order,
          partition and offset guard against stale updates
```

Bronze stores the raw Kafka key and value (including the original Debezium
envelope), topic, partition, offset, Kafka timestamp, schema, before/after,
source metadata, transaction metadata, and Spark batch ID. A tombstone is stored
as a Bronze row with `is_tombstone=true`. Spark’s checkpoint directory is
`data/checkpoints/cdc_v2/employee-cdc`; keep it with the Kafka offsets when
restarting the same query. DuckDB’s `cdc_event_ledger` prevents duplicate
offsets from being applied twice. Silver upserts compare source LSN first,
then source event time, transaction order, partition, and offset.

### Local Runbook

Start PostgreSQL, Kafka, topic initialization, and Debezium Connect:

```zsh
scripts/start_services.sh
```

The local PostgreSQL database is `streamlake_source`; the default local-only
credentials are `streamlake` / `streamlake_local_only`. Override them in `.env`
before first initialization if needed. The SQL bootstrap sets `wal_level=logical`,
`max_wal_senders`, `max_replication_slots`, `REPLICA IDENTITY FULL`, and the
`streamlake_publication` publication for companies, policies, employees, and
dependents.

Seed a small source dataset, register the connector, and check its state/topics:

```zsh
.venv/bin/python scripts/seed_postgres.py --employees 50 --seed 42
scripts/register_connector.sh
scripts/verify_kafka_topics.sh
```

Start the consumer independently from the database/connect services:

```zsh
scripts/start_spark_consumer.sh
```

`start_spark_consumer.sh` builds the optional Spark profile and starts the
Structured Streaming query. For a host Spark installation, install
`.[streaming]` and run the script with the same connector coordinate shown in
the version table. Do not remove `data/checkpoints/cdc_v2/employee-cdc` during a
normal restart.

In another terminal, generate a real INSERT, UPDATE, and DELETE:

```zsh
scripts/insert_employee.sh EMP-CDC-0001
scripts/update_employee.sh EMP-CDC-0001 cdc.updated@example.test 750000
scripts/delete_employee.sh EMP-CDC-0001
```

Inspect the stream outputs and current employee state:

```zsh
.venv/bin/python scripts/inspect_cdc_data.py --limit 5
```

`data/bronze/cdc_v2/` is append-oriented and partitioned by Spark batch, event
date, and source table. `data/silver/cdc_employee_current_v2.parquet` is a
snapshot of DuckDB current state. Deletes remain there as rows with
`record_status=deleted`. Invalid messages are written to
`data/quarantine/cdc_v2/` and the `employee_dead_letter` topic. The `cdc_v2`
paths preserve the first pre-decoder test run's local artifacts.

### Phase 2 Verification

The unit suite is infrastructure-independent:

```zsh
.venv/bin/python -m pytest
.venv/bin/python -m pip check
```

The live Compose run completed PostgreSQL INSERT, UPDATE, and DELETE changes
for all three demo employees. It recorded **96 unique valid CDC events** across
the four captured tables. The employee topic included INSERT (`c`), UPDATE
(`u`), and DELETE (`d`) operations; the corresponding Kafka tombstones (`t`)
were retained as Bronze events. Deleted employees remained in Silver with
`record_status=deleted`.

Spark was restarted using its existing checkpoint. The replay did not create
duplicate event IDs in the event ledger. Debezium source metadata in the
captured event payload reported version **3.2.4.Final**.

Malformed-message verification also completed against the live stream. A
`not-json` value on `employee_changes` was retained in Bronze, counted as one
rejected ledger event, written as one row under `data/quarantine/cdc_v2/`, and
published to `employee_dead_letter` with its topic, partition, offset, raw
payload, and parse error. This probe is an additional rejected event; after it,
the local ledger contained 97 distinct IDs (96 valid CDC events plus the one
malformed probe).

The unit suite reports **23 passing tests**, covering CDC operations,
deduplication, malformed messages, ordering, connectivity probing, and
checkpoint behavior alongside the batch pipeline. `.venv/bin/python -m pip
check` reported no broken requirements.

## Phase 3: Spark, dbt, and Data Quality

Phase 3 consumes the reproducible Phase 1 batch inputs at
`data/incoming/` and produces tested Silver and dimensional outputs without
changing the Phase 1/2 artifacts. Install the local extras and run the Spark
batch transform followed by dbt:

```zsh
python -m pip install -e '.[phase3]'
python scripts/run_phase3_spark.py
cd transform/dbt
dbt build --target local
```

The Spark modules under `src/streamlake/processing/spark/` use DataFrame
expressions, built-in SQL functions, and window functions. Employee cleaning
normalizes text, email, dates, timestamps, and coverage; rows with mandatory
field, email, date, or coverage failures go to quarantine with quality reasons.
Employee versions are ordered by source event time, operation precedence, and
stable content keys when source timestamps tie. A `row_number()` window selects
the current record, while `lead()` creates SCD2 effective intervals and stable
version keys. Delete inputs carry their last
known business attributes into Silver while preserving `record_status=deleted`.
Dependent rows are checked against employee keys. Policy and company totals
are aggregated with Spark SQL. Writes coalesce small local outputs and keep
separate entity directories under `data/phase3/silver/`.

When `data/bronze/cdc_v2/` exists, the same run also transforms valid employee
INSERT/UPDATE/DELETE events from Phase 2 Kafka Bronze Parquet. It excludes
tombstones and malformed records from business-state transforms and keeps the
event IDs, offsets, LSNs, before/after values, and topic lineage in the change
fact output. CDC deletes remain deleted in employee history and current state.

Incremental transforms accept existing state and new batches, union by name,
then resolve the latest record per employee with the same deterministic
window ordering. Source file, source batch ID, and source timestamps are kept
through the Silver and employee-change outputs. The Spark quality report
reconciles input, accepted, and rejected counts and stores rejected employee
and dependent rows under `data/phase3/quarantine/`.

The dbt project at `transform/dbt/` reads those Parquet outputs into staging
models, then builds the requested dimensions, facts, and eight business
summaries. `ref()` dependencies make the model graph explicit. Employee SCD2
intervals are produced in Spark; `snapshot_employee` is also provided for
future current-state check snapshots. The local profile writes
`data/streamlake_phase3.duckdb`. A BigQuery profile is included for a
credentialed cloud target; install `.[dbt]` and set `GCP_PROJECT_ID` and
`BIGQUERY_DATASET` to use it.

The Phase 3 extra pins PySpark 4.0.1, dbt Core 1.11.17, and dbt-duckdb 1.10.1.
`stg_employees` is incremental on source ingestion timestamps. The employee
dimension is rebuilt from the complete SCD2 history so prior current rows are
closed correctly when a later event arrives; this keeps the local dimension
consistent across repeat builds.

`schema.yml` files document staging and mart models and define uniqueness,
not-null, and accepted-value tests. Singular dbt tests check employee to
company references, positive coverage, valid SCD2 ranges, and exactly one
current employee version. The Python
quality framework in `src/streamlake/quality/framework.py` exposes reusable
completeness, uniqueness, reference, date, email, and coverage checks and
returns both a reconciliation report and error-annotated quarantine rows.

Spark tests require the `phase3` extra and a Java 17 runtime. Re-run Spark
transformations after new source batches, then run `dbt build`; content hashes
identify source batches, and the employee staging model incrementally replaces
changed employee keys. Verify mart outputs with DuckDB:

```zsh
python - <<'PY'
import duckdb
db = duckdb.connect('data/streamlake_phase3.duckdb', read_only=True)
print(db.sql('select count(*) as employees from dim_employee').fetchall())
print(db.sql('select record_status, count(*) from fact_employee_enrollment group by 1').fetchall())
print(db.sql('select count(*) from fact_employee_changes').fetchall())
PY
```

### Phase 3 Local Verification

Executed on the Phase 1 sample dataset plus Phase 2 CDC Bronze, with Python
3.11. The Spark run produced 10,053 employee current-state rows, 10,657 SCD2
employee versions, 2,999 valid dependents, 39 policy aggregates, and five
company aggregates. Four employee rows and three dependent rows were
quarantined; one duplicate employee source event was removed. The quality
report reconciled both employee and dependent source-to-target counts.

The DuckDB mart query returned 10,657 `dim_employee` versions (10,053 current,
including 9,951 active and 102 deleted), five companies, 39 policies, 7,306
calendar dates, six departments, 10,053 enrollment facts, 10,661 employee
change facts, and 39 policy utilization facts. The employee-to-company
relationship and SCD2 range checks returned zero failures. CDC change facts
preserved 10,057 inserts, 502 updates, and 102 deletes across batch and Kafka
events. A repeated dbt build retained those counts.

`dbt build --target local` completed **64/64** nodes successfully (23 models,
one snapshot, and 41 data tests). The full unit suite completed with **28
passed**, including Spark transformation, ordering, and quality tests. `pip check`
reported no broken requirements.

## Phase 4: Airflow Workflow Orchestration

Airflow 3.4.0 runs in a separate local Compose project, using the official
`apache/airflow:3.4.0-python3.11` image, LocalExecutor, and a dedicated
PostgreSQL 16.4 metadata database. The image adds Java 21 for the finite
PySpark batch job plus the existing StreamLake batch, analytics, dbt, and test
dependencies. Airflow does not launch the Kafka consumer: Kafka, Kafka Connect,
and the Phase 2 streaming consumer remain independently managed services.

Start Airflow from the repository root:

```zsh
docker compose -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.airflow.yml up -d
```

Open <http://localhost:8080> and sign in as `airflow`. On first startup,
Airflow generates its local SimpleAuthManager password in
`airflow/config/simple_auth_manager_passwords.json.generated`; read that
git-ignored file for the password. Keep the UI bound to a trusted local machine.
The API server, scheduler, DAG processor, and metadata database have health
checks. Airflow logs are under `airflow/logs/`; the generated local auth
password file is under `airflow/config/` and is git-ignored.
The Compose file shares its local API signing key across services; set
`STREAMLAKE_AIRFLOW_API_SECRET_KEY` in the shell or an ignored `.env` file to
replace the development default.

The scheduled `streamlake_batch_ingestion` DAG runs daily at 02:00 UTC by
default (`STREAMLAKE_BATCH_SCHEDULE` changes the cron schedule). It discovers
CSV/XLS inputs, skips completed content fingerprints, validates schemas,
ingests selected files through the existing BatchPipeline, runs the finite
Phase 3 Spark transform, verifies Silver and quality outputs, checks Gold,
runs `dbt build --target local`, and writes a run audit JSON to
`data/airflow/audit/`. Row validation failures are quarantined by the existing
ingestion and quality stages; missing columns or unreadable files fail the
Airflow task. Tasks use two retries, a one-minute local retry delay, two-hour task
timeouts, and a configurable callback via
`STREAMLAKE_AIRFLOW_FAILURE_CALLBACK=package.module:function`.

The other DAGs support explicit Bronze-to-Silver, Silver-to-Gold, daily data
quality, dbt-only, and daily reconciliation runs. They are named
`streamlake_bronze_to_silver`, `streamlake_silver_to_gold`,
`streamlake_data_quality`, `streamlake_dbt_transformation`, and
`streamlake_daily_reconciliation`. Daily quality runs at 03:00 UTC and
reconciliation at 04:00 UTC. All DAGs disable catchup and limit active runs
to one.

Trigger a batch from the UI, or from the CLI with optional parameters:

```zsh
docker compose -f docker-compose.airflow.yml exec airflow-api-server \
  airflow dags trigger streamlake_batch_ingestion \
  --conf '{"input_dir":"data/incoming","reprocess":false}'
```

`input_dir` must be a directory under the mounted project. Set `reprocess` to
true to select completed inputs again. For historical replay, pass `batch_id`
or `start_date` and `end_date` in `YYYY-MM-DD` format; matching source files
must still be present in the selected input directory. Batch ingestion itself
retains fingerprint idempotence, while Spark, quality, Gold verification, and
dbt rebuild current output state. Keep archived source versions if a historical
file has since changed.

Select a DAG run in the UI to inspect task status, duration, logs, retries, and
XCom results. Failed tasks can be cleared and retried from the Grid view. Each
successful main run writes batch ID, Airflow run ID, timestamps, duration,
processed/rejected counts, and reconciliation status to its audit JSON. Daily
reconciliation writes a separate report to `data/airflow/reconciliation/`.

Run the orchestration checks in the Airflow image (the host Python environment
does not need Airflow installed):

```zsh
docker compose -f docker-compose.airflow.yml exec airflow-api-server \
  python -m pytest /opt/streamlake/tests/airflow -q
```

The required local run is the `streamlake_batch_ingestion` DAG. Wait for all
tasks, including Spark and dbt, to finish successfully before considering
Phase 4 complete. Airflow task logs are the source for live task start/end
times; the audit report captures pipeline-level timing and record counts.
