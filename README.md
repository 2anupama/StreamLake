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

## Optional Cloud Adapters

Local runs use filesystem Parquet and DuckDB only. Install the optional GCP
libraries before constructing the cloud adapters:

```zsh
python -m pip install -e '.[gcp]'
```

docker/                   Local service definitions and images
docs/                     Architecture and operational documentation
GCP credentials are not stored in the repository. GCS and BigQuery adapters use
Application Default Credentials when instantiated; no cloud connection is
created by local generation, validation, ingestion, or reporting.