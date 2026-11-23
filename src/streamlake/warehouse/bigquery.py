"""On-demand BigQuery warehouse operations for StreamLake datasets and models."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Iterable, Sequence

logger = logging.getLogger(__name__)

DATASETS = ("streamlake_bronze", "streamlake_silver", "streamlake_gold", "streamlake_audit")
TABLES: dict[str, dict[str, tuple[tuple[str, str, str], ...]]] = {
    "streamlake_bronze": {
        "raw_employees": (("employee_id", "STRING", "REQUIRED"), ("ingestion_timestamp", "TIMESTAMP", "NULLABLE"), ("batch_id", "STRING", "NULLABLE"), ("source_file", "STRING", "NULLABLE"), ("payload", "JSON", "NULLABLE")),
        "raw_dependents": (("employee_id", "STRING", "NULLABLE"), ("dependent_id", "STRING", "NULLABLE"), ("ingestion_timestamp", "TIMESTAMP", "NULLABLE"), ("batch_id", "STRING", "NULLABLE"), ("payload", "JSON", "NULLABLE")),
        "raw_policies": (("policy_id", "STRING", "NULLABLE"), ("ingestion_timestamp", "TIMESTAMP", "NULLABLE"), ("batch_id", "STRING", "NULLABLE"), ("payload", "JSON", "NULLABLE")),
        "raw_employee_events": (("event_id", "STRING", "REQUIRED"), ("employee_id", "STRING", "NULLABLE"), ("operation", "STRING", "NULLABLE"), ("event_timestamp", "TIMESTAMP", "NULLABLE"), ("ingestion_timestamp", "TIMESTAMP", "NULLABLE"), ("payload", "JSON", "NULLABLE")),
    },
    "streamlake_silver": {
        "employees": (("employee_id", "STRING", "REQUIRED"), ("employee_name", "STRING", "NULLABLE"), ("email", "STRING", "NULLABLE"), ("company_id", "STRING", "NULLABLE"), ("department", "STRING", "NULLABLE"), ("policy_id", "STRING", "NULLABLE"), ("sum_insured", "NUMERIC", "NULLABLE"), ("record_status", "STRING", "NULLABLE"), ("updated_at", "TIMESTAMP", "NULLABLE")),
        "dependents": (("dependent_id", "STRING", "REQUIRED"), ("employee_id", "STRING", "REQUIRED"), ("dependent_name", "STRING", "NULLABLE"), ("relationship", "STRING", "NULLABLE"), ("record_status", "STRING", "NULLABLE"), ("updated_at", "TIMESTAMP", "NULLABLE")),
        "policies": (("policy_id", "STRING", "REQUIRED"), ("company_id", "STRING", "NULLABLE"), ("policy_name", "STRING", "NULLABLE"), ("sum_insured", "NUMERIC", "NULLABLE"), ("updated_at", "TIMESTAMP", "NULLABLE")),
        "employee_history": (("employee_id", "STRING", "REQUIRED"), ("effective_start_date", "DATE", "REQUIRED"), ("effective_end_date", "DATE", "NULLABLE"), ("is_current", "BOOL", "REQUIRED"), ("record_status", "STRING", "NULLABLE"), ("source_batch_id", "STRING", "NULLABLE")),
    },
    "streamlake_gold": {
        "dim_employee": (("employee_key", "INT64", "NULLABLE"), ("employee_id", "STRING", "REQUIRED"), ("company_id", "STRING", "NULLABLE"), ("employee_name", "STRING", "NULLABLE"), ("effective_start_date", "DATE", "NULLABLE"), ("effective_end_date", "DATE", "NULLABLE"), ("is_current", "BOOL", "NULLABLE")),
        "dim_company": (("company_key", "INT64", "NULLABLE"), ("company_id", "STRING", "REQUIRED"), ("company_name", "STRING", "NULLABLE")),
        "dim_policy": (("policy_key", "INT64", "NULLABLE"), ("policy_id", "STRING", "REQUIRED"), ("company_id", "STRING", "NULLABLE"), ("policy_name", "STRING", "NULLABLE")),
        "fact_employee_enrollment": (("employee_id", "STRING", "REQUIRED"), ("company_id", "STRING", "NULLABLE"), ("policy_id", "STRING", "NULLABLE"), ("joining_date", "DATE", "NULLABLE"), ("sum_insured", "NUMERIC", "NULLABLE")),
        "fact_employee_changes": (("event_id", "STRING", "REQUIRED"), ("employee_id", "STRING", "REQUIRED"), ("operation", "STRING", "NULLABLE"), ("event_timestamp", "TIMESTAMP", "NULLABLE")),
        "company_employee_summary": (("company_id", "STRING", "REQUIRED"), ("company_name", "STRING", "NULLABLE"), ("employee_count", "INT64", "NULLABLE"), ("total_sum_insured", "NUMERIC", "NULLABLE"), ("as_of_date", "DATE", "NULLABLE")),
        "monthly_enrollment_summary": (("month", "DATE", "REQUIRED"), ("company_id", "STRING", "NULLABLE"), ("employee_additions", "INT64", "NULLABLE"), ("employee_deletions", "INT64", "NULLABLE")),
    },
    "streamlake_audit": {
        "ingestion_audit": (("batch_id", "STRING", "REQUIRED"), ("source_file", "STRING", "NULLABLE"), ("started_at", "TIMESTAMP", "NULLABLE"), ("completed_at", "TIMESTAMP", "NULLABLE"), ("rows_loaded", "INT64", "NULLABLE"), ("rows_rejected", "INT64", "NULLABLE"), ("status", "STRING", "NULLABLE")),
    },
}


class BigQueryWarehouse:
    """Creates managed datasets/tables and runs monitored batch load and MERGE jobs."""

    def __init__(self, project_id: str | None = None, region: str = "us-central1", client=None,
                 bigquery_module=None, credentials=None, default_dataset: str = "streamlake_silver") -> None:
        if bigquery_module is None:
            try:
                from google.cloud import bigquery as bigquery_module
            except ImportError as error:
                raise RuntimeError("BigQuery support requires `pip install -e '.[gcp]'`") from error
        self.bigquery = bigquery_module
        self.client = client or bigquery_module.Client(project=project_id, location=region, credentials=credentials)
        self.project_id = project_id or self.client.project
        self.region = region
        self.default_dataset = default_dataset

    def ensure_datasets(self) -> list[str]:
        created = []
        for name in DATASETS:
            dataset_id = f"{self.project_id}.{name}"
            dataset = self.bigquery.Dataset(dataset_id)
            dataset.location = self.region
            self.client.create_dataset(dataset, exists_ok=True)
            created.append(dataset_id)
        return created

    def ensure_table(self, dataset_id: str, table_id: str, schema: Sequence[tuple[str, str, str]] | None = None):
        dataset_name = dataset_id.rsplit(".", 1)[-1]
        if schema is None:
            try:
                schema = TABLES[dataset_name][table_id]
            except KeyError as error:
                raise ValueError(f"No managed schema registered for {dataset_name}.{table_id}") from error
        fields = [self.bigquery.SchemaField(name, field_type, mode=mode) for name, field_type, mode in schema]
        table = self.bigquery.Table(f"{self.project_id}.{dataset_name}.{table_id}", schema=fields)
        if table_id in {"raw_employees", "raw_dependents", "raw_policies"}:
            table.time_partitioning = self.bigquery.TimePartitioning(type_=self.bigquery.TimePartitioningType.DAY, field="ingestion_timestamp")
        elif table_id in {"raw_employee_events", "fact_employee_changes"}:
            table.time_partitioning = self.bigquery.TimePartitioning(type_=self.bigquery.TimePartitioningType.DAY, field="event_timestamp")
        elif table_id == "monthly_enrollment_summary":
            table.time_partitioning = self.bigquery.TimePartitioning(field="month")
        elif table_id in {"employees", "dependents", "policies"} and dataset_name == "streamlake_silver":
            table.time_partitioning = self.bigquery.TimePartitioning(type_=self.bigquery.TimePartitioningType.DAY, field="updated_at")
        elif table_id == "employee_history":
            table.time_partitioning = self.bigquery.TimePartitioning(field="effective_start_date")
        elif table_id == "fact_employee_enrollment":
            table.time_partitioning = self.bigquery.TimePartitioning(field="joining_date")
        elif table_id == "company_employee_summary":
            table.time_partitioning = self.bigquery.TimePartitioning(field="as_of_date")
        cluster_fields = {"raw_employees": ["employee_id"], "raw_dependents": ["employee_id"], "raw_employee_events": ["employee_id"], "employees": ["company_id", "employee_id"], "dependents": ["employee_id"], "dim_employee": ["employee_id", "company_id"], "fact_employee_changes": ["employee_id", "operation"]}
        if table_id in cluster_fields:
            table.clustering_fields = cluster_fields[table_id]
        return self.client.create_table(table, exists_ok=True)

    def ensure_model_tables(self) -> list[str]:
        self.ensure_datasets()
        return [self.ensure_table(dataset, table) for dataset, tables in TABLES.items() for table in tables]

    def load_file(self, source: str | Path, dataset_id: str, table_id: str, *, file_format: str | None = None,
                  write_disposition: str = "WRITE_APPEND", schema: Sequence[tuple[str, str, str]] | None = None) -> dict[str, Any]:
        """Load CSV/Parquet from local disk or a GCS URI and wait for the job."""
        source_string = str(source)
        fmt = (file_format or ("PARQUET" if source_string.lower().endswith(".parquet") else "CSV")).upper()
        if fmt not in {"CSV", "PARQUET"}:
            raise ValueError("file_format must be CSV or PARQUET")
        table = self.ensure_table(dataset_id, table_id, schema=schema)
        config = self.bigquery.LoadJobConfig(
            source_format=self.bigquery.SourceFormat.PARQUET if fmt == "PARQUET" else self.bigquery.SourceFormat.CSV,
            write_disposition=write_disposition,
            autodetect=fmt == "CSV",
            skip_leading_rows=1 if fmt == "CSV" else None,
        )
        if source_string.startswith("gs://"):
            job = self.client.load_table_from_uri(source_string, table, job_config=config, location=self.region)
        else:
            with Path(source_string).open("rb") as file_obj:
                job = self.client.load_table_from_file(file_obj, table, job_config=config, rewind=True, location=self.region)
        return self._job_result(job)

    def merge(self, dataset_id: str, target_table: str, staging_table: str, keys: Sequence[str], columns: Sequence[str]) -> dict[str, Any]:
        """MERGE staging rows into a target table using validated identifiers."""
        for identifier in (dataset_id, target_table, staging_table, *keys, *columns):
            if not identifier.replace("_", "").isalnum():
                raise ValueError(f"Invalid BigQuery identifier: {identifier}")
        if not keys or not columns:
            raise ValueError("keys and columns must not be empty")
        target = f"`{self.project_id}.{dataset_id}.{target_table}`"
        source = f"`{self.project_id}.{dataset_id}.{staging_table}`"
        condition = " AND ".join(f"T.{key}=S.{key}" for key in keys)
        updates = ", ".join(f"{column}=S.{column}" for column in columns if column not in keys)
        names = ", ".join(columns)
        values = ", ".join(f"S.{column}" for column in columns)
        matched = f"WHEN MATCHED THEN UPDATE SET {updates} " if updates else ""
        sql = f"MERGE {target} T USING {source} S ON {condition} {matched}WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values})"
        return self._job_result(self.client.query(sql, location=self.region))

    def load_incremental(self, source: str | Path, dataset_id: str, target_table: str,
                         staging_table: str, keys: Sequence[str], columns: Sequence[str],
                         *, file_format: str | None = None) -> dict[str, Any]:
        """Load a batch to a staging table then transactionally merge it to the target."""
        dataset_name = dataset_id.rsplit(".", 1)[-1]
        try:
            stage_schema = TABLES[dataset_name][target_table]
        except KeyError as error:
            raise ValueError(f"No managed schema registered for {dataset_name}.{target_table}") from error
        self.load_file(source, dataset_id, staging_table, file_format=file_format,
                       write_disposition="WRITE_TRUNCATE", schema=stage_schema)
        return self.merge(dataset_id, target_table, staging_table, keys, columns)

    def query(self, sql: str, parameters: Iterable[Any] = ()) -> list[Any]:
        """Run a parameterized SQL query and return rows after job completion."""
        config = self.bigquery.QueryJobConfig(query_parameters=list(parameters))
        job = self.client.query(sql, job_config=config, location=self.region)
        return list(job.result())

    def get_job(self, job_id: str):
        return self.client.get_job(job_id, location=self.region)

    @staticmethod
    def _job_result(job) -> dict[str, Any]:
        job.result()
        logger.info("BigQuery job completed", extra={"job_id": job.job_id, "job_type": job.job_type, "state": job.state})
        return {"job_id": job.job_id, "job_type": job.job_type, "state": job.state,
                "output_rows": getattr(job, "output_rows", None),
                "errors": getattr(job, "errors", None)}


def create_warehouse(settings):
    """Build the selected warehouse adapter; DuckDB remains lazy/local by default."""
    if settings.warehouse_backend == "bigquery":
        credentials = None
        if settings.google_application_credentials:
            try:
                from google.oauth2 import service_account
            except ImportError as error:
                raise RuntimeError("GCP credentials require `pip install -e '.[gcp]'`") from error
            credentials = service_account.Credentials.from_service_account_file(
                str(settings.google_application_credentials)
            )
        return BigQueryWarehouse(settings.gcp_project_id, settings.gcp_region, credentials=credentials,
                                 default_dataset=settings.bigquery_dataset)
    if settings.warehouse_backend == "duckdb":
        import duckdb
        return duckdb.connect(str(settings.duckdb_path))
    raise ValueError(f"Unsupported warehouse backend: {settings.warehouse_backend}")
