"""Cloud adapters are tested with in-memory SDK fakes; no credentials are needed."""

from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from streamlake.common.settings import Settings
from streamlake.storage.adapters import GCSStorageAdapter, LocalParquetStorage, create_storage_adapter
from streamlake.warehouse.bigquery import BigQueryWarehouse, DATASETS, TABLES


class FakeBlob:
    def __init__(self, name):
        self.name = name
        self.size = 23
        self.updated = None
        self.content_type = "application/octet-stream"
        self.generation = 9
        self.uploads = 0

    def upload_from_filename(self, source):
        self.uploads += 1
        if self.uploads == 1:
            raise OSError("transient upload error")

    def download_to_filename(self, target):
        Path(target).write_text("downloaded")

    def reload(self):
        return None


class FakeGCSClient:
    project = "unit-project"

    def __init__(self):
        self.blobs = {}

    def bucket(self, name):
        client = self

        class Bucket:
            def __init__(self):
                self.name = name

            def blob(self, path):
                return client.blobs.setdefault(path, FakeBlob(path))

        return Bucket()

    def list_blobs(self, bucket, prefix=""):
        return [blob for name, blob in self.blobs.items() if name.startswith(prefix)]


def test_local_adapter_file_operations_and_partitioned_parquet(tmp_path):
    source = tmp_path / "source.txt"
    source.write_text("sample")
    adapter = LocalParquetStorage(tmp_path / "lake")
    assert adapter.upload_file(source, "bronze/input.txt").endswith("bronze/input.txt")
    destination = adapter.download_file("bronze/input.txt", tmp_path / "copy.txt")
    assert destination.read_text() == "sample"
    frame = pd.DataFrame({"employee_id": ["E-1"]})
    written = adapter.write_partitioned_frame(frame, "bronze", "2026-11-23", "additions", "b1", 0)
    assert written.exists()
    assert "ingestion_date=2026-11-23" in written.as_posix()
    assert adapter.get_metadata("bronze/input.txt").size == 6
    assert adapter.list_files("bronze")


def test_gcs_adapter_upload_download_list_metadata_and_retry(tmp_path):
    client = FakeGCSClient()
    adapter = GCSStorageAdapter("unit-bucket", prefix="streamlake-data", client=client, max_attempts=2)
    source = tmp_path / "input.csv"
    source.write_text("x")
    uri = adapter.upload_file(source, "bronze/input.csv")
    assert uri == "gs://unit-bucket/streamlake-data/bronze/employees/input.csv"
    assert adapter.list_files("bronze") == ["streamlake-data/bronze/employees/input.csv"]
    metadata = adapter.get_metadata("bronze/input.csv")
    assert metadata.generation == "9"
    downloaded = adapter.download_file("bronze/input.csv", tmp_path / "download.csv")
    assert downloaded.read_text() == "downloaded"
    assert adapter._name("bronze/additions/ingestion_date=2026-11-23/x.parquet").startswith(
        "streamlake-data/bronze/employees/"
    )


class FakeJob:
    job_id = "job-123"
    job_type = "LOAD"
    state = "DONE"
    output_rows = 11
    errors = None

    def result(self):
        return []


class FakeBigQueryModule:
    SourceFormat = SimpleNamespace(PARQUET="PARQUET", CSV="CSV")
    TimePartitioningType = SimpleNamespace(DAY="DAY")
    TimePartitioning = lambda **kwargs: SimpleNamespace(**kwargs)
    SchemaField = lambda name, field_type, mode="NULLABLE": (name, field_type, mode)
    LoadJobConfig = lambda **kwargs: SimpleNamespace(**kwargs)
    QueryJobConfig = lambda **kwargs: SimpleNamespace(**kwargs)
    Dataset = lambda dataset_id: SimpleNamespace(dataset_id=dataset_id, location=None)
    Table = lambda table_id, schema: SimpleNamespace(table_id=table_id, schema=schema)


class FakeBigQueryClient:
    project = "unit-project"

    def __init__(self):
        self.datasets = []
        self.tables = []
        self.queries = []
        self.loads = []

    def create_dataset(self, dataset, exists_ok):
        self.datasets.append(dataset)
        return dataset

    def create_table(self, table, exists_ok):
        self.tables.append(table)
        return table

    def load_table_from_file(self, file_obj, table, **kwargs):
        self.loads.append(("file", table, kwargs))
        return FakeJob()

    def load_table_from_uri(self, uri, table, **kwargs):
        self.loads.append((uri, table, kwargs))
        return FakeJob()

    def query(self, sql, **kwargs):
        self.queries.append((sql, kwargs))
        return FakeJob()

    def get_job(self, job_id, location):
        return FakeJob()


def test_bigquery_datasets_tables_partitioning_and_job_monitoring():
    client = FakeBigQueryClient()
    warehouse = BigQueryWarehouse("unit-project", client=client, bigquery_module=FakeBigQueryModule)
    assert len(warehouse.ensure_datasets()) == len(DATASETS)
    tables = warehouse.ensure_model_tables()
    assert len(tables) == sum(len(items) for items in TABLES.values())
    employee_table = next(table for table in client.tables if table.table_id.endswith(".employees"))
    assert employee_table.clustering_fields == ["company_id", "employee_id"]
    assert warehouse.get_job("job-123").state == "DONE"


def test_bigquery_load_merge_and_parameterized_query(tmp_path):
    client = FakeBigQueryClient()
    warehouse = BigQueryWarehouse("unit-project", client=client, bigquery_module=FakeBigQueryModule)
    csv_file = tmp_path / "employees.csv"
    csv_file.write_text("employee_id\nE1\n")
    loaded = warehouse.load_file(csv_file, "streamlake_silver", "employees", file_format="CSV")
    assert loaded["output_rows"] == 11
    merged = warehouse.merge("streamlake_silver", "employees", "employees_stage", ["employee_id"], ["employee_id", "employee_name"])
    assert merged["state"] == "DONE"
    assert "MERGE `unit-project.streamlake_silver.employees`" in client.queries[-1][0]
    marker = object()
    warehouse.query("SELECT @employee_id", [marker])
    assert client.queries[-1][1]["job_config"].query_parameters == [marker]


def test_bigquery_parquet_uri_load_and_incremental_merge(tmp_path):
    client = FakeBigQueryClient()
    warehouse = BigQueryWarehouse("unit-project", client=client, bigquery_module=FakeBigQueryModule)
    parquet = warehouse.load_file(
        "gs://unit-bucket/bronze/employees/part.parquet", "streamlake_bronze", "raw_employees"
    )
    assert parquet["state"] == "DONE"
    assert client.loads[-1][0] == "gs://unit-bucket/bronze/employees/part.parquet"
    csv_file = tmp_path / "employees.csv"
    csv_file.write_text("employee_id,employee_name\nE1,Example\n")
    merged = warehouse.load_incremental(
        csv_file, "streamlake_silver", "employees", "employees_stage",
        ["employee_id"], ["employee_id", "employee_name"], file_format="CSV",
    )
    assert merged["state"] == "DONE"
    assert client.loads[-1][2]["job_config"].write_disposition == "WRITE_TRUNCATE"


def test_backend_defaults_stay_local_and_cloud_requires_bucket():
    defaults = Settings(_env_file=None)
    assert defaults.storage_backend == "local"
    assert defaults.warehouse_backend == "duckdb"
    assert isinstance(create_storage_adapter(defaults), LocalParquetStorage)
    with pytest.raises(ValueError, match="GCS_BUCKET"):
        GCSStorageAdapter("")
