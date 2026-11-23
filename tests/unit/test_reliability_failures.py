"""Failure simulation for retry, replay, database, and unavailable services."""

from dataclasses import dataclass
import pytest
from pydantic import SecretStr

from streamlake.common.settings import Settings
from streamlake.ingestion.batch.pipeline import BatchPipeline
from streamlake.ingestion.streaming.dlq import replay_corrected_records
from streamlake.monitoring.kafka import collect_partition_lag
from streamlake.monitoring.alerts import send_alert
from streamlake.storage.adapters import GCSStorageAdapter
from streamlake.warehouse.bigquery import BigQueryWarehouse


def test_database_connection_failure_is_visible_and_does_not_ingest(tmp_path):
    database_directory = tmp_path / "database-is-a-directory"
    database_directory.mkdir()
    with pytest.raises(Exception):
        BatchPipeline(Settings(data_root=tmp_path / "data", duckdb_path=database_directory))


class UnavailableBlob:
    name = "bronze/file"

    def upload_from_filename(self, source):
        raise OSError("GCS unavailable")


class UnavailableStorageClient:
    def bucket(self, name):
        return type("Bucket", (), {"name": name, "blob": lambda self, key: UnavailableBlob()})()


def test_gcs_unavailable_retries_then_fails(tmp_path):
    source = tmp_path / "source.txt"
    source.write_text("data")
    storage = GCSStorageAdapter("bucket", client=UnavailableStorageClient(), max_attempts=2)
    with pytest.raises(OSError, match="GCS unavailable"):
        storage.upload_file(source, "bronze/file")


class FakeJob:
    job_id = "failed-job"
    job_type = "LOAD"
    state = "FAILED"
    output_rows = None
    errors = ["simulated load failure"]

    def result(self):
        raise RuntimeError("BigQuery job failed")


class FakeBQ:
    SourceFormat = type("SourceFormat", (), {"CSV": "CSV", "PARQUET": "PARQUET"})
    LoadJobConfig = lambda **kwargs: type("Config", (), kwargs)()
    Dataset = lambda dataset_id: type("Dataset", (), {"dataset_id": dataset_id, "location": None})()
    Table = lambda table_id, schema: type("Table", (), {"table_id": table_id, "schema": schema})()
    SchemaField = staticmethod(lambda *args, **kwargs: args)
    TimePartitioning = staticmethod(lambda **kwargs: kwargs)
    TimePartitioningType = type("TimePartitioningType", (), {"DAY": "DAY"})


class FailedBQClient:
    project = "unit-project"

    def create_table(self, table, exists_ok):
        return table

    def load_table_from_file(self, *args, **kwargs):
        return FakeJob()


def test_bigquery_job_failure_is_propagated(tmp_path):
    source = tmp_path / "employees.csv"
    source.write_text("employee_id\nE1\n")
    warehouse = BigQueryWarehouse("unit-project", client=FailedBQClient(), bigquery_module=FakeBQ)
    with pytest.raises(RuntimeError, match="BigQuery job failed"):
        warehouse.load_file(source, "streamlake_silver", "employees", file_format="CSV")


def test_corrected_dlq_replay_validates_and_publishes_provenance_topic():
    class Producer:
        def __init__(self):
            self.messages = []

        def produce(self, **kwargs):
            self.messages.append(kwargs)

        def flush(self, timeout):
            return 0

    producer = Producer()
    rows = [{"source_topic": "employee_changes", "key": '{"payload":{"employee_id":"E1"}}',
             "value": {"payload": {"before": None, "after": {"employee_id": "E1"},
                                    "source": {"table": "employees", "lsn": 10}, "op": "c"}}}]
    assert replay_corrected_records(rows, producer) == 1
    assert producer.messages[0]["topic"] == "employee_changes"
    assert producer.messages[0]["headers"][0][0] == "streamlake-dlq-replay"
    with pytest.raises(ValueError, match="source_topic"):
        replay_corrected_records([{"source_topic": "employee_dead_letter", "value": "{}"}], producer)


def test_kafka_lag_is_computed_from_broker_end_offsets():
    @dataclass(frozen=True)
    class TP:
        topic: str
        partition: int

    class Future:
        def __init__(self, offset):
            self.offset = offset

        def result(self):
            return self

    class Admin:
        def __init__(self, config):
            assert config["bootstrap.servers"] == "broker:9092"

        def list_offsets(self, requested, **kwargs):
            return {tp: Future(12) for tp in requested}

    values = collect_partition_lag(
        "broker:9092", [("employee_changes", 2, 8)], admin_client_factory=Admin,
        topic_partition_factory=TP, latest_offset_factory=lambda: "latest",
    )
    assert values == [{"topic": "employee_changes", "partition": 2,
                       "consumed_offset": 8, "end_offset": 12, "lag_records": 3}]


def test_airflow_retry_callback_records_observed_attempt(monkeypatch):
    from streamlake.orchestration import airflow_tasks

    recorded = []
    monkeypatch.setattr(airflow_tasks, "_record_airflow_event", lambda context, metric, status: recorded.append((metric, status)))
    airflow_tasks.retry_notification_callback({"task_instance": type("TI", (), {
        "dag": type("DAG", (), {"dag_id": "dag"})(), "task_id": "transform",
        "run_id": "run-1", "try_number": 2,
    })()})
    assert recorded == [("airflow_task_retries", "retrying")]


def test_smtp_alert_uses_configured_credentials_without_logging_them(monkeypatch, caplog):
    sent = []

    class SMTP:
        def __init__(self, host, port, timeout):
            assert (host, port, timeout) == ("mail.example.test", 587, 15)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def starttls(self):
            return None

        def login(self, username, password):
            assert (username, password) == ("streamlake", "secret-value")

        def send_message(self, message):
            sent.append(message)

    monkeypatch.setattr("streamlake.monitoring.alerts.smtplib.SMTP", SMTP)
    settings = Settings(_env_file=None, smtp_host="mail.example.test", smtp_username="streamlake",
                        smtp_password=SecretStr("secret-value"), smtp_from_email="from@example.test",
                        alert_to_email="ops@example.test")
    assert send_alert("pipeline failed", "task failed", settings)
    assert sent[0]["To"] == "ops@example.test"
    assert "secret-value" not in caplog.text


def test_airflow_failure_persists_run_task_and_error(monkeypatch, tmp_path):
    import duckdb
    import streamlake.orchestration.airflow_tasks as tasks

    monkeypatch.setattr(tasks, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(tasks, "send_alert", lambda *args: False)
    task_instance = type("TI", (), {
        "dag": type("DAG", (), {"dag_id": "streamlake_test"})(), "task_id": "load_gold",
        "run_id": "manual__failure", "try_number": 3,
        "start_date": None, "end_date": None,
    })()
    tasks.failure_notification_callback({"task_instance": task_instance,
                                         "exception": RuntimeError("warehouse unavailable")})
    connection = duckdb.connect(str(tmp_path / "data" / "streamlake.duckdb"), read_only=True)
    try:
        result = connection.execute(
            "SELECT pipeline_name, source_name, execution_status, error_message "
            "FROM pipeline_audit WHERE pipeline_name='streamlake_test'"
        ).fetchone()
    finally:
        connection.close()
    assert result == ("streamlake_test", "streamlake_test.load_gold", "failed", "warehouse unavailable")
