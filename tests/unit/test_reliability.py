"""Reliability telemetry and schema evolution are driven by real ingestions."""

import json

import pandas as pd

from streamlake.common.settings import Settings
from streamlake.ingestion.batch.pipeline import BatchPipeline
from streamlake.ingestion.batch.schemas import EMPLOYEE_COLUMNS
from streamlake.monitoring.reliability import ensure_reliability_tables, record_debezium_schema


def _employee_row(employee_id: str, *, department: str = "HR", extra: str | None = None) -> dict[str, str]:
    row = {column: "" for column in EMPLOYEE_COLUMNS}
    row.update({
        "employee_id": employee_id, "employee_name": "Test Employee",
        "email": f"{employee_id.lower()}@example.test", "gender": "Other",
        "date_of_birth": "1990-01-01", "joining_date": "2024-01-01",
        "company_id": "C-1", "company_name": "Example Co", "department": department,
        "designation": "Engineer", "sum_insured": "100000", "policy_id": "P-1",
        "relationship": "Self", "record_status": "active",
    })
    if extra is not None:
        row["region"] = extra
    return row


def _write(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def test_execution_registry_lineage_metrics_and_duplicate_replay(tmp_path):
    settings = Settings(data_root=tmp_path / "data", duckdb_path=tmp_path / "data" / "streamlake.duckdb",
                        ingestion_audit_path=tmp_path / "audit.jsonl")
    source = _write(settings.data_root / "incoming" / "employees_additions.csv", [_employee_row("E1")])
    pipeline = BatchPipeline(settings)
    first = pipeline.ingest_file(source)
    replay = pipeline.ingest_file(source)
    connection = pipeline._connect()
    try:
        audits = connection.execute("SELECT execution_status, records_read, records_written FROM pipeline_audit ORDER BY start_time").fetchall()
        lineage = connection.execute("SELECT DISTINCT target_table FROM data_lineage WHERE batch_id = ?", [first.batch_id]).fetchall()
        metrics = connection.execute("SELECT metric_name, metric_value FROM pipeline_metrics WHERE dimensions_json::VARCHAR LIKE ?", [f"%{first.batch_id}%"]).fetchall()
    finally:
        connection.close()
    assert first.rows_loaded == 1 and replay.skipped
    assert {row[0] for row in audits} == {"completed", "skipped_duplicate"}
    assert {row[0] for row in lineage} >= {"bronze", "silver_events", "gold_employee_master"}
    assert {row[0] for row in metrics} >= {"records_ingested", "records_processed", "execution_seconds"}


def test_additive_optional_schema_change_is_versioned_and_compatible(tmp_path):
    settings = Settings(data_root=tmp_path / "data", duckdb_path=tmp_path / "data" / "streamlake.duckdb",
                        ingestion_audit_path=tmp_path / "audit.jsonl")
    first_path = _write(settings.data_root / "incoming" / "employees_first.csv", [_employee_row("E1")])
    pipeline = BatchPipeline(settings)
    first = pipeline.ingest_file(first_path)
    second_path = _write(settings.data_root / "incoming" / "employees_second.csv",
                         [_employee_row("E2", extra="west")])
    second = pipeline.ingest_file(second_path)
    connection = pipeline._connect()
    try:
        versions = connection.execute("SELECT schema_version, changes_json, backward_compatible FROM schema_registry WHERE source_type='additions' ORDER BY schema_version").fetchall()
    finally:
        connection.close()
    assert first.status == second.status == "completed"
    assert [version[0] for version in versions] == [1, 2]
    assert json.loads(versions[1][1])["added"] == ["region"]
    assert versions[1][2] is True


def test_incompatible_type_change_is_quarantined(tmp_path):
    settings = Settings(data_root=tmp_path / "data", duckdb_path=tmp_path / "data" / "streamlake.duckdb",
                        ingestion_audit_path=tmp_path / "audit.jsonl")
    pipeline = BatchPipeline(settings)
    first = _write(settings.data_root / "incoming" / "employees_first.csv", [_employee_row("E1", department="HR")])
    pipeline.ingest_file(first)
    changed = _write(settings.data_root / "incoming" / "employees_second.csv", [_employee_row("E2", department="123")])
    result = pipeline.ingest_file(changed)
    repeated_schema = _write(settings.data_root / "incoming" / "employees_third.csv",
                             [_employee_row("E3", department="456")])
    repeated = pipeline.ingest_file(repeated_schema)
    quarantine_files = list((settings.data_root / "quarantine").rglob("*.parquet"))
    quarantined = pd.concat([pd.read_parquet(path) for path in quarantine_files], ignore_index=True)
    assert result.rows_loaded == 0 and result.rows_rejected == 1
    assert repeated.rows_loaded == 0 and repeated.rows_rejected == 1
    assert quarantined["error_reason"].str.contains("Incompatible schema type change: department").any()


def test_specific_batch_reprocess_and_silver_gold_rebuild_are_idempotent(tmp_path):
    settings = Settings(data_root=tmp_path / "data", duckdb_path=tmp_path / "data" / "streamlake.duckdb",
                        ingestion_audit_path=tmp_path / "audit.jsonl")
    source = _write(settings.data_root / "incoming" / "employees_additions.csv", [_employee_row("E1")])
    pipeline = BatchPipeline(settings)
    first = pipeline.ingest_file(source)
    replay = pipeline.reprocess_batch(first.batch_id)
    assert replay.status == "completed" and replay.rows_loaded == 1
    assert pipeline.report()["active_employees"] == 1
    silver = pipeline.rebuild_silver()
    gold = pipeline.rebuild_gold()
    assert silver == {"employee_rows": 1, "dependent_rows": 0, "event_files": 2}
    assert gold["gold_employee_master"] == 1


def test_historical_replay_does_not_overwrite_newer_business_state(tmp_path):
    settings = Settings(data_root=tmp_path / "data", duckdb_path=tmp_path / "data" / "streamlake.duckdb",
                        ingestion_audit_path=tmp_path / "audit.jsonl")
    first_row = _employee_row("E1")
    first_row["updated_at"] = "2024-01-01T00:00:00"
    first_path = _write(settings.data_root / "incoming" / "employees_additions.csv", [first_row])
    pipeline = BatchPipeline(settings)
    original = pipeline.ingest_file(first_path)
    newer_row = _employee_row("E1")
    newer_row["email"] = "newer@example.test"
    newer_row["updated_at"] = "2025-01-01T00:00:00"
    update_path = _write(settings.data_root / "incoming" / "employees_updates.csv", [newer_row])
    pipeline.ingest_file(update_path)
    replay = pipeline.reprocess_batch(original.batch_id)
    assert replay.rows_loaded == 0
    connection = pipeline._connect()
    try:
        email = connection.execute("SELECT email FROM silver_employee_master WHERE employee_id='E1'").fetchone()[0]
    finally:
        connection.close()
    assert email == "newer@example.test"


def test_debezium_schema_registry_versions_additive_and_breaking_fields():
    import duckdb

    connection = duckdb.connect(":memory:")
    ensure_reliability_tables(connection)
    base = {"type": "struct", "fields": [{"field": "after", "fields": [
        {"field": "employee_id", "type": "string"}, {"field": "email", "type": "string"}
    ]}]}
    first = record_debezium_schema(connection, "employees", json.dumps(base))
    additive = {"type": "struct", "fields": [{"field": "after", "fields": [
        {"field": "employee_id", "type": "string"}, {"field": "email", "type": "string"},
        {"field": "department", "type": "string"}
    ]}]}
    second = record_debezium_schema(connection, "employees", json.dumps(additive))
    breaking = {"type": "struct", "fields": [{"field": "after", "fields": [
        {"field": "employee_id", "type": "int64"}, {"field": "email", "type": "string"},
        {"field": "department", "type": "string"}
    ]}]}
    third = record_debezium_schema(connection, "employees", json.dumps(breaking))
    repeated_breaking = record_debezium_schema(connection, "employees", json.dumps(breaking))
    assert first["version"] == 1
    assert second["added"] == ["department"] and second["backward_compatible"]
    assert third["incompatible_columns"] == ["employee_id"]
    assert repeated_breaking["incompatible_columns"] == ["employee_id"]
    connection.close()
