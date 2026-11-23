"""Infrastructure-independent CDC parsing, ordering, and recovery tests."""

import json
import base64
from contextlib import nullcontext

import pandas as pd

from streamlake.common.settings import Settings
from streamlake.ingestion.streaming.cdc import (
    apply_employee_event,
    checkpoint_location,
    kafka_is_reachable,
    parse_cdc_message,
    parse_lsn,
)
from streamlake.ingestion.streaming.spark_consumer import _apply_duckdb_batch


def make_event(operation: str, offset: int, lsn: int, after=None, before=None):
    payload = {
        "before": before,
        "after": after,
        "source": {
            "name": "streamlake",
            "db": "streamlake_source",
            "schema": "public",
            "table": "employees",
            "lsn": lsn,
            "ts_ms": 1_800_000_000_000 + lsn,
        },
        "op": operation,
        "ts_ms": 1_800_000_000_000 + lsn,
        "transaction": {"total_order": str(offset)},
    }
    return parse_cdc_message(
        json.dumps({"schema": {"type": "struct", "fields": []}, "payload": payload}),
        topic="employee_changes",
        partition=0,
        offset=offset,
        kafka_timestamp_ms=1_800_000_000_000 + lsn,
        key=json.dumps({"payload": {"employee_id": "EMP-1"}}),
    )


def test_insert_and_update_preserve_ops_and_before_after() -> None:
    inserted = make_event("c", 0, 100, after={"employee_id": "EMP-1", "employee_name": "Ada"})
    updated = make_event(
        "u", 1, 200,
        before={"employee_id": "EMP-1", "employee_name": "Ada"},
        after={"employee_id": "EMP-1", "employee_name": "Ada Lovelace"},
    )

    state, status = apply_employee_event(None, inserted)
    updated_state, update_status = apply_employee_event(state, updated)

    assert status == "applied"
    assert update_status == "applied"
    assert inserted.operation == "c"
    assert updated.operation == "u"
    assert updated.before["employee_name"] == "Ada"
    assert updated.after["employee_name"] == "Ada Lovelace"
    assert updated_state["data"]["employee_name"] == "Ada Lovelace"
    assert updated_state["record_status"] == "active"


def test_delete_becomes_business_tombstone_and_keeps_last_values() -> None:
    inserted = make_event("c", 0, 100, after={"employee_id": "EMP-1", "employee_name": "Ada"})
    deleted = make_event("d", 1, 200, before={"employee_id": "EMP-1", "employee_name": "Ada"})
    state, _ = apply_employee_event(None, inserted)

    deleted_state, status = apply_employee_event(state, deleted)

    assert status == "applied"
    assert deleted_state["record_status"] == "deleted"
    assert deleted_state["data"]["employee_name"] == "Ada"
    assert deleted_state["last_operation"] == "d"


def test_malformed_and_unsupported_messages_are_retained_as_errors() -> None:
    malformed = parse_cdc_message(
        b"not-json", topic="employee_changes", partition=2, offset=19
    )
    unsupported = parse_cdc_message(
        '{"payload":{"op":"x"}}', topic="employee_changes", partition=0, offset=20
    )

    assert malformed.error_reason
    assert malformed.raw_value == "not-json"
    assert unsupported.error_reason == "Unsupported Debezium operation: 'x'"


def test_duplicate_coordinates_and_older_source_events_are_rejected() -> None:
    newer = make_event("u", 11, 500, after={"employee_id": "EMP-1", "employee_name": "New"})
    older = make_event("u", 12, 300, after={"employee_id": "EMP-1", "employee_name": "Old"})
    duplicate = make_event("u", 11, 500, after={"employee_id": "EMP-1", "employee_name": "New"})

    state, _ = apply_employee_event(None, newer)
    stale_state, status = apply_employee_event(state, older)
    duplicate_state, duplicate_status = apply_employee_event(state, duplicate)

    assert duplicate.event_id == newer.event_id
    assert status == "stale"
    assert duplicate_status == "stale"
    assert stale_state["data"]["employee_name"] == "New"
    assert duplicate_state["data"]["employee_name"] == "New"


def test_payload_only_json_and_lsn_formats_parse() -> None:
    parsed = parse_cdc_message(
        '{"before":null,"after":{"employee_id":"EMP-1"},"source":{"table":"employees","lsn":"0/10"},"op":"r"}',
        topic="employee_changes",
        partition=1,
        offset=4,
    )

    assert parsed.operation == "r"
    assert parsed.source_lsn == 16
    assert parse_lsn('["20", "32"]') == 32


def test_schema_bearing_logical_dates_and_decimals_are_decoded() -> None:
    raw_decimal = base64.b64encode((50_000_000).to_bytes(4, "big", signed=True)).decode()
    value_schema = {
        "type": "struct",
        "fields": [
            {
                "type": "struct",
                "field": "before",
                "fields": [
                    {"type": "int32", "name": "io.debezium.time.Date", "field": "date_of_birth"},
                    {
                        "type": "bytes",
                        "name": "org.apache.kafka.connect.data.Decimal",
                        "parameters": {"scale": "2"},
                        "field": "sum_insured",
                    },
                ],
            },
            {
                "type": "struct",
                "field": "after",
                "fields": [
                    {"type": "int32", "name": "io.debezium.time.Date", "field": "date_of_birth"},
                    {
                        "type": "bytes",
                        "name": "org.apache.kafka.connect.data.Decimal",
                        "parameters": {"scale": "2"},
                        "field": "sum_insured",
                    },
                ],
            },
        ],
    }
    raw_value = json.dumps(
        {
            "schema": value_schema,
            "payload": {
                "before": None,
                "after": {"employee_id": "EMP-1", "date_of_birth": 1, "sum_insured": raw_decimal},
                "source": {"table": "employees", "lsn": 100},
                "op": "c",
            },
        }
    )

    parsed = parse_cdc_message(raw_value, topic="employee_changes", partition=0, offset=1)

    assert parsed.after["date_of_birth"] == "1970-01-02"
    assert parsed.after["sum_insured"] == "500000.00"
    assert parsed.raw_value == raw_value


def test_kafka_tombstone_and_stable_checkpoint_path() -> None:
    tombstone = parse_cdc_message(
        None,
        topic="employee_changes",
        partition=0,
        offset=22,
        key='{"payload":{"employee_id":"EMP-1"}}',
    )

    assert tombstone.tombstone
    assert tombstone.employee_id == "EMP-1"
    assert checkpoint_location("data/checkpoints/cdc") == checkpoint_location(
        "data/checkpoints/cdc", "employee-cdc"
    )


def test_kafka_connectivity_probe_uses_requested_broker(monkeypatch) -> None:
    def fake_connection(address, timeout):
        if address == ("localhost", 29092):
            return nullcontext()
        raise OSError("connection refused")

    monkeypatch.setattr("streamlake.ingestion.streaming.cdc.socket.create_connection", fake_connection)

    assert kafka_is_reachable("localhost", 29092)
    assert not kafka_is_reachable("localhost", 9092)


def test_checkpoint_path_rejects_traversal() -> None:
    try:
        checkpoint_location("data/checkpoints/cdc", "../other")
    except ValueError as error:
        assert "query_name" in str(error)
    else:
        raise AssertionError("path traversal query name must be rejected")


def test_duckdb_batch_deduplicates_merges_updates_and_keeps_deletes(tmp_path) -> None:
    settings = Settings(
        cdc_silver_path=tmp_path / "silver" / "employees.parquet",
    )
    stage_path = tmp_path / "stage" / "batch_id=spark-00001"
    stage_path.mkdir(parents=True)
    initial = make_event("c", 0, 100, after={"employee_id": "EMP-1", "employee_name": "Ada", "email": "ada@example.test"})
    update = make_event(
        "u", 1, 200,
        before={"employee_id": "EMP-1", "employee_name": "Ada", "email": "ada@example.test"},
        after={"employee_id": "EMP-1", "email": "ada.updated@example.test"},
    )
    stale = make_event("u", 2, 150, after={"employee_id": "EMP-1", "employee_name": "Stale"})
    events = [initial, update, stale]
    records = [_stage_record(item, "spark-00001") for item in events]
    pd.DataFrame.from_records(records).to_parquet(stage_path / "part-00000.parquet", index=False)

    result = _apply_duckdb_batch(tmp_path / "cdc.duckdb", stage_path, settings)

    assert result == {"input": 3, "deduplicated": 3, "applied": 1, "rejected": 0}
    silver = pd.read_parquet(settings.cdc_silver_path)
    assert len(silver) == 1
    assert silver.iloc[0]["record_status"] == "active"
    silver_data = json.loads(silver.iloc[0]["data_json"])
    assert silver_data["employee_name"] == "Ada"
    assert silver_data["email"] == "ada.updated@example.test"

    delete_path = tmp_path / "stage" / "batch_id=spark-00002"
    delete_path.mkdir(parents=True)
    delete_before = {
        "employee_id": "EMP-1",
        "employee_name": "Ada",
        "email": "ada.updated@example.test",
    }
    delete_record = _stage_record(
        make_event("d", 3, 300, before=delete_before), "spark-00002"
    )
    pd.DataFrame([delete_record]).to_parquet(delete_path / "part-00000.parquet", index=False)
    delete_result = _apply_duckdb_batch(tmp_path / "cdc.duckdb", delete_path, settings)
    assert delete_result["applied"] == 1
    silver = pd.read_parquet(settings.cdc_silver_path)
    assert silver.iloc[0]["record_status"] == "deleted"

    replay_path = tmp_path / "stage" / "batch_id=spark-00003"
    replay_path.mkdir(parents=True)
    replay = _stage_record(initial, "spark-00003")
    pd.DataFrame([replay]).to_parquet(replay_path / "part-00000.parquet", index=False)
    replay_result = _apply_duckdb_batch(tmp_path / "cdc.duckdb", replay_path, settings)
    assert replay_result["deduplicated"] == 0
    assert replay_result["applied"] == 0


def _stage_record(event_record, batch_id: str) -> dict:
    record = event_record.to_record()
    record.update(
        employee_id=event_record.employee_id,
        table=event_record.table,
        spark_batch_id=batch_id,
        processing_timestamp="2026-11-23T00:00:00Z",
        event_date="2026-11-23",
    )
    return record