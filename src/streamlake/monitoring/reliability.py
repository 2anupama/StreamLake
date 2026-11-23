"""Durable execution, metric, lineage, and schema telemetry in DuckDB."""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable

logger = logging.getLogger("streamlake.monitoring")


def ensure_reliability_tables(connection) -> None:
    """Create the appendable operational tables in the selected DuckDB database."""
    connection.execute("""CREATE TABLE IF NOT EXISTS pipeline_audit (
        execution_id VARCHAR PRIMARY KEY, batch_id VARCHAR, pipeline_name VARCHAR NOT NULL,
        source_name VARCHAR, start_time TIMESTAMP NOT NULL, end_time TIMESTAMP,
        records_read BIGINT DEFAULT 0, records_written BIGINT DEFAULT 0,
        records_rejected BIGINT DEFAULT 0, execution_status VARCHAR NOT NULL,
        error_message VARCHAR, duration_seconds DOUBLE)""")
    connection.execute("""CREATE TABLE IF NOT EXISTS data_lineage (
        lineage_id VARCHAR PRIMARY KEY, execution_id VARCHAR, batch_id VARCHAR,
        source_system VARCHAR, source_name VARCHAR, transformation_name VARCHAR NOT NULL,
        target_table VARCHAR NOT NULL, processing_timestamp TIMESTAMP NOT NULL,
        records_read BIGINT DEFAULT 0, records_written BIGINT DEFAULT 0,
        records_rejected BIGINT DEFAULT 0)""")
    connection.execute("""CREATE TABLE IF NOT EXISTS pipeline_metrics (
        metric_id VARCHAR PRIMARY KEY, pipeline_name VARCHAR NOT NULL,
        metric_name VARCHAR NOT NULL, metric_value DOUBLE NOT NULL,
        observed_at TIMESTAMP NOT NULL, dimensions_json JSON)""")
    connection.execute("""CREATE TABLE IF NOT EXISTS schema_registry (
        source_type VARCHAR NOT NULL, schema_version INTEGER NOT NULL,
        schema_hash VARCHAR NOT NULL, columns_json JSON NOT NULL, types_json JSON NOT NULL,
        changes_json JSON NOT NULL, backward_compatible BOOLEAN NOT NULL,
        observed_at TIMESTAMP NOT NULL, PRIMARY KEY(source_type, schema_version))""")


def record_metric(connection, pipeline_name: str, name: str, value: float, dimensions: dict[str, Any] | None = None) -> None:
    connection.execute(
        "INSERT INTO pipeline_metrics VALUES (?, ?, ?, ?, ?, ?)",
        [str(uuid.uuid4()), pipeline_name, name, float(value), datetime.now(timezone.utc),
         json.dumps(dimensions or {}, sort_keys=True, default=str)],
    )


def record_lineage(connection, *, execution_id: str | None, batch_id: str | None,
                   source_system: str | None, source_name: str | None,
                   transformation_name: str, target_table: str,
                   records_read: int | None = 0, records_written: int | None = 0,
                   records_rejected: int | None = 0) -> None:
    connection.execute(
        "INSERT INTO data_lineage VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [str(uuid.uuid4()), execution_id, batch_id, source_system, source_name,
         transformation_name, target_table, datetime.now(timezone.utc),
         records_read, records_written, records_rejected],
    )


def observe_schema(connection, source_type: str, columns: list[str], types: dict[str, str], required: Iterable[str]) -> dict[str, Any]:
    """Version a file schema and report additive/removal/type drift."""
    ensure_reliability_tables(connection)
    previous = connection.execute(
        "SELECT schema_version, columns_json, types_json, changes_json, backward_compatible FROM schema_registry "
        "WHERE source_type = ? ORDER BY schema_version DESC LIMIT 1", [source_type]
    ).fetchone()
    old_columns = json.loads(previous[1]) if previous else []
    old_types = json.loads(previous[2]) if previous else {}
    if previous and old_columns == columns and old_types == types:
        previous_changes = json.loads(previous[3])
        changed_types = previous_changes.get("changed_types", {})
        incompatible = sorted(set(changed_types) | (set(previous_changes.get("removed", [])) & set(required)))
        return {**previous_changes, "incompatible_columns": incompatible,
                "backward_compatible": bool(previous[4])}
    added = sorted(set(columns) - set(old_columns))
    removed = sorted(set(old_columns) - set(columns))
    changed = {column: {"previous": old_types[column], "observed": types[column]}
               for column in set(types) & set(old_types)
               if old_types[column] not in {types[column], "NULL", "MIXED"}
               and types[column] != "NULL"
               and types[column] != "MIXED"
               and not (old_types[column] == "INT64" and types[column] == "NUMERIC")
               and not (old_types[column] == "DATE" and types[column] == "TIMESTAMP")}
    required_set = set(required)
    incompatible = sorted((set(removed) & required_set) | set(changed))
    changes = {"added": added, "removed": removed, "changed_types": changed,
               "version": 1 if not previous else int(previous[0]) + (old_columns != columns or old_types != types)}
    schema_version = int(changes["version"])
    schema_hash = __import__("hashlib").sha256(
        json.dumps({"columns": columns, "types": types}, sort_keys=True).encode()
    ).hexdigest()
    if not previous or schema_version != int(previous[0]):
        compatible = not incompatible
        connection.execute(
            "INSERT INTO schema_registry VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [source_type, schema_version, schema_hash, json.dumps(columns), json.dumps(types, sort_keys=True),
             json.dumps(changes, sort_keys=True), compatible, datetime.now(timezone.utc)],
        )
        logger.info("Observed source schema", extra={"source_type": source_type, "version": schema_version,
                                                      "added": added, "removed": removed,
                                                      "changed_types": changed, "compatible": compatible})
    return {**changes, "incompatible_columns": incompatible,
            "backward_compatible": not incompatible}


def record_debezium_schema(connection, source_table: str, schema_json: str | None) -> dict[str, Any] | None:
    """Version Debezium's observed `after` record schema from a real CDC event."""
    if not schema_json:
        return None
    try:
        schema = json.loads(schema_json)
    except (TypeError, json.JSONDecodeError):
        return None
    fields = schema.get("fields", []) if isinstance(schema, dict) else []
    after = next((field for field in fields if field.get("field") == "after"), None)
    if after is None:
        return None
    columns = [str(field.get("field")) for field in after.get("fields", []) if field.get("field")]
    types = {str(field["field"]): str(field.get("name") or field.get("type") or "UNKNOWN")
             for field in after.get("fields", []) if field.get("field")}
    required = ("employee_id",) if source_table == "employees" else ()
    return observe_schema(connection, f"cdc:{source_table}", columns, types, required)


def record_batch_audit(connection, *, execution_id: str, batch_id: str, source_name: str,
                       start_time: datetime, end_time: datetime | None, status: str,
                       records_read: int | None = None, records_written: int | None = None,
                       records_rejected: int | None = None, error_message: str | None = None,
                       pipeline_name: str = "streamlake_batch_ingestion") -> None:
    if end_time and start_time.tzinfo is None and end_time.tzinfo is not None:
        end_time = end_time.replace(tzinfo=None)
    elif end_time and start_time.tzinfo is not None and end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=start_time.tzinfo)
    duration = (end_time - start_time).total_seconds() if end_time else None
    connection.execute(
        """INSERT INTO pipeline_audit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT (execution_id) DO UPDATE SET batch_id=excluded.batch_id,
           source_name=excluded.source_name, end_time=excluded.end_time,
           records_read=excluded.records_read, records_written=excluded.records_written,
           records_rejected=excluded.records_rejected, execution_status=excluded.execution_status,
           error_message=excluded.error_message, duration_seconds=excluded.duration_seconds""",
        [execution_id, batch_id, pipeline_name, source_name, start_time, end_time,
         records_read, records_written, records_rejected, status, error_message, duration],
    )
    if end_time:
        if records_read is not None:
            record_metric(connection, pipeline_name, "records_ingested", records_read, {"batch_id": batch_id})
        if records_written is not None:
            record_metric(connection, pipeline_name, "records_processed", records_written, {"batch_id": batch_id})
        if records_rejected is not None:
            record_metric(connection, pipeline_name, "records_rejected", records_rejected, {"batch_id": batch_id})
        record_metric(connection, pipeline_name, "execution_seconds", duration or 0, {"batch_id": batch_id})


def monitoring_snapshot(connection, pipeline_name: str | None = None) -> dict[str, Any]:
    """Read actual persisted metrics, lineage, and audit outcomes."""
    ensure_reliability_tables(connection)
    where = "WHERE pipeline_name = ?" if pipeline_name else ""
    args = [pipeline_name] if pipeline_name else []
    metrics = connection.execute(
        f"""SELECT metric_name, sum(metric_value), avg(metric_value), max(observed_at), count(*)
            FROM pipeline_metrics {where} GROUP BY metric_name ORDER BY metric_name""", args
    ).fetchall()
    failures = connection.execute(
        f"SELECT execution_id, pipeline_name, source_name, end_time, error_message FROM pipeline_audit "
        f"{where + ' AND ' if where else 'WHERE '}execution_status = 'failed' "
        "ORDER BY end_time DESC NULLS LAST LIMIT 20", args
    ).fetchall()
    recent = connection.execute(
        f"SELECT execution_id, pipeline_name, source_name, start_time, end_time, "
        f"records_read, records_written, records_rejected, execution_status, duration_seconds "
        f"FROM pipeline_audit {where} ORDER BY start_time DESC LIMIT 20", args
    ).fetchall()
    return {
        "metrics": [dict(zip(("name", "sum", "average", "last_observed", "samples"), row)) for row in metrics],
        "failures": [dict(zip(("execution_id", "pipeline_name", "source_name", "end_time", "error_message"), row)) for row in failures],
        "recent_executions": [dict(zip(("execution_id", "pipeline_name", "source_name", "start_time", "end_time",
                                        "records_read", "records_written", "records_rejected", "status", "duration_seconds"), row))
                              for row in recent],
    }
