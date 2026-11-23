"""Pure parsing and state-transition helpers for Debezium CDC envelopes."""

from __future__ import annotations

import json
import socket
import base64
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

SUPPORTED_OPERATIONS = {"c", "u", "d", "r"}
TOPIC_TABLES = {
    "employee_changes": "employees",
    "dependent_changes": "dependents",
    "policy_changes": "policies",
    "company_changes": "companies",
}


@dataclass(frozen=True)
class CDCEvent:
    event_id: str
    topic: str
    partition: int
    offset: int
    kafka_timestamp_ms: int | None
    raw_key: str | None
    raw_value: str | None
    operation: str | None
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    source: dict[str, Any]
    schema: dict[str, Any] | None
    transaction: dict[str, Any] | None
    table: str
    employee_id: str | None
    event_timestamp_ms: int | None
    source_lsn: int | None
    error_reason: str | None = None
    tombstone: bool = False

    @property
    def order_key(self) -> tuple[int, int, int, int, int]:
        transaction_order = _integer((self.transaction or {}).get("total_order")) or 0
        return (
            self.source_lsn or 0,
            self.event_timestamp_ms or 0,
            transaction_order,
            self.partition,
            self.offset,
        )

    def to_record(self) -> dict[str, Any]:
        """Return a JSON-safe row with raw payload and extracted CDC metadata."""
        return {
            "event_id": self.event_id,
            "topic": self.topic,
            "partition": self.partition,
            "offset": self.offset,
            "kafka_timestamp_ms": self.kafka_timestamp_ms,
            "raw_key": self.raw_key,
            "raw_payload": self.raw_value,
            "operation": self.operation,
            "before_json": _json_or_none(self.before),
            "after_json": _json_or_none(self.after),
            "source_json": json.dumps(self.source, separators=(",", ":"), default=str),
            "schema_json": _json_or_none(self.schema),
            "transaction_json": _json_or_none(self.transaction),
            "source_name": self.source.get("name"),
            "source_database": self.source.get("db"),
            "source_schema": self.source.get("schema"),
            "source_table": self.source.get("table") or self.table,
            "source_lsn": self.source_lsn,
            "event_timestamp_ms": self.event_timestamp_ms,
            "error_reason": self.error_reason,
            "is_tombstone": self.tombstone,
        }


def parse_cdc_message(
    value: bytes | str | None,
    *,
    topic: str,
    partition: int,
    offset: int,
    kafka_timestamp_ms: int | None = None,
    key: bytes | str | None = None,
) -> CDCEvent:
    """Parse one Kafka value, retaining malformed and tombstone records."""
    raw_key = _decode(key)
    raw_value = _decode(value)
    table = TOPIC_TABLES.get(topic, "unknown")
    event_id = f"{topic}:{partition}:{offset}"
    if value is None:
        key_record = _decode_json(raw_key)
        key_payload = key_record.get("payload", key_record) if isinstance(key_record, dict) else {}
        employee_id = key_payload.get("employee_id") if isinstance(key_payload, dict) else None
        return CDCEvent(
            event_id, topic, partition, offset, kafka_timestamp_ms, raw_key, None,
            "t", None, None, {}, None, None, table,
            str(employee_id) if employee_id is not None else None,
            kafka_timestamp_ms, None, tombstone=True,
        )

    try:
        envelope = json.loads(raw_value or "")
        if not isinstance(envelope, dict):
            raise ValueError("CDC message must be a JSON object")
        payload = envelope.get("payload", envelope)
        if not isinstance(payload, dict):
            raise ValueError("CDC payload must be a JSON object")
        operation = payload.get("op")
        if operation not in SUPPORTED_OPERATIONS:
            raise ValueError(f"Unsupported Debezium operation: {operation!r}")
        before, after = payload.get("before"), payload.get("after")
        source = payload.get("source") or {}
        transaction = payload.get("transaction")
        if not isinstance(source, dict):
            raise ValueError("CDC source metadata must be a JSON object")
        if before is not None and not isinstance(before, dict):
            raise ValueError("CDC before value must be an object or null")
        if after is not None and not isinstance(after, dict):
            raise ValueError("CDC after value must be an object or null")
        value_schema = envelope.get("schema")
        before = _decode_row(before, value_schema, "before")
        after = _decode_row(after, value_schema, "after")
        event_row = after or before or {}
        key_record = _decode_json(raw_key)
        key_payload = key_record.get("payload", key_record) if isinstance(key_record, dict) else {}
        employee_id = event_row.get("employee_id") if isinstance(event_row, dict) else None
        if employee_id is None and isinstance(key_payload, dict):
            employee_id = key_payload.get("employee_id")
        event_table = str(source.get("table") or table)
        if event_table == "employees" and employee_id is None:
            raise ValueError("Employee CDC event is missing employee_id")
        event_timestamp = (
            _integer(source.get("ts_ms"))
            or _integer(payload.get("ts_ms"))
            or kafka_timestamp_ms
        )
        return CDCEvent(
            event_id=event_id,
            topic=topic,
            partition=partition,
            offset=offset,
            kafka_timestamp_ms=kafka_timestamp_ms,
            raw_key=raw_key,
            raw_value=raw_value,
            operation=operation,
            before=before,
            after=after,
            source=source,
            schema=value_schema,
            transaction=transaction if isinstance(transaction, dict) else None,
            table=event_table,
            employee_id=str(employee_id) if employee_id is not None else None,
            event_timestamp_ms=event_timestamp,
            source_lsn=parse_lsn(source.get("lsn")),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as error:
        return CDCEvent(
            event_id, topic, partition, offset, kafka_timestamp_ms, raw_key, raw_value,
            None, None, None, {}, None, None, table, None, kafka_timestamp_ms, None,
            error_reason=str(error),
        )


def parse_lsn(value: Any) -> int | None:
    """Convert common Debezium/PostgreSQL LSN forms to a sortable integer."""
    if isinstance(value, str):
        value = value.strip()
        if value.startswith("["):
            try:
                parts = json.loads(value)
                value = parts[-1] if parts else None
            except json.JSONDecodeError:
                return None
        if isinstance(value, str) and "/" in value:
            high, low = value.split("/", maxsplit=1)
            try:
                return (int(high, 16) << 32) + int(low, 16)
            except ValueError:
                return None
    return _integer(value)


def apply_employee_event(
    current: dict[str, Any] | None,
    event: CDCEvent,
) -> tuple[dict[str, Any] | None, str]:
    """Apply an employee event only if its source order is newer than state."""
    if event.error_reason or event.tombstone:
        return current, "invalid" if event.error_reason else "tombstone"
    if event.table != "employees" or event.employee_id is None:
        return current, "not_employee"
    if current and event.order_key <= tuple(current["order_key"]):
        return current, "stale"

    current_data = dict((current or {}).get("data", {}))
    if event.operation == "d":
        current_data.update(event.before or {})
        status = "deleted"
    else:
        current_data.update(event.after or {})
        status = "active"
    current_data["employee_id"] = event.employee_id
    return {
        "employee_id": event.employee_id,
        "record_status": status,
        "data": current_data,
        "last_operation": event.operation,
        "event_id": event.event_id,
        "source_lsn": event.source_lsn,
        "event_timestamp_ms": event.event_timestamp_ms,
        "topic": event.topic,
        "partition": event.partition,
        "offset": event.offset,
        "order_key": event.order_key,
    }, "applied"


def checkpoint_location(root: str, query_name: str = "employee-cdc") -> str:
    """Return a stable checkpoint directory for restart recovery."""
    if not query_name or "/" in query_name or ".." in query_name:
        raise ValueError("query_name must be a simple non-empty path component")
    return f"{root.rstrip('/')}/{query_name}"


def kafka_is_reachable(host: str, port: int, timeout: float = 1.0) -> bool:
    """Return whether a TCP connection to the configured Kafka broker succeeds."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _integer(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _decode(value: bytes | str | None) -> str | None:
    if value is None:
        return None
    return value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value)


def _decode_json(value: str | None) -> Any:
    if not value:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def _json_or_none(value: Any) -> str | None:
    return json.dumps(value, separators=(",", ":"), default=str) if value is not None else None


def _decode_row(
    row: dict[str, Any] | None,
    envelope_schema: dict[str, Any] | None,
    envelope_field: str,
) -> dict[str, Any] | None:
    if row is None or not isinstance(envelope_schema, dict):
        return row
    field_schema = next(
        (
            item
            for item in envelope_schema.get("fields", [])
            if item.get("field") == envelope_field
        ),
        None,
    )
    if not isinstance(field_schema, dict):
        return row
    schemas = {
        item.get("field"): item
        for item in field_schema.get("fields", [])
        if isinstance(item, dict) and item.get("field")
    }
    return {name: _decode_value(value, schemas.get(name)) for name, value in row.items()}


def _decode_value(value: Any, schema: dict[str, Any] | None) -> Any:
    if value is None or not isinstance(schema, dict):
        return value
    logical_type = schema.get("name")
    if logical_type == "io.debezium.time.Date":
        try:
            return (date(1970, 1, 1) + timedelta(days=int(value))).isoformat()
        except (TypeError, ValueError, OverflowError):
            return value
    if logical_type == "org.apache.kafka.connect.data.Decimal" and isinstance(value, str):
        try:
            unscaled = int.from_bytes(base64.b64decode(value, validate=True), "big", signed=True)
            scale = int(schema.get("parameters", {}).get("scale", 0))
            return format(Decimal(unscaled).scaleb(-scale), "f")
        except (ValueError, TypeError, OverflowError):
            return value
    if logical_type in {"io.debezium.time.Timestamp", "org.apache.kafka.connect.data.Timestamp"}:
        try:
            return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc).isoformat()
        except (TypeError, ValueError, OverflowError, OSError):
            return value
    if logical_type in {"io.debezium.time.MicroTimestamp", "io.debezium.time.ZonedTimestamp"}:
        if logical_type == "io.debezium.time.ZonedTimestamp" or isinstance(value, str):
            return value
        try:
            return datetime.fromtimestamp(int(value) / 1_000_000, tz=timezone.utc).isoformat()
        except (TypeError, ValueError, OverflowError, OSError):
            return value
    return value