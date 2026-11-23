"""Validate and replay operator-corrected dead-letter records to source topics."""

from __future__ import annotations

import json
from typing import Any, Iterable

from streamlake.ingestion.streaming.cdc import parse_cdc_message


def replay_corrected_records(records: Iterable[dict[str, Any]], producer) -> int:
    """Publish corrected envelopes; reject records lacking provenance or valid CDC shape.

    Each JSONL input row has ``source_topic``, optional ``key``, and corrected
    ``value`` (an object or serialized JSON). The source offset is deliberately
    not reused: Kafka assigns a new offset and the CDC ledger deduplicates event
    IDs on ordinary replay while a corrected envelope receives a fresh ID.
    """
    sent = 0
    delivery_errors = []

    def on_delivery(error, _message):
        if error:
            delivery_errors.append(str(error))

    for row_number, record in enumerate(records, start=1):
        topic = str(record.get("source_topic") or "").strip()
        if not topic or topic.endswith("_dead_letter"):
            raise ValueError(f"record {row_number}: source_topic must name a source topic")
        value = record.get("value")
        if isinstance(value, (dict, list)):
            value = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
        if not isinstance(value, str) or not value:
            raise ValueError(f"record {row_number}: corrected value must be non-empty JSON")
        key = record.get("key")
        event = parse_cdc_message(value, topic=topic, partition=0, offset=0, key=key)
        if event.error_reason or event.tombstone:
            raise ValueError(f"record {row_number}: corrected event is invalid: {event.error_reason or 'tombstone'}")
        producer.produce(topic=topic, key=key, value=value,
                         headers=[("streamlake-dlq-replay", b"corrected")], on_delivery=on_delivery)
        sent += 1
    if sent:
        remaining = producer.flush(30)
        if remaining:
            raise RuntimeError(f"{remaining} corrected dead-letter messages were not delivered")
        if delivery_errors:
            raise RuntimeError("Corrected dead-letter replay failed: " + "; ".join(delivery_errors))
    return sent
