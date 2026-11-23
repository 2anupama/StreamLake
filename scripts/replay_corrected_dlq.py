#!/usr/bin/env python3
"""Publish corrected CDC JSONL records to their original Kafka topics."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from streamlake.common.settings import Settings
from streamlake.ingestion.streaming.dlq import replay_corrected_records


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="JSONL with source_topic, optional key, and corrected value")
    args = parser.parse_args()
    try:
        from confluent_kafka import Producer
    except ImportError as error:
        raise SystemExit("Install the Kafka extra first: pip install -e '.[kafka]'") from error
    settings = Settings()
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers,
                         "enable.idempotence": True, "acks": "all"})
    with args.input.open(encoding="utf-8") as stream:
        records = (json.loads(line) for line in stream if line.strip())
        count = replay_corrected_records(records, producer)
    print(json.dumps({"replayed": count, "input": str(args.input)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
