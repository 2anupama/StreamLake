"""Kafka offset lag collected from real broker end offsets."""

from __future__ import annotations

from typing import Callable, Iterable


def collect_partition_lag(bootstrap_servers: str, consumed_offsets: Iterable[tuple[str, int, int]],
                          *, admin_client_factory: Callable | None = None,
                          topic_partition_factory: Callable | None = None,
                          latest_offset_factory: Callable | None = None) -> list[dict[str, int | str]]:
    """Compare batch-consumed offsets with broker latest offsets per partition.

    Broker/API errors are propagated to the caller so the monitoring layer can
    record collection failures instead of showing a fabricated zero lag.
    """
    if admin_client_factory is None or topic_partition_factory is None or latest_offset_factory is None:
        try:
            from confluent_kafka import TopicPartition
            from confluent_kafka.admin import AdminClient, OffsetSpec
        except ImportError as error:
            raise RuntimeError("Kafka lag collection requires the streaming extra (confluent-kafka)") from error
        topic_partition_factory = topic_partition_factory or TopicPartition
        latest_offset_factory = latest_offset_factory or OffsetSpec.latest
        admin_client_factory = admin_client_factory or AdminClient

    consumed: dict[tuple[str, int], int] = {}
    for topic, partition, offset in consumed_offsets:
        key = (str(topic), int(partition))
        consumed[key] = max(consumed.get(key, -1), int(offset))
    if not consumed:
        return []
    admin = admin_client_factory({"bootstrap.servers": bootstrap_servers})
    requested = {
        topic_partition_factory(topic, partition): latest_offset_factory()
        for topic, partition in consumed
    }
    futures = admin.list_offsets(requested, request_timeout=10)
    result = []
    for topic_partition, future in futures.items():
        end_offset = int(future.result().offset)
        last_processed = consumed[(topic_partition.topic, topic_partition.partition)]
        result.append({"topic": topic_partition.topic, "partition": int(topic_partition.partition),
                       "consumed_offset": last_processed, "end_offset": end_offset,
                       "lag_records": max(0, end_offset - last_processed - 1)})
    return result
