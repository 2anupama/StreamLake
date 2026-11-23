#!/usr/bin/env bash
set -euo pipefail

bootstrap_servers="kafka:9092"
topics=(
  employee_changes
  dependent_changes
  policy_changes
  company_changes
  employee_dead_letter
  __debezium-heartbeat.streamlake
  streamlake.transaction
)

for topic in "${topics[@]}"; do
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$bootstrap_servers" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1 \
    --config cleanup.policy=delete
done