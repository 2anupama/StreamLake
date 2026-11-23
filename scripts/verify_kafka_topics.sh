#!/usr/bin/env bash
set -euo pipefail

topics=(
  employee_changes
  dependent_changes
  policy_changes
  company_changes
  employee_dead_letter
)

for topic in "${topics[@]}"; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 \
    --describe \
    --topic "$topic"
done

python scripts/verify_connector.py