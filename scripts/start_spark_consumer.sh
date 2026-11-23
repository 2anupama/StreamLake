#!/usr/bin/env bash
set -euo pipefail

docker compose --profile streaming up -d --build spark-consumer
docker compose logs --tail=40 spark-consumer