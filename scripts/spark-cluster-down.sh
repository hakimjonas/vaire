#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_DIR="$(cd "$SCRIPT_DIR/../docker/spark-cluster" && pwd)"

echo "--- Stopping Spark cluster ---"
docker compose -f "$COMPOSE_DIR/docker-compose.yml" down

echo "--- Spark cluster stopped ---"
