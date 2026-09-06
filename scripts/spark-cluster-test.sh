#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_DIR="$PROJECT_DIR/docker/spark-cluster"

cleanup() {
  echo "--- Tearing down Spark cluster ---"
  docker compose -f "$COMPOSE_DIR/docker-compose.yml" down
}
trap cleanup EXIT

echo "--- Compiling project ---"
(cd "$PROJECT_DIR" && sbt compile Test/compile)

echo "--- Building Docker image ---"
docker compose -f "$COMPOSE_DIR/docker-compose.yml" build

echo "--- Starting Spark cluster ---"
docker compose -f "$COMPOSE_DIR/docker-compose.yml" up -d

echo "--- Waiting for master to accept connections ---"
for i in $(seq 1 30); do
  if curl -sf http://127.0.0.1:8080 > /dev/null 2>&1; then
    echo "Master is up."
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "ERROR: Spark master did not start in time." >&2
    exit 1
  fi
  sleep 1
done

echo "--- Waiting for 2 workers to register ---"
for i in $(seq 1 30); do
  alive=$(curl -sf http://127.0.0.1:8080/json/ 2>/dev/null | grep -o '"ALIVE"' | wc -l || echo 0)
  if [ "$alive" -ge 2 ]; then
    echo "2 workers registered."
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "ERROR: Workers did not register in time." >&2
    exit 1
  fi
  sleep 1
done

echo "--- Running Spark tests against cluster ---"
(cd "$PROJECT_DIR" && sbt -Dspark.test.master=spark://127.0.0.1:7077 spark/test)

echo "--- All tests passed ---"
