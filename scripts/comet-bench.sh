#!/usr/bin/env bash
set -euo pipefail

# Measures Vairë's fixed Spark plan set twice inside the cluster image: once on plain Spark, once
# with Apache DataFusion Comet on the classpath. Both runs use the assembled test jar built by
# `sbt spark/assembly`, the Spark distribution in the image, and `--master local[4]` (one JVM, so
# the Scala 3 classpath question stays out of scope).
#
# Usage: scripts/comet-bench.sh
#   VAIRE_BENCH_DIR  host directory for the generated Parquet (default /tmp/vaire-comet); mounted
#                    into the container at the same path.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_DIR/docker/spark-cluster/docker-compose.yml"
IMAGE="vaire-spark-cluster"

BENCH_DIR="${VAIRE_BENCH_DIR:-/tmp/vaire-comet}"
LOG_DIR="$PROJECT_DIR/target/comet-bench-logs"
COMET_JAR="/opt/comet/comet-spark-spark4.1_2.13-1.1.0.jar"
BENCH_JAR_CONT="/opt/vaire/vaire-spark-bench.jar"

mkdir -p "$LOG_DIR" "$BENCH_DIR"

echo "--- Building bench jar (sbt spark/assembly) ---"
(cd "$PROJECT_DIR" && sbt -batch spark/assembly)

BENCH_JAR_HOST=""
while IFS= read -r candidate; do
  BENCH_JAR_HOST="$candidate"
done < <(find "$PROJECT_DIR/target/out/jvm" -name 'vaire-spark-bench.jar' -type f 2>/dev/null | sort)
if [ -z "$BENCH_JAR_HOST" ]; then
  BENCH_JAR_HOST="$(find "$PROJECT_DIR" -name 'vaire-spark-bench.jar' -type f | head -1)"
fi
if [ -z "$BENCH_JAR_HOST" ]; then
  echo "ERROR: vaire-spark-bench.jar not found after assembly." >&2
  exit 1
fi
echo "Bench jar: $BENCH_JAR_HOST"

echo "--- Building cluster image ---"
docker compose -f "$COMPOSE_FILE" build

run_block() {
  local mode="$1"
  shift
  local log="$LOG_DIR/${mode}.log"
  echo "--- Bench run: $mode ---"
  docker run --rm --user 0:0 \
    -v "$BENCH_JAR_HOST:$BENCH_JAR_CONT:ro" \
    -v "$BENCH_DIR:$BENCH_DIR" \
    --entrypoint /opt/spark/bin/spark-submit \
    "$IMAGE" \
    --master local[4] \
    --driver-memory 4g \
    --conf spark.ui.enabled=false \
    --conf spark.driver.host=127.0.0.1 \
    --driver-java-options "-Dvaire.bench.dir=$BENCH_DIR" \
    "$@" \
    --class net.ghoula.vaire.spark.CometBench \
    "$BENCH_JAR_CONT" 2>&1 | tee "$log"
  return "${PIPESTATUS[0]}"
}

echo "--- Control run (no Comet) ---"
if ! run_block off; then
  echo "ERROR: control run failed; refusing to interpret a Comet run." >&2
  exit 1
fi

echo "--- Comet run ---"
if ! run_block on \
  --jars "$COMET_JAR" \
  --driver-class-path "$COMET_JAR" \
  --conf spark.plugins=org.apache.spark.CometPlugin \
  --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
  --conf spark.comet.explain.fallback.enabled=true \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=4g \
  --conf spark.executor.memoryOverhead=2g; then
  echo "ERROR: Comet run failed." >&2
  exit 1
fi

extract_results() {
  awk '/^##RESULT /{
    plan=""; scale=""; ms=""; rows=""; dig=""; ops="";
    for (i = 2; i <= NF; i++) {
      n = split($i, p, "=");
      if (n == 2) {
        if (p[1] == "plan") plan = p[2];
        else if (p[1] == "scale") scale = p[2];
        else if (p[1] == "median_ms") ms = p[2];
        else if (p[1] == "rows") rows = p[2];
        else if (p[1] == "digest") dig = p[2];
        else if (p[1] == "comet_ops") ops = p[2];
      }
    }
    if (plan != "") print plan "|" scale "|" ms "|" rows "|" dig "|" ops;
  }' "$1"
}

extract_results "$LOG_DIR/off.log" > "$LOG_DIR/off.results"
extract_results "$LOG_DIR/on.log" > "$LOG_DIR/on.results"

echo
echo "=================== version lines ==================="
grep '^##VERSION' "$LOG_DIR/off.log" "$LOG_DIR/on.log" || true

echo
echo "=================== per-plan comparison ==================="
awk -F'|' '
  FNR == NR {
    key = $1 "|" $2; off[key] = $0; order[++n] = key; next
  }
  { key = $1 "|" $2; on[key] = $0 }
  END {
    printf "%-15s %9s %11s %11s %9s %9s %9s %-8s %s\n", "plan", "scale", "off_ms", "on_ms", "speedup", "off_rows", "on_rows", "digest", "comet_ops"
    for (i = 1; i <= n; i++) {
      key = order[i]; split(off[key], o, "|"); split(on[key], p, "|");
      sp = (p[3] + 0 > 0) ? (o[3] / p[3]) : 0;
      same = (o[5] == p[5]) ? "match" : "MISMATCH";
      printf "%-15s %9s %11.1f %11.1f %8.2fx %9s %9s %-8s %s\n", o[1], o[2], o[3], p[3], sp, o[4], p[4], same, p[6];
    }
  }' "$LOG_DIR/off.results" "$LOG_DIR/on.results"

echo
echo "=================== plans with no Comet operator ==================="
awk -F'|' '$6 == "-" { print "  " $1 " (scale " $2 ")" }' "$LOG_DIR/on.results"
awk -F'|' '$6 != "-" { found = 1 } END { if (!found) print "  (none: every plan produced a Comet operator)" }' "$LOG_DIR/on.results"

echo
echo "=================== digest verdict ==================="
mismatches="$(awk -F'|' '
  FNR == NR { off[$1 "|" $2] = $5; next }
  { key = $1 "|" $2; if (key in off && off[key] != $5) print $1 " (scale " $2 ")" }
' "$LOG_DIR/off.results" "$LOG_DIR/on.results")"

if [ -n "$mismatches" ]; then
  echo "MISMATCH between Comet off and on for:"
  echo "$mismatches"
  exit 1
fi
echo "All digests match between Comet off and on."

echo
echo "Logs: $LOG_DIR/off.log $LOG_DIR/on.log"
