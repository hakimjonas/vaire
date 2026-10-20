#!/usr/bin/env bash
set -euo pipefail

# Measures Vairë's fixed Spark plan set twice inside the cluster image: once on plain Spark, once
# with Apache DataFusion Comet on the classpath. Both runs use the assembled test jar built by
# `sbt spark/assembly`, the Spark distribution in the image, and `--master local[4]` (one JVM, so
# the Scala 3 classpath question stays out of scope).
#
# Usage: scripts/comet-bench.sh
#   VAIRE_BENCH_DIR          host directory for the generated Parquet (default /tmp/vaire-comet);
#                            mounted into the container at the same path.
#   VAIRE_BENCH_FACT_SIZES   comma-separated fact scales, in block order (default 1000000,5000000).
#                            Reversing it checks that the timings are order-independent.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_DIR/docker/spark-cluster/docker-compose.yml"
IMAGE="vaire-spark-cluster"

BENCH_DIR="${VAIRE_BENCH_DIR:-/tmp/vaire-comet}"
FACT_SIZES="${VAIRE_BENCH_FACT_SIZES:-1000000,5000000}"
LOG_DIR="$PROJECT_DIR/target/comet-bench-logs"
WORK_DIR="$LOG_DIR/work-dir"
COMET_JAR="/opt/comet/comet-spark-spark4.2_2.13-1.2.0-20261020.040418-9.jar"
BENCH_JAR_CONT="/opt/vaire/vaire-spark-bench.jar"

# A ratio is printed only when the Comet-off and Comet-on sample ranges do not overlap and the
# longer median clears this floor; otherwise the difference is inside the run-to-run spread and is
# reported as `unresolvable`.
RES_MS=10

mkdir -p "$LOG_DIR" "$WORK_DIR" "$BENCH_DIR"

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

# Mirror the repo's forked test JVM (build.sbt:147-163): ZGC and the --add-opens set, plus the
# bench-dir property. `--driver-java-options` is applied to the driver JVM by the Spark launcher.
DRIVER_JAVA_OPTIONS=(
  "-Xss4M"
  "-XX:+UseZGC"
  "--add-opens=java.base/java.lang=ALL-UNNAMED"
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
  "--add-opens=java.base/java.io=ALL-UNNAMED"
  "--add-opens=java.base/java.net=ALL-UNNAMED"
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
  "--add-opens=java.base/java.util=ALL-UNNAMED"
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED"
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
  "-Dvaire.bench.dir=$BENCH_DIR"
  "-Dvaire.bench.fact.sizes=$FACT_SIZES"
)

run_block() {
  local mode="$1"
  shift
  local log="$LOG_DIR/${mode}.log"
  echo "--- Bench run: $mode ---"
  docker run --rm --user 0:0 \
    -v "$BENCH_JAR_HOST:$BENCH_JAR_CONT:ro" \
    -v "$BENCH_DIR:$BENCH_DIR" \
    -v "$WORK_DIR:/opt/spark/work-dir" \
    --entrypoint /opt/spark/bin/spark-submit \
    "$IMAGE" \
    --master local[4] \
    --driver-memory 4g \
    --conf spark.ui.enabled=false \
    --conf spark.driver.host=127.0.0.1 \
    --driver-java-options "${DRIVER_JAVA_OPTIONS[*]}" \
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
    plan=""; scale=""; n=""; mn=""; md=""; mx=""; smp=""; rows=""; dig=""; ops="";
    for (i = 2; i <= NF; i++) {
      k = split($i, p, "=");
      if (k == 2) {
        if (p[1] == "plan") plan = p[2];
        else if (p[1] == "scale") scale = p[2];
        else if (p[1] == "n") n = p[2];
        else if (p[1] == "min_ms") mn = p[2];
        else if (p[1] == "median_ms") md = p[2];
        else if (p[1] == "max_ms") mx = p[2];
        else if (p[1] == "samples") smp = p[2];
        else if (p[1] == "rows") rows = p[2];
        else if (p[1] == "digest") dig = p[2];
        else if (p[1] == "comet_ops") ops = p[2];
      }
    }
    if (plan != "") print plan "|" scale "|" n "|" mn "|" md "|" mx "|" smp "|" rows "|" dig "|" ops;
  }' "$1"
}

extract_results "$LOG_DIR/off.log" > "$LOG_DIR/off.results"
extract_results "$LOG_DIR/on.log" > "$LOG_DIR/on.results"

if grep -q '^##WARMUP' "$LOG_DIR/off.results" "$LOG_DIR/on.results"; then
  echo "ERROR: ##WARMUP leaked into the extracted results." >&2
  exit 1
fi

echo
echo "=================== version lines ==================="
grep '^##VERSION' "$LOG_DIR/off.log" "$LOG_DIR/on.log" || true
echo "Comet native library lines in the control log: $(grep -c 'Comet native library' "$LOG_DIR/off.log" || true)"
echo "##WARMUP lines (excluded from every table): off=$(grep -c '^##WARMUP' "$LOG_DIR/off.log" || true), on=$(grep -c '^##WARMUP' "$LOG_DIR/on.log" || true)"

echo
echo "=================== per-plan comparison ==================="
awk -F'|' -v resms="$RES_MS" '
  FNR == NR { key = $1 "|" $2; off[key] = $0; order[++n] = key; next }
  { key = $1 "|" $2; on[key] = $0 }
  END {
    printf "%-13s %9s %9s %9s %9s %9s %9s %9s %-12s %-6s %s\n", "plan", "scale", "off_min", "off_med", "off_max", "on_min", "on_med", "on_max", "speedup", "digest", "comet_ops"
    for (i = 1; i <= n; i++) {
      key = order[i]; split(off[key], o, "|"); split(on[key], p, "|");
      ops = p[10];
      disjoint = (o[4] + 0 > p[6] + 0) || (p[4] + 0 > o[6] + 0)
      mag = (o[5] + 0 > p[5] + 0) ? o[5] + 0 : p[5] + 0
      if (ops == "-") {
        sp = "n/a"
      } else if (disjoint && mag >= resms) {
        sp = sprintf("%.2fx", o[5] / p[5])
      } else {
        sp = "unresolvable"
      }
      same = (o[9] == p[9]) ? "match" : "MISMATCH"
      printf "%-13s %9s %9s %9s %9s %9s %9s %9s %-12s %-6s %s\n", o[1], o[2], o[4], o[5], o[6], p[4], p[5], p[6], sp, same, ops
    }
  }' "$LOG_DIR/off.results" "$LOG_DIR/on.results"
echo "(a ratio needs disjoint off/on sample ranges and a longer median >= ${RES_MS} ms; otherwise 'unresolvable')"

echo
echo "=================== measured samples (off | on) ==================="
awk -F'|' '
  FNR == NR { key = $1 "|" $2; off[key] = $7; order[++n] = key; next }
  { key = $1 "|" $2; on[key] = $7 }
  END {
    for (i = 1; i <= n; i++) {
      key = order[i]
      printf "%-13s %9s  off[%s]  on[%s]\n", key, "", off[key], on[key]
    }
  }' "$LOG_DIR/off.results" "$LOG_DIR/on.results"

echo
echo "=================== window tie check ==================="
grep '^##CHECK' "$LOG_DIR/off.log" "$LOG_DIR/on.log" || true

echo
echo "=================== plans with no Comet operator ==================="
awk -F'|' '$10 == "-" { print "  " $1 " (scale " $2 ")" }' "$LOG_DIR/on.results"
awk -F'|' '$10 != "-" { found = 1 } END { if (!found) print "  (none: every plan produced a Comet operator)" }' "$LOG_DIR/on.results"

echo
echo "=================== digest verdict ==================="
mismatches="$(awk -F'|' '
  FNR == NR { off[$1 "|" $2] = $9; next }
  { key = $1 "|" $2; if (key in off && off[key] != $9) print $1 " (scale " $2 ")" }
' "$LOG_DIR/off.results" "$LOG_DIR/on.results")"

failures="$(grep -h '^##CHECK.*ok=false' "$LOG_DIR/off.log" "$LOG_DIR/on.log" || true)"

if [ -n "$mismatches" ]; then
  echo "MISMATCH between Comet off and on for:"
  echo "$mismatches"
  exit 1
fi
if [ -n "$failures" ]; then
  echo "CHECK FAILED:"
  echo "$failures"
  exit 1
fi
echo "All digests match between Comet off and on; all plan checks passed."

echo
echo "Logs: $LOG_DIR/off.log $LOG_DIR/on.log"
