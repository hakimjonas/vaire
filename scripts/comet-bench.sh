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

# The ratio uses exec_ms. A plan is overhead-dominated when its Comet-off exec median grows by less
# than this factor from the smaller to the larger reported scale (its work does not scale). A ratio
# is printed only when the longer exec median clears RES_MS and the off/on exec sample ranges are
# disjoint; otherwise the row is `unresolvable`.
GROWTH_FACTOR=1.5
RES_MS=50

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
# bench-dir and scale-order properties. `--driver-java-options` is applied to the driver JVM by the
# Spark launcher.
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
  --conf spark.comet.explain.fallback.log.enabled=true \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=4g \
  --conf spark.executor.memoryOverhead=2g; then
  echo "ERROR: Comet run failed." >&2
  exit 1
fi

extract_results() {
  awk '/^##RESULT /{
    plan=""; scale=""; n=""; cmin=""; cmed=""; cmax=""; csmp=""; emin=""; emed=""; emax=""; esmp=""; rows=""; dig=""; ops="";
    for (i = 2; i <= NF; i++) {
      k = split($i, p, "=");
      if (k == 2) {
        if (p[1] == "plan") plan = p[2];
        else if (p[1] == "scale") scale = p[2];
        else if (p[1] == "n") n = p[2];
        else if (p[1] == "collect_min_ms") cmin = p[2];
        else if (p[1] == "collect_median_ms") cmed = p[2];
        else if (p[1] == "collect_max_ms") cmax = p[2];
        else if (p[1] == "collect_samples") csmp = p[2];
        else if (p[1] == "exec_min_ms") emin = p[2];
        else if (p[1] == "exec_median_ms") emed = p[2];
        else if (p[1] == "exec_max_ms") emax = p[2];
        else if (p[1] == "exec_samples") esmp = p[2];
        else if (p[1] == "rows") rows = p[2];
        else if (p[1] == "digest") dig = p[2];
        else if (p[1] == "comet_ops") ops = p[2];
      }
    }
    if (plan != "")
      print plan "|" scale "|" n "|" cmin "|" cmed "|" cmax "|" csmp "|" emin "|" emed "|" emax "|" esmp "|" rows "|" dig "|" ops;
  }' "$1"
}

extract_results "$LOG_DIR/off.log" > "$LOG_DIR/off.results"
extract_results "$LOG_DIR/on.log" > "$LOG_DIR/on.results"

if grep -q '^##WARMUP' "$LOG_DIR/off.results" "$LOG_DIR/on.results"; then
  echo "ERROR: ##WARMUP leaked into the extracted results." >&2
  exit 1
fi

# Window the Comet driver log between ##PLAN-BEGIN/##PLAN-END and harvest the `[COMET: ...]`
# reasons. A reason naming the harness's own write path is counted as instrument, not a plan
# property; this rule is stated in the output below.
fallback_inventory() {
  awk -F'|' '
    FNR == NR {
      key = $1 "|" $2
      claimed[key] = ($14 == "-") ? 0 : split($14, x, ",")
      order[++n] = key
      next
    }
    /^##PLAN-BEGIN plan=/ {
      plan = ""; scale = ""
      nf = split($0, f, " ")
      for (i = 1; i <= nf; i++) { split(f[i], q, "="); if (q[1] == "plan") plan = q[2]; else if (q[1] == "scale") scale = q[2] }
      inwin = 1; next
    }
    /^##PLAN-END plan=/ { inwin = 0; next }
    inwin && /\[COMET: / {
      line = $0; key = plan "|" scale
      while (match(line, /\[COMET: [^]]*\]/)) {
        r = substr(line, RSTART + 8, RLENGTH - 9)
        if (!(key SUBSEP r in seen)) {
          seen[key SUBSEP r] = 1
          if (r ~ /WriteFiles|DataWritingCommand|ExecutedCommand|InsertIntoHadoopFsRelation|NoopDataSource|noop/) instrument[key]++
          else { reasons[key]++; detail[key] = detail[key] (detail[key] == "" ? "" : "; ") r }
        }
        line = substr(line, RSTART + RLENGTH)
      }
    }
    END {
      for (i = 1; i <= n; i++) {
        key = order[i]; split(key, ks, "|")
        printf "##FALLBACK plan=%s scale=%s claimed=%d reasons=%d instrument=%d detail=\"%s\"\n",
          ks[1], ks[2], claimed[key], (key in reasons ? reasons[key] : 0),
          (key in instrument ? instrument[key] : 0), (key in detail ? detail[key] : "")
      }
    }' "$LOG_DIR/on.results" "$LOG_DIR/on.log"
}

fallback_inventory > "$LOG_DIR/on.fallback"

echo
echo "=================== version lines ==================="
grep '^##VERSION' "$LOG_DIR/off.log" "$LOG_DIR/on.log" || true
echo "Comet native library lines in the control log: $(grep -c 'Comet native library' "$LOG_DIR/off.log" || true)"
echo "##WARMUP lines (excluded from every table): off=$(grep -c '^##WARMUP' "$LOG_DIR/off.log" || true), on=$(grep -c '^##WARMUP' "$LOG_DIR/on.log" || true)"

echo
echo "=================== rules ==================="
echo "ratio column = exec_ms (whole plan forced by a noop write, nothing returned to the driver); collect_ratio is the 789235e8-style collect_ms ratio, shown for continuity only."
echo "row decision order: (1) comet_ops=- -> n/a; (2) plan overhead-dominated when its Comet-off exec median grows < ${GROWTH_FACTOR}x from the smaller to the larger scale; (3) longer exec median < ${RES_MS} ms -> unresolvable; (4) off/on exec sample ranges disjoint -> off/on ratio, else unresolvable."
echo "instrument rule: a [COMET: ...] reason whose text names WriteFiles/DataWritingCommand/ExecutedCommand/InsertIntoHadoopFsRelation/NoopDataSource/noop is the harness write, counted as instrument=, not reasons=."

echo
echo "=================== per-plan growth (Comet-off exec median) ==================="
awk -F'|' -v growth="$GROWTH_FACTOR" '
  {
    p = $1; s = $2 + 0; e = $9 + 0
    if (!(p in smin) || s < smin[p]) { smin[p] = s; emin_of[p] = e }
    if (!(p in smax) || s > smax[p]) { smax[p] = s; emax_of[p] = e }
    plan[p] = 1
  }
  END {
    for (p in plan) {
      if (smax[p] > smin[p]) {
        g = emax_of[p] / emin_of[p]
        printf "  %-14s %d->%d  growth=%.2fx  %s\n", p, smin[p], smax[p], g, (g < growth ? "OVERHEAD-DOMINATED" : "scales")
      } else {
        printf "  %-14s single scale  growth=n/a\n", p
      }
    }
  }' "$LOG_DIR/off.results"

echo
echo "=================== per-plan comparison ==================="
awk -F'|' -v growth="$GROWTH_FACTOR" -v resms="$RES_MS" '
  FNR == NR {
    key = $1 "|" $2; off[key] = $0; order[++n] = key
    p = $1; s = $2 + 0; e = $9 + 0
    if (!(p in smin) || s < smin[p]) { smin[p] = s; emin_of[p] = e }
    if (!(p in smax) || s > smax[p]) { smax[p] = s; emax_of[p] = e }
    next
  }
  { on[$1 "|" $2] = $0 }
  END {
    for (p in smin) if (smax[p] > smin[p]) overhead[p] = (emax_of[p] / emin_of[p] < growth)
    printf "%-14s %8s | %-20s %-20s | %-16s %-8s %-6s %s\n", "plan", "scale", "off_exec[min,med,max]", "on_exec[min,med,max]", "exec_decision", "col_ratio", "digest", "comet_ops"
    for (i = 1; i <= n; i++) {
      key = order[i]; split(off[key], o, "|"); split(on[key], onf, "|")
      ops = onf[14]
      mag = (o[9] + 0 > onf[9] + 0) ? o[9] + 0 : onf[9] + 0
      disjoint = (o[8] + 0 > onf[10] + 0) || (onf[8] + 0 > o[10] + 0)
      if (ops == "-") decision = "n/a"
      else if (overhead[o[1]]) decision = "overhead-dominated"
      else if (mag < resms) decision = "unresolvable (small)"
      else if (disjoint) decision = sprintf("%.2fx", o[9] / onf[9])
      else decision = "unresolvable"
      cratio = (ops == "-") ? "n/a" : sprintf("%.2fx", o[5] / onf[5])
      printf "%-14s %8s | %5.1f[%5.1f,%6.1f]     %5.1f[%5.1f,%6.1f]     | %-16s %-8s %-6s %s\n",
        o[1], o[2], o[8], o[9], o[10], onf[8], onf[9], onf[10], decision, cratio, ((o[13] == onf[13]) ? "match" : "MISMATCH"), ops
    }
  }' "$LOG_DIR/off.results" "$LOG_DIR/on.results"

echo
echo "=================== exec growth check ==================="
awk -F'|' -v growth="$GROWTH_FACTOR" '
  {
    p = $1; s = $2 + 0; e = $9 + 0
    if (!(p in smin) || s < smin[p]) { smin[p] = s; emin_of[p] = e }
    if (!(p in smax) || s > smax[p]) { smax[p] = s; emax_of[p] = e }
    plan[p] = 1
  }
  END {
    bad = 0
    for (p in plan) if (smax[p] > smin[p]) {
      g = emax_of[p] / emin_of[p]
      if (g >= growth) printf "  %-14s exec grows %.2fx with scale  OK\n", p, g
      else { printf "  %-14s exec grows %.2fx (< %.2fx)  overhead-dominated, ratio suppressed\n", p, g, growth; bad++ }
    }
    if (bad > 0) printf "  (%d plan(s) did not grow and are classified overhead-dominated)\n", bad
  }' "$LOG_DIR/on.results"

echo
echo "=================== comet fallback inventory ==================="
cat "$LOG_DIR/on.fallback"

echo
echo "=================== window tie check ==================="
grep '^##CHECK' "$LOG_DIR/off.log" "$LOG_DIR/on.log" || true

echo
echo "=================== plans with no Comet operator ==================="
awk -F'|' '$14 == "-" { print "  " $1 " (scale " $2 ")" }' "$LOG_DIR/on.results"
awk -F'|' '$14 != "-" { found = 1 } END { if (!found) print "  (none: every plan produced a Comet operator)" }' "$LOG_DIR/on.results"

echo
echo "=================== digest verdict ==================="
mismatches="$(awk -F'|' '
  FNR == NR { off[$1 "|" $2] = $13; next }
  { key = $1 "|" $2; if (key in off && off[key] != $13) print $1 " (scale " $2 ")" }
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

# Validation: time-bucket leaves time_bucket on Spark, so it must show at least one non-instrument
# fallback reason. Zero means the windowing is broken and the inventory is not trustworthy.
tb_reasons="$(sed -n 's/^##FALLBACK plan=time-bucket .* reasons=\([0-9][0-9]*\) .*/\1/p' "$LOG_DIR/on.fallback" | head -1)"
if [ -z "$tb_reasons" ] || [ "$tb_reasons" -lt 1 ]; then
  echo "STOP: time-bucket shows no non-instrument fallback reason; the log windowing is not validated." >&2
  echo "raw window lines:" >&2
  awk '/^##PLAN-BEGIN plan=time-bucket /{p=1} p{print} /^##PLAN-END plan=time-bucket /{p=0}' "$LOG_DIR/on.log" | grep '\[COMET: ' >&2 || true
  exit 1
fi
echo "time-bucket fallback validation: reasons=$tb_reasons"

echo
echo "Logs: $LOG_DIR/off.log $LOG_DIR/on.log"
