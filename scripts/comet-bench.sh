#!/usr/bin/env bash
set -euo pipefail

# Measures Vairë's fixed Spark plan set inside the cluster image: twice on plain Spark and twice
# with Apache DataFusion Comet on the classpath, each pair at the default fact-scale order and at
# the reversed order. The two repeats per arm and order give the order-effect test a within-order
# spread to compare against, and mirror the four configurations the previous increment measured.
# Every arm uses the assembled test jar built by `sbt spark/assembly`, the Spark distribution in
# the image, and `--master local[4]` (one JVM, so the Scala 3 classpath question stays out of
# scope).
#
# The two orders exist so the harness can test for a scale-order effect instead of requiring a
# fixed cross-run ratio tolerance (see the order-effect table).
#
# Plan and inventory caveat: `comet_ops` and the fallback inventory are read from
# `df.queryExecution`, the read plan. `exec_ms` times a separate `noop` write whose child is that
# read plan, so the timing describes the write plan while the inventory describes the read plan.
# `collect_ms` and the digest come from `df.collect()`, also the read plan.
#
# Usage:
#   scripts/comet-bench.sh [bench|snapshot|check] [--reuse] [--inventory PATH]
#
#   bench      (default) build and run, print the tables, write artifacts under
#              target/comet-bench-logs/ (gitignored). Does not touch the baseline.
#   snapshot   as `bench`, then rewrite comet-coverage.json from the run's inventory.
#   check      as `bench`, then compare the run's inventory against comet-coverage.json and exit
#              non-zero on a regression.
#   --reuse    skip the sbt build and the container runs; use the artifacts already under
#              target/comet-bench-logs/.
#   --inventory PATH
#              use PATH (a file in the comet-coverage.json schema) as the run inventory instead of
#              the one harvested from the logs. Implies --reuse. Used to check a perturbed copy of
#              the baseline without a full run.
#
# Environment:
#   VAIRE_BENCH_DIR          host directory for the generated Parquet (default /tmp/vaire-comet);
#                            mounted into the container at the same path.
#   VAIRE_BENCH_FACT_SIZES   comma-separated fact scales, in the default block order (default
#                            1000000,5000000). The reverse of this list is run as the second order.
#   VAIRE_BENCH_DRIVER_MEMORY  Spark driver heap (default 4g). Raise it for a wider local run.
#   VAIRE_BENCH_OFFHEAP_SIZE   Spark off-heap size (default 4g).
#   VAIRE_BENCH_EXECUTOR_OVERHEAD  Spark executor memory overhead (default 2g).
#   VAIRE_BENCH_COLLECT      set to 1 to also measure collect_ms (default 0). collect_ms
#                            materialises the whole result on the driver; the default run never does.
# The three memory settings exist so a local run can raise them without touching this file or the
# workflow; their defaults are unchanged.
#
# Ratchet vs measurement: the digest, row count and window check come from one streaming pass over
# the result (toLocalIterator), outside every timed region. collect_ms is a continuity column,
# measured only when VAIRE_BENCH_COLLECT=1, and the collect_ratio column is printed only then.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_DIR/docker/spark-cluster/docker-compose.yml"
IMAGE="vaire-spark-cluster"

BENCH_DIR="${VAIRE_BENCH_DIR:-/tmp/vaire-comet}"
FACT_SIZES="${VAIRE_BENCH_FACT_SIZES:-1000000,5000000}"
DRIVER_MEMORY="${VAIRE_BENCH_DRIVER_MEMORY:-4g}"
OFFHEAP_SIZE="${VAIRE_BENCH_OFFHEAP_SIZE:-4g}"
EXECUTOR_OVERHEAD="${VAIRE_BENCH_EXECUTOR_OVERHEAD:-2g}"
COLLECT_ENABLED="${VAIRE_BENCH_COLLECT:-0}"
LOG_DIR="$PROJECT_DIR/target/comet-bench-logs"
WORK_DIR="$LOG_DIR/work-dir"
BASELINE_FILE="$PROJECT_DIR/comet-coverage.json"

# Pinned Comet snapshot, sha1-gated at image build (docker/spark-cluster/Dockerfile). The harness
# records the jar filename and the sha1 read back from the image so the baseline cannot be
# compared against a different Comet build.
COMET_VERSION="1.2.0-20261020.040418-9"
COMET_SHA1="fc71728177ff7293f21c6a01f50ace357e1b62c7"
COMET_JAR_BASE="comet-spark-spark4.2_2.13-${COMET_VERSION}.jar"
COMET_JAR="/opt/comet/${COMET_JAR_BASE}"
BENCH_JAR_CONT="/opt/vaire/vaire-spark-bench.jar"

# The ratio uses exec_ms. A plan is overhead-dominated when its Comet-off exec median grows by less
# than this factor from the smaller to the larger reported scale (its work does not scale). A ratio
# is printed only when the longer exec median clears RES_MS, the off/on exec sample ranges are
# disjoint, and the longer arm's sample spread (max - min) is at most SPREAD_FRACTION of its median;
# the spread bound is the precision criterion (see the rules output for the justification).
GROWTH_FACTOR=1.5
RES_MS=50
SPREAD_FRACTION=0.10

MODE="bench"
REUSE=0
INVENTORY_OVERRIDE=""
while [ $# -gt 0 ]; do
  case "$1" in
    bench|snapshot|check) MODE="$1" ;;
    --reuse) REUSE=1 ;;
    --inventory)
      shift
      if [ $# -eq 0 ]; then echo "ERROR: --inventory needs a path." >&2; exit 2; fi
      INVENTORY_OVERRIDE="$1"
      REUSE=1
      ;;
    *)
      echo "ERROR: unknown argument: $1" >&2
      exit 2
      ;;
  esac
  shift
done

reverse_csv() {
  local IFS=','
  local -a arr=($1)
  local out=""
  local i
  for ((i = ${#arr[@]} - 1; i >= 0; i--)); do out+="${out:+,}${arr[i]}"; done
  printf '%s' "$out"
}

REVERSED_SIZES="$(reverse_csv "$FACT_SIZES")"

mkdir -p "$LOG_DIR" "$WORK_DIR" "$BENCH_DIR"

# Mirror the repo's forked test JVM (build.sbt Test/javaOptions): ZGC and the --add-opens set, plus
# the bench-dir property. The scale order is appended per run. `--driver-java-options` is applied to
# the driver JVM by the Spark launcher.
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
)

BENCH_JAR_HOST=""
ACTUAL_SHA1="$COMET_SHA1"

run_block() {
  local mode="$1"
  local logname="$2"
  local sizes="$3"
  local log="$LOG_DIR/${logname}.log"
  local -a flags=()
  if [ "$mode" = on ]; then
    flags=(
      --jars "$COMET_JAR"
      --driver-class-path "$COMET_JAR"
      --conf spark.plugins=org.apache.spark.CometPlugin
      --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager
      --conf spark.comet.explain.fallback.enabled=true
      --conf spark.comet.explain.fallback.log.enabled=true
      --conf spark.memory.offHeap.enabled=true
      --conf spark.memory.offHeap.size=$OFFHEAP_SIZE
      --conf spark.executor.memoryOverhead=$EXECUTOR_OVERHEAD
    )
  fi
  echo "--- Bench run: $logname (scale order $sizes, driver-memory $DRIVER_MEMORY, offheap $OFFHEAP_SIZE) ---"
  docker run --rm --user 0:0 \
    -v "$BENCH_JAR_HOST:$BENCH_JAR_CONT:ro" \
    -v "$BENCH_DIR:$BENCH_DIR" \
    -v "$WORK_DIR:/opt/spark/work-dir" \
    --entrypoint /opt/spark/bin/spark-submit \
    "$IMAGE" \
    --master local[4] \
    --driver-memory "$DRIVER_MEMORY" \
    --conf spark.ui.enabled=false \
    --conf spark.driver.host=127.0.0.1 \
    --driver-java-options "${DRIVER_JAVA_OPTIONS[*]} -Dvaire.bench.fact.sizes=$sizes -Dvaire.bench.collect=$COLLECT_ENABLED" \
    "${flags[@]}" \
    --class net.ghoula.vaire.spark.CometBench \
    "$BENCH_JAR_CONT" 2>&1 | tee "$log"
  return "${PIPESTATUS[0]}"
}

if [ "$REUSE" != 1 ]; then
  echo "--- Building bench jar (sbt spark/assembly) ---"
  (cd "$PROJECT_DIR" && sbt -batch spark/assembly)

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

  # Read the pinned jar's sha1 back from the image, so the recorded guard is the artefact the runs
  # actually used rather than the constant.
  ACTUAL_SHA1="$(docker run --rm --entrypoint sha1sum "$IMAGE" "$COMET_JAR" | awk '{print $1}')"
  if [ -z "$ACTUAL_SHA1" ]; then
    echo "STOP: could not read the Comet jar sha1 from the image; the guard would be unverified." >&2
    exit 1
  fi
  if [ "$ACTUAL_SHA1" != "$COMET_SHA1" ]; then
    echo "STOP: Comet jar sha1 in the image is $ACTUAL_SHA1, expected $COMET_SHA1." >&2
    echo "The pinned snapshot moved or the image is stale; re-pin deliberately." >&2
    exit 1
  fi
  printf '%s\n' "$ACTUAL_SHA1" > "$LOG_DIR/comet-sha1.txt"

  echo "--- Control run, default order, repeat 1 (no Comet) ---"
  if ! run_block off off "$FACT_SIZES"; then
    echo "ERROR: control run failed; refusing to interpret a Comet run." >&2
    exit 1
  fi

  echo "--- Comet run, default order, repeat 1 ---"
  if ! run_block on on "$FACT_SIZES"; then
    echo "ERROR: Comet run failed." >&2
    exit 1
  fi

  echo "--- Control run, default order, repeat 2 (no Comet) ---"
  if ! run_block off off-2 "$FACT_SIZES"; then
    echo "ERROR: second control run failed." >&2
    exit 1
  fi

  echo "--- Comet run, default order, repeat 2 ---"
  if ! run_block on on-2 "$FACT_SIZES"; then
    echo "ERROR: second Comet run failed." >&2
    exit 1
  fi

  echo "--- Control run, reversed order, repeat 1 (no Comet) ---"
  if ! run_block off off-rev "$REVERSED_SIZES"; then
    echo "ERROR: reversed control run failed." >&2
    exit 1
  fi

  echo "--- Comet run, reversed order, repeat 1 ---"
  if ! run_block on on-rev "$REVERSED_SIZES"; then
    echo "ERROR: reversed Comet run failed." >&2
    exit 1
  fi

  echo "--- Control run, reversed order, repeat 2 (no Comet) ---"
  if ! run_block off off-rev-2 "$REVERSED_SIZES"; then
    echo "ERROR: second reversed control run failed." >&2
    exit 1
  fi

  echo "--- Comet run, reversed order, repeat 2 ---"
  if ! run_block on on-rev-2 "$REVERSED_SIZES"; then
    echo "ERROR: second reversed Comet run failed." >&2
    exit 1
  fi
else
  if [ -f "$LOG_DIR/comet-sha1.txt" ]; then
    ACTUAL_SHA1="$(cat "$LOG_DIR/comet-sha1.txt")"
  fi
  for required in off on off-2 on-2 off-rev on-rev off-rev-2 on-rev-2; do
    if [ ! -f "$LOG_DIR/${required}.log" ]; then
      echo "ERROR: --reuse needs $LOG_DIR/${required}.log; run a full harness first." >&2
      exit 1
    fi
  done
fi

extract_results() {
  awk '/^##RESULT /{
    plan=""; scale=""; n=""; cmin="-"; cmed="-"; cmax="-"; csmp="-"; emin=""; emed=""; emax=""; esmp=""; rows=""; dig=""; ops="";
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
extract_results "$LOG_DIR/off-2.log" > "$LOG_DIR/off-2.results"
extract_results "$LOG_DIR/on-2.log" > "$LOG_DIR/on-2.results"
extract_results "$LOG_DIR/off-rev.log" > "$LOG_DIR/off-rev.results"
extract_results "$LOG_DIR/on-rev.log" > "$LOG_DIR/on-rev.results"
extract_results "$LOG_DIR/off-rev-2.log" > "$LOG_DIR/off-rev-2.results"
extract_results "$LOG_DIR/on-rev-2.log" > "$LOG_DIR/on-rev-2.results"

if grep -q '^##WARMUP' "$LOG_DIR/off.results" "$LOG_DIR/on.results" "$LOG_DIR/off-2.results" "$LOG_DIR/on-2.results" \
  "$LOG_DIR/off-rev.results" "$LOG_DIR/on-rev.results" "$LOG_DIR/off-rev-2.results" "$LOG_DIR/on-rev-2.results"; then
  echo "ERROR: ##WARMUP leaked into the extracted results." >&2
  exit 1
fi

# collect_ms is present in the results only when the run was started with VAIRE_BENCH_COLLECT=1; the
# extracted collect fields are "-" otherwise.
COLLECT_MEASURED=0
if [ "$(awk -F'|' 'NR == 1 { print $4 }' "$LOG_DIR/on.results")" != "-" ]; then
  COLLECT_MEASURED=1
fi

# Window the Comet driver log between ##PLAN-BEGIN/##PLAN-END and harvest the `[COMET: ...]`
# reasons. A reason naming the harness's own write path is counted as instrument, not a plan
# property; this rule is stated in the output below. The sorted Comet* operator names are carried
# alongside the count so a lost operator is identifiable, not merely a changed integer.
fallback_inventory() {
  awk -F'|' '
    FNR == NR {
      key = $1 "|" $2
      claimed[key] = ($14 == "-") ? 0 : split($14, x, ",")
      ops[key] = $14
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
        printf "##FALLBACK plan=%s scale=%s claimed=%d reasons=%d instrument=%d ops=\"%s\" detail=\"%s\"\n",
          ks[1], ks[2], claimed[key], (key in reasons ? reasons[key] : 0),
          (key in instrument ? instrument[key] : 0), (key in ops ? ops[key] : "-"),
          (key in detail ? detail[key] : "")
      }
    }' "$LOG_DIR/on.results" "$LOG_DIR/on.log"
}

fallback_inventory > "$LOG_DIR/on.fallback"

# Order-effect test. For each plan and scale, and separately for the Comet-off and Comet-on medians,
# an order effect is present when the default order's median and the reversed order's median are
# separated by more than the larger of the two within-order spreads (max - min of that order's
# seven samples). Rows whose largest median is below RES_MS are `small` and carry no verdict; the
# rule is not applied inside the measurement noise floor. Output `plan|scale;offA;offB;offSpread;offVerdict;onA;onB;onSpread;onVerdict;mag`.
order_effect_rows() {
  awk -F'|' -v resms="$RES_MS" '
    function sortnum(a, n,   i, j, t) { for (i = 1; i <= n; i++) for (j = i + 1; j <= n; j++) if (a[i] > a[j]) { t = a[i]; a[i] = a[j]; a[j] = t } }
    {
      key = $1 "|" $2
      arm = (FILENAME ~ /\/on(-2|-rev|-rev-2)?\.results$/) ? "on" : "off"
      ord = (FILENAME ~ /rev/) ? "rev" : "def"
      bucket = arm "/" ord "/" key
      n = split($11, p, ",")
      for (i = 1; i <= n; i++) samp[bucket] = samp[bucket] (samp[bucket] == "" ? "" : ",") p[i]
      if (!(key in seen)) { seen[key] = 1; keys[++nk] = key }
    }
    END {
      for (i = 1; i <= nk; i++) {
        k = keys[i]
        nd = split(samp["off/def/" k], d, ","); nr = split(samp["off/rev/" k], r, ",")
        for (j = 1; j <= nd; j++) da[j] = d[j] + 0
        for (j = 1; j <= nr; j++) ra[j] = r[j] + 0
        sortnum(da, nd); sortnum(ra, nr)
        oD = da[int(nd / 2) + 1]; oR = ra[int(nr / 2) + 1]
        oDs = da[nd] - da[1]; oRs = ra[nr] - ra[1]; oSpr = (oDs > oRs ? oDs : oRs)
        nd = split(samp["on/def/" k], d, ","); nr = split(samp["on/rev/" k], r, ",")
        for (j = 1; j <= nd; j++) na[j] = d[j] + 0
        for (j = 1; j <= nr; j++) nra[j] = r[j] + 0
        sortnum(na, nd); sortnum(nra, nr)
        nD = na[int(nd / 2) + 1]; nR = nra[int(nr / 2) + 1]
        nDs = na[nd] - na[1]; nRs = nra[nr] - nra[1]; nSpr = (nDs > nRs ? nDs : nRs)
        dSep = oD - oR; if (dSep < 0) dSep = -dSep
        tSep = nD - nR; if (tSep < 0) tSep = -tSep
        mag = oD; if (oR > mag) mag = oR; if (nD > mag) mag = nD; if (nR > mag) mag = nR
        if (mag < resms) { oe = "small"; ne = "small" }
        else { oe = (dSep > oSpr ? "EFFECT" : "none"); ne = (tSep > nSpr ? "EFFECT" : "none") }
        printf "%s;%.1f;%.1f;%.1f;%s;%.1f;%.1f;%.1f;%s;%.1f\n", k, oD, oR, oSpr, oe, nD, nR, nSpr, ne, mag
      }
    }' "$LOG_DIR/off.results" "$LOG_DIR/off-2.results" "$LOG_DIR/off-rev.results" "$LOG_DIR/off-rev-2.results" \
       "$LOG_DIR/on.results" "$LOG_DIR/on-2.results" "$LOG_DIR/on-rev.results" "$LOG_DIR/on-rev-2.results"
}

order_effect_rows > "$LOG_DIR/order-effect.tsv"

SPARK_VERSION="$(grep -h '^##VERSION' "$LOG_DIR/on.log" | sed -n 's/.*spark=\([^ ]*\).*/\1/p' | head -1)"
if [ -z "$SPARK_VERSION" ]; then
  echo "ERROR: no spark= field in the ##VERSION line; the run is not interpretable." >&2
  exit 1
fi

echo
echo "=================== version lines ==================="
grep '^##VERSION' "$LOG_DIR"/off.log "$LOG_DIR"/on.log "$LOG_DIR"/off-2.log "$LOG_DIR"/on-2.log \
  "$LOG_DIR"/off-rev.log "$LOG_DIR"/on-rev.log "$LOG_DIR"/off-rev-2.log "$LOG_DIR"/on-rev-2.log || true
echo "spark.version=$SPARK_VERSION  comet.jar=$COMET_JAR_BASE  comet.sha1=$ACTUAL_SHA1"
echo "Comet native library lines in the control log: $(grep -c 'Comet native library' "$LOG_DIR/off.log" || true)"
echo "##WARMUP lines (excluded from every table): off=$(grep -c '^##WARMUP' "$LOG_DIR/off.log" || true), on=$(grep -c '^##WARMUP' "$LOG_DIR/on.log" || true)"

echo
SPREAD_FRACTION_PCT="$(awk -v f="$SPREAD_FRACTION" 'BEGIN { printf "%.0f", f * 100 }')"
echo "=================== rules ==================="
echo "ratio column = exec_ms (whole plan forced by a noop write, nothing returned to the driver)."
echo "lower bound: exec_ms is a lower bound on Comet's advantage. The noop write carries unaccelerated writer setup and per-row DataWriter cost on BOTH arms, so the exec ratio understates Comet's speed-up rather than being neutral."
echo "inventory caveat: comet_ops is read from df.queryExecution (the read plan) while exec_ms times the noop write whose child is that plan; the operator inventory describes the read plan, the timing the write plan."
echo "row decision order: (1) comet_ops=- -> n/a; (2) an order effect on either side -> ratio suppressed; (3) plan overhead-dominated when its Comet-off exec median grows < ${GROWTH_FACTOR}x from the smaller to the larger scale; (4) longer exec median < ${RES_MS} ms -> unresolvable (small); (5) off/on exec sample ranges not disjoint -> unresolvable; (6) longer arm's spread > ${SPREAD_FRACTION_PCT}% of its median -> noisy; else the off/on ratio with its interval."
echo "precision rule: the longer arm's spread (max - min of its exec samples) must be <= ${SPREAD_FRACTION_PCT}% of its median. On the 8g run this separates clean rows from heap-contaminated ones: wide-sort-limit@5M is 0.04 and resolves; wide-join-on@5M is 0.36 and wide-tags-transform@5M is 0.37 and are marked noisy. ${SPREAD_FRACTION_PCT}% sits ~2.5x above the clean rows and ~3.5x below the contaminated ones."
echo "collect_ratio is the 789235e8-style collect_ms ratio; the column is printed only when the run measured collect_ms (VAIRE_BENCH_COLLECT=1), which materialises the result on the driver."
echo "instrument rule: a [COMET: ...] reason whose text names WriteFiles/DataWritingCommand/ExecutedCommand/InsertIntoHadoopFsRelation/NoopDataSource/noop is the harness write, counted as instrument=, not reasons=."
echo "order-effect rule: each order's median and spread are pooled over its two repeats; a side is EFFECT when |default median - reversed median| exceeds the larger of the two orders' pooled spreads (max - min). Rows with magnitude < ${RES_MS} ms are 'small' and not judged."

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
echo "exec ratio is a lower bound (see rules): the noop write's unaccelerated writer cost sits on both arms."
awk -F'|' -v growth="$GROWTH_FACTOR" -v resms="$RES_MS" -v frac="$SPREAD_FRACTION" -v collect="$COLLECT_MEASURED" '
  FNR == NR {
    nf = split($0, f, ";"); split(f[1], ks, "|")
    oe[ks[1] "|" ks[2]] = ((f[5] == "EFFECT" || f[9] == "EFFECT") ? "yes" : "no")
    next
  }
  FILENAME ~ /off\.results$/ {
    key = $1 "|" $2; off[key] = $0; order[++n] = key
    p = $1; s = $2 + 0; e = $9 + 0
    if (!(p in smin) || s < smin[p]) { smin[p] = s; emin_of[p] = e }
    if (!(p in smax) || s > smax[p]) { smax[p] = s; emax_of[p] = e }
    next
  }
  { on[$1 "|" $2] = $0 }
  END {
    for (p in smin) if (smax[p] > smin[p]) overhead[p] = (emax_of[p] / emin_of[p] < growth)
    if (collect == 1)
      printf "%-14s %8s | %-20s %-20s | %-22s %-8s %-15s %-8s %-6s %s\n", "plan", "scale", "off_exec[min,med,max]", "on_exec[min,med,max]", "exec_decision", "ratio", "ratio_interval", "col_ratio", "digest", "comet_ops"
    else
      printf "%-14s %8s | %-20s %-20s | %-22s %-8s %-15s %-6s %s\n", "plan", "scale", "off_exec[min,med,max]", "on_exec[min,med,max]", "exec_decision", "ratio", "ratio_interval", "digest", "comet_ops"
    for (i = 1; i <= n; i++) {
      key = order[i]; split(off[key], o, "|"); split(on[key], onf, "|")
      ops = onf[14]
      mag = (o[9] + 0 > onf[9] + 0) ? o[9] + 0 : onf[9] + 0
      disjoint = (o[8] + 0 > onf[10] + 0) || (onf[8] + 0 > o[10] + 0)
      if (o[9] + 0 >= onf[9] + 0) { longerMed = o[9] + 0; longerSpread = (o[10] - o[8]) + 0 }
      else { longerMed = onf[9] + 0; longerSpread = (onf[10] - onf[8]) + 0 }
      spreadFrac = (longerMed > 0) ? longerSpread / longerMed : 0
      ratio = "n/a"; interval = "n/a"
      if (ops == "-") decision = "n/a"
      else if (oe[key] == "yes") decision = "order-effect"
      else if (overhead[o[1]]) decision = "overhead-dominated"
      else if (mag < resms) decision = "unresolvable (small)"
      else if (!disjoint) decision = "unresolvable"
      else if (spreadFrac > frac) decision = "noisy"
      else {
        decision = sprintf("%.2fx", o[9] / onf[9])
        ratio = decision
        interval = sprintf("[%.2f,%.2f]", o[8] / onf[10], o[10] / onf[8])
      }
      cratio = "n/a"
      if (collect == 1 && ops != "-" && oe[key] != "yes") cratio = sprintf("%.2fx", o[5] / onf[5])
      dig = (o[13] == onf[13]) ? "match" : "MISMATCH"
      if (collect == 1)
        printf "%-14s %8s | %5.1f[%5.1f,%6.1f]     %5.1f[%5.1f,%6.1f]     | %-22s %-8s %-15s %-8s %-6s %s\n", o[1], o[2], o[8], o[9], o[10], onf[8], onf[9], onf[10], decision, ratio, interval, cratio, dig, ops
      else
        printf "%-14s %8s | %5.1f[%5.1f,%6.1f]     %5.1f[%5.1f,%6.1f]     | %-22s %-8s %-15s %-6s %s\n", o[1], o[2], o[8], o[9], o[10], onf[8], onf[9], onf[10], decision, ratio, interval, dig, ops
    }
  }' "$LOG_DIR/order-effect.tsv" "$LOG_DIR/off.results" "$LOG_DIR/on.results"

echo
echo "=================== order-effect test (default vs reversed order) ==================="
echo "both sides' exec medians per row, pooled over the two repeats of each order; a side is EFFECT when the medians separate by more than the larger pooled within-order spread."
awk -F';' '
  {
    split($1, ks, "|")
    printf "%-14s %8s | off %7.1f %7.1f (spread %5.1f) %-6s | on %7.1f %7.1f (spread %5.1f) %-6s\n",
      ks[1], ks[2], $2, $3, $4, $5, $6, $7, $8, $9
  }' "$LOG_DIR/order-effect.tsv"

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
grep '^##CHECK' "$LOG_DIR"/off.log "$LOG_DIR"/on.log "$LOG_DIR"/off-2.log "$LOG_DIR"/on-2.log \
  "$LOG_DIR"/off-rev.log "$LOG_DIR"/on-rev.log "$LOG_DIR"/off-rev-2.log "$LOG_DIR"/on-rev-2.log || true

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

failures="$(grep -h '^##CHECK.*ok=false' "$LOG_DIR"/off.log "$LOG_DIR"/on.log "$LOG_DIR"/off-2.log "$LOG_DIR"/on-2.log \
  "$LOG_DIR"/off-rev.log "$LOG_DIR"/on-rev.log "$LOG_DIR"/off-rev-2.log "$LOG_DIR"/on-rev-2.log || true)"

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

# Build the baseline-schema JSON for the current run. One line per plan and scale, sorted, so the
# snapshot is stable and hand-diffable; the meta line records the guard values that make the
# inventory interpretable (Comet jar filename and sha1, spark.version). No JSON dependency: the
# format is fixed and parsed with index/substr.
inventory_from_run() {
  local out="$1"
  local sha="$2"
  local tmp
  tmp="$(mktemp)"
  awk '
    $1 == "##FALLBACK" {
      plan = ""; scale = ""; reasons = "0"; ops = ""
      for (i = 2; i <= NF; i++) {
        if ($i ~ /^plan=/) { plan = $i; sub(/^plan=/, "", plan) }
        else if ($i ~ /^scale=/) { scale = $i; sub(/^scale=/, "", scale) }
        else if ($i ~ /^reasons=/) { reasons = $i; sub(/^reasons=/, "", reasons) }
        else if ($i ~ /^ops=/) { ops = $i; sub(/^ops=/, "", ops); gsub(/"/, "", ops) }
        else if ($i ~ /^detail=/) break
      }
      arr = ""
      if (ops != "" && ops != "-") {
        m = split(ops, parts, ",")
        for (j = 1; j <= m; j++) arr = arr (j > 1 ? ", " : "") "\"" parts[j] "\""
      }
      printf "%s|%s\t[%s]\t%s\n", plan, scale, arr, reasons
    }' "$LOG_DIR/on.fallback" | LC_ALL=C sort > "$tmp"

  local -a entries=()
  local line key ops reasons
  while IFS=$'\t' read -r key ops reasons; do
    entries+=("$(printf '  "%s": {"operators": %s, "reasons": %s}' "$key" "$ops" "$reasons")")
  done < "$tmp"
  rm -f "$tmp"

  {
    echo "{"
    if [ "${#entries[@]}" -eq 0 ]; then
      printf '  "comet": {"jar": "%s", "sha1": "%s", "spark": "%s"}\n' "$COMET_JAR_BASE" "$sha" "$SPARK_VERSION"
    else
      printf '  "comet": {"jar": "%s", "sha1": "%s", "spark": "%s"},\n' "$COMET_JAR_BASE" "$sha" "$SPARK_VERSION"
      local i last
      last=$(( ${#entries[@]} - 1 ))
      for i in "${!entries[@]}"; do
        if [ "$i" -lt "$last" ]; then
          printf '%s,\n' "${entries[$i]}"
        else
          printf '%s\n' "${entries[$i]}"
        fi
      done
    fi
    echo "}"
  } > "$out"
}

inventory_entries() {
  awk '
    /"operators":/ {
      line = $0
      s = line; sub(/^[ \t]*/, "", s)
      q1 = index(s, "\""); rest = substr(s, q1 + 1); q2 = index(rest, "\""); key = substr(rest, 1, q2 - 1)
      lb = index(line, "["); rb = index(line, "]")
      ops = substr(line, lb + 1, rb - lb - 1); gsub(/"/, "", ops); gsub(/, /, ",", ops)
      ri = index(line, "\"reasons\": "); reasons = substr(line, ri + 11) + 0
      printf "%s\t%s\t%d\n", key, ops, reasons
    }' "$1"
}

inventory_meta() {
  sed -n "s/.*\"$2\": \"\([^\"]*\)\".*/\1/p" "$1" | head -1
}

compare_inventories() {
  local baseline="$1"
  local current="$2"
  local rc=0

  local b_jar c_jar b_sha c_sha b_spark c_spark
  b_jar="$(inventory_meta "$baseline" jar)"
  c_jar="$(inventory_meta "$current" jar)"
  b_sha="$(inventory_meta "$baseline" sha1)"
  c_sha="$(inventory_meta "$current" sha1)"
  b_spark="$(inventory_meta "$baseline" spark)"
  c_spark="$(inventory_meta "$current" spark)"

  echo "ratchet: Comet operators may only be added and non-instrument fallback reasons may only decrease; a lost operator or a gained reason exits non-zero."
  echo "guards: comet.jar baseline=$b_jar current=$c_jar"
  echo "guards: comet.sha1 baseline=$b_sha current=$c_sha"
  echo "guards: spark baseline=$b_spark current=$c_spark"

  if [ "$b_jar" != "$c_jar" ]; then
    echo "GUARD FAIL: Comet jar does not match the baseline."
    rc=1
  fi
  if [ "$b_sha" != "$c_sha" ]; then
    echo "GUARD FAIL: Comet jar sha1 does not match the baseline."
    rc=1
  fi
  if [ "$b_spark" != "$c_spark" ]; then
    echo "GUARD FAIL: spark.version does not match the baseline."
    rc=1
  fi

  local btsv ctsv report entry_rc=0
  btsv="$(mktemp)"
  ctsv="$(mktemp)"
  inventory_entries "$baseline" | LC_ALL=C sort > "$btsv"
  inventory_entries "$current" | LC_ALL=C sort > "$ctsv"

  report="$(awk -F'\t' '
    FNR == NR { bops[$1] = $2; breasons[$1] = $3; order[++n] = $1; next }
    { cops[$1] = $2; creasons[$1] = $3; seen[$1] = 1 }
    END {
      reg = 0
      for (i = 1; i <= n; i++) {
        k = order[i]
        if (!(k in seen)) { printf "REGRESSION %s: missing from the run inventory\n", k; reg++; continue }
        if (bops[k] != "") {
          nb = split(bops[k], bp, ",")
          cstr = "," cops[k] ","
          for (j = 1; j <= nb; j++) if (index(cstr, "," bp[j] ",") == 0) { printf "REGRESSION %s: lost operator %s\n", k, bp[j]; reg++ }
        }
        if (cops[k] != "") {
          nc = split(cops[k], cp, ",")
          bstr = "," bops[k] ","
          for (j = 1; j <= nc; j++) if (index(bstr, "," cp[j] ",") == 0) printf "IMPROVEMENT %s: gained operator %s\n", k, cp[j]
        }
        if (creasons[k] + 0 > breasons[k] + 0) { printf "REGRESSION %s: reasons %s -> %s\n", k, breasons[k], creasons[k]; reg++ }
        else if (creasons[k] + 0 < breasons[k] + 0) printf "IMPROVEMENT %s: reasons %s -> %s\n", k, breasons[k], creasons[k]
      }
      exit (reg > 0)
    }' "$btsv" "$ctsv")" || entry_rc=$?
  rm -f "$btsv" "$ctsv"

  if [ -n "$report" ]; then
    printf '%s\n' "$report"
  fi

  local nreg nimp
  nreg="$(printf '%s\n' "$report" | grep -c '^REGRESSION' || true)"
  nimp="$(printf '%s\n' "$report" | grep -c '^IMPROVEMENT' || true)"
  echo "summary: $nreg regression(s), $nimp improvement(s)"

  if [ "$entry_rc" != 0 ]; then rc=1; fi

  if [ "$rc" = 0 ]; then
    echo "ratchet ok: no plan lost a Comet operator and no plan gained a non-instrument fallback reason."
    if [ "$nimp" -gt 0 ]; then
      echo "improvement(s) reported: re-snapshot deliberately to fold them into the baseline."
    fi
  fi
  return "$rc"
}

case "$MODE" in
  bench)
    ;;
  snapshot|check)
    if [ "$MODE" = snapshot ]; then
      inventory_from_run "$BASELINE_FILE" "$ACTUAL_SHA1"
      echo
      echo "=================== comet coverage snapshot ==================="
      echo "snapshot written: comet-coverage.json ($(grep -c '"operators"' "$BASELINE_FILE") plans; comet.jar=$COMET_JAR_BASE spark=$SPARK_VERSION)"
      echo "re-run 'scripts/comet-bench.sh check' to gate against it."
    else
      current="$LOG_DIR/current-inventory.json"
      inventory_from_run "$current" "$ACTUAL_SHA1"
      if [ -n "$INVENTORY_OVERRIDE" ]; then
        current="$INVENTORY_OVERRIDE"
      fi
      echo
      echo "=================== comet coverage ratchet ==================="
      if ! compare_inventories "$BASELINE_FILE" "$current"; then
        echo "comet coverage regressed." >&2
        exit 1
      fi
    fi
    ;;
esac

echo
echo "Logs: $LOG_DIR/{off,on,off-2,on-2,off-rev,on-rev,off-rev-2,on-rev-2}.log"
