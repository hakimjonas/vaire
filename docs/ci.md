# CI and testing strategy

Status: current
Audience: contributor.

Vairë's test suite is split into three tiers so the pull-request gate stays fast and
memory-frugal on shared CI, while the heavy suites still run somewhere.

## Tiers

| Tier | Scope | Where | Trigger |
|---|---|---|---|
| Fast (PR gate) | `check` + `testAll` with `Slow` excluded | `.github/workflows/ci.yml` | every push / PR |
| Slow | 5M-row stress + TPC-H end-to-end (`Slow`-tagged) | `.github/workflows/stress.yml` | scheduled (weekly) + manual |
| Comet coverage | generated-plan Comet operator inventory ratchet | `scripts/comet-bench.sh check` in `.github/workflows/comet.yml` | scheduled (weekly) + manual |
| Benchmark | micro-benchmarks (`Benchmark`-tagged) | excluded from CI | `BenchRunner` / local only |

## Why not run everything on every push?

- **SparkContext startup dominates.** Each `SparkSession` start/stop costs ~2-3s and spins
  up executor threads, RPC endpoints, and the driver. The suite uses one shared session per
  JVM (see `SparkTestSession`, the same pattern as Spark's own `SharedSparkContext`) so this
  cost is paid once, not once per suite.
- **The 5M-row stress suite is memory-bound.** It needs a 4G fork heap and several minutes;
  running it on every push would burn shared-runner minutes for no additional signal.
- **Resource fairness.** Shared CI runners (GitHub-hosted, and Codeberg before it) are
  metered. Oversized heaps are antisocial and an OOM/eviction accident waiting to happen.
  CI heaps are right-sized; local development keeps the larger local `.jvmopts` copied from `.jvmopts.example`.

## Memory model

| Context | sbt JVM | forked test JVM |
|---|---|---|
| `.github/workflows/ci.yml` (fast) | 3G | 4G |
| `.github/workflows/stress.yml` / `.github/workflows/release.yml` (slow) | 3G | 4G |

The fork heap is a fixed `-Xmx4G` (the 5M stress suite needs it; the fast suites use far less
but the ceiling is only a maximum, and ZGC commits only what it uses). The sbt heap comes from
the workflow `JAVA_OPTS`/`SBT_OPTS`. `.jvmopts` is developer-local — excluded per clone via `.git/info/exclude`
(see `.jvmopts.example`). `FAST_TESTS` controls only *which* tests run (it excludes the `Slow` tag),
not the heap.

## Running locally

```bash
sbt check                          # doc coverage + scalafix + scalafmt (CI gate)
sbt testAll                        # core + spark + docTool, full (includes slow)
FAST_TESTS=1 sbt testAll              # what ci.yml runs (Slow excluded)
sbt testSlow                       # 5M stress + TPC-H only
```

## Scaladoc coverage gate

`check` enforces the pana-style scaladoc ratchet: every public member of `src/main/scala`
(defs, vals, types, classes/objects/enums, enum cases, extension methods) needs a non-empty
`/** */` doc. Overrides, givens, exports, private/protected members, and test sources are
exempt. Coverage may only improve — the checked-in `doc-coverage.json` records the per-file
baseline, and `check` fails when a file's undocumented count grows (new member without a doc,
or a doc removed). Documenting members never fails.

```bash
sbt docCoverage            # ratchet check (also runs as part of check)
sbt docCoverageSnapshot    # regenerate the baseline after intentional doc work
```

## Claims coverage ledger

`docs/backend-claims.md` records every expression or dataset-transformation rejection in both
modules and the test that pins it. Unlike `doc-coverage.json`, it is not a gate: nothing fails
when a new rejection site appears without an entry. The ledger states the proposed drift check;
building it is a separate change. A maintainer adding a rejection site updates the ledger in the
same change.

## Comet coverage gate

Comet is a Spark plugin, so Vairë has no compile-time dependency on it: it is switched on entirely
by `spark-submit` configuration. Comet accelerates the operators it recognises and silently falls
back on the rest, so a Vairë change that starts emitting something Comet cannot execute loses the
acceleration without failing any test. This gate makes that loss visible.

`scripts/comet-bench.sh` builds the bench jar (`sbt spark/assembly`), builds the cluster image
(`docker/spark-cluster/Dockerfile`, base `apache/spark:4.2.0-java17`), and runs the fixed plan set
in `CometBench` inside that image — on plain Spark and with the pinned Comet snapshot on the
classpath, twice at the default and twice at the reversed fact-scale order. It harvests the `Comet*`
operators from each plan's physical plan and the non-instrument `[COMET: ...]` fallback reasons
from the driver log, and records them per plan and scale in `comet-coverage.json`.

The plan set runs twice: once over the narrow integer fact table and once over a wide table that
adds a low-cardinality string, a unique string, a nullable `decimal(12, 2)`, a nested
`Seq[String]`, a boolean, and a timestamp. The same seven shapes run over both tables, so the
effect of data shape is read off a controlled comparison; wide rows carry the `wide-` prefix. Three
wide-only plans exercise the new types: a string filter-and-group, a nullable-decimal `max`/`min`
aggregate (with nulls present), and a nested-list `transform`. The wide table is generated with the
narrow key and value distributions, so joins and groupings behave identically.

```bash
scripts/comet-bench.sh snapshot   # rewrite comet-coverage.json from a fresh run
scripts/comet-bench.sh check      # run, then ratchet the inventory against the baseline
scripts/comet-bench.sh check --reuse   # skip the build and runs; check the last artifacts
```

The driver heap and off-heap size default to `4g` and `4g` and read from `VAIRE_BENCH_DRIVER_MEMORY`
and `VAIRE_BENCH_OFFHEAP_SIZE`; `VAIRE_BENCH_EXECUTOR_OVERHEAD` controls the executor memory
overhead (`2g` by default). The defaults are the ones the scheduled job runs.

The digest, the row count and the window-permutation check come from one streaming pass
(`df.toLocalIterator`), not from `df.collect()`: each projected row is hashed into an
order-independent 256-bit multiset accumulator and the window check keeps a per-key count and a
bitset bounded by the row count. A default run therefore holds O(1) rows on the driver. `collect_ms`
is a continuity column retained from `789235e8` and is measured only when `VAIRE_BENCH_COLLECT=1`;
it materialises the whole result and must not be re-enabled by default. The `collect_ratio` column
is printed only when it was measured.

The ratchet direction matches `doc-coverage.json`: Comet operators may only be **added** and
non-instrument fallback reasons may only **decrease**. A plan that loses an operator or gains a
reason exits non-zero; a gained operator or a lost reason is reported but passes, so it can be
re-snapshotted deliberately. The baseline also records the Comet jar filename and sha1 and
`spark.version`; `check` fails when either differs from the run. Timings never enter the ratchet —
it compares inventories only.

Because the gate needs Docker, the Spark 4.2.0 image, and the pinned Comet snapshot, it does not
run in `.github/workflows/ci.yml`. It runs from `.github/workflows/comet.yml` on a weekly schedule and on `workflow_dispatch`.

Three points are printed by the harness and matter when reading it:

- **The plan set is a sample, not the whole backend.** The narrow table exercises roughly eight of
  Vairë's `Expr` cases and nine operators; the wide table adds strings, a decimal, a nested array
  and a boolean. The ratchet is only as broad as this set; broadening it is a separate change.
- **`exec_ms` is a lower bound on Comet's advantage.** The `noop` write that forces the plan
  carries unaccelerated writer setup and per-row `DataWriter` cost on both arms, so the off/on
  ratio understates Comet rather than being neutral.
- **The inventory and the timing describe different plans.** `comet_ops` and the fallback
  inventory come from `df.queryExecution` (the read plan); `exec_ms` times a `noop` write whose
  child is that read plan. The operator inventory therefore describes the read plan, while the
  timing describes the write plan.

The harness also replaces a fixed cross-run ratio tolerance with an order-effect test: for each
plan and scale it pools the two repeats of each order, compares the default order's exec median
against the reversed order's, separately for the Comet-off and Comet-on sides, and suppresses the
ratio when they separate by more than the larger pooled within-order spread (rows below 50 ms are
not judged).

Disjointness is not precision, so a ratio is printed only when the longer arm's exec sample spread
(max − min) is at most 10% of its median; rows that fail are reported as **noisy** rather than
unresolvable, and every printed ratio is accompanied by its interval (`[off_min/on_max,
off_max/on_min]`). The fraction is justified against the 8g run: the clean wide-sort-limit@5M row
is 4% and resolves, while the heap-contaminated wide-join-on@5M (36%) and wide-tags-transform@5M
(37%) are suppressed — 10% sits about 2.5× above the clean rows and 3.5× below the contaminated
ones.
