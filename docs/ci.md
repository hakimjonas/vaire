# CI and testing strategy

Strongbow's test suite is split into three tiers so the pull-request gate stays fast and
memory-frugal on shared CI, while the heavy suites still run somewhere.

## Tiers

| Tier | Scope | Where | Trigger |
|---|---|---|---|
| Fast (PR gate) | `check` + `core/testFull` + `spark/testFull` with `Slow` excluded | `ci.yml` | every push / PR |
| Slow | 5M-row stress + TPC-H end-to-end (`Slow`-tagged) | `stress.yml` | scheduled (weekly) + manual |
| Benchmark | micro-benchmarks (`Benchmark`-tagged) | excluded from CI | `BenchRunner` / local only |

## Why not run everything on every push?

- **SparkContext startup dominates.** Each `SparkSession` start/stop costs ~2-3s and spins
  up executor threads, RPC endpoints, and the driver. The suite uses one shared session per
  JVM (see `SparkTestSession`, the same pattern as Spark's own `SharedSparkContext`) so this
  cost is paid once, not once per suite.
- **The 5M-row stress suite is memory-bound.** It needs a 4G fork heap and several minutes;
  running it on every push would burn shared-runner minutes for no additional signal.
- **Resource fairness.** Free/shared CI runners (Codeberg, etc.) are metered. Oversized
  heaps are antisocial and an OOM/eviction accident waiting to happen. CI heaps are
  right-sized; local development keeps the larger `.jvmopts`.

## Memory model

| Context | sbt JVM | forked test JVM |
|---|---|---|
| `ci.yml` (fast) | 3G | 4G |
| `stress.yml` / `release.yml` (slow) | 3G | 4G |

The fork heap is a fixed `-Xmx4G` (the 5M stress suite needs it; the fast suites use far less
but the ceiling is only a maximum, and ZGC commits only what it uses). The sbt heap comes from
the workflow `JAVA_OPTS`/`SBT_OPTS`. `.jvmopts` is developer-local — excluded per clone via `.git/info/exclude`
(see `.jvmopts.example`). `FAST_TESTS` controls only *which* tests run (it excludes the `Slow` tag),
not the heap.

## Running locally

```bash
sbt check                          # scalafix + scalafmt (CI gate)
sbt testAll                        # core + spark, full (includes slow)
FAST_TESTS=1 sbt spark/Test/testFull   # what ci.yml runs
sbt testSlow                       # 5M stress + TPC-H only
```
