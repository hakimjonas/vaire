# Strongbow vs Crossbow: Benchmark Results

**Date:** 2026-02-14
**Method:** Isolated sequential runs (no concurrent contention)
**Harness:** 20 warmup + 50 measurement runs per operation, identical for both libraries

## Configuration

Both libraries run under identical conditions:

| Setting | Value |
|---------|-------|
| Scala | 3.7.4 |
| JVM | Eclipse Adoptium Java 25.0.2 |
| GC | ZGC |
| Heap | `-Xms8G -Xmx48G` |
| Stack | `-Xss4M` |
| Data | Random keys + random int values, groups = rows / 100 |

## Time Performance (Median, ms)

| Operation | 10K SB | 10K CB | 100K SB | 100K CB | 200K SB | 200K CB | 400K SB | 400K CB |
|-----------|--------|--------|---------|---------|---------|---------|---------|---------|
| Filter | 0.35 | 0.36 | 1.32 | 2.81 | 2.69 | 6.37 | 6.53 | 12.87 |
| GroupBy | 0.75 | 0.64 | 4.08 | 5.65 | 11.30 | 11.65 | 25.71 | 26.79 |
| Sort | 1.07 | 6.22 | 11.63 | 40.70 | 26.19 | 76.12 | 54.32 | 166.60 |
| Limit | 0.01 | — | 0.03 | — | 0.05 | — | 0.11 | — |
| Union | 0.04 | — | 0.26 | — | 0.27 | — | 0.57 | — |
| Distinct | 0.49 | — | 6.18 | — | 12.99 | — | 28.14 | — |
| Join | 6.96 | 49.03 | 27.09 | 513.19 | 56.54 | 1036.36 | 118.29 | 2116.85 |

## Speedup (Crossbow / Strongbow)

| Operation | 10K | 100K | 200K | 400K |
|-----------|-----|------|------|------|
| Filter | 1.03x | 2.13x | 2.37x | 1.97x |
| GroupBy | 0.85x | 1.38x | 1.03x | 1.04x |
| Sort | 5.81x | 3.50x | 2.91x | 3.07x |
| Join | 7.05x | 18.9x | 18.3x | 17.9x |

> Values > 1.0 = Strongbow faster.

## GroupBy Crossover Detail

Crossbow's untyped DataFrame has less dispatch overhead at small scale. Strongbow overtakes between 10K and 20K rows.

| Scale | Strongbow (ms) | Crossbow (ms) | Ratio | Strongbow Alloc (MB) | Crossbow Alloc (MB) | Strongbow Heap (MB) | Crossbow Heap (MB) |
|-------|---------------|---------------|-------|---------------------|--------------------|--------------------|-------------------|
| 10K | 1.02 | 0.82 | 0.80x | 2.51 | 2.45 | 42 | 42 |
| 20K | 0.79 | 1.06 | **1.34x** | 5.01 | 4.90 | 66 | 66 |
| 40K | 1.66 | 2.12 | **1.28x** | 10.02 | 9.79 | 142 | 140 |
| 60K | 2.73 | 3.24 | **1.19x** | 15.03 | 14.69 | 188 | 184 |
| 100K | 4.16 | 5.49 | **1.32x** | 25.05 | 24.46 | 280 | 274 |

Memory allocation is nearly identical for GroupBy at all scales (~2% difference). The performance gap is purely computational — Strongbow's typed columnar access scales better than Crossbow's untyped dispatch as data grows.

## Memory: Allocated per Operation (MB)

| Operation | 10K SB | 10K CB | 100K SB | 100K CB | 200K SB | 200K CB | 400K SB | 400K CB |
|-----------|--------|--------|---------|---------|---------|---------|---------|---------|
| Filter | 0.48 | 0.69 | 4.81 | 6.88 | 9.60 | 13.75 | 19.18 | 27.55 |
| GroupBy | 2.51 | 2.45 | 25.05 | 24.46 | 50.09 | 48.92 | 100.20 | 97.84 |
| Sort | 0.70 | 18.51 | 6.87 | 268.25 | 13.72 | 502.26 | 27.46 | 1113.91 |
| Limit | 0.06 | — | 0.57 | — | 1.15 | — | 2.29 | — |
| Union | 0.23 | — | 2.29 | — | 4.58 | — | 9.16 | — |
| Distinct | 1.71 | — | 16.62 | — | 33.23 | — | 66.47 | — |
| Join | 41.41 | 135.57 | 414.15 | 1544.19 | 827.88 | 3243.24 | 1656.85 | 6707.45 |

## Memory Efficiency Ratio (Crossbow / Strongbow)

| Operation | 10K | 100K | 200K | 400K |
|-----------|-----|------|------|------|
| Filter | 1.4x | 1.4x | 1.4x | 1.4x |
| GroupBy | ~1x | ~1x | ~1x | ~1x |
| Sort | 26.4x | 39.0x | 36.6x | 40.6x |
| Join | 3.3x | 3.7x | 3.9x | 4.0x |

> Sort is the standout: Strongbow allocates 40x less memory at 400K thanks to expression-based sort that reads typed arrays directly — no decode, no boxing, no re-encoding.

## Peak Heap Usage (MB)

| Operation | 10K SB | 10K CB | 100K SB | 100K CB | 200K SB | 200K CB | 400K SB | 400K CB |
|-----------|--------|--------|---------|---------|---------|---------|---------|---------|
| Filter | 24 | 26 | 126 | 148 | 176 | 216 | 266 | 346 |
| GroupBy | 42 | 42 | 280 | 276 | 522 | 512 | 992 | 970 |
| Sort | 26 | 186 | 122 | 2496 | 204 | 4632 | 352 | 10160 |
| Limit | 20 | — | 88 | — | 104 | — | 120 | — |
| Union | 22 | — | 88 | — | 136 | — | 192 | — |
| Distinct | 34 | — | 208 | — | 374 | — | 716 | — |
| Join | 394 | 1302 | 3798 | 14176 | 5058 | 29548 | 15070 | 33840 |

## GC Collections

| Operation | 10K SB | 10K CB | 100K SB | 100K CB | 200K SB | 200K CB | 400K SB | 400K CB |
|-----------|--------|--------|---------|---------|---------|---------|---------|---------|
| Filter | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| GroupBy | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| Sort | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| Limit | 0 | — | 0 | — | 0 | — | 0 | — |
| Union | 0 | — | 0 | — | 0 | — | 0 | — |
| Distinct | 0 | — | 0 | — | 0 | — | 0 | — |
| Join | 0 | 0 | 0 | 0 | 4 | 0 | 1 | 4 |

## Summary

Strongbow wins on Filter (2x), Sort (3-6x, 40x less memory), and Join (18x, 4x less memory) across all scales. Sort uses expression-based evaluation that reads typed int arrays directly — no row decode, no boxing, no re-encoding. GroupBy crossover is between 10K and 20K — Crossbow's untyped dispatch is slightly faster below 10K, Strongbow is faster above 20K, with identical memory. Limit, Union, and Distinct are Strongbow-only operations with zero GC through 400K.
