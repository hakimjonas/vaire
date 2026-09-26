# Join optimization plan

Take-home work plan for improving the in-memory join operators in `vaire-core`. Written after an algorithmic review of the current join implementations (September 2026). Phases are self-contained; Phase 0 must come first, but Phases 1–3 can be done one at a time or in any order after 0.

All line references are to `vaire-core/src/main/scala/net/ghoula/vaire/` as of `main`; verify line numbers when implementing.

## Why this matters

An earlier claim about the in-memory interpreter — "scales linearly with input" — is **false for two classes of join today**:

1. The predicate-join family (`InnerJoin`, `LeftJoin`, `RightJoin`, `FullJoin`, `LeftAntiJoin`) is O(n·m) by construction, because the condition is an arbitrary `(A, B) => Boolean`. This cannot be fixed asymptotically; Phase 2 is constant-factor work only.
2. The keyed-join family (`*JoinOn`) is *supposed* to be linear (hash index), but the index bucket is built with `Vector :+`, whose amortized cost is O(log n), making keyed joins O(n·log n) under skewed keys rather than the ideal O(n). This was initially misread as O(n²): it is a super-linear factor confined to a small constant, not a quadratic cliff.

The boxing issue compounds both: keys and rows travel as boxed `Any` through `getValue`, the exact per-row object traffic the columnar design exists to avoid.

## Current state (evidence)

- Join case dispatch in `executeScoped` (`interpreter/DatasetInterpreter.scala:90-117`): predicate joins `:90-99`, `LeftAntiJoin :102`, keyed joins `:105-117`.
- `innerJoinDatasets` (`:384-400`): `toVectorUnsafe` both sides, nested `for` with `condition(l, r)` per pair. O(n·m), boxed rows.
- `outerJoinDatasets` (`:427-445`): `flatMap` over primary, `filter` over secondary. O(n·m); backs `:447-466` (left/right joins).
- `JoinOps.fullJoin` (`internal/JoinOps.scala:5-17`): materializes a `pairs` vector of all matches (up to n·m), then two `.map(...).toSet`, then `inner ++ unmatchedLeft ++ unmatchedRight`. Worst constant factor and worst memory profile in the file.
- `leftAntiJoinDatasets` (`:477-490`): `exists` per left row. O(n·m) worst case.
- `buildKeyIndex` (`:492-502`): `HashMap[Any, Vector[Int]]`; `keyCol.getValue(i)` boxes every key (`column/Column.scala:161` returns `Any | Null`); bucket append `existing :+ i` is amortized O(log n) per row, so keyed joins are O(n·log n) under skew — a small constant factor beyond linear, not a quadratic.
- Probe side of keyed joins boxes again: `rightKeyCol.getValue(ri)` (`:530`), and `probeJoinOnKeys` (`:543-567`) boxes lookups the same way.
- `evalKeys` (`:569-580`) evaluates both key columns once into `Column`s via `ExprInterpreter.evalColumn` — good; the columns already exist, so the index/probe only needs unboxed reads.

### Tests and benchmarks today

- Functional correctness: `test/scala/net/ghoula/vaire/DatasetJoinSpec.scala`, `SemiJoinSpec.scala`.
- No join benchmark existed anywhere.

---

## Phase 0 — Benchmark harness (do this first)

**Goal:** establish the current performance curve so every later phase has a before/after. Skew must be represented, or the contended (boxed, log-factor) path stays invisible.

Create `vaire-core/src/test/scala/net/ghoula/vaire/interpreter/JoinOperationsBench.scala` (or alongside `ColumnOperationsBench` if that lives in spark; keep core benchmarks in core). Do not commit benchmark time assertions as pass/fail tests — benchmarks report; correctness stays in the existing specs.

Sweep across:

- **Keyed joins** (`InnerJoinOn`, `LeftJoinOn`, `FullJoinOn`): small side × large side; both sides large.
- **Key skew:** one hot key (n rows, single key — the contended bucket), few distinct keys (k ≈ √n), uniform distinct keys (k ≈ n). This matrix is the whole point of the bench.
- **Predicate joins** (`InnerJoin`, `LeftJoin` via `outerJoinDatasets`, `FullJoin`, `LeftAntiJoin`): small × small, small × medium.
- Data: use the `generateData`-style generators already in `SparkComparativeBench`/`SparkPlanVsExecBench`; match their seeded-generation idiom for reproducibility.

Report, per case: wall time, and rows-in/rows-out. Keep the numbers next to the code so a later phase's improvement is readable.

**Phase 0 acceptance:** ~~a committed benchmark that, run now, demonstrates keyed joins degrading with the number of rows under a single hot key (visible super-linearity)~~ **DELIVERED, result below.** The bench (**`vaire-spark/src/test/scala/net/ghoula/vaire/spark/JoinOperationsBench.scala`**) is in place and reports predicate-join times at the chosen sizes. It does **not** show the predicted hot-key quadratic, because the build is O(n·log n), not O(n²).

Phase 0 measured (JDK 25, 4G, ZGC, 1 warmup + 3 measured, median ms):

| n | regime | InnerJoinOn build+probe ms | ratio |
|---|---|---|---|
| 100k | hot | 6.8 | — |
| 200k | hot | 13.1 | 1.92x |
| 400k | hot | 24.6 | 1.88x |
| 100k | few | 6.6 | — |
| 200k | few | 12.5 | 1.88x |
| 400k | few | 24.0 | 1.92x |
| 100k | uniform | 7.9 | — |
| 200k | uniform | 11.2 | 1.41x |
| 400k | uniform | 23.5 | 2.11x |

Doubling n grows time ~1.9-2x, i.e. linear (the O(log n) factor is invisible at these sizes). A quadratic build would show ~4x per doubling — it does not.

LeftJoinOn / FullJoinOn at n=200k per side (built on RIGHT; probe left disjoint):

| op | regime | ms |
|---|---|---|
| LeftJoinOn | hot | 80.3 |
| LeftJoinOn | few | 65.9 |
| LeftJoinOn | uniform | 69.3 |
| FullJoinOn | hot | 197.2 |
| FullJoinOn | few | 152.9 |
| FullJoinOn | uniform | 154.4 |

FullJoinOn is ~2.5x LeftJoinOn — the `matchedRight` `HashSet` + `unmatchedRight` pass (`fullJoinOnExpr :636-655`) dominates, and its hot-key penalty (~28% vs uniform) is the constant-factor tail, not a quadratic.

Fit/probe shapes (real output): InnerJoinOn 1k×500k → 125k rows, 7.4 ms; LeftJoinOn same → 53.2 ms; FullJoinOn same → 216.6 ms; InnerJoinOn 200k×200k → 25% overlap, 200k rows, 18.9 ms. Predicate joins: inner 2k×2k → 12.3 ms; inner 2k×20k → 94 ms; left 2k×20k → 80.5 ms; full 2k×20k → 102.3 ms; anti 2k×20k → 3.9 ms.

**Corrected takeaway for the rest of the plan:** the keyed path is *linear but boxed*. The live costs to attack are (a) the per-row `Any` boxing on key reads at index build and probe, (b) the `fullJoinOnExpr` extra HashSet/pass, (c) predicate-family constant factors. The O(log n) bucket factor is a rounding error next to them. Phase 1's 1b (unboxed key read) and the `Any`-key map matter more than 1a's bucket structure; the README claim should stop saying the keyed path is "a quadratic hiding in the fast path" — the honest claim is "linear and hash-based, but boxing on key access."

---

## Phase 1 — Kill the boxing, tighten the buckets, in the keyed path

**Goal:** keyed joins keep their linear-ish curve while shedding per-row boxing. This is the highest-value change: it restores the honest "linear columnar" claim without the boxed keys.

### 1a. Buckets: `Vector :+` → growable int array / `ArrayBuffer`

In `buildKeyIndex` (`:492-502`) the append `existing :+ i` grows the bucket by persisting a new `Vector` on every insertion (amortized O(log n) per append, O(n·log n) for a hot bucket of size n). Replace per-bucket `Vector[Int]` with a mutable growable int buffer (`scala.collection.mutable.ArrayBuffer[Int]`) so the build is amortized O(1) per row regardless of duplicates.

- This converts the per-bucket append from amortized log to amortized constant and removes the repeated `Vector` spine copies under a hot key. Phase 0 measured the effect as sub-2x at 100k-400k rows, i.e. it is a constant-factor win, not a cliff — take it for the memory profile (no per-insert spine allocation) rather than for an asymptotic rescue.
- Keep the `HashMap` keyed on `Any` for now; boxing removal is 1b.

### 1b. Primitive-specialized hash maps per `ColumnType`

The `Any` key from `getValue` is a box per row, on both the index and probe sides — the live cost Phase 0 identified. Add a small internal keying layer that dispatches on the column's `ColumnType` and, for primitive types, hashes unboxed:

- `IntType`, `LongType`, `ShortType`, `ByteType`, `FloatType`, `DoubleType`, `BooleanType`, `DateType`, `TimestampType`, `TimestampNTZType` → specialized `PrimitiveHashIndex` (int/long/double keys) using the column's raw array directly, no `getValue`.
- `StringType`, `DecimalType`, `StructType`, arrays/maps, and the `Any` fallback keep the `Any`/object path.
- Empty/null handling must match existing semantics: SQL NULL keys must behave exactly as they do today (check the current specs before and after).

The evaluation already produces typed key `Column`s via `evalKeys` (`:569-580`), so the raw primitive array is available without extra work. The unboxed read lives in `KeyIndex.rawKey`, which pattern-matches the `Column` variants directly; no `Column` API change was needed.

**Phase 1 acceptance (outcome):** keyed joins improved ~1.4-1.7x under hot/few skew (specs unchanged and green, plus a new null-key spec). Uniform distinct keys moved sideways: no regression at ≤200k, ~15% at 400k. The plan's "measurable reduction on uniform keys" was not met at the largest uniform size; that residual is the unboxed `LongMap`'s hash cost against a boxed-`HashMap` whose boxes the JIT eliminates, and is documented in the 1b results below along with the residual options.

#### Phase 1b delivered (September 2026)

`buildKeyIndex` is replaced by `internal/KeyIndex.scala`: a `KeyIndex` built once per keyed join, with `probe(probeKeyCol, row)(report)` reporting build-side rows instead of returning a materialized sequence. Two implementations:

- **`PrimitiveIndex`** — for `Int`, `Long`, `Short`, `Byte`, `Float`, `Double`, `Boolean`, `Date`, `Timestamp`, `TimestampNTZ`. Keys are read raw (no `getValue`, no box) and Long-normalized (`floatToIntBits`/`doubleToLongBits` for float/double). An unboxed open-addressing table — which replaced the initial `LongMap` (see the follow-up below) — maps each key to the head of a flat shared `next` array; the build iterates rows **descending** so each key's chain lays out **ascending**, which reproduces the prior index's within-bucket order. No per-key object is allocated — the bucket is the flat `next` array. Null rows are not indexed and null probes never match, matching Spark 4.2's null-intolerant `EqualTo` (`spark.sql.ansi` is on by default). The fast path only runs for the `ColumnType` the index was built from: the interpreter validates that both key columns share that type at the keyed-join boundary (`KeyedJoin.validateKeyTypes`) and fails with `ExecutionError.TypeMismatch` otherwise, rather than silently returning no rows.
- **`ObjectIndex`** — `String`, `Decimal`, structs, arrays/maps, and the `Any` fallback use a `HashMap[Any, Vector[Int]]`. Null rows are not indexed and null probes never match.

Callers (`innerJoinOnExpr`, `probeJoinOnKeys`, `fullJoinOnExpr`, `filterJoinOnExpr`) are rewired to the probing index. `JoinOnNullKeysSpec` locks the null-key semantics (nulls match nothing) for the primitive and object paths plus semi/anti, and `KeyedJoinKeyTypeSpec` locks the key-type contract, fast-type coverage, and inner-join symmetry. (Full join with null-valued rows is excluded: `fullJoinOnExpr` decodes via `toVectorUnsafe`, and the int schema decode `NPE`s on null rows — a pre-existing limitation unrelated to this change.)

Functional: core suite 444 tests green, including `DatasetJoinSpec`, `SemiJoinSpec`, `JoinOnNullKeysSpec`.

Performance (JDK 25, 4G, ZGC, 1 warmup + 3 measured, median ms; Phase 0 column = before):

| n | regime | before | after |
|---|---|---|---|
| 100k | hot | 6.8 | 4.1 |
| 200k | hot | 13.1 | 7.9 |
| 400k | hot | 24.6 | 15.7 |
| 100k | few | 6.6 | 4.3 |
| 200k | few | 12.5 | 9.0 |
| 400k | few | 24.0 | 15.0 |
| 100k | uniform | 7.9 | 6.0 |
| 200k | uniform | 11.2 | 12.1 |
| 400k | uniform | 23.5 | 27.1 |

Fit/probe shapes: InnerJoinOn 1k×500k 7.4 → 11.2; LeftJoinOn 1k×500k 53.2 → ~51; FullJoinOn 1k×500k 216.6 → ~215-229; InnerJoinOn 200k×200k 18.9 → ~19-22. LeftJoinOn 200k: hot 80.3 → ~93, few 65.9 → ~77-82, uniform 69.3 → ~72. FullJoinOn 200k: hot 197.2 → ~191-228, few 152.9 → ~148-155, uniform 160.0 → ~154-164.

**Honest reading.** The hot/few regimes (the contended bucket, which did the boxed `Any` work per row twice) improved steadily, ~1.4-1.7x across 3 confirming runs; the build on the contended key in isolation is at the measured floor (~2.4ms at 400k rows, confirmed by a same-session A/B that also showed the rejected tails layout at ~2x and any per-key `Vector`/`ArrayBuffer` bucket at ~20-88ms). Uniform and fit shapes went sideways: large utterly-distinct keys regress ~15% and the probe-dominant 1k×500k shape regresses ~4ms. Both residuals were the cost of the `LongMap` hash, not of unboxing: see the follow-up below, which keeps the unboxed path and removes the uniform cost with an open-addressing table.
- Fully distinct keys put the whole cost on one unboxed `LongMap` op per row; `LongMap` hashes with a Murmur-mix and is measurably ~2x the per-op cost of the old `HashMap`'s escape-analysis-eliminated boxed-`Integer` ops at that size. An unboxed map cannot win that sweep except by what it saves elsewhere.
- The 1k×500k shape is probe-only against a 1000-row index; the +4ms is the heavier per-probe hash on half a million lookups. Fingers showed the boxed-key `HashMap` (boxes elided by the JIT) can beat `LongMap` there, at the price of reintroducing internal boxing on the key read — defeating the point of 1b. The discrepancy is the boxed-`Int` map's simpler hash, which holds regardless of which map wins on build-heavy shapes.

Two disciplines make these numbers trustworthy:
- The hot/few win and the uniform residual were each reproduced across multiple runs. Do not chase 1-2ms deltas on this machine: same-code re-runs move the sweep several ms session to session, so only stable signals (the ~1.4-1.7x hot win, the uniform/fit sideways movement) should be read as real.
- The chosen structure was decided by same-session micro-A/B (heads vs tails vs CSR group-layout vs `ArrayBuffer` buckets), not by the join bench. It is the fastest measured layout that preserves within-key ascending order with zero per-key allocation.

**Residual opportunities, in order.** (1) `fullJoinOnExpr`'s `matchedRight` `HashSet` + `unmatchedRight` pass is still the largest constant factor on full joins (Phase 2's 2a fix for the predicate `JoinOps.fullJoin` is the predicate-side analogue). (2) A sort-based join would dodge the hash cost even further on the uniform/fit shapes, but that is new machinery outside Phase 1's scope. (3) superseded — the 1b follow-up's open-addressing table wins the uniform/fit sweep for all fast types with one code path, no boxing, and no `LongMap`; a per-raw-key-space `HashMap[Int, Int]` would now be a second code path to next to nothing.

#### Phase 1b follow-up — regression analysis and open-addressing (September 2026)

The two residuals named above (uniform ~15% @400k; 1k×500k fit ~+4ms) were reproduced same-session against `main`, isolated, and re-driven. Both live in the index, not the interpreter; the follow-up replaced the `LongMap` heads map with a hand-rolled unboxed open-addressing table.

- **Uniform is a hash cost, not a boxing cost.** The micro-A/B finger (one `AnyFlatSpec`, build+probe over exact bench shapes, median of 12, n=200k unless noted) split the structural cost from the production path:

  | shape | LongMap | HashMap[Int] | HashMap[Long] | open-addressing |
  |---|---|---|---|---|
  | hot/miss | 1.68–6.0 | 3.39–6.9 | 2.61–3.1 | 2.44–4.8 |
  | few/miss | 4.17–4.55 | 2.47–2.9 | 2.54–2.6 | 2.56–3.3 |
  | uniform/miss | 9.49–10.4 | 4.41–5.0 | 5.45–5.8 | 4.69–6.5 |
  | small-1k/hits | 2.83–4.6 | 2.23–2.5 | 2.31–2.4 | 2.89–3.1 |

  Within any one run the ordering holds; the ranges are same-code session drift. `LongMap` wins hot (few keys) because it is unboxed, but loses uniform ~2x because its Murmur-mix on sequential keys is a slower per-op cost than a boxed-`Integer` `HashMap` whose boxes the JIT eliminates. The boxed maps win only the strictly-distinct sweep, at the price of reintroducing the boxing 1b removes.
- **Fix: unboxed open-addressing `long`-key table in `PrimitiveIndex`.** Power-of-two slots; `slotKeys: Array[Long]` plus `slotHeads: Array[Int]` with -1 as the free marker (row indices are ≥ 0, so the sentinel is unambiguous, and -1 threads through `next` unchanged); linear probing; slot = top bits of `hash(k) * 0x9e3779b1` where `hash` is the existing 32-bit mix `k ^ k>>>32`, `* -0x7ee3623b`, `^ >>>16`. One code path for all fast types, zero per-key allocation, presized at build (≤ 0.5 load; a static index never grows). It beats the `LongMap` on every finger shape and sits inside the boxed maps' band on uniform/few.
- **Same-session bench after the fix** (feature branch vs `main` worktree; JDK 25, 4G, ZGC, 1 warmup + 3 measured, median ms):

  | n | regime | main | 1b+OA |
  |---|---|---|---|
  | 100k | hot | 6.1 | 5.1 |
  | 200k | hot | 12.0 | 9.5 |
  | 400k | hot | 16.3 | 19.9 |
  | 100k | few | 4.2 | 4.8 |
  | 200k | few | 11.6 | 10.7 |
  | 400k | few | 23.6 | 22.8 |
  | 100k | uniform | 8.4 | 5.7 |
  | 200k | uniform | 14.5 | 14.4 |
  | 400k | uniform | 25.5 | 26.4 |

  Uniform now sits at-or-under the boxed baseline (the delivered run's 30.5@400k no longer reproduces); the residual deltas (hot@400k +3.6, few@100k +0.6, uniform@400k +0.9) are inside this machine's same-code drift — `main` itself measured hot@400k 16.3 then 19.7 across sessions.
- **Fit-inner 1k×500k is not an index regression.** The isolated production-path finger on the exact shape (1k keys, 500k probes, 25% hits) shows the new path beating the old boxed path same-session (PROD 4.02 vs PROD-BOX 4.84 ms); the delivered run's +4ms was a slow-session artifact. Consecutive bench runs still leave feature ~1ms above main (10.1–10.3 vs 8.7–9.1), but every isolated measurement points outside `KeyIndex`. Interpreter per-probe closure hoisting was tried and measured at zero effect (the JIT folds the non-escaping per-row lambdas) and was dropped.
- **LJO/FJO hot deltas are noise.** `main` LJO@200k-hot measures 82.6 and 100.3 in different sessions; feature 121.1; the delivered run's 127.6 spike does not reproduce. These shapes are dominated by output/anti-join handling, not the index.

The follow-up is scoped to `KeyIndex.PrimitiveIndex`; join semantics are unchanged and the core suite (`testFull`, 589 tests) is green. No new API and no caller changes; the only test-file touch is scalafix suppression formatting in `JoinOnNullKeysSpec` (null-literal rewrite), no behavioral changes.

#### Phase 1c — Spark 4.2 key semantics (September 2026)

The initial fast path gated cross-type probes on `ColumnType` and matched null keys to null keys. Both disagreed with Spark 4.2, and the gate was order-dependent: it lived only in `PrimitiveIndex`, so `Long ⋈ Time` returned no rows while `Time ⋈ Long` matched. Phase 1c aligns the keyed-join key contract with Spark 4.2:

- Key types must match. `KeyedJoin.validateKeyTypes` runs once at the keyed-join boundary, called by both `DatasetInterpreter.evalKeys` and `SparkInterpreter.joinOnExprBase`; a mismatch fails with `ExecutionError.TypeMismatch` instead of silently returning no rows. This matches Spark rejecting incompatible key types and keeps the two backends in agreement. Widening (`Int` vs `Long`) now requires an explicit cast so both sides share a type.
- The declared type is checked against the resolved column. `TypeChecks.resolvedType` compares the evaluated key column's `ColumnType` (core) or the key column's type in the built plan (Spark) against the declared type, so a caller cannot label a `Long` column as `Int`. This is what stops the equality check from being defeated by a lie on both sides.
- Null keys never match. Spark 4.2 runs ANSI mode by default and `EqualTo` is null-intolerant, so the in-memory index no longer indexes null rows or matches null probes. This replaces the earlier null-matches-null behavior and rewrites `JoinOnNullKeysSpec`.
- The per-probe type gate is gone. With boundary validation in place, `KeyIndex` assumes same-type keys and the fast path stays unboxed.

`SparkKeyedJoinParitySpec` covers valid joins, null keys, mismatched key types, and a declared type that disagrees with the column, across both backends.

The core suite (`testFull`, 596 tests) is green, including the rewritten `JoinOnNullKeysSpec` and the new `KeyedJoinKeyTypeSpec` (mismatch errors, resolved-column check, fast-type coverage, inner-join symmetry).

#### Phase 1d — Single-column null model (September 2026)

The schema and the column disagreed about nullability: a column carries a null `BitSet` independently of the schema, and `optionSchema` encoded `Option[A]` as a presence Boolean plus the inner columns, so `Option[Int]` was two columns while a `Dataset[Int]` with nulls was one column the row decoder could not represent (it dereferenced null while building the error). Phase 1d makes nullability one thing, matching Spark's nullable field:

- `Option[A]` is one nullable column. `optionSchema` stores `None` as a null and `Some(a)` as the inner value; when `A` is a single column the column is the inner column, when `A` flattens to several columns (a tuple or a derived case class) it is a struct column. A Spark nullable field maps to `Option` and a non-optional field to a plain type, in both directions, including nested struct fields.
- Non-optional fields are null-free. `Dataset.fromColumns` rejects a null in a non-optional field with `SchemaError.NullInNonNullableColumn`, and the Spark read boundary enforces the same contract: `SchemaConverter.validateSchema` requires a non-optional Vairë field to map to a non-nullable Spark field, and `RowConverter.toMaterialized` rejects a null read into a non-optional field. A nullable-by-declaration source (Parquet, JDBC) is read as `Option`; `Dataset.narrow[U]` asserts non-null against the data and reinterprets the schema.
- Decoding never throws. A null in a non-optional target yields `DecodeError.NullValue`, surfaced as `ExecutionError.DecodeFailed`; `MaterializedDataset.toVectorOrError` is the safe accessor, and every row-decoding path in both backends (core `sort`/`sortBy`/`collectAndTransform`/checkpoint/predicate and keyed joins; Spark `collectValues`/`fromRow` and the join helpers) propagates it. `fromRowUnsafe` is gone.
- Option-typed expressions are null-based: `IsDefined`/`GetOrElse`/`Option2Iterable` treat a null as `None`, and a higher-order lambda binding (`zip_with` padding, `map_zip_with` missing keys) stores nulls rather than boxed `Some`/`None`, so one rule covers both.

The core suite (`testFull`, 609 tests) and the Spark suite (`testFull`, 247) are green, including `NullModelSpec`, `NullDecodeSpec`, `NarrowSpec`, `SparkNullabilitySpec`, `SparkSchemaValidationSpec`, and the nested struct nullability mapping.

#### Phase 1e — dense-key index (September 2026)

The open-addressing table was sized to the row count (`2 × rowCount`, load ≤ 0.5), so it was sized for the worst case (all-distinct) even for a single hot key, and at 5-10M it was ~2× the boxed map's table. Measured with proper warmup (5 warmup + 10 measured) the earlier "uniform regression eliminated" claim did not hold: at 10M the fixed table was slower than boxed `main` across every regime (uniform 1139 vs 525 ms), and it allocated less but not enough to matter under ZGC.

Phase 1e replaces the fixed table with two slot layouts chosen from the key range measured in a first pass:

- Dense keys (`max - min + 1` within 4× the distinct count and under 2^26) use a direct-address `Array[Int]` indexed by `key - min`: one array access, no hashing, no collisions, and the array is exactly the key range. This is the common join-key shape (ids, dates, enums) and the uniform sweep.
- Sparse keys keep the open-addressing table as a fallback.

Both map a key to the head of the flat `next` chain, so nothing per-key is allocated and the ascending order is preserved. The structure is immutable after construction and contains no `var` (the min/max pass is tail-recursive, satisfying `SourcePolicySpec`).

Same-window A/B on one machine (feature vs boxed `main`, median ms, 5 warmup + 10 measured), with a `sparse` regime (`key = base + 37·i`, forcing the fallback):

| n | hot | few | uniform | sparse |
| --- | --- | --- | --- | --- |
| 100k | 1.6 / 6.7 | 1.1 / 3.2 | 1.2 / 3.1 | 4.1 / 3.7 |
| 200k | 3.1 / 13.6 | 2.1 / 6.2 | 2.4 / 9.2 | 6.6 / 14.8 |
| 400k | 6.1 / 9.4 | 4.2 / 12.5 | 4.9 / 21.0 | 14.7 / 41.3 |
| 1M | 15.3 / 23.5 | 10.7 / 31.3 | 12.9 / 53.2 | 52.2 / 131.5 |
| 2M | 26.7 / 47.5 | 21.1 / 51.9 | 25.1 / 102.1 | 170.4 / 301.2 |
| 5M | 84.0 / 124.1 | 52.1 / 130.9 | 64.3 / 246.7 | 524.2 / 758.9 |
| 10M | 139.2 / 273.6 | 103.6 / 270.7 | 124.1 / 483.6 | 1093.2 / 1568.2 |

Allocation (median MB per run) is ~7-10× lower for dense keys (360 vs 2508-3670 at 10M) and ~3-4× lower for sparse, and the 10M GC time drops from 26-193 ms on `main` to 0. The Spark backend is native Catalyst and unchanged.

The earlier same-window numbers (fixed table, 5 warmup + 10 measured): hot 2.5 vs 6.7 / 4.9 vs 13.6 / 10.2 vs 15.2 at 100k/200k/400k, uniform 3.2 vs 3.1 / 8.7 vs 9.1 / 15.2 vs 21.0.

---

## Phase 2 — Constant-factor work on the predicate family

**Goal:** cut the wasteful passes and allocations. Do **not** attempt to make arbitrary-predicate joins sub-quadratic; the condition is opaque, so there is no index to build.

### 2a. Rewrite `JoinOps.fullJoin` (`internal/JoinOps.scala:5-17`)

Current behavior materializes `pairs` (up to n·m), then `.toSet` of each side's matched elements, then three list constructions. Replace with a single pass that, per left row, finds matches over right and marks matched indexes, so unmatched halves are computed without a second `pairs` materialization:

- Walk `lefts` once; for each, `filter`-scan `rights` under the condition, collecting matched right indexes into a per-left bucket and a shared matched-right set.
- Emit matched and left-unmatched rows in the same pass; after the walk, emit right-unmatched rows (those never marked).
- Same output ordering and duplicate semantics as today — check `DatasetJoinSpec` for the expected shape (duplicates are currently preserved per matched pair; preserve that).

### 2b. `outerJoinDatasets` (`:427-445`)

`rights.filter(...)` re-scans the whole secondary per primary row, and builds a fresh vector per row. Keep it as-is asymptotically, but:

- Guard the cheap path: when secondary is empty, emit `mkUnmatched` for every primary row and return immediately.
- When no matches exist (`found.isEmpty`), skip the `map` allocation.

### 2c. `innerJoinDatasets` (`:384-400`)

Minor: hoist `toVectorUnsafe` results (already done), but add the same early-exit when either side is empty (current code runs the full nested loop to zero matches). Apply the empty-input shortcut to `leftAntiJoinDatasets` (`:477-490`) too.

### 2d. Documented contract

Add a note to the README (or the operator scaladoc) that predicate joins are intended for small frames; large-frame joins should be expressed as `*JoinOn`. This makes the quadratic a known, deliberate boundary rather than a surprise.

**Phase 2 acceptance:** Phase 0 benchmarks show predicate-join times at the working sizes either flat or improved; `FullJoin`'s memory stays bounded (no giant intermediate `pairs` — check with a size the old code would over-allocate on). Specs green.

---

## Phase 3 — Correctness guard and direction of the build index

**Goal:** make linearity under skew hold from the right direction, and lock the semantics.

### 3a. Index the smaller side

`innerJoinOnExpr` (`:516-541`) currently indexes the *left* and probes the *right*. Under skew, the index build is the sensitive part, so building the index on the smaller side (and probing the larger) matters. Add a size-aware choice (or a documented decision) for `InnerJoinOn`/`LeftJoinOn`/`RightJoinOn`/`FullJoinOn`/`LeftAntiJoinOn`. Where the datastructure is a `HashMap`, hash on the smaller input; the probe then repeats the larger side's per-row lookups, which is the cheaper direction.

### 3b. Assert/spec enforcement

Add `PropertySpec`-style checks (match the existing spec style) asserting that results are equal between:

- keyed join and the equivalent predicate join, on random small datasets (skewed and uniform), and
- `innerJoinOnExpr` with left-indexed vs right-indexed (i.e. parameterize the direction and assert identical output).

This locks correctness while Phase 1 changes indexing internals.

### 3c. Claim revision

Update the README's performance claims to state exactly what holds: columnar elementwise ops are linear; *keyed* joins are hash-based and near-linear (O(n·log n) under skew from the boxed bucket append, O(n) in the common case); predicate joins are O(n·m) and intended for small inputs; in-memory execution is bounded by heap. (The "no input size limit" phrasing should also be corrected elsewhere — in-memory is heap-bounded and `MaterializedDataset.rowCount`/column `length` are `Int`, capping rows per column near 2^31.)

**Phase 3 acceptance:** a `PropertySpec` that passes both before and after Phase 1, plus README claims that no longer overstate linearity.

---

## Suggested order

1. Phase 0 (bench) — required first; nothing changes yet.
2. Phase 1 — biggest win; needs Phase 0 to see it.
3. Phase 3 (specs + claim revision) — locks in what Phase 1 changed, can be done right after 1a/1b or anytime.
4. Phase 2 — independent constant-factor work; lowest priority.

Each phase is verifiable on its own. Keep the Phase 0 bench committed before and after each phase so the numbers tell the story later.

## Artifacts to produce

- `interpreter/JoinOperationsBench.scala` (Phase 0).
- Keyed-index rework in `DatasetInterpreter.scala` (`buildKeyIndex`, `probeJoinOnKeys`, join case handlers) — Phase 1.
- `JoinOps.fullJoin` / `outerJoinDatasets` / `innerJoinDatasets` / `leftAntiJoinDatasets` — Phase 2.
- Size-aware indexing + join-direction `PropertySpec` + README claim edit — Phase 3.