# Strongbow Columnar Architecture Plan

## Vision

Strongbow has columnar storage — typed arrays (`Array[Int]`, `Array[Long]`,
`Array[Double]`, `Array[String]`) tracked by `Column` enum cases. But the
expression interpreter has been treating this as row-oriented data with a
columnar storage backend. The `eval` method reads a single value from a
column, computes a single result, wraps it in `Either`, and returns it.
Then `evalColumnRowByRow` loops `eval` to rebuild a column from individual
values. That's column → row → column — a round trip through the type
boundary that creates the Cell boundary cast and allocates an `Either` per
row per expression node.

The correct architecture: **evaluate expressions as column transformations**.
An `Add(left, right)` takes two `IntColumn`s and produces an `IntColumn`.
A `Lower(expr)` takes a `StringColumn` and produces a `StringColumn`. The
typed arrays flow through directly. No per-row dispatch, no Cell boundary
cast, no `Either` per row. Zero `asInstanceOf` anywhere.

This is what Spark does. Catalyst compiles expressions to columnar batch
operations via Tungsten code generation. dwh-core's entire trajectory is
toward eliminating per-row evaluation (UDFs, typed Dataset.map). Strongbow
should be columnar from the ground up — not a row engine with columnar
storage bolted on.

## Current State

ExprInterpreter has five evaluation methods:

| Method | Path | Casts | Role |
|---|---|---|---|
| `eval` | Per-row, typed | 7 (Cell boundary) | Primary row evaluator |
| `evalAny` | Per-row, untyped | ~143 | "Fast path" that discards types |
| `evalBoolean` | Per-row, untyped bool | ~24 | Boolean shortcut via evalAny |
| `evalColumn` | Columnar, partially vectorized | ~34 | Bulk path for some ops |
| `evalAggregation` | Per-row via eval | ~22 | Aggregation accumulation |

Total: 235 `asInstanceOf` calls. Only 7 are at a justified boundary.

`evalColumn` is already partially columnar — it has vectorized paths for
arithmetic (Add, Sub, Mul, Div for Int/Long/Double), comparisons, boolean
logic, Concat, Length, When, and Cell/Const. Everything else falls through
to `evalColumnRowByRow` which loops `eval`.

## Target State

| Method | Path | Casts | Role |
|---|---|---|---|
| `evalColumn` | Columnar, exhaustive | 0 | The single evaluation path |
| `evalAggregation` | Columnar | 0 | Aggregation on typed columns |

Total: 0 `asInstanceOf` calls. Every Expr case evaluates as a column
transformation on typed arrays. The `eval`, `evalAny`, and `evalBoolean`
methods no longer exist.

## Why This Eliminates All Casts

The Cell boundary cast exists because `eval` reads a single value from a
typed array into the generic GADT type parameter `a`:

```scala
column.getInt(idx).asInstanceOf[a]
```

In the columnar model, we never extract a single value. We operate on the
entire `Array[Int]` directly:

```scala
case add: Expr.Add[Row] =>
  for {
    leftCol <- evalColumn(add.left, columns, ColumnType.IntType)
    rightCol <- evalColumn(add.right, columns, ColumnType.IntType)
  } yield {
    (leftCol, rightCol) match {
      case (Column.IntColumn(ld, ln), Column.IntColumn(rd, rn)) =>
        val out = new Array[Int](rowCount)
        var i = 0
        while (i < rowCount) { out(i) = ld(i) + rd(i); i += 1 }
        Column.int(out, ln | rn)
    }
  }
```

`ld` is `Array[Int]` from pattern matching the sealed enum. `rd` is
`Array[Int]`. `out` is `Array[Int]`. Zero casts. The types flow through
Scala's pattern matching on sealed enums.

## Why This Reduces Memory Pressure

Per-row `eval` allocates:
- One `Either[ExecutionError, A]` per row per expression node
- Boxed primitives when `A` is `Int`/`Long`/`Double`/`Boolean` (JVM boxes
  primitives in `Either`/`Right`)

For a 1M row dataset with `filter(Add(cell1, cell2) > Const(100))`:
- `eval(Add(...))`: 1M `Right(Int)` allocations (boxed Int)
- `eval(Gt(...))`: 1M `Right(Boolean)` allocations (boxed Boolean)
- `eval(cell1)`: 1M `Right(Int)` allocations
- `eval(cell2)`: 1M `Right(Int)` allocations
- Total: ~4M object allocations for a simple filter

Columnar `evalColumn`:
- `evalColumn(cell1)`: returns existing `IntColumn` reference (0 allocations)
- `evalColumn(cell2)`: returns existing `IntColumn` reference (0 allocations)
- `evalColumn(Add(...))`: allocates one `Array[Int]` of 1M elements
- `evalColumn(Gt(...))`: allocates one `Array[Boolean]` of 1M elements
- Total: 2 array allocations

The difference: ~4M boxed object allocations vs 2 primitive array
allocations. On a 1M row dataset, that's the difference between triggering
GC and not triggering GC.

## Implementation Plan

### Phase 1: Make evalColumn Exhaustive

Currently `evalColumn` handles ~20 Expr cases with vectorized paths and
falls through to `evalColumnRowByRow` for everything else. Make it handle
all 167 cases.

For each Expr case, the pattern is:
1. Recursively `evalColumn` the sub-expressions to get typed Columns
2. Pattern match the Column enum to access typed arrays
3. Operate on the arrays
4. Construct the result Column

**String operations** (Lower, Upper, Trim, etc.):
```scala
case lo: Expr.Lower[Row] =>
  evalColumn(lo.expr, columns, ColumnType.StringType).map {
    case Column.StringColumn(data, nulls) =>
      val out = new Array[String](rowCount)
      var i = 0
      while (i < rowCount) { out(i) = if (nulls.contains(i)) null else data(i).toLowerCase; i += 1 }
      Column.string(out, nulls)
  }
```

**Math operations** (Sqrt, Log, Sin, etc.):
```scala
case sq: Expr.Sqrt[Row] =>
  evalColumn(sq.expr, columns, ColumnType.DoubleType).map {
    case Column.DoubleColumn(data, nulls) =>
      val out = new Array[Double](rowCount)
      var i = 0
      while (i < rowCount) { out(i) = Math.sqrt(data(i)); i += 1 }
      Column.double(out, nulls)
  }
```

**Comparisons** already have `vectorizedComparison` but with Ordering casts.
Rewrite with typed column access:
```scala
case gt: Expr.Gt[Row, a] =>
  val opType = inferExprColumnType(gt.left, columns)
  for {
    leftCol <- evalColumn(gt.left, columns, opType)
    rightCol <- evalColumn(gt.right, columns, opType)
  } yield {
    val out = new Array[Boolean](rowCount)
    (leftCol, rightCol) match {
      case (Column.IntColumn(ld, _), Column.IntColumn(rd, _)) =>
        var i = 0; while (i < rowCount) { out(i) = ld(i) > rd(i); i += 1 }
      case (Column.DoubleColumn(ld, _), Column.DoubleColumn(rd, _)) =>
        var i = 0; while (i < rowCount) { out(i) = ld(i) > rd(i); i += 1 }
      case (Column.LongColumn(ld, _), Column.LongColumn(rd, _)) =>
        var i = 0; while (i < rowCount) { out(i) = ld(i) > rd(i); i += 1 }
      case (Column.StringColumn(ld, _), Column.StringColumn(rd, _)) =>
        var i = 0; while (i < rowCount) { out(i) = ld(i).compareTo(rd(i)) > 0; i += 1 }
      case _ =>
        var i = 0; while (i < rowCount) { out(i) = gt.ordering.gt(leftCol.getValue(i).asInstanceOf[a], rightCol.getValue(i).asInstanceOf[a]); i += 1 }
    }
    Column.boolean(out)
  }
```

Wait — that last case still has `asInstanceOf`. For non-primitive orderings
(custom types stored in AnyColumn), we can't avoid reading via `getValue`
which returns `Any`. But we can limit this to the AnyColumn fallback and
keep all primitive paths cast-free.

**Revised target**: 0 casts for primitive column types (Int, Long, Double,
String, Boolean, Date). A small number of casts for AnyColumn operations
where type erasure is unavoidable (custom types, collection elements).
This matches the Column storage model — typed columns have typed access,
AnyColumn has untyped access.

### Phase 2: Rewrite evalAggregation as Column-Level

Instead of per-row accumulation:
```scala
case sum: Expr.Sum[Row] =>
  evalColumn(sum.expr, columns, ColumnType.IntType).map {
    case Column.IntColumn(data, nulls) =>
      var total = 0
      var i = 0
      while (i < data.length) {
        if (!nulls.contains(i)) total += data(i)
        i += 1
      }
      total
  }
```

Operates on the typed `Array[Int]` directly. Zero boxing, zero Either
allocation per row.

For aggregations that need multiple sub-expressions (Corr, CovarSamp):
evaluate each to a typed Column, then iterate both arrays together.

### Phase 3: Delete eval, evalAny, evalBoolean

After Phases 1-2, nothing calls `eval`. Delete it and its ~500 lines of
per-row logic. Delete `evalAny` (~500 lines) and `evalBoolean` (~120 lines).

`evalColumnRowByRow` also goes — it was the bridge from `evalColumn` to
`eval`, and with `evalColumn` exhaustive, it's unused.

### Phase 4: Update DatasetInterpreter

`DatasetInterpreter.filter` currently calls `evalBoolean`. Replace with
columnar filter:

```scala
ExprInterpreter.evalColumn(predicate, dataset.columns, ColumnType.BooleanType).map {
  case Column.BooleanColumn(data, _) =>
    val indices = (0 until data.length).filter(data(_)).toArray
    // slice all columns by matching indices
}
```

This is the filter-by-boolean-mask pattern used by NumPy, Arrow, Polars,
and every other columnar engine. Evaluate predicate to BooleanColumn, then
slice.

### Phase 5: Keep eval as Test Convenience (Optional)

If per-row evaluation is useful for tests, keep a thin wrapper:
```scala
def eval[Row, A](expr: Expr[Row, A], columns: Vector[Column], rowIdx: RowIndex): Either[ExecutionError, A] = {
  val ct = inferExprColumnType(expr, columns)
  evalColumn(expr, columns, ct).map(col => col.getValue(rowIdx.toInt).asInstanceOf[A])
}
```

This has ONE cast — extracting a single value from a column. It's a
convenience that delegates to the columnar path, not a separate evaluation
engine. And it only exists for tests.

Or: tests use `evalColumn` and extract values via typed column accessors
(`getInt`, `getString`, etc.) — zero casts even in tests.

## Null Handling

The columnar model handles nulls naturally. Every Column has a `BitSet nulls`
tracking which indices are NULL. Column operations propagate nulls:

```scala
case lo: Expr.Lower[Row] =>
  evalColumn(lo.expr, columns, ColumnType.StringType).map {
    case Column.StringColumn(data, nulls) =>
      val out = new Array[String](rowCount)
      var i = 0
      while (i < rowCount) {
        out(i) = if (nulls.contains(i)) null else data(i).toLowerCase
        i += 1
      }
      Column.string(out, nulls)
  }
```

The null stays in the BitSet. The `null` in the array is a placeholder
that's never read (callers check `nulls` first). This is the same model
as Apache Arrow, Parquet, and Spark's Tungsten. No `Right(null)` in the
typed eval path — nulls are metadata, not values.

For GetJsonObject, the result column's `nulls` BitSet marks missing paths.
No `Right(null)` needed.

## Expected Outcome

- `evalColumn` is the single evaluation path — columnar, typed, exhaustive
- `evalAggregation` operates on typed columns directly
- `eval`, `evalAny`, `evalBoolean`, `evalColumnRowByRow` deleted
  (~1200+ lines removed)
- Zero `asInstanceOf` for primitive column types
- Small number of unavoidable casts for AnyColumn (custom types, collection
  elements) — these are the true storage boundary, same category as Arrow's
  untyped buffer access
- Memory pressure dramatically reduced — primitive array allocations instead
  of boxed Either allocations per row per expression node
- All 356 tests pass (rewritten to use columnar evaluation)
- `sbt prepare` clean

## Verification

- `sbt prepare` passes
- `sbt core/test` — all tests pass
- `sbt spark/compile` — spark module compiles
- `grep -c 'asInstanceOf' ExprInterpreter.scala` — target: <10 (AnyColumn
  fallback paths only)
- Zero `asInstanceOf` in any primitive column evaluation path
- Benchmark: filter + arithmetic on 1M rows should show measurable reduction
  in GC pressure vs current implementation
