# DataSource Abstraction Plan

## Problem

`Dataset.Root` hardcodes `Vector[Column[?]]` as the physical storage:

```scala
case Root[T](columns: Vector[Column[?]], schema: Schema[T]) extends Dataset[T]
```

This couples the logical plan to the in-memory columnar engine. Every
Spark execution pays the cost of building a DataFrame from arrays, even
when the data already lives in Spark. This violates Principle IV of the
Manifesto: separation of description from execution.

Consequence: Strongbow's Spark path is 2-3x slower than dwh-core on
operations like GroupBy and Union purely because of this conversion
overhead — not because of any difference in Spark execution.

## Root Cause

The `Dataset[T]` GADT is supposed to be a purely logical plan. But its
leaf node (`Root`) is physically coupled to columnar arrays. When
`SparkInterpreter.buildPlan` encounters a `Root`, it must convert
`Vector[Column[?]]` to a Spark DataFrame via `DataFrameBuilder.fromColumns`
— boxing every value, creating `Row` objects, calling `spark.createDataFrame`.

dwh-core avoids this entirely because its `TypedDataset.Root` directly
holds a Spark `Dataset[T]`.

## Solution: DataSource Trait

Abstract the physical storage behind a trait in core. The trait is open
so that downstream modules (spark, future backends) can provide
implementations without polluting core's dependencies.

### 1. Define DataSource in core

```scala
// strongbow-core/src/main/scala/net/ghoula/strongbow/DataSource.scala

/** Capability trait representing a physical data source.
  *
  * Interpreters pattern match on concrete implementations to access
  * data in their native format. The trait is open — backend modules
  * extend it without modifying core.
  */
trait DataSource

/** In-memory columnar storage. The default source for Dataset.fromColumns. */
case class InMemorySource(columns: Vector[Column[?]]) extends DataSource
```

### 2. Update Dataset.Root

```scala
case Root[T](source: DataSource, schema: Schema[T]) extends Dataset[T]
```

The GADT case count stays at 35. No new cases. The `source` field
replaces `columns`.

### 3. Define SparkSource in the spark module

```scala
// strongbow-spark/src/main/scala/net/ghoula/strongbow/spark/SparkSource.scala

case class SparkSource(df: DataFrame) extends DataSource
```

No Spark dependency leaks into core. `SparkSource` lives entirely in
`strongbow-spark`.

### 4. Update SparkInterpreter.buildPlan

```scala
case root: Dataset.Root[T] =>
  root.source match {
    case SparkSource(df) =>
      Right(SparkPlan(df, root.schema))

    case InMemorySource(columns) =>
      val df = DataFrameBuilder.fromColumns(spark, columns, root.schema)
      Right(SparkPlan(df, root.schema))

    case other =>
      Left(ExecutionError.UnsupportedOperation(
        s"SparkInterpreter does not support ${other.getClass.getSimpleName}"))
  }
```

Zero `asInstanceOf`. Pattern matching on concrete case classes provides
type refinement. The catch-all `case other` is required by the compiler
for an open trait and returns a proper `Left`.

### 5. Update DatasetInterpreter

```scala
case root: Dataset.Root[T] =>
  root.source match {
    case InMemorySource(columns) =>
      // existing columnar logic
    case other =>
      Left(ExecutionError.UnsupportedOperation(
        s"DatasetInterpreter does not support ${other.getClass.getSimpleName}"))
  }
```

### 6. Factory methods

**Existing** (unchanged API, internal wrapping):

```scala
// Dataset.fromColumns stays the same for callers
def fromColumns[T](columns: Vector[Column[?]], schema: Schema[T]): Either[SchemaError, Dataset[T]] = {
  // validation...
  Right(Root(InMemorySource(columns), schema))
}
```

**New** (in spark module):

```scala
// SparkInterpreter or a companion object
object SparkDatasets {
  def fromDataFrame[T](df: DataFrame, schema: Schema[T]): Dataset[T] =
    Dataset.Root(SparkSource(df), schema)
}
```

## Files Changed

### Core module
- **New:** `DataSource.scala` — trait + `InMemorySource`
- **Modified:** `Dataset.scala` — `Root` holds `DataSource` instead of `Vector[Column[?]]`
- **Modified:** `DatasetInterpreter.scala` — pattern match on `InMemorySource`
- **Modified:** `MaterializedDataset.scala` — `toDataset` wraps in `InMemorySource`
- **Modified:** `DatasetExplainer.scala` — display source type in plan
- **Modified:** `DatasetActions.scala` — if it accesses `root.columns`

### Spark module
- **New:** `SparkSource.scala`
- **New:** `SparkDatasets.scala` — `fromDataFrame` factory
- **Modified:** `SparkInterpreter.scala` — pattern match on source type

### Tests
- Any test that constructs `Dataset.Root` directly (rare — most use
  `Dataset.fromColumns` which handles the wrapping)

## What This Enables

### Zero-overhead Spark pipelines

```scala
val df = spark.read.parquet("/data/sales")
val ds = SparkDatasets.fromDataFrame(df, salesSchema)
val result = ds
  .filter(amountCell > Expr.lit(1000.0))
  .groupByAgg[Summary](keys, aggs)
val outDf = sparkInterpreter.toDataFrame(result).toOption.get
outDf.write.parquet("/output/summary")
```

Data never leaves Catalyst. No array building. No driver materialization.

### Fair benchmarking

Both Strongbow and dwh-core start from a Spark-native data source.
The benchmark measures plan building + Spark execution only — no
physical conversion overhead.

### Future backends

A hypothetical `ArrowSource`, `DeltaSource`, or `IcebergSource` follows
the same pattern: define a case class extending `DataSource`, pattern
match in the interpreter. Core stays untouched.

## Non-exhaustive match handling

`DataSource` is an open trait. Pattern matches on it are inherently
non-exhaustive. The compiler requires a catch-all branch. This is
correct — each interpreter handles the sources it understands and
rejects others via `Left(ExecutionError.UnsupportedOperation(...))`.

No `@unchecked` needed. The catch-all is honest error handling, not
suppression.

## Migration

The change is backwards-compatible at the API level:

- `Dataset.fromColumns(columns, schema)` continues to work — it wraps
  in `InMemorySource` internally
- `sparkInterpreter.execute(ds)` continues to work — `InMemorySource`
  follows the existing conversion path
- `sparkInterpreter.toDataFrame(ds)` continues to work

The only breaking change is code that pattern matches on `Dataset.Root`
and accesses `.columns` directly. That code changes to match on
`root.source` → `InMemorySource(columns)`.
