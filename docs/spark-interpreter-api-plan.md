# SparkInterpreter API Plan

## Problem

`SparkInterpreter` has a single public method:

```scala
def execute[T](dataset: Dataset[T]): Either[ExecutionError, MaterializedDataset[T]]
```

This always calls `df.collect()` internally, pulling all data to the driver
and converting to columnar `MaterializedDataset`. There is no way to:

- Get a row count without collecting all data
- Write results to Parquet/storage without collecting to driver
- Chain Strongbow operations with native Spark operations
- Register results as temp views for SQL queries
- Stream results without full materialization

dwh-core exposes the Spark `Dataset[T]` directly, giving users full control
over when and how to materialize. This is a production-critical gap.

## Current Architecture

```
Dataset[T] (AST)
    │
    ▼
SparkInterpreter.execute()
    │
    ├── buildPlan() ──► SparkPlan(df: DataFrame, schema: Schema[T])  [private]
    │
    ├── df.collect()                                                  [forced]
    │
    └── RowConverter.toMaterialized() ──► MaterializedDataset[T]      [forced]
```

`buildPlan` is private. `SparkPlan` is a private case class. The user has
no access to the intermediate DataFrame.

## Proposed API

### Option A: Add `toDataFrame` to SparkInterpreter

```scala
class SparkInterpreter(spark: SparkSession) extends Interpreter {

  // Existing — collect to columnar storage
  override def execute[T](dataset: Dataset[T]): Either[ExecutionError, MaterializedDataset[T]]

  // New — build Spark DataFrame without collecting
  def toDataFrame[T](dataset: Dataset[T]): Either[ExecutionError, DataFrame]
}
```

Usage:

```scala
val interpreter = SparkInterpreter(spark)
val ds = dataset.filter(valCell > Expr.lit(500)).groupByAgg[Result](keys, aggs)

// Lazy — stays in Spark
val df = interpreter.toDataFrame(ds).toOption.get
df.count()                          // distributed count, single Long
df.write.parquet("/output/path")    // write without collecting
df.createTempView("results")        // use in SQL

// Eager — collect to driver (existing behavior)
val materialized = interpreter.execute(ds).toOption.get
```

### Option B: Return a richer result type

```scala
case class SparkResult[T](df: DataFrame, schema: Schema[T]) {
  def collect(): Either[ExecutionError, MaterializedDataset[T]] =
    RowConverter.toMaterialized(df.collect(), schema)

  def count(): Long = df.count()

  def write: DataFrameWriter[Row] = df.write

  def toDataFrame: DataFrame = df
}

class SparkInterpreter(spark: SparkSession) extends Interpreter {
  override def execute[T](dataset: Dataset[T]): Either[ExecutionError, MaterializedDataset[T]]

  def plan[T](dataset: Dataset[T]): Either[ExecutionError, SparkResult[T]]
}
```

### Option C: Separate Interpreter trait

The `Interpreter` trait forces `execute` to return `MaterializedDataset`.
Add a Spark-specific trait:

```scala
trait SparkPlanner {
  def toDataFrame[T](dataset: Dataset[T]): Either[ExecutionError, DataFrame]
}

class SparkInterpreter(spark: SparkSession) extends Interpreter with SparkPlanner {
  // ...
}
```

## Recommendation

**Option A** is the simplest and most principled:

- One new public method, minimal API surface
- `buildPlan` stays private — implementation detail unchanged
- No new types to maintain
- Users get the DataFrame and use Spark's existing API
- `execute` remains for users who want the columnar materialization path
- The `Interpreter` trait contract is preserved

Option B adds convenience but introduces a new type that partially
duplicates Spark's DataFrame API. Option C adds a trait for one method.

## Implementation

The change is mechanical — `toDataFrame` delegates to the existing
`buildPlan` and extracts the DataFrame:

```scala
def toDataFrame[T](dataset: Dataset[T]): Either[ExecutionError, DataFrame] =
  buildPlan(dataset).map(_.df)
```

One line of logic. The rest is testing.

## Tests Needed

1. `toDataFrame` returns a valid DataFrame for each Dataset case
   (filter, join, groupBy, sort, etc.)
2. `df.count()` matches `execute().map(_.rowCount)`
3. `df.collect()` decoded manually matches `execute().toVectorUnsafe`
4. DataFrame can be written to Parquet and read back
5. DataFrame can be registered as temp view and queried via SQL

## Benchmark Impact

With `toDataFrame`, the Spark benchmark becomes fair:

```scala
// Strongbow
val df = sparkInterpreter.toDataFrame(dataset.filter(...)).toOption.get
df.count()  // same as dwh-core's TypedDatasetOnDataset(result).count()
```

Both frameworks: build plan → Spark execution → distributed count.
Same starting point, same ending point.

## Expr.Sum Type Fix (Resolved)

Fixed: `Expr.Sum[Row](expr: Expr[Row, Int])` now extends `Expr[Row, Long]`.
A sum of integers can exceed `Int.MaxValue` — the return type must be `Long`.
Both the columnar interpreter and Spark bridge now return `Long` consistently.
