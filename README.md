# Vairë

[![Maven Central](https://img.shields.io/maven-central/v/net.ghoula/vaire-core_3?label=Maven%20Central)](https://github.com/hakimjonas/vaire/releases)
[![CI](https://github.com/hakimjonas/vaire/actions/workflows/ci.yml/badge.svg)](https://github.com/hakimjonas/vaire/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-LGPLv3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0)

A type-safe columnar dataset library for Scala 3, with one plan that runs on an in-memory
columnar engine or Apache Spark.

> *Named after Vairë the Weaver, who weaves all things that have been in Time into her storied
> webs — columns into the record.*

## What is Vairë?

Vairë describes data processing as an immutable plan. A `Dataset[T]` is a typed tree of
transformations, and you decide how to run it: the in-memory columnar interpreter for tests, small
data, and per-row error handling, or the Spark backend, which translates the same plan to native
Catalyst expressions with no UDFs and no RDDs.

The type parameter is checked at compile time. A `Dataset[Trade]` filtered and grouped into a
`Dataset[SymbolTotal]` is a compile error if the types do not line up, so a mistyped transformation
fails at build time rather than on a cluster. `-Yexplicit-nulls` and `-language:strictEquality` are
on for the whole codebase.

## Quick start

Vairë is published to Maven Central:

```scala
libraryDependencies ++= Seq(
  "net.ghoula" %% "vaire-core" % "1.1.0-alpha",
  "net.ghoula" %% "vaire-spark" % "1.1.0-alpha" // Spark backend (optional)
)
```

`vaire-spark` pulls in Spark SQL 4.2.0 (`Provided` scope in the build; declare your own Spark
dependency to match your cluster).

The latest release is `1.1.0-alpha`; see [Releases](https://github.com/hakimjonas/vaire/releases)
and [CHANGELOG.md](CHANGELOG.md) for the change history.

Build a dataset from columns, transform it, and collect the result:

```scala
import net.ghoula.vaire.prelude.*

case class Trade(symbol: String, price: Double, quantity: Int)
given Schema[Trade] = Schema.derived

case class SymbolTotal(symbol: String, total: Double)
given Schema[SymbolTotal] = Schema.derived

val trades: Dataset[Trade] = Dataset
  .fromColumns(
    Vector(
      Column.string(Array("ACME", "GLOB", "ACME")),
      Column.double(Array(120.5, 85.0, 99.9)),
      Column.int(Array(10, 40, 5))
    ),
    summon[Schema[Trade]]
  )
  .toOption.get

val totals = trades
  .where(_.quantity > 5)
  .groupByAgg[SymbolTotal](
    keys = Vector(KeySpec("symbol", Expr.Cell[Trade, String]("symbol", ColumnIndex(0)), ColumnType.StringType)),
    aggs = Vector(agg.sumDouble[Trade](_.price).as("total"))
  )

totals.collect match
  case Right(rows) => println(rows) // Vector(SymbolTotal(ACME, 120.5), SymbolTotal(GLOB, 85.0))
  case Left(err)   => println(err)
```

`collect` runs the in-memory interpreter. `where` and the other lambda helpers compile to typed
`Expr` values at compile time, so there is no reflection and no per-row dispatch at run time.

## Common operations

Filter and transform with lambdas, which compile to typed `Expr`:

```scala
trades.where(_.price > 100.0)
trades.withFields(t => t.copy(quantity = t.quantity * 2))
trades.sortByColumn(_.price)
```

Project to a subset of fields by matching names between the two case classes:

```scala
case class SymbolPrice(symbol: String, price: Double)
given Schema[SymbolPrice] = Schema.derived

trades.project[SymbolPrice]
```

Join with a key expression, so the in-memory engine builds a hash index and Spark pushes an
equi-join:

```scala
val prices: Dataset[SymbolPrice] = ???
val tradeKey: Expr[Trade, String] = Expr.Cell("symbol", ColumnIndex(0))
val priceKey: Expr[SymbolPrice, String] = Expr.Cell("symbol", ColumnIndex(0))

trades.joinOn(prices, tradeKey, priceKey, ColumnType.StringType, ColumnType.StringType)
```

The predicate joins (`join`, `leftJoin`, `rightJoin`, `fullJoin`, `antiJoin`) take an arbitrary
`(T, U) => Boolean` and compare every pair of rows. They are for small frames; use the `*JoinOn`
family for anything large.

## Running on Spark

Read a DataFrame into a `Dataset`, run the same plan, and write the result back:

```scala
import net.ghoula.vaire.spark.{SparkDatasets, SparkInterpreter}

val interpreter = SparkInterpreter(spark)

// A derived schema names its columns symbol_value, price_value, quantity_value;
// rename the source to match before wrapping it.
val df = spark.read.parquet("/data/trades").toDF("symbol_value", "price_value", "quantity_value")
val trades = SparkDatasets.fromDataFrame(df, summon[Schema[Trade]])

val filtered = trades.where(_.price > 100.0)
val out = interpreter.toDataFrame(filtered).toOption.get
out.write.parquet("/data/filtered")
```

`collect(using interpreter)` runs the plan on Spark and returns the rows, and
`SparkInterpreter.toDataFrame` returns the DataFrame for the plan. Every `Expr` case maps to a
native Spark SQL function, so Catalyst optimizes the whole plan.

## Type safety

- `Column[+A]` GADT — 20 typed columnar storage variants with compile-time guarantees
- `Expr[Row, A]` GADT — 349 expression cases, all type-checked
- `Dataset[T]` invariant GADT — pattern matching proves transformation types
- `-Yexplicit-nulls` — compiler-enforced null safety
- `-language:strictEquality` — no accidental equality comparisons
- 14 `asInstanceOf` casts, each on a scalafix-suppressed line: 5 in `ExprCompiler`
  quote/splice, 4 in `Schema` (Mirror derivation and the `Vector[Any]` encode/decode
  boundary), and 5 at erased storage-access boundaries in the interpreter (reading typed
  values from `AnyColumn`, where type erasure rules out pattern matching)
- Zero `var`, zero `throw`, zero `return` in production code

## Performance

In-memory execution is columnar: expressions evaluate whole columns at a time, so elementwise
operations are linear in the row count with no per-row dispatch. The joins are the part worth
stating precisely:

- **Keyed joins** (`joinOn`, `leftJoinOn`, `rightJoinOn`, `fullJoinOn`, `antiJoinOn`, `semiJoinOn`)
  are hash-based: the index build and probe are linear in the input rows, and the output size is the
  remaining term. The index is unboxed — dense keys use a direct-address array, sparse keys an
  open-addressing table.
- **Predicate joins** (`join`, `leftJoin`, `rightJoin`, `fullJoin`, `antiJoin`) evaluate the
  condition on every pair of rows (O(n·m)) and are intended for small frames. Express large-frame
  joins as `*JoinOn` so the in-memory interpreter builds a key index and Spark pushes a native
  equi-join.
- **In-memory execution is heap-bounded.** A column holds its rows in an `Array` and
  `MaterializedDataset.rowCount` is an `Int`, so a single column is capped near 2^31 rows. Use the
  Spark backend for data that does not fit the driver heap.

## Design and internals

The library separates the description of a computation from its execution. `Dataset[T]`,
`Expr[Row, A]`, and `Column[+A]` are sealed Scala 3 enums; building a plan performs no computation,
and an `Interpreter` executes it. The in-memory interpreter evaluates `Expr` with `evalColumn`, the
single columnar evaluation path, and the Spark backend translates the same `Expr` tree to Catalyst
`Column`s.

All Spark 4.2 types are covered with typed, unboxed storage:

| Category        | Types                                                             | Storage                                                |
|-----------------|-------------------------------------------------------------------|--------------------------------------------------------|
| Primitive       | Int, Long, Double, Float, Short, Byte                             | `Array[T]` (unboxed)                                   |
| String          | String, Char(n), Varchar(n)                                       | `Array[String\|Null]`                                  |
| Boolean         | Boolean                                                           | `Array[Boolean]`                                       |
| Temporal        | Date, Timestamp, TimestampNTZ, Time, YearMonthInterval, DayTimeInterval | Opaque types over `Array[Int/Long]`              |
| Binary          | Binary                                                            | Flat Arrow-style layout (`Array[Byte]` + offset array) |
| Decimal         | Decimal(p, s) where p <= 18                                       | Unscaled `Array[Long]` + precision/scale metadata      |
| Nested          | Array, Map, Struct                                                | Typed child columns + offset arrays (unboxed elements) |
| Semi-structured | Variant                                                           | `AnyColumn` (Spark VariantVal round-trip)              |

BitSet null tracking — SQL NULL as metadata, not values. `Array.tabulate` and `foldLeft` throughout.

## Modules

| Module            | Dependencies    | Purpose                                             |
|-------------------|-----------------|-----------------------------------------------------|
| `vaire-core`  | Rumil, Sarati   | Dataset/Expr/Column GADT, interpreters, Schema      |
| `vaire-spark` | Spark SQL 4.2.0 | Spark backend, ExprToColumn translation, benchmarks |

## Expression reference

349 Expr cases covering Spark SQL's function surface:

| Category      | Examples                                                                                     |
|---------------|----------------------------------------------------------------------------------------------|
| Arithmetic    | Add, Sub, Mul, Div, Mod, Abs, Negate, Round, Floor, Ceil                                     |
| String        | Lower, Upper, Trim, Substring, Replace, RegexpReplace, Split, Like                            |
| Comparison    | Gt, Lt, Eq, Neq, Between, In, IsNull, Coalesce, Greatest, Least                               |
| Math          | Sqrt, Pow, Log, Exp, Sin, Cos, Atan2, Signum                                                  |
| Date/Time     | DateAddDays, DateDiff, ExtractYear, DateTrunc, TimeBucket, TimeToSeconds                     |
| Aggregation   | Sum, Avg, Max, Min, Count, StdDev, Corr, Median, Mode, Percentile, RegrSlope                 |
| Window        | RowNumber, Rank, DenseRank, Lag, Lead, NTile, PercentRank                                     |
| Collection    | ArraySize, ArrayContains, Flatten, MapKeys, MapValues, ArrayDistinct, ElementAt               |
| Higher-order  | Transform, Filter, Exists, ForAll, Aggregate, ZipWith, MapFilter, MapZipWith, ArraySortComparator |
| JSON          | GetJsonObject, JsonTuple, FromJson, ToJson, SchemaOfJson, JsonArrayLength (via Rumil parser) |
| XML           | Xpath, XpathString, XpathBoolean, XpathShort/Int/Long/Float/Double (try variants in-memory)  |
| Variant       | ParseJson, VariantGet, TryVariantGet, IsVariantNull, SchemaOfVariant                          |
| Crypto        | Md5, Sha1, Sha2, Hex, Base64Encode, Crc32, Xxhash64, AesEncrypt, AesDecrypt                  |
| Datasketches  | TupleSketchAgg, SketchEstimate, KllSketchAgg (Spark-only)                                     |
| Casting       | CastToLong, CastToDouble, CastToString                                                        |

Documented exceptions: the try-xpath variants are in-memory-only (Spark's Column model cannot catch
per-row evaluation failures), and a few generator-style expressions (`explode`, `variant_explode`)
need Dataset-level handling in the in-memory interpreter.

## Build

```bash
sbt prepare          # scalafmt + scalafix + compile
sbt core/testFull    # full core suite
sbt spark/testFull   # full Spark suite (includes Spark round-trip parity)
```

`sbt 2.0` runs `test` incrementally and caches results; use `testFull` for a full run. CI runs these
suites with `FAST_TESTS=1` (excluding the `Slow`-tagged stress suites); see [docs/ci.md](docs/ci.md).

Public API carries enforced scaladoc coverage: every public member needs a `/** */` doc, and the
`check` gate ratchets coverage so it can only improve (`sbt docCoverage` / `sbt docCoverageSnapshot`).

## Dependencies and the Arda Ecosystem

Vairë depends on two Arda libraries (both declared in `build.sbt`):

- [Rumil](https://github.com/hakimjonas/rumil) — parser combinators with left recursion; the JSON, XML and XPath parsers behind Vairë's JSON and XML functions
- [Sarati](https://github.com/hakimjonas/sarati) — binary codec with compile-time derivation and the structural AST layers (JSON, XML) that Vairë's JSON/XPath evaluation runs on

Sibling Arda libraries (not Vairë dependencies): [Eru](https://github.com/hakimjonas/eru) — typed effect system (`Eru[E, A]`) with Virtual Thread fibers and resource safety — and [Valar](https://github.com/hakimjonas/valar) — type-safe validation with compile-time derivation and error accumulation.

All Arda libraries share the same principles: Scala 3 native, compile-time metaprogramming, zero `asInstanceOf` in core logic, `-Yexplicit-nulls`, `-language:strictEquality`.

## License

Vairë is licensed under [LGPL-3.0-or-later](https://www.gnu.org/licenses/lgpl-3.0.txt) — same as the rest of the Arda stack; see [LICENSE](LICENSE).

---

*Designed and developed by Hakim Jonas Ghoula.*
