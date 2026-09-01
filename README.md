# Strongbow

A type-safe columnar dataset library for Scala 3 with Spark integration.

> *Named after Beleg Strongbow, chief of the Marchwardens of Doriath*

## What is Strongbow?

Strongbow is a columnar data processing library where the type system proves correctness at compile time. Datasets are immutable descriptions of computation — pure Scala 3 enums interpreted by pluggable backends (in-memory columnar or Apache Spark).

```scala
import net.ghoula.strongbow.prelude.*

case class Trade(symbol: String, price: Double, quantity: Int)
given Schema[Trade] = Schema.derived
case class SymbolTotal(symbol: String, total: Double)
given Schema[SymbolTotal] = Schema.derived

val columns = Vector(
  Column.string(Array("ACME", "GLOB")),
  Column.double(Array(120.5, 85.0)),
  Column.int(Array(10, 40))
)

val trades: Dataset[Trade] = Dataset.fromColumns(columns, summon[Schema[Trade]]).toOption.get

val result = trades
  .filter(Expr.Cell[Trade, Double]("price_value", ColumnIndex(1)) > Expr.const(100.0))
  .groupByAgg[SymbolTotal](
    keys = Vector(KeySpec("symbol", Expr.Cell("symbol_value", ColumnIndex(0)), ColumnType.StringType)),
    aggs = Vector(agg.sumDouble[Trade](_.price).as("total"))
  )

result.collect match
  case Right(rows) => println(rows) // Vector(SymbolTotal(ACME,120.5))
  case Left(err)   => println(err)
```

The same Dataset plan executes on the in-memory columnar interpreter or pushes to Apache Spark via native Catalyst expressions — no UDFs, no RDDs, no serialization overhead.

## Installation

Strongbow publishes to the Codeberg Maven registry:

```scala
resolvers += "codeberg" at "https://codeberg.org/api/packages/hakim/maven"
libraryDependencies ++= Seq(
  "net.ghoula" %% "strongbow-core" % "0.0.7",
  "net.ghoula" %% "strongbow-spark" % "0.0.7" // Spark backend (optional)
)
```

`strongbow-spark` pulls in Spark SQL 4.2.0 (`Provided` scope in the build; declare your own Spark dependency to match your cluster).

## Key Properties

**Type Safety**
- `Column[+A]` GADT — 20 typed columnar storage variants with compile-time guarantees
- `Expr[Row, A]` GADT — 349 expression cases, all type-checked
- `Dataset[T]` invariant GADT — pattern matching proves transformation types
- `-Yexplicit-nulls` — compiler-enforced null safety
- `-language:strictEquality` — no accidental equality comparisons
- 13 `asInstanceOf` sites in the codebase, each documented and scalafix-suppressed: 8 at
  inline-metaprogramming boundaries (ExprCompiler quote/splice, Schema Mirror
  derivation) and 4 at erased storage-access boundaries in the interpreter (reading
  typed values from `AnyColumn` where type erasure makes pattern matching impossible)
- Zero `var`, zero `throw`, zero `return` in production code

**Columnar Storage**

All Spark 4.2 types covered with typed, unboxed storage:

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

BitSet null tracking — SQL NULL as metadata, not values. All evaluation is columnar — no per-row dispatch. `Array.tabulate` and `foldLeft` throughout.

**Spark Integration**
- Every Expr case maps to a native Spark SQL function, with documented exceptions: the
  try-xpath variants are in-memory-only (Spark's Column model cannot catch per-row
  evaluation failures), and a handful of generator-style expressions (`explode`,
  `variant_explode`) require Dataset-level handling in the in-memory interpreter
- Expression-based joins push to Spark equi-joins
- GroupByAgg pushes to Spark groupBy + agg
- Window functions push to Spark window specs
- No UDFs — everything goes through Catalyst optimization
- Benchmark suites (`SparkOverheadBench`, `SparkComparativeBench`,
  `SparkPlanVsExecBench`) measure Strongbow plan/execute overhead against native Spark
  per release; run them locally for current numbers

## Modules

| Module            | Dependencies    | Purpose                                             |
|-------------------|-----------------|-----------------------------------------------------|
| `strongbow-core`  | Rumil, Sarati   | Dataset/Expr/Column GADT, interpreters, Schema      |
| `strongbow-spark` | Spark SQL 4.2.0 | Spark backend, ExprToColumn translation, benchmarks |

## Compiler Settings

```
-Werror -Wunused:all -language:strictEquality -Yexplicit-nulls -no-indent
```

Scala 3.8.4 on JDK 25 (core) / JDK 21 (Spark module).

## Expression Coverage

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

## Build

```bash
sbt prepare          # scalafmt + scalafix + compile
sbt core/testFull    # full core suite
sbt spark/testFull   # full Spark suite (includes Spark round-trip parity)
```

`sbt 2.0` runs `test` incrementally and caches results; use `testFull` for a full run. CI runs these suites with `FAST_TESTS=1` (excluding the `Slow`-tagged stress suites); see [docs/ci.md](docs/ci.md).

## Dependencies and the Arda Ecosystem

Strongbow depends on two Arda libraries (both declared in `build.sbt`):

- [Rumil](https://codeberg.org/hakim/rumil) — parser combinators with left recursion; the JSON, XML and XPath parsers behind Strongbow's JSON and XML functions
- [Sarati](https://codeberg.org/hakim/sarati) — binary codec with compile-time derivation and the structural AST layers (JSON, XML) that Strongbow's JSON/XPath evaluation runs on

Sibling Arda libraries (not Strongbow dependencies): [Eru](https://codeberg.org/hakim/eru) — typed effect system (`Eru[E, A]`) with Virtual Thread fibers and resource safety — and [Valar](https://codeberg.org/hakim/valar) — type-safe validation with compile-time derivation and error accumulation.

All Arda libraries share the same principles: Scala 3 native, compile-time metaprogramming, zero `asInstanceOf` in core logic, `-Yexplicit-nulls`, `-language:strictEquality`.

---

*Designed and developed by Hakim Jonas Ghoula.*
