# Strongbow

A type-safe columnar dataset library for Scala 3 with Spark integration.

> *Named after Beleg Strongbow, chief of the Marchwardens of Doriath*

## What is Strongbow?

Strongbow is a columnar data processing library where the type system proves correctness at compile time. Datasets are immutable descriptions of computation — pure Scala 3 enums interpreted by pluggable backends (in-memory columnar or Apache Spark).

```scala
import net.ghoula.strongbow.prelude.*

case class Trade(symbol: String, price: Double, quantity: Int)
given Schema[Trade] = Schema.derived

val trades: Dataset[Trade] = Dataset.fromColumns(columns, schema).toOption.get

val result = trades
  .filter(Expr.Cell[Trade, Double]("price_value", ColumnIndex(1)) > Expr.const(100.0))
  .groupByAgg[AggResult](
    keys = Vector(KeySpec("symbol", Expr.Cell("symbol_value", ColumnIndex(0)), ColumnType.StringType)),
    aggs = Vector(agg.sumDouble[Trade](_.price).as("total"))
  )
```

The same Dataset plan executes on the in-memory columnar interpreter or pushes to Apache Spark via native Catalyst expressions — no UDFs, no RDDs, no serialization overhead.

## Key Properties

**Type Safety**
- `Column[+A]` GADT — typed columnar storage with compile-time guarantees
- `Expr[Row, A]` GADT — 167 expression cases, all type-checked
- `Dataset[T]` invariant GADT — pattern matching proves transformation types
- `-Yexplicit-nulls` — compiler-enforced null safety
- `-language:strictEquality` — no accidental equality comparisons
- 6 `asInstanceOf` in the entire codebase (all in macro metaprogramming)

**Columnar Architecture**
- Typed primitive arrays (`Array[Int]`, `Array[Long]`, `Array[Double]`) — no boxing
- BitSet null tracking — SQL NULL as metadata, not values
- All evaluation is columnar — no per-row dispatch
- `Array.tabulate` and `foldLeft` throughout — zero `var`, zero `return`, zero `throw`

**Spark Integration**
- Every Expr case maps to a native Spark SQL function
- Expression-based joins push to Spark equi-joins
- GroupByAgg pushes to Spark groupBy + agg
- Window functions push to Spark window specs
- No UDFs — everything goes through Catalyst optimization

## Modules

| Module | Dependencies | Purpose |
|---|---|---|
| `strongbow-core` | Rumil, Sarati | Dataset/Expr/Column GADT, interpreters, Schema |
| `strongbow-spark` | Spark SQL 4.1.1 | Spark backend, ExprToColumn translation |
| `strongbow-bench` | — | Benchmarks |

## Compiler Settings

```
-Werror -Wunused:all -language:strictEquality -Yexplicit-nulls -no-indent
```

Scala 3.8.2 on JDK 25 (core) / JDK 21 (Spark module).

## Expression Coverage

167 Expr cases covering Spark SQL's function surface:

| Category | Examples |
|---|---|
| Arithmetic | Add, Sub, Mul, Div, Mod, Abs, Negate, Round, Floor, Ceil |
| String | Lower, Upper, Trim, Substring, Replace, RegexpReplace, Split, Like |
| Comparison | Gt, Lt, Eq, Neq, Between, In, IsNull, Coalesce |
| Math | Sqrt, Pow, Log, Exp, Sin, Cos, Atan2, Signum |
| Date | DateAddDays, DateDiff, ExtractYear, Quarter, DateTrunc, DateFormat |
| Aggregation | Sum, Avg, Max, Min, Count, StdDev, Variance, Corr, Median, Mode |
| Window | RowNumber, Rank, DenseRank, Lag, Lead, NTile, PercentRank |
| Collection | ArraySize, ArrayContains, Flatten, MapKeys, MapValues, MapConcat |
| Hashing | Md5, Sha1, Sha2, Hex, Base64Encode, UrlEncode |
| JSON | GetJsonObject (via Rumil parser) |
| Casting | CastToLong, CastToDouble, CastToString |

## Build

```bash
sbt prepare       # scalafmt + scalafix + compile
sbt core/test      # run tests (330 passing)
sbt spark/compile  # verify Spark module
```

## Part of the Arda Ecosystem

Strongbow is built on the Arda family of Scala 3 libraries:

- [Rumil](https://codeberg.org/hakim/rumil) — parser combinators with left recursion, zero-allocation backtracking, and built-in JSON/CSV/XML parsers
- [Sarati](https://codeberg.org/hakim/sarati) — binary codec with compile-time derivation and structural AST layers (JSON, TOML, YAML, XML)
- [Eru](https://codeberg.org/hakim/eru) — typed effect system (`Eru[E, A]`) with Virtual Thread fibers, resource safety, and concurrency primitives (Ref, Semaphore, Queue, Promise)
- [Valar](https://codeberg.org/hakim/valar) — type-safe validation with compile-time derivation and error accumulation

All Arda libraries share the same principles: Scala 3 native, compile-time metaprogramming, zero `asInstanceOf` in core logic, `-Yexplicit-nulls`, `-language:strictEquality`.

---

*Designed and developed by Hakim Jonas Ghoula.*
