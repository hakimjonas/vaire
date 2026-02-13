# Strongbow: Zero-Cast Columnar Dataset Library

**A type-safe, high-performance Scala 3 dataset library with zero-cast architecture**

[![Scala 3.7.4](https://img.shields.io/badge/scala-3.7.4-red.svg)](https://www.scala-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## Overview

Strongbow is a columnar dataset library that achieves both **compile-time type safety** and **runtime performance** through Scala 3's GADT (Generalized Algebraic Data Types) pattern matching. It provides:

- **Zero-cast expression evaluation** - No `asInstanceOf` in hot paths
- **Columnar storage** - Efficient memory layout with type-specialized columns
- **Pure functional API** - Immutable transformations with interpreter pattern
- **Dual execution** - Single plan compiles to both columnar and Spark execution

## Performance Characteristics

We benchmarked against [Crossbow](https://github.com/audienceproject/crossbow), a production-proven Scala 3 DataFrame library, to validate our architectural choices. All benchmarks use identical configuration (Scala 3.7.4, ZGC, 128GB heap) for fair comparison.

### 100K rows

| Operation | Strongbow | Crossbow | Ratio | Memory Ratio |
|-----------|---------|----------|-------|--------------|
| Filter | 3.42 ms | 2.99 ms | 0.87x | 0.60x |
| GroupBy | 7.19 ms | 9.38 ms | 1.30x | 1.10x |
| Sort | 16.01 ms | 59.61 ms | 3.72x | 4.38x |
| Join | 27.38 ms | 510.21 ms | 18.6x | 3.71x |

### Scaling behavior

| Scale | Strongbow | Crossbow |
|-------|---------|----------|
| 100K rows | Completes all operations | Completes all operations |
| 500K rows | Completes all operations | Crashes on Join |
| 1M rows | Completes (313.80ms Join) | Not reached |

**Observations:**
- Type-specialized columns provide significant performance advantages on complex operations (Join, Sort)
- Zero-boxing architecture enables stable operation at higher scales
- Both libraries achieve zero GC at 100K with ZGC
- Crossbow's row-oriented approach has lower overhead for simple filter operations

See [FAIR-COMPARISON-RESULTS.md](FAIR-COMPARISON-RESULTS.md) for detailed analysis and [CROSSBOW-CRASH-ANALYSIS.md](CROSSBOW-CRASH-ANALYSIS.md) for technical explanation of scaling limitations.

## Architecture

### Zero-Cast GADT Expression Evaluation

```scala
enum Expr[Row, +A] {
  case Add[Row](left: Expr[Row, Int], right: Expr[Row, Int]) extends Expr[Row, Int]
  case Mul[Row](left: Expr[Row, Int], right: Expr[Row, Int]) extends Expr[Row, Int]
  case Sum[Row]() extends Expr[Row, Long]
  // ... more cases
}

def eval[Row, A](expr: Expr[Row, A], ...): Either[ExecutionError, A] = expr match {
  case add: Expr.Add[Row] =>
    for {
      l <- eval(add.left, columns, rowIdx)   // l: Int by GADT
      r <- eval(add.right, columns, rowIdx)  // r: Int by GADT
    } yield l + r  // NO CAST NEEDED!
}
```

Scala 3 GADT pattern matching provides **automatic type refinement**, eliminating the need for runtime casts in expression evaluation and aggregation logic.

### Columnar Storage

```scala
enum Column {
  case IntColumn(data: Array[Int]) extends Column
  case StringColumn(data: Array[String]) extends Column
  case BooleanColumn(data: Array[Boolean]) extends Column
  // ...
}
```

Type-specialized columns provide:
- Efficient memory layout (no boxing)
- Cache-friendly access patterns
- Direct primitive operations

### Interpreter Pattern

```scala
enum Dataset[+T] {
  case Root[T](columns: Vector[Column], schema: Schema[T])
  case Filter[T](parent: Dataset[T], predicate: Expr[T, Boolean])
  case GroupBy[K, V](parent: Dataset[V], key: V => K)
  // ... transformations as pure data
}

object DatasetInterpreter {
  def execute[T](dataset: Dataset[T]): MaterializedDataset[T]
}

object SparkInterpreter {  // Phase 4
  def toSpark[T](dataset: Dataset[T]): org.apache.spark.sql.Dataset[T]
}
```

One plan, multiple backends - columnar for in-memory, Spark for distributed.

## Features

### Dataset Operations

- **Row selection:** `filter`, `limit`, `distinct`
- **Sorting:** `sort`, `sortBy`
- **Combining:** `union`
- **Grouping:** `groupBy`, `keyBy`
- **Column operations:** `select` (expression-based)

### Expressions

- **Arithmetic:** `+`, `-`, `*`, `/`
- **Comparison:** `>`, `<`, `>=`, `<=`, `==`, `!=`
- **Boolean:** `&&`, `||`, `!`
- **String:** `concat`, `length`
- **Conditional:** `when/otherwise` (CASE expressions)
- **Literals:** `lit()` for ergonomic literal creation
- **Naming:** `as()` for column renaming

### Aggregations

- `sum`, `count`, `max`, `min`, `avg`
- All with zero-cast evaluation via GADT

### Grouped Operations

- `reduceByKey` - Reduce values by key
- `mapValues` - Transform values without regrouping
- `flatMapValues` - Expand grouped values
- `filterKeys` - Filter groups by key predicate
- **Joins:** `join`, `leftJoin`, `rightJoin`, `fullJoin`

## Example Usage

```scala
import net.ghoula.strongbow.prelude.*

// Create dataset
val data = Seq(("Alice", 25), ("Bob", 30), ("Charlie", 25))
val dataset = Dataset.fromSeq(data)

// Filter and transform
val adults = dataset
  .filter { case (name, age) => age >= 18 }
  .sortBy(_._2)

// Group and aggregate
val grouped = dataset
  .groupBy(_._2)  // Group by age
  .mapValues(_.map(_._1))  // Extract names

// Execute
val result = DatasetInterpreter.execute(adults)
```

## Project Status

**Current:** Phase 3.5 Complete ✅

- ✅ Phase 1: Core foundation (Column, Schema, Dataset ADT)
- ✅ Phase 2: Columnar interpreter with zero-cast expressions
- ✅ Phase 3: Grouped operations, aggregations, joins
- ✅ Phase 3.5: Complete parity with Crossbow core + benchmarks
- 🚧 Phase 4: Spark interpreter (planned)

**Test Coverage:** 41 tests, 100% passing

## Documentation

- [MANIFESTO.md](MANIFESTO.md) - Project vision and principles
- [ZERO-CAST-ARCHITECTURE.md](ZERO-CAST-ARCHITECTURE.md) - Technical deep-dive on zero-cast design
- [FAIR-COMPARISON-RESULTS.md](FAIR-COMPARISON-RESULTS.md) - Benchmark comparison with Crossbow (Scala 3.7.4, ZGC)
- [CROSSBOW-CRASH-ANALYSIS.md](CROSSBOW-CRASH-ANALYSIS.md) - Analysis of boxing overhead at scale
- [RIGOROUS-BENCHMARK-RESULTS.md](RIGOROUS-BENCHMARK-RESULTS.md) - 10K baseline benchmarks
- [SCALING-BENCHMARK-RESULTS.md](SCALING-BENCHMARK-RESULTS.md) - Scaling results with G1GC
- [CROSSBOW-PARITY-ANALYSIS.md](CROSSBOW-PARITY-ANALYSIS.md) - Feature comparison matrix
- [PHASE3.5-COMPLETE.md](PHASE3.5-COMPLETE.md) - Phase 3.5 completion report

## Building

```bash
# Compile
sbt compile

# Run tests
sbt test

# Run benchmarks
sbt "bench/runMain net.ghoula.strongbow.bench.StrongbowVsCrossbow"
```

**Requirements:**
- Scala 3.7.4
- Java 21+
- sbt 1.12.2

## Design Principles

1. **Zero Dependencies** - Core library depends only on Scala stdlib
2. **Type Safety** - Compile-time guarantees via GADTs and refined types
3. **Performance** - Zero-cast architecture for hot paths
4. **Simplicity** - Pure data structures, interpreter pattern
5. **Testability** - Pure functions, no side effects in plans

## License

MIT License - See [LICENSE](LICENSE) for details

## Contributing

This is a research project exploring Scala 3's type system capabilities. Feedback and contributions welcome!

## Author

Built by Hakim as an exploration of Scala 3 GADTs and zero-cost abstractions.
