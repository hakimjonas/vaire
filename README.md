# Strongbow

**A strongly-typed, zero-cast DataFrame library for Scala 3**

[![Scala 3.7.4](https://img.shields.io/badge/scala-3.7.4-red.svg)](https://www.scala-lang.org/)

> *Named after Beleg Strongbow, chief of the Marchwardens of Doriath*

## What is Strongbow?

Strongbow is a columnar dataset library that leverages Scala 3's GADT (Generalized Algebraic Data Types) to achieve compile-time type safety without runtime overhead. The architecture eliminates type casts in expression evaluation while maintaining strong type guarantees.

**Core Design:**
- **Zero-cast expression evaluation** - GADT pattern matching eliminates `asInstanceOf` in hot paths
- **Type-specialized columnar storage** - Efficient memory layout with primitive arrays
- **Pure functional API** - Immutable transformation plans with no vars in public API
- **Interpreter pattern** - Single logical plan, multiple execution backends

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

**Architecture:**
- [MANIFESTO.md](MANIFESTO.md) - Project vision and principles
- [ZERO-CAST-ARCHITECTURE.md](ZERO-CAST-ARCHITECTURE.md) - Technical deep-dive on zero-cast design

**Benchmarks & Analysis:**
- [FAIR-COMPARISON-RESULTS.md](FAIR-COMPARISON-RESULTS.md) - Performance validation benchmarks
- [CROSSBOW-CRASH-ANALYSIS.md](CROSSBOW-CRASH-ANALYSIS.md) - Scaling behavior analysis
- Additional benchmark reports available in repository

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

## Architectural Signature

**One Cast at Boundary, Zero in Logic:**
```scala
// Single cast when reading from typed column
case cell: Expr.Cell[Row, a] =>
  column.getInt(idx).asInstanceOf[a]  // Only cast

// Zero casts in expression logic - GADT provides types
case add: Expr.Add[Row] =>
  for {
    l <- eval(add.left, ...)   // l: Int (proven by GADT)
    r <- eval(add.right, ...)  // r: Int (proven by GADT)
  } yield l + r                // No cast needed!
```

**Design Principles:**

1. **Type Safety First** - Compile-time guarantees via GADTs, no unsafe casts in hot paths
2. **Zero Dependencies** - Core library depends only on Scala stdlib
3. **Pure Functional** - Immutable plans, no vars in public API, referentially transparent
4. **Interpreter Pattern** - Separate logical plan from execution strategy
5. **Principled Performance** - Performance through architecture, not shortcuts

## License

MIT License - See [LICENSE](LICENSE) for details

## Contributing

This is a research project exploring Scala 3's type system capabilities. Feedback and contributions welcome!

## Author

Built by Hakim as an exploration of Scala 3 GADTs and zero-cost abstractions.
