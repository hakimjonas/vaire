# Strongbow Implementation Plan

> **"The dichotomies of yesterday are the synergies of tomorrow"**
>
> A principled, type-safe dataset library embracing modern Scala 3

---

## The Strongbow Manifesto

### Vision

Strongbow is a **logically pure, execution-flexible dataset library** that treats datasets as **immutable descriptions of computation**, not eager collections. Transformations are represented as data, allowing multiple interpreters to execute the same logical plan on different backends (in-memory columnar, Spark, streaming).

**Core Insight:** Separate description (what) from execution (how). The logical plan is pure Scala 3 ADTs with **zero dependencies**. Interpreters provide execution strategies.

**Key Design Elements:**
- Modern Scala 3 (native enums, GADTs, opaque types, inline)
- Zero dependencies core
- Multiple interpreters from one plan
- Type-specialized columnar storage for performance
- GADT expressions for compile-time type safety

### Architectural Principles

#### I. Zero Dependencies Core

- **strongbow-core**: Pure Scala 3 stdlib only—no external dependencies
- **Logical plan as data**: Dataset[T], Grouped[K, V], Expr[Row, A] are just enums/traits
- **Portable**: Can be used standalone or with optional integration modules
- **Integration modules**: Optional packages add specific capabilities (Spark, validation, effects)

**Principle:** The core is a foundation, not a framework. Integration is opt-in, not mandatory.

#### II. Modern Scala 3 All The Way
*(Embrace the Type System)*

- **Native enums**: For Dataset, Grouped, Expr, Column, ColumnType, Exit, etc.
- **Opaque types**: Zero-cost wrappers for RowIndex, ColumnIndex, Schema identifiers
- **Union types**: Precise error domains without complex hierarchies
- **Extension methods**: Fluent API on enums and opaque types
- **Inline and transparent inline**: Zero-overhead abstractions
- **Given instances**: Ergonomic implicit resolution
- **Prelude pattern**: Single canonical import

**Principle:** The type system is our ally. Use its power to prevent errors at compile time.

#### III. Correctness Through Types
*(Types Guarantee Invariants)*

- **GADT for expressions**: Expr[Row, A] carries type evidence—pattern matching refines types automatically
- **Typed columnar storage**: IntColumn(Array[Int]), not Column(Array[Any])—preserves types at storage layer
- **Minimal boundary casts**: One cast at column access (Cell evaluation), then fully typed expression evaluation
- **Schema as evidence**: Compile-time tracking of dataset structure
- **Smart constructors**: Invalid states unrepresentable
- **Pattern matching refines types**: GADT cases provide evidence in each branch
- **Explicit boundaries**: `unsafe*` methods mark transitions from invariant-checked to raw

**Principle:** If it compiles, the plan is structurally valid. Runtime errors emerge only from data quality, not type mistakes.

**Cast Philosophy:**
- **Columnar interpreter**: ONE cast at Cell access boundary, zero casts in expression logic (we control storage)
- **Spark interpreter**: Unavoidable casts when translating to untyped Spark Column (Spark's limitation)
- Goal: Exploit typed arrays in columnar, accept Spark's constraints where necessary

#### IV. Separation of Description and Execution
*(Pure Programs)*

- **Lazy plan construction**: Building Dataset[T] performs zero computation
- **Explicit interpretation**: Choose execution strategy at the boundary
- **Inspectable plans**: Plans are first-class values for analysis, optimization, serialization
- **Effect boundaries**: Optional Eru integration wraps I/O; core uses Either/Option

**Principle:** Separate what you want (the plan) from how you get it (the interpreter). Description is pure.

#### V. Encapsulated Mutation
*(Purity Outside, Performance Inside)*

- **Smart constructors**: All public constructors return immutable types
- **Private mutable state**: Mutation lives inside interpreters, never exposed
- **Tail recursion preferred**: Use `@tailrec` in hot paths where equivalent
- **Explicit freeze boundaries**: Methods like `unsafeFreeze` mark mutable→immutable

**Principle:** Mutation is an implementation detail. Zero vars in public API.

#### VI. Correctness First, Performance Later
*(Build It Right, Then Make It Fast)*

- **Phase 1-5:** Implement full API with correctness as only goal
- **Phase 6:** Comprehensive test suite covering all operations
- **Phase 7:** Benchmark and identify bottlenecks
- **Phase 8:** Optimize hot paths while preserving correctness

**Principle:** Premature optimization is the root of all evil. A slow correct implementation beats a fast broken one.

#### VII. Optional Arda Integration
*(Composition Without Coupling)*

- **strongbow-validation**: Valar integration for schema validation
- **strongbow-io**: Rumil parsing + Eru effects for I/O
- **strongbow-effects**: Eru wrapping for effect-tracked execution
- **Core remains pure**: No dependency on Arda stack

**Principle:** Each library does one thing well. Composition creates power. Integration is opt-in.

---

## Architecture Overview

```
┌────────────────────────────────────────────────────────┐
│  strongbow-core (ZERO dependencies)                      │
│  - Dataset[T]: Immutable transformation AST            │
│  - Grouped[K, V]: Key-value operations                 │
│  - Expr[Row, A]: Type-safe expressions                 │
│  - Schema[T]: Compile-time evidence                    │
│  - Column: Specialized storage                         │
│  - prelude: Single canonical import                    │
└────────────────────────────────────────────────────────┘
                         ↓ interpret
         ┌───────────────┴───────────────┐
         ↓                               ↓
┌──────────────────────┐        ┌──────────────────────┐
│  strongbow-columnar    │        │  strongbow-spark       │
│  (In-Memory)         │        │  (Distributed)       │
│  - Depends: core     │        │  - Depends: core     │
│  - Column storage    │        │    + spark-sql       │
│  - Returns: Either   │        │  - Translates to     │
└──────────────────────┘        │    Spark Dataset     │
         ↓                      └──────────────────────┘
┌──────────────────────┐                 ↓
│  MaterializedDataset │        ┌──────────────────────┐
│  (Vector[Column])    │        │  Spark Dataset[T]    │
└──────────────────────┘        └──────────────────────┘

Optional Integrations:
┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
│  strongbow-validation  │  │  strongbow-io          │  │  strongbow-effects     │
│  Valar schemas       │  │  Rumil + Eru I/O     │  │  Eru wrapping        │
└──────────────────────┘  └──────────────────────┘  └──────────────────────┘
```

### Key Design Patterns

1. **Logical Plan as Enums**
   - Each operation is an enum case
   - Forms immutable DAG
   - Zero execution at plan level

2. **Interpreter Pattern**
   - Pattern match on plan nodes
   - Recursive interpretation
   - Multiple interpreters, one plan

3. **Prelude Pattern** (from Eru)
   - Single import: `import net.ghoula.strongbow.prelude.*`
   - Re-exports entire public API
   - Hides internal structure

4. **Opaque Types** (from Eru)
   - Zero-cost wrappers
   - Extension methods for operations
   - Example: FiberId, RowIndex, ColumnIndex

5. **Rich Enums** (from Eru)
   - Enum cases with data
   - Pattern matching refines types
   - Example: Exit, InterruptCause

6. **Effect Boundaries** (from Eru)
   - Core uses Either/Option
   - Optional Eru module for effect tracking
   - Explicit `unsafe*` methods

---

## Module Structure

```
strongbow/
├── project/
│   ├── build.properties              # sbt.version=1.12.2
│   └── plugins.sbt                   # scalafmt, scalafix
│
├── .scalafmt.conf                    # Identical to Eru
├── .scalafix.conf                    # Identical to Eru (noVars, etc.)
├── build.sbt                         # Scala 3.7.4, Java 21
├── MANIFESTO.md                      # This document
│
├── strongbow-core/                     # ⚡ ZERO dependencies
│   ├── src/main/scala/com/h3/strongbow/
│   │   ├── Dataset.scala             # Dataset[T] enum
│   │   ├── Grouped.scala             # Grouped[K, V] enum
│   │   ├── Expr.scala                # Expr[Row, A] enum
│   │   ├── Schema.scala              # Schema[T] trait
│   │   ├── Column.scala              # Column enum
│   │   ├── ColumnType.scala          # ColumnType enum
│   │   ├── Aggregator.scala          # Monoid-based aggregation
│   │   ├── types/
│   │   │   ├── RowIndex.scala        # Opaque type
│   │   │   └── ColumnIndex.scala     # Opaque type
│   │   ├── errors/
│   │   │   ├── SchemaError.scala     # Schema validation errors
│   │   │   ├── DecodeError.scala     # Decoding errors
│   │   │   └── ExecutionError.scala  # Runtime errors
│   │   └── prelude.scala             # ⭐ Single canonical import
│   └── src/test/scala/com/h3/strongbow/
│       ├── DatasetLawSuite.scala     # Property-based laws
│       └── ExprLawSuite.scala
│
├── strongbow-columnar/                 # In-memory interpreter
│   ├── src/main/scala/com/h3/strongbow/columnar/
│   │   ├── ColumnarInterpreter.scala # Main interpreter
│   │   ├── ExprInterpreter.scala     # Expression eval
│   │   ├── GroupByInterpreter.scala  # GroupBy with encapsulation
│   │   ├── JoinInterpreter.scala     # Join algorithms
│   │   ├── MaterializedDataset.scala # Result type
│   │   ├── ColumnRow.scala           # Type-safe row access
│   │   └── prelude.scala             # Re-export core + columnar
│   └── src/test/scala/
│       ├── ColumnarInterpreterSuite.scala
│       └── CorrectnessSpec.scala
│
├── strongbow-spark/                    # Spark interpreter
│   ├── src/main/scala/com/h3/strongbow/spark/
│   │   ├── SparkInterpreter.scala    # Spark execution
│   │   ├── ExprToColumn.scala        # Expr → SparkColumn
│   │   ├── DatasetToSpark.scala      # Plan → Spark Dataset
│   │   └── prelude.scala             # Re-export core + spark
│   └── src/test/scala/
│       └── SparkInterpreterSuite.scala
│
├── strongbow-validation/               # ⚡ Optional Valar integration
│   ├── src/main/scala/com/h3/strongbow/validation/
│   │   ├── SchemaValidator.scala     # Valar-powered validation
│   │   └── prelude.scala             # Re-export validation
│   └── src/test/scala/
│       └── ValidationSuite.scala
│
├── strongbow-io/                       # ⚡ Optional Rumil + Eru I/O
│   ├── src/main/scala/com/h3/strongbow/io/
│   │   ├── CsvParser.scala           # Rumil CSV parsing
│   │   ├── JsonParser.scala          # Rumil JSON parsing
│   │   ├── ParquetReader.scala       # Parquet support
│   │   └── prelude.scala             # Re-export I/O
│   └── src/test/scala/
│       └── ParserSuite.scala
│
├── strongbow-effects/                  # ⚡ Optional Eru effects
│   ├── src/main/scala/com/h3/strongbow/effects/
│   │   ├── EruInterpreter.scala      # Eru[E, A] wrapping
│   │   └── prelude.scala             # Re-export effects
│   └── src/test/scala/
│       └── EffectsSuite.scala
│
└── strongbow-bench/                    # Benchmarks
    └── src/test/scala/com/h3/strongbow/bench/
        ├── ColumnarBench.scala       # vs Crossbow
        ├── GroupByBench.scala
        └── JoinBench.scala
```

---

## Phase 1: Core Foundation (Week 1-2)

### Goal: Pure logical plan with zero dependencies

#### 1.1 Expression GADT

**File:** `strongbow-core/src/main/scala/com/h3/strongbow/Expr.scala`

```scala
package net.ghoula.strongbow

/** Type-safe expression language for dataset operations.
  *
  * Expr[Row, A] is a GADT that carries type evidence through pattern matching.
  * No casts needed—types are refined correctly in each case.
  *
  * @tparam Row The row type this expression operates on
  * @tparam A The result type of evaluating this expression
  */
enum Expr[Row, +A]:
  // Leaf nodes
  case Cell[Row, A](name: String, index: ColumnIndex) extends Expr[Row, A]
  case Const[Row, A](value: A) extends Expr[Row, A]

  // Numeric operations
  case Add[Row](left: Expr[Row, Int], right: Expr[Row, Int]) extends Expr[Row, Int]
  case Sub[Row](left: Expr[Row, Int], right: Expr[Row, Int]) extends Expr[Row, Int]
  case Mul[Row](left: Expr[Row, Int], right: Expr[Row, Int]) extends Expr[Row, Int]
  case Div[Row](left: Expr[Row, Int], right: Expr[Row, Int]) extends Expr[Row, Int]

  // Comparisons (GADT carries Ordering evidence)
  case Gt[Row, A](left: Expr[Row, A], right: Expr[Row, A])(using ord: Ordering[A])
    extends Expr[Row, Boolean]:
    def ordering: Ordering[A] = ord

  case Lt[Row, A](left: Expr[Row, A], right: Expr[Row, A])(using ord: Ordering[A])
    extends Expr[Row, Boolean]:
    def ordering: Ordering[A] = ord

  case Eq[Row, A](left: Expr[Row, A], right: Expr[Row, A]) extends Expr[Row, Boolean]

  // Boolean operations
  case And[Row](left: Expr[Row, Boolean], right: Expr[Row, Boolean]) extends Expr[Row, Boolean]
  case Or[Row](left: Expr[Row, Boolean], right: Expr[Row, Boolean]) extends Expr[Row, Boolean]
  case Not[Row](expr: Expr[Row, Boolean]) extends Expr[Row, Boolean]

  // String operations
  case Concat[Row](left: Expr[Row, String], right: Expr[Row, String]) extends Expr[Row, String]
  case Length[Row](expr: Expr[Row, String]) extends Expr[Row, Int]

  // Option operations
  case IsDefined[Row, A](expr: Expr[Row, Option[A]]) extends Expr[Row, Boolean]
  case GetOrElse[Row, A](expr: Expr[Row, Option[A]], default: A) extends Expr[Row, A]

  // Aggregations (special handling in interpreter)
  case Sum[Row](expr: Expr[Row, Int]) extends Expr[Row, Int]
  case Count[Row]() extends Expr[Row, Long]
  case Max[Row, A](expr: Expr[Row, A])(using ord: Ordering[A]) extends Expr[Row, Option[A]]:
    def ordering: Ordering[A] = ord
  case Min[Row, A](expr: Expr[Row, A])(using ord: Ordering[A]) extends Expr[Row, Option[A]]:
    def ordering: Ordering[A] = ord
  case Avg[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

object Expr:
  // Smart constructors
  def cell[Row, A](name: String, index: ColumnIndex): Expr[Row, A] =
    Cell(name, index)

  def const[Row, A](value: A): Expr[Row, A] =
    Const(value)

  // Extension methods for fluent syntax
  extension [Row, A](left: Expr[Row, A])
    def ===(right: Expr[Row, A]): Expr[Row, Boolean] =
      Eq(left, right)

    def !==(right: Expr[Row, A]): Expr[Row, Boolean] =
      Not(Eq(left, right))

  extension [Row, A: Ordering](left: Expr[Row, A])
    def >(right: Expr[Row, A]): Expr[Row, Boolean] =
      Gt(left, right)

    def <(right: Expr[Row, A]): Expr[Row, Boolean] =
      Lt(left, right)

    def >=(right: Expr[Row, A]): Expr[Row, Boolean] =
      Or(Gt(left, right), Eq(left, right))

    def <=(right: Expr[Row, A]): Expr[Row, Boolean] =
      Or(Lt(left, right), Eq(left, right))

  extension [Row](left: Expr[Row, Int])
    inline def +(right: Expr[Row, Int]): Expr[Row, Int] = Add(left, right)
    inline def -(right: Expr[Row, Int]): Expr[Row, Int] = Sub(left, right)
    inline def *(right: Expr[Row, Int]): Expr[Row, Int] = Mul(left, right)
    inline def /(right: Expr[Row, Int]): Expr[Row, Int] = Div(left, right)

  extension [Row](left: Expr[Row, Boolean])
    inline def &&(right: Expr[Row, Boolean]): Expr[Row, Boolean] = And(left, right)
    inline def ||(right: Expr[Row, Boolean]): Expr[Row, Boolean] = Or(left, right)
    inline def unary_! : Expr[Row, Boolean] = Not(left)

  extension [Row](left: Expr[Row, String])
    inline def ++(right: Expr[Row, String]): Expr[Row, String] = Concat(left, right)
    inline def length: Expr[Row, Int] = Length(left)

  extension [Row, A](expr: Expr[Row, Option[A]])
    inline def isDefined: Expr[Row, Boolean] = IsDefined(expr)
    inline def getOrElse(default: A): Expr[Row, A] = GetOrElse(expr, default)
```

**Key innovations:**
- Native enum with GADT cases
- Inline extension methods for zero overhead
- Ordering evidence stored in GADT constructor
- Zero asInstanceOf—pattern matching refines types

#### 1.2 Opaque Types

**File:** `strongbow-core/src/main/scala/com/h3/strongbow/types/ColumnIndex.scala`

```scala
package net.ghoula.strongbow.types

/** Zero-cost column index wrapper.
  *
  * Opaque type compiles to raw Int at runtime—no allocation overhead.
  */
opaque type ColumnIndex = Int

object ColumnIndex:
  inline def apply(i: Int): ColumnIndex = i

  extension (idx: ColumnIndex)
    inline def toInt: Int = idx
    inline def +(other: Int): ColumnIndex = idx + other
    inline def -(other: Int): ColumnIndex = idx - other

/** Zero-cost row index wrapper. */
opaque type RowIndex = Int

object RowIndex:
  inline def apply(i: Int): RowIndex = i

  extension (idx: RowIndex)
    inline def toInt: Int = idx
    inline def +(other: Int): RowIndex = idx + other
    inline def -(other: Int): RowIndex = idx - other
```

**Key innovations:**
- Opaque types for type safety without runtime cost
- Inline extensions compile away completely
- Pattern from Eru's FiberId

#### 1.3 Column Storage

**File:** `strongbow-core/src/main/scala/com/h3/strongbow/Column.scala`

```scala
package net.ghoula.strongbow

import scala.collection.immutable.BitSet

/** Specialized columnar storage avoiding boxing.
  *
  * Each column type uses primitive arrays where possible.
  * Nullability tracked via BitSet for memory efficiency.
  */
enum Column:
  case IntColumn(data: Array[Int], nulls: BitSet)
  case LongColumn(data: Array[Long], nulls: BitSet)
  case DoubleColumn(data: Array[Double], nulls: BitSet)
  case StringColumn(data: Array[String], nulls: BitSet)
  case BooleanColumn(data: Array[Boolean], nulls: BitSet)
  case AnyColumn(data: Array[Any], nulls: BitSet)

  inline def length: Int = this match
    case IntColumn(data, _) => data.length
    case LongColumn(data, _) => data.length
    case DoubleColumn(data, _) => data.length
    case StringColumn(data, _) => data.length
    case BooleanColumn(data, _) => data.length
    case AnyColumn(data, _) => data.length

  inline def columnType: ColumnType = this match
    case IntColumn(_, _) => ColumnType.IntType
    case LongColumn(_, _) => ColumnType.LongType
    case DoubleColumn(_, _) => ColumnType.DoubleType
    case StringColumn(_, _) => ColumnType.StringType
    case BooleanColumn(_, _) => ColumnType.BooleanType
    case AnyColumn(_, _) => ColumnType.AnyType

  inline def isNull(index: RowIndex): Boolean = this match
    case IntColumn(_, nulls) => nulls.contains(index.toInt)
    case LongColumn(_, nulls) => nulls.contains(index.toInt)
    case DoubleColumn(_, nulls) => nulls.contains(index.toInt)
    case StringColumn(_, nulls) => nulls.contains(index.toInt)
    case BooleanColumn(_, nulls) => nulls.contains(index.toInt)
    case AnyColumn(_, nulls) => nulls.contains(index.toInt)

object Column:
  // Smart constructors
  inline def int(data: Array[Int], nulls: BitSet = BitSet.empty): Column =
    IntColumn(data, nulls)

  inline def long(data: Array[Long], nulls: BitSet = BitSet.empty): Column =
    LongColumn(data, nulls)

  inline def double(data: Array[Double], nulls: BitSet = BitSet.empty): Column =
    DoubleColumn(data, nulls)

  inline def string(data: Array[String], nulls: BitSet = BitSet.empty): Column =
    StringColumn(data, nulls)

  inline def boolean(data: Array[Boolean], nulls: BitSet = BitSet.empty): Column =
    BooleanColumn(data, nulls)

  inline def any(data: Array[Any], nulls: BitSet = BitSet.empty): Column =
    AnyColumn(data, nulls)

/** Column type enumeration. */
enum ColumnType:
  case IntType
  case LongType
  case DoubleType
  case StringType
  case BooleanType
  case OptionType(inner: ColumnType)
  case AnyType
```

**Key innovations:**
- Native enum for Column
- Inline methods for zero overhead
- Pattern matching for type-safe access

#### 1.4 Dataset Plan

**File:** `strongbow-core/src/main/scala/com/h3/strongbow/Dataset.scala`

```scala
package net.ghoula.strongbow

/** Immutable description of dataset transformations.
  *
  * Dataset[T] is pure data—just an enum describing computation.
  * No execution happens until interpreted.
  */
enum Dataset[+T]:
  case Root[T](columns: Vector[Column], schema: Schema[T]) extends Dataset[T]
  case Filter[T](parent: Dataset[T], predicate: Expr[T, Boolean]) extends Dataset[T]
  case Map[A, B](parent: Dataset[A], func: A => B) extends Dataset[B]
  case FlatMap[A, B](parent: Dataset[A], func: A => Iterable[B]) extends Dataset[B]
  case Select[T, U](parent: Dataset[T], projection: T => U) extends Dataset[U]
  case Distinct[T](parent: Dataset[T]) extends Dataset[T]
  case Limit[T](parent: Dataset[T], n: Int) extends Dataset[T]
  case Union[T](left: Dataset[T], right: Dataset[T]) extends Dataset[T]
  case Sort[T](parent: Dataset[T])(using ord: Ordering[T]) extends Dataset[T]:
    def ordering: Ordering[T] = ord
  case SortBy[T, K](parent: Dataset[T], key: T => K)(using ord: Ordering[K]) extends Dataset[T]:
    def ordering: Ordering[K] = ord

object Dataset:
  /** Smart constructor with validation using Either (no deps).
    *
    * Returns Either[NonEmptyList[SchemaError], Dataset[T]]
    * NonEmptyList ensures at least one error on Left.
    */
  def fromColumns[T](
    cols: Vector[Column],
    schema: Schema[T]
  ): Either[errors.NonEmptyList[errors.SchemaError], Dataset[T]] =
    val validations = List(
      validateColumnCount(cols, schema),
      validateColumnTypes(cols, schema),
      validateColumnLengths(cols)
    )

    val errors = validations.collect { case Left(e) => e }.flatten
    if errors.isEmpty then
      Right(Root(cols, schema))
    else
      Left(errors.NonEmptyList.fromListUnsafe(errors))

  /** Unsafe constructor for internal use when validation already done. */
  private[strongbow] inline def unsafeRoot[T](cols: Vector[Column], schema: Schema[T]): Dataset[T] =
    Root(cols, schema)

  // Extension methods on Dataset
  extension [T](ds: Dataset[T])
    inline def filter(predicate: Expr[T, Boolean]): Dataset[T] =
      Filter(ds, predicate)

    inline def map[U](f: T => U): Dataset[U] =
      Map(ds, f)

    inline def flatMap[U](f: T => Iterable[U]): Dataset[U] =
      FlatMap(ds, f)

    inline def distinct: Dataset[T] =
      Distinct(ds)

    inline def limit(n: Int): Dataset[T] =
      Limit(ds, n)

    inline def union(other: Dataset[T]): Dataset[T] =
      Union(ds, other)

    inline def sort(using ord: Ordering[T]): Dataset[T] =
      Sort(ds)

    inline def sortBy[K](key: T => K)(using ord: Ordering[K]): Dataset[T] =
      SortBy(ds, key)

    inline def groupBy[K](key: T => K): Grouped[K, T] =
      Grouped.GroupBy(ds, key)

    inline def keyBy[K](key: T => K): Grouped[K, T] =
      groupBy(key)

  // Validation helpers (return Option[NonEmptyList[Error]])
  private def validateColumnCount[T](
    cols: Vector[Column],
    schema: Schema[T]
  ): Either[List[errors.SchemaError], Unit] =
    if cols.length == schema.columnCount then
      Right(())
    else
      Left(List(errors.SchemaError.ColumnCountMismatch(schema.columnCount, cols.length)))

  private def validateColumnTypes[T](
    cols: Vector[Column],
    schema: Schema[T]
  ): Either[List[errors.SchemaError], Unit] =
    val mismatches = cols.zip(schema.columnTypes).zipWithIndex.collect {
      case ((col, expectedType), idx) if col.columnType != expectedType =>
        errors.SchemaError.ColumnTypeMismatch(idx, expectedType, col.columnType)
    }

    if mismatches.isEmpty then Right(()) else Left(mismatches.toList)

  private def validateColumnLengths(
    cols: Vector[Column]
  ): Either[List[errors.SchemaError], Unit] =
    if cols.isEmpty then
      Right(())
    else
      val expectedLength = cols.head.length
      val mismatches = cols.zipWithIndex.collect {
        case (col, idx) if col.length != expectedLength =>
          errors.SchemaError.ColumnLengthMismatch(idx, expectedLength, col.length)
      }

      if mismatches.isEmpty then Right(()) else Left(mismatches.toList)
```

**Key innovations:**
- Native enum for Dataset plan
- Inline extension methods
- Either for errors (no Valar dependency)
- GADT stores Ordering evidence

#### 1.5 Prelude Pattern

**File:** `strongbow-core/src/main/scala/com/h3/strongbow/prelude.scala`

```scala
package net.ghoula.strongbow

/** Unified public prelude for Strongbow.
  *
  * Usage: import net.ghoula.strongbow.prelude.*
  *
  * This prelude re-exports the complete public surface so users get
  * a single canonical import with no exposure of internal packages.
  *
  * @example
  * {{{
  * import net.ghoula.strongbow.prelude.*
  *
  * // All types available
  * val plan: Dataset[User] = Dataset.fromColumns(cols, schema) match
  *   case Right(ds) => ds.filter(_.age > 18).map(_.name)
  *   case Left(errors) => ???
  *
  * // Expression DSL
  * val expr: Expr[User, Boolean] =
  *   col("age") > const(18) && col("active") === const(true)
  *
  * // Grouped operations
  * val grouped: Grouped[String, Int] =
  *   dataset.groupBy(_.city).aggregate(count)
  * }}}
  */
object prelude:
  // Core types
  export net.ghoula.strongbow.{Dataset, Grouped, Expr, Column, ColumnType, Schema, Aggregator}

  // Type aliases for discoverability
  type Dataset[+T] = net.ghoula.strongbow.Dataset[T]
  type Grouped[K, +V] = net.ghoula.strongbow.Grouped[K, V]
  type Expr[Row, +A] = net.ghoula.strongbow.Expr[Row, A]
  type Column = net.ghoula.strongbow.Column
  type ColumnType = net.ghoula.strongbow.ColumnType
  type Schema[T] = net.ghoula.strongbow.Schema[T]

  // Opaque types
  export net.ghoula.strongbow.types.{ColumnIndex, RowIndex}
  type ColumnIndex = net.ghoula.strongbow.types.ColumnIndex
  type RowIndex = net.ghoula.strongbow.types.RowIndex

  // Companion objects
  val Dataset = net.ghoula.strongbow.Dataset
  val Grouped = net.ghoula.strongbow.Grouped
  val Expr = net.ghoula.strongbow.Expr
  val Column = net.ghoula.strongbow.Column
  val ColumnType = net.ghoula.strongbow.ColumnType

  // Error types
  export net.ghoula.strongbow.errors.{SchemaError, DecodeError, ExecutionError, NonEmptyList}

  // Export extension methods (makes them available with prelude.*)
  export net.ghoula.strongbow.Dataset.extension.*
  export net.ghoula.strongbow.Grouped.extension.*
  export net.ghoula.strongbow.Expr.extension.*
```

**Key innovations:**
- Single import pattern from Eru
- Re-exports entire public API
- Type aliases for discoverability
- Hides internal package structure

---

## Phase 2: Columnar Interpreter (Week 3-4)

### Goal: Correct in-memory execution

**File:** `strongbow-columnar/src/main/scala/com/h3/strongbow/columnar/ColumnarInterpreter.scala`

```scala
package net.ghoula.strongbow.columnar

import net.ghoula.strongbow.{Dataset, Column, Schema}
import net.ghoula.strongbow.errors.ExecutionError

/** Columnar interpreter for in-memory execution.
  *
  * Interprets Dataset[T] plans into MaterializedDataset[T].
  * Uses Either for errors—no external dependencies.
  */
object ColumnarInterpreter:
  /** Execute a dataset plan.
    *
    * Returns Either[ExecutionError, MaterializedDataset[T]]
    */
  def execute[T](plan: Dataset[T]): Either[ExecutionError, MaterializedDataset[T]] =
    plan match
      case Dataset.Root(columns, schema) =>
        Right(MaterializedDataset.prepare(columns, schema))

      case Dataset.Filter(parent, predicate) =>
        for
          parentResult <- execute(parent)
          filtered <- filterDataset(parentResult, predicate)
        yield filtered

      case Dataset.Map(parent, func) =>
        for
          parentResult <- execute(parent)
          mapped <- mapDataset(parentResult, func)
        yield mapped

      // TODO: Implement remaining cases

  private def filterDataset[T](
    data: MaterializedDataset[T],
    predicate: Expr[T, Boolean]
  ): Either[ExecutionError, MaterializedDataset[T]] =
    val columns = data.unsafeColumns
    val rowCount = data.rowCount

    // Tail-recursive index building
    @tailrec
    def buildIndex(i: Int, acc: List[Int]): List[Int] =
      if i < rowCount then
        val row = ColumnRow.at(columns, RowIndex(i))
        val passes = ExprInterpreter.eval(predicate, row)
        if passes then
          buildIndex(i + 1, i :: acc)
        else
          buildIndex(i + 1, acc)
      else
        acc.reverse

    val passingIndices = buildIndex(0, Nil)

    // Build new columns—TODO: implement for all column types
    val newColumns = columns.map { col =>
      col match
        case Column.IntColumn(data, nulls) =>
          val newData = passingIndices.map(data(_)).toArray
          val newNulls = passingIndices.zipWithIndex.foldLeft(BitSet.empty) {
            case (acc, (oldIdx, newIdx)) =>
              if nulls.contains(oldIdx) then acc + newIdx else acc
          }
          Column.IntColumn(newData, newNulls)
        // TODO: Other column types
    }

    Right(MaterializedDataset.prepare(newColumns, data.schema))
```

**Key innovations:**
- Returns Either, not Eru effect
- Tail recursion, no vars
- Pattern matching on enum refines types

---

## Phase 3-8: Additional Phases

*(Detailed plans for Grouped, Spark interpreter, optional integrations, testing, and performance optimization follow the same patterns—let me know if you want me to expand these now or if this foundation is sufficient for starting implementation.)*

---

## Build Configuration

### build.sbt

```scala
ThisBuild / organization := "net.ghoula"
ThisBuild / scalaVersion := "3.7.4"
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision

// Java 21
ThisBuild / javacOptions ++= Seq("--release", "21")

// Compiler flags matching Eru
lazy val sharedScalacOptions = Seq(
  "-feature",
  "-Xfatal-warnings",
  "-Wunused:all",
  "-Wrecurse-with-default",
  "-no-indent",
  "-language:strictEquality"
)

lazy val testScalacOptions = Seq(
  "-Wunused:imports"
)

// Dependencies
val valarVersion = "0.1.0-SNAPSHOT"
val rumilVersion = "0.1.0-SNAPSHOT"
val eruVersion = "0.1.0-SNAPSHOT"
val sparkVersion = "4.2.0"

lazy val root = project
  .in(file("."))
  .aggregate(core, columnar, spark, validation, io, effects, bench)
  .settings(
    name := "strongbow",
    publish / skip := true
  )

lazy val core = project
  .in(file("strongbow-core"))
  .settings(
    name := "strongbow-core",
    scalacOptions ++= sharedScalacOptions,
    // ⚡ ZERO dependencies—only Scala stdlib
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.18" % Test,
      "org.scalacheck" %% "scalacheck" % "1.17.0" % Test
    )
  )

lazy val columnar = project
  .in(file("strongbow-columnar"))
  .dependsOn(core)
  .settings(
    name := "strongbow-columnar",
    scalacOptions ++= sharedScalacOptions
    // No extra dependencies—just core
  )

lazy val spark = project
  .in(file("strongbow-spark"))
  .dependsOn(core)
  .settings(
    name := "strongbow-spark",
    scalacOptions ++= sharedScalacOptions,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
    )
  )

lazy val validation = project
  .in(file("strongbow-validation"))
  .dependsOn(core)
  .settings(
    name := "strongbow-validation",
    scalacOptions ++= sharedScalacOptions,
    libraryDependencies ++= Seq(
      "net.ghoula" %% "valar-core" % valarVersion
    )
  )

lazy val io = project
  .in(file("strongbow-io"))
  .dependsOn(core, columnar)
  .settings(
    name := "strongbow-io",
    scalacOptions ++= sharedScalacOptions,
    libraryDependencies ++= Seq(
      "net.ghoula" %% "rumil-core" % rumilVersion,
      "net.ghoula" %% "eru-core" % eruVersion
    )
  )

lazy val effects = project
  .in(file("strongbow-effects"))
  .dependsOn(core, columnar)
  .settings(
    name := "strongbow-effects",
    scalacOptions ++= sharedScalacOptions,
    libraryDependencies ++= Seq(
      "net.ghoula" %% "eru-core" % eruVersion
    )
  )

lazy val bench = project
  .in(file("strongbow-bench"))
  .dependsOn(core, columnar)
  .settings(
    name := "strongbow-bench",
    scalacOptions ++= testScalacOptions,
    publish / skip := true
  )
```

### .scalafmt.conf (Identical to Eru)

```hocon
version = 3.9.1
maxColumn = 120
align.preset = none
continuationIndent.defnSite = 2
assumeStandardLibraryStripMargin = true
docstrings.style = SpaceAsterisk
docstrings.wrapMaxColumn = 100
lineEndings = unix
includeCurlyBraceInSelectChains = false
optIn.annotationNewlines = true
runner.dialect = scala3
project.layout = StandardConvention

rewrite.rules = []
```

### .scalafix.conf (Identical to Eru)

```hocon
rules = [
  ExplicitResultTypes,
  LeakingImplicitClassVal,
  NoValInForComprehension,
  ProcedureSyntax,
  RemoveUnused,
  DisableSyntax,
  OrganizeImports,
  RedundantSyntax,
]

ExplicitResultTypes {
  fetchScala3CompilerArtifactsOnVersionMismatch = true
}

OrganizeImports {
  groupedImports = Keep
  groups = ["*", "re:(javax?|scala)\\.", "net.ghoula.strongbow"]
  removeUnused = true
  targetDialect = Scala3
}

DisableSyntax {
  noVars = true
  noThrows = true
  noNulls = true
  noReturns = true
  noIsInstanceOf = true
  noFinalVal = true
  noFinalize = true
  noCovariant = true
  noContravariant = true
  noAsInstanceOf = true
}

// Allow vars and throws in tests
DisableSyntax.noVars = false
DisableSyntax.noThrows = false
```

---

## Timeline & Success Criteria

**Reordered for Logical Dependencies:** Core interpreters (columnar + Spark) complete the vision, then test/optimize, then add optional integrations.

| Phase | Duration | Deliverable | Success Criteria |
|-------|----------|-------------|------------------|
| **Phase 1: Core** | Week 1-2 | Expr, Dataset, Grouped enums, prelude | ✓ Zero dependencies<br>✓ All types compile<br>✓ Inline/opaque types used |
| **Phase 2: Columnar** | Week 3-4 | In-memory interpreter | ✓ Filter/map/groupBy work<br>✓ Returns Either<br>✓ One-cast-at-boundary<br>✓ Zero vars in public API |
| **Phase 3: Grouped** | Week 5 | GroupBy + aggregations | ✓ Encapsulated mutation<br>✓ Tail recursive<br>✓ Correct results |
| **Phase 4: Spark** | Week 6-7 | Spark interpreter | ✓ Expr → SparkColumn<br>✓ Plan → Dataset<br>✓ Integration tests<br>✓ Vision complete: both backends work |
| **Phase 5: Testing** | Week 8 | Comprehensive test suite | ✓ >90% coverage<br>✓ Property-based laws<br>✓ All laws pass<br>✓ Both interpreters tested |
| **Phase 6: Performance** | Week 9-10 | Benchmarks + optimization | ✓ Within 2x Crossbow<br>✓ Document trade-offs<br>✓ Correctness preserved |
| **Phase 7: Valar** | Optional | Optional validation module | ✓ Schema validation<br>✓ Error accumulation<br>✓ Core still dep-free |
| **Phase 8: Rumil/Eru** | Optional | Optional I/O + effects | ✓ CSV parsing<br>✓ Effect tracking<br>✓ Core still dep-free |

**Key Change:** Phases 7-8 are now explicitly optional Arda ecosystem integrations, not prerequisites for Spark. The core vision (columnar + Spark interpreters) is complete by Phase 4.

---

## Principles Summary

1. **Zero Dependencies Core:** Pure Scala 3, portable everywhere
2. **Modern Scala 3:** Native enums, opaque types, union types, inline
3. **Prelude Pattern:** Single canonical import, clean API surface
4. **Correctness Through Types:** GADT, smart constructors, explicit boundaries
5. **Pure Description:** Logical plan is data, interpretation is separate
6. **Encapsulated Mutation:** Never exposed, tail recursion preferred
7. **Correctness First:** Build it right, optimize later
8. **Optional Integration:** Eru/Valar/Rumil are opt-in modules

---

*Strongbow is designed and will be developed by Hakim Jonas Ghoula.*
