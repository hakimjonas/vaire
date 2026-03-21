# Strongbow Architecture

## Module Structure

```
strongbow/
├── strongbow-core/          # Rumil + Sarati dependencies
│   ├── Dataset.scala        # Dataset[T] invariant GADT (35 cases)
│   ├── Expr.scala           # Expr[Row, +A] GADT (167 expression cases)
│   ├── Column.scala         # Column[+A] GADT (typed columnar storage)
│   ├── ColumnType.scala     # Column type metadata
│   ├── Schema.scala         # Schema[T] with compile-time derivation
│   ├── ExprInterpreter.scala    # Columnar expression evaluation
│   ├── DatasetInterpreter.scala # Dataset plan interpretation
│   ├── ExprMacro.scala      # Compile-time lambda → Expr compilation
│   ├── MaterializedDataset.scala # Execution result
│   ├── DatasetActions.scala  # Terminal operations (collect, show, count)
│   ├── DatasetExplainer.scala # Plan explanation
│   ├── WindowSpec.scala      # Window function specification
│   ├── Interpreter.scala     # Interpreter trait
│   ├── prelude.scala         # Single canonical import
│   ├── types/
│   │   ├── ColumnIndex.scala # Opaque type
│   │   ├── RowIndex.scala    # Opaque type
│   │   └── Date.scala        # Opaque type over java.time.LocalDate
│   ├── errors/
│   │   ├── ExecutionError.scala
│   │   ├── DecodeError.scala
│   │   ├── SchemaError.scala
│   │   └── NonEmptyList.scala
│   ├── specs/
│   │   ├── KeySpec.scala
│   │   ├── AggSpec.scala
│   │   ├── SortSpec.scala
│   │   └── WindowExprSpec.scala
│   └── agg/
│       └── AggBuilders.scala # Ergonomic aggregation helpers
│
├── strongbow-spark/         # Spark SQL 4.1.1
│   ├── SparkInterpreter.scala   # Dataset → Spark DataFrame plan
│   ├── ExprToColumn.scala       # Expr → native Spark SQL Column
│   ├── RowConverter.scala       # Spark Row ↔ Column conversion
│   ├── DataFrameBuilder.scala   # Column → Spark DataFrame
│   ├── SchemaConverter.scala    # ColumnType ↔ Spark DataType
│   └── prelude.scala
│
└── strongbow-bench/         # Benchmarks
    └── ScalingBenchmark.scala
```

## Core Design Patterns

### 1. Triple GADT Architecture

Three GADTs with type parameters proven through pattern matching:

**Expr[Row, +A]** — 167 expression cases. Each case declares its input row
type and output value type. Pattern matching refines both.

**Column[+A]** — Typed columnar storage. IntColumn extends Column[Int],
StringColumn extends Column[String]. Pattern matching gives typed arrays
(Array[Int], Array[String | Null]) without casts.

**Dataset[T]** — Invariant transformation AST. 35 cases. Invariance enables
GADT equality refinement: matching InnerJoin[a, b] proves T = (a, b).

### 2. Columnar Evaluation

All expression evaluation is columnar. evalColumn takes an Expr and produces
a Column — no per-row dispatch, no Either-per-row allocation.

For a filter on 1M rows:
- Evaluate predicate to BooleanColumn (1 array allocation)
- Collect passing indices
- Slice all columns by indices

No per-row boxing. No per-row Either wrapping.

### 3. Null Safety Model

SQL NULL tracked by BitSet per column. Array values at null positions are
placeholders (0 for Int, null for String | Null). The BitSet is authoritative.

Column[+A] GADT pattern matching always gives data AND nulls together —
impossible to access one without the other. `-Yexplicit-nulls` makes the
compiler enforce null handling at every String | Null access site.

### 4. Interpreter Pattern

Dataset[T] is data — an immutable enum describing computation. Two
interpreters execute the plan:

**DatasetInterpreter** — In-memory columnar. Pattern matches Dataset cases,
delegates expression evaluation to ExprInterpreter.evalColumn. Returns
MaterializedDataset[T] (Vector[Column[?]] + Schema[T]).

**SparkInterpreter** — Apache Spark. Pattern matches Dataset cases, builds
Spark DataFrame plan. Expr cases translate to native Spark SQL functions
via ExprToColumn. Everything pushes to Catalyst — no UDFs, no RDDs.

### 5. Macro System

ExprMacro compiles Scala 3 lambdas to Expr AST at compile time:

- `dataset.where(_.price > 100.0)` → Expr.Gt(Cell, Const)
- `dataset.withFields(_.copy(name = _.name.toLowerCase))` → SelectExprs
- `dataset.sortByColumn(_.age)` → SortByExpr with Cell

The macro inspects lambda bodies via Quotes reflect, pattern matches on
Scala 3 AST nodes (Apply, Select, Block), and emits typed Expr values.

## Compiler Settings

```
-Werror -Wunused:all -language:strictEquality -Yexplicit-nulls -no-indent
```

Scala 3.8.2 on JDK 25 (core) / JDK 21 (Spark module). ZGC for sbt and tests.

### 6. Aggregation Type Safety

Aggregation return types reflect mathematical reality:

- **Sum(Expr[Row, Int])** returns **Long** — a sum of ints can exceed Int.MaxValue
- **SumLong(Expr[Row, Long])** returns **Long**
- **SumDouble(Expr[Row, Double])** returns **Double**
- **Count** returns **Long**

The type tells the truth about what the aggregation produces. Both the
columnar interpreter and the Spark bridge produce the same types.
