# Strongbow Columnar Architecture Plan

## Vision

Strongbow is a columnar engine with typed storage and GADT-based expression
evaluation. Every operation flows through typed arrays via sealed enum
pattern matching. Zero asInstanceOf in expression logic. The type system
proves correctness — if it compiles, the types are right.

This is not a compromise or a best-effort. Eru achieves zero casts in
2100+ lines of interpreter. Rumil achieves 1 strategic cast in its entire
runtime. Strongbow will achieve the same standard for columnar data
processing.

## Completed

### Phase 1: Exhaustive evalColumn — DONE

evalColumn handles all 167 Expr cases as column transformations. Zero
asInstanceOf. Typed paths for all primitive column types (Int, Long,
Double, String, Boolean, Date). AnyColumn comparison returns
UnsupportedOperation — no Ordering casts. evalColumnRowByRow deleted.

### Phase 2: Columnar evalAggregation — DONE

Fixed-type aggregations (Sum, Avg, StdDev, Variance, Corr, BoolAnd, etc.)
operate on typed columns directly via evalColumn. Generic aggregations
(Max, Min, First, Collect, etc.) use per-row eval with GADT type variable
binding. Zero asInstanceOf in evalAggregation.

### Phase 3: Delete evalAny + evalBoolean — DONE

evalAny deleted (~500 lines, 143 casts). evalBoolean deleted (~115 lines,
24 casts). DatasetInterpreter.filter rewritten as columnar boolean-mask
operation via evalColumn → BooleanColumn → index collection → column
slicing. eval kept with 7 Cell boundary casts — needed by evalAggregation
for generic aggregation cases.

ExprInterpreter: 235 → 7 asInstanceOf. All at Cell boundary.

## Remaining

### Phase 4: DatasetInterpreter GADT Refinement

DatasetInterpreter.scala has 35 asInstanceOf calls. These are in the
Dataset execution pattern matching — casting between GADT-refined types
and the generic output type `T`.

Example pattern:
```scala
case jn: Dataset.InnerJoin[a, b] =>
  // execute produces MaterializedDataset[(a, b)]
  // but return type is MaterializedDataset[T]
  // currently: result.asInstanceOf[MaterializedDataset[T]]
```

The fix: GADT refinement on Dataset enum cases should give the compiler
enough information to unify the types. When matching
`Dataset.InnerJoin[a, b]`, the compiler knows `T = (a, b)`. The result
`MaterializedDataset[(a, b)]` should typecheck as `MaterializedDataset[T]`
without a cast.

This needs careful analysis — read every cast site, understand why the
compiler currently can't prove the type, and determine if Scala 3.8.2's
GADT solver handles it. Some cases may be genuine limitations of the
type system that require workarounds.

Scope:
- Read DatasetInterpreter.scala exhaustively
- Categorize all 35 casts by root cause
- Determine which are fixable with GADT refinement
- Determine which are genuine type system boundaries
- Implement fixes, verify compiler accepts each one

### Phase 5: GADT Column

Parameterize the Column enum as `Column[+A]`:

```scala
enum Column[+A] {
  case IntColumn(data: Array[Int], nulls: BitSet) extends Column[Int]
  case LongColumn(data: Array[Long], nulls: BitSet) extends Column[Long]
  case DoubleColumn(data: Array[Double], nulls: BitSet) extends Column[Double]
  case StringColumn(data: Array[String], nulls: BitSet) extends Column[String]
  case BooleanColumn(data: Array[Boolean], nulls: BitSet) extends Column[Boolean]
  case DateColumn(data: Array[Int], nulls: BitSet) extends Column[Date]
  case AnyColumn(data: Array[Any], nulls: BitSet) extends Column[Any]
}
```

This enables double GADT refinement: Expr gives the type parameter `a`,
Column[a] confirms it via pattern matching. The compiler proves types
through without any cast — including the 7 Cell boundary casts in eval.

With GADT Column:
- evalColumn returns `Column[A]` instead of `Column`
- evalAggregation's generic cases pattern match on `Column[a]` to get
  typed array access without per-row eval
- eval becomes unnecessary — all evaluation is columnar
- MaterializedDataset holds `Vector[Column[?]]` (existential wildcard)
- The 7 Cell boundary casts in eval are eliminated
- eval can be deleted entirely

Ripple scope: Column.scala, MaterializedDataset.scala, Schema.scala,
DatasetInterpreter.scala, ExprInterpreter.scala, RowConverter.scala
(Spark module), all test files that construct columns.

This would make strongbow the only columnar engine with compile-time
type-safe column access through GADT refinement — genuinely novel
compared to Arrow, Polars, and DuckDB which all use runtime dispatch.

## Cast Count

| Location | Current | After Phase 4 | After Phase 5 |
|---|---|---|---|
| ExprInterpreter evalColumn | 0 | 0 | 0 |
| ExprInterpreter evalAggregation | 0 | 0 | 0 |
| ExprInterpreter eval (Cell) | 7 | 7 | 0 (eval deleted) |
| DatasetInterpreter | 35 | ~0 | 0 |
| **Total** | **42** | **~7** | **0** |

## Verification (current state)

- `sbt prepare` passes
- `sbt core/test` — 356 tests pass
- `sbt spark/compile` passes
- ExprInterpreter asInstanceOf: 7 (Cell boundary) + 1 (scaladoc)
- DatasetInterpreter asInstanceOf: 35 (pre-existing, Phase 4 target)
- Zero evalAny, zero evalBoolean anywhere in codebase
