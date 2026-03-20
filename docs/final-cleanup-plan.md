# Strongbow Final Cleanup Plan

## Problem Statement

The subagents introduced systemic issues that were not caught in review:
- 28 asInstanceOf suppressions across 4 files
- 168 var suppressions in ExprInterpreter (pattern copying, not justified)
- 43 null suppressions (potential poison pills in a columnar engine)
- 25 throw suppressions (bypassing the Either error channel)
- 10 return suppressions (early returns in FP code)

Phase 5 was incomplete: eval was supposed to be deleted but wasn't.
The subagent claimed the Cell boundary casts were "irreducible" without
verifying — our experiments proved otherwise.

## Phase 6: Complete Phase 5 — Delete eval

### 6a. Rewrite evalAggregation generic cases using Column[A] GADT

16 per-row eval calls in evalAggregation for: Max, Min, First, ExprLast,
AnyValue, Mode, Collect, CollectSet, CountDistinct, ApproxCountDistinct,
MaxBy, MinBy, MaxN, MinN, MaxByN, MinByN.

Replace with: `evalColumn(expr, columns, colType)` → pattern match
`Column[a]` → typed array iteration.

For primitive columns (Int, Long, Double, String, Boolean, Date): GADT
refinement gives typed access. `max.ordering: Ordering[a]` becomes
`Ordering[Int]` when matching IntColumn.

For AnyColumn: `a = Any`, values are `Any`. Operations like Collect,
CountDistinct work on `Any` values via `getValue`. Max/Min need Ordering
which requires the same typed dispatch as evalColumn's typedComparison.

### 6b. Delete eval method

After 6a, nothing calls eval. Delete the entire method (~650 lines).
The 7 Cell boundary casts go with it. eval's functionality is fully
subsumed by evalColumn + evalAggregation.

### 6c. Remove eval from public API

Remove `eval` from ExprInterpreter's public methods. If tests need
per-row access, they use evalColumn + getValue at a specific index.

## Phase 7: Eliminate throw and return

### 7a. Replace all throw with Left(ExecutionError)

25 throw suppressions. Every throw site is inside a method that returns
Either. Replace with Left.

Example:
```scala
// Before:
throw new ArithmeticException("Division by zero") // scalafix:ok
// After:
return Left(ExecutionError.DivisionByZero(i))
```

Wait — return is also banned. Use restructured control flow:
```scala
// Correct:
Left(ExecutionError.DivisionByZero(i))
```

Inside while loops that need early termination, use a flag:
```scala
var error: Option[ExecutionError] = None
while (i < n && error.isEmpty) { ... }
error.map(Left(_)).getOrElse(Right(result))
```

Or better — use a fold/foldLeft pattern that short-circuits on error.

### 7b. Eliminate all return statements

10 return suppressions. Each is an early return inside a while loop.
Replace with loop restructuring or foldLeft that carries error state.

## Phase 8: Eliminate vars

### 8a. Simple array transforms → Array.tabulate

The majority of 168 vars are this pattern:
```scala
val out = new Array[Int](rowCount)
var i = 0
while (i < rowCount) { out(i) = f(data(i)); i += 1 }
Column.int(out, nulls)
```

Replace with:
```scala
Column.int(Array.tabulate(rowCount)(i => f(data(i))), nulls)
```

Or for null-aware transforms:
```scala
Column.int(
  Array.tabulate(rowCount)(i =>
    if (nulls.contains(i)) 0 else f(data(i))
  ),
  nulls
)
```

### 8b. Accumulations → foldLeft

Pattern:
```scala
var total = 0
var i = 0
while (i < n) { total += data(i); i += 1 }
```

Replace with:
```scala
val total = (0 until n).foldLeft(0)((acc, i) => acc + data(i))
```

Or for null-aware:
```scala
val total = (0 until n).foldLeft(0)((acc, i) =>
  if (nulls.contains(i)) acc else acc + data(i)
)
```

### 8c. Justified vars (rare, documented)

Some var uses may be genuinely justified — multi-accumulator loops where
foldLeft would allocate tuples, or loops with early termination flags.
Each must be individually justified, not blanket-accepted.

## Phase 9: Audit null usage

### 9a. Column array placeholders

`Array[String]` at null positions needs a placeholder value. Scala
arrays of reference types can hold null. This is the SQL NULL storage
boundary — the BitSet is authoritative, the null in the array is never
read by correct code.

These nulls are justified but should be reviewed to ensure the BitSet
is always checked before array access.

### 9b. Eval/evalColumn null returns

`Right(null)` in eval for GetJsonObject missing paths — this was
discussed and accepted as the SQL NULL value channel. After eval is
deleted (Phase 6), check if evalColumn's GetJsonObject implementation
uses BitSet instead of null values.

### 9c. Any other null usage

Each remaining null must be individually justified or eliminated.

## Phase 10: Audit remaining asInstanceOf

### 10a. ExprMacro.scala (10 casts)

Read each cast. Macro-generated code operates at the type erasure
boundary — some casts may be genuinely unavoidable. But each must be
verified.

### 10b. SparkInterpreter.scala (9 casts)

Spark's Row interface is untyped (Row.get returns Any). But with
Column[A] GADT, some of these may now be fixable via pattern matching.
Read each one.

### 10c. Schema.scala (2 casts)

Read and determine if eliminable.

## Execution Order

1. Phase 6: Delete eval (biggest impact — removes 7 casts + 650 lines)
2. Phase 7: Eliminate throw/return (25 + 10 = 35 suppressions)
3. Phase 8: Eliminate vars (168 suppressions)
4. Phase 9: Audit nulls (43 suppressions)
5. Phase 10: Audit remaining casts (21 suppressions across 3 files)

## Target

Zero scalafix:ok DisableSyntax.asInstanceOf in ExprInterpreter.
Zero scalafix:ok DisableSyntax.throw in the entire codebase.
Zero scalafix:ok DisableSyntax.return in the entire codebase.
Minimal scalafix:ok DisableSyntax.var (only individually justified).
Minimal scalafix:ok DisableSyntax.null (only SQL NULL storage boundary).
