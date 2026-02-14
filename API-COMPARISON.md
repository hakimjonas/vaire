# Strongbow API Comparison & Gap Analysis

**Date:** 2026-02-13
**Purpose:** Identify feature gaps before implementing Spark interpreter

This document compares Strongbow's current API against:
1. **dwh-core** (LiveIntent's production TypedDataset/Grouped)
2. **Crossbow** (Production Scala DataFrame library)
3. **Industry patterns** (Spark, Pandas, Polars)

---

## Current Strongbow API Surface

### Dataset[T] Methods

✅ **Implemented:**
1. `filter(predicate: Expr[T, Boolean]): Dataset[T]`
2. `map[U](f: T => U): Dataset[U]`
3. `flatMap[U](f: T => Iterable[U]): Dataset[U]`
4. `distinct: Dataset[T]`
5. `limit(n: Int): Dataset[T]`
6. `union(other: Dataset[T]): Dataset[T]`
7. `sort(using Ordering[T]): Dataset[T]`
8. `sortBy[K](key: T => K)(using Ordering[K]): Dataset[T]`
9. `groupBy[K](key: T => K): Grouped[K, T]`
10. `keyBy[K](key: T => K): Grouped[K, T]` (alias)
11. `select(exprs: (String, Expr[T, Any], ColumnType)*): Dataset[T]`

❌ **Not Implemented:**
12. `join[U](...)` - Stub throws UnsupportedOperationException

### Grouped[K, V] Methods

✅ **Implemented:**
1. `values(using Schema[V]): Dataset[V]`
2. `keys(using Schema[K]): Dataset[K]`
3. `toPairs(using Schema[K], Schema[V]): Dataset[(K, V)]`
4. `mapValues[U](f: V => U): Grouped[K, U]`
5. `flatMapValues[U](f: V => Iterable[U]): Grouped[K, U]`
6. `filterKeys(predicate: K => Boolean): Grouped[K, V]`
7. `join[U](other: Grouped[K, U]): Grouped[K, (V, U)]` (inner)
8. `leftJoin[U](other: Grouped[K, U]): Grouped[K, (V, Option[U])]`
9. `rightJoin[U](other: Grouped[K, U]): Grouped[K, (Option[V], U)]`
10. `fullJoin[U](other: Grouped[K, U]): Grouped[K, (Option[V], Option[U])]`
11. `reduceByKey(f: (V, V) => V): Grouped[K, V]`

❌ **Not Implemented:**
12. `leftAntiJoin` - Anti join pattern
13. `mapValuesWithKey` - Transform with key access
14. `aggregateByKey` - Multi-arity aggregations
15. `sortByKey` - Sort grouped data by key
16. `union` / `++` - Union of grouped datasets

### Expression-Based Aggregations (Expr[Row, R])

✅ **Implemented:**
1. `Count[Row](): Expr[Row, Long]`
2. `Sum[Row](expr: Expr[Row, Int]): Expr[Row, Int]`
3. `Avg[Row](expr: Expr[Row, Double]): Expr[Row, Double]`
4. `Max[Row, A](expr: Expr[Row, A], Ordering[A]): Expr[Row, Option[A]]`
5. `Min[Row, A](expr: Expr[Row, A], Ordering[A]): Expr[Row, Option[A]]`

---

## Gap Analysis vs dwh-core TypedDataset

### ✅ Feature Parity (We Have These)

| Feature | Strongbow | dwh-core | Notes |
|---------|-----------|----------|-------|
| **Transformation** | | | |
| distinct | ✅ | ✅ | Identical |
| filter | ✅ | ✅ | Expr-based vs macro-based |
| map | ✅ | ✅ | Function-based vs macro |
| flatMap | ✅ | ✅ | Function-based vs macro |
| **Grouping** | | | |
| groupBy | ✅ | ✅ | Returns Grouped[K, V] |
| keyBy | ✅ | ✅ | Alias for groupBy |
| **Sorting** | | | |
| sort | ✅ | ✅ | Requires Ordering[T] |
| sortBy | ✅ | ✅ | Sorts by key function |
| **Union** | | | |
| union | ✅ | ✅ | Combines datasets |
| **Limiting** | | | |
| limit | ✅ | ✅ | Takes first n rows |
| **Aggregation Primitives** | | | |
| Count | ✅ | ✅ | Via Expr |
| Sum | ✅ | ✅ | Via Expr |
| Avg | ✅ | ✅ | Via Expr |
| Max | ✅ | ✅ | Via Expr |
| Min | ✅ | ✅ | Via Expr |

### ❌ Missing from Strongbow (dwh-core has)

#### High Priority - Core Functionality

| Feature | dwh-core | Impact | Implementation Complexity |
|---------|----------|--------|---------------------------|
| **sample(fraction, seed, withReplacement)** | ✅ | High - Testing/exploration | Low |
| **partition(f: T => Boolean)** | ✅ | Medium - Split datasets | Low |
| **zipWithIndex()** | ✅ | Medium - Row numbering | Low |
| **zipWithUniqueId()** | ✅ | Medium - Unique IDs | Low |
| **collect(): Seq[T]** | ✅ | High - Materialize results | Low |
| **count(): Long** | ✅ | High - Row counting | Low |
| **isEmpty(): Boolean** | ✅ | Low - Convenience | Low |
| **++(other)** operator | ✅ | Low - Alias for union | Trivial |

#### Medium Priority - Ergonomics

| Feature | dwh-core | Impact | Implementation Complexity |
|---------|----------|--------|---------------------------|
| **project[U](implicit Projector[T, U])** | ✅ | Medium - Type-safe projection | Medium |
| **persist()** | ✅ | Medium - Performance hint | Low (no-op until Spark) |
| **checkpoint()** | ✅ | Medium - Plan truncation | Medium |
| **rebalance()** | ✅ | Low - Skew handling | Medium |
| **rebalanceWithFixedNumberOfParts(n)** | ✅ | Low - Partition control | Medium |
| **explain(): String** | ✅ | High - Debugging | Low |
| **debug()** | ✅ | High - Debugging | Low |

#### Low Priority - Multi-Arity Aggregations

| Feature | dwh-core | Impact | Complexity |
|---------|----------|--------|------------|
| **aggregate(agg1, agg2)** | ✅ | Medium | Low |
| **aggregate(agg1, agg2, agg3)** | ✅ | Medium | Low |
| **aggregate(agg1...agg7)** | ✅ | Low | Low |

#### Low Priority - Advanced Aggregations

| Feature | dwh-core | Impact | Complexity |
|---------|----------|--------|------------|
| **countDistinct** | ✅ | Medium | Medium |
| **countAllDistinct** | ✅ | Low | Medium |
| **countIf** | ✅ | Medium | Low |
| **stddev / stddevPop** | ✅ | Low | Low |
| **percentileApprox** | ✅ | Low | Medium |
| **medianApprox** | ✅ | Low | Medium |
| **maxN / minN** | ✅ | Low | Medium |
| **maxBy / minBy / maxByN / minByN** | ✅ | Low | Medium |

#### Low Priority - I/O (Defer to Spark Layer)

| Feature | dwh-core | Impact | Complexity |
|---------|----------|--------|------------|
| **writeTo[S <: WritableSource]** | ✅ | Low | High (needs Source abstraction) |
| **writeToRebalanced** | ✅ | Low | High |
| **writeToFixedNumberOfParts** | ✅ | Low | High |

---

## Gap Analysis vs dwh-core Grouped

### ✅ Feature Parity (We Have These)

| Feature | Strongbow | dwh-core | Notes |
|---------|-----------|----------|-------|
| **Key/Value Access** | | | |
| keys | ✅ | ✅ | Returns Dataset[K] |
| values | ✅ | ✅ | Returns Dataset[V] |
| pairs / toPairs | ✅ | ✅ | Returns Dataset[(K, V)] |
| **Transformations** | | | |
| mapValues | ✅ | ✅ | Transform values |
| flatMapValues | ✅ | ✅ | Transform & flatten |
| filterKeys | ✅ | ✅ | Filter by key |
| **Joins** | | | |
| join (inner) | ✅ | ✅ | Inner join |
| leftJoin | ✅ | ✅ | Left outer join |
| rightJoin | ✅ | ✅ | Right outer join |
| fullJoin | ✅ | ✅ | Full outer join |
| **Reduction** | | | |
| reduceByKey | ✅ | ✅ | Binary reduction |

### ❌ Missing from Strongbow (dwh-core Grouped has)

#### High Priority

| Feature | dwh-core | Impact | Complexity |
|---------|----------|--------|------------|
| **leftAntiJoin[U]** | ✅ | Medium - Anti-join pattern | Low |
| **aggregateByKey(agg1, agg2, ...)** | ✅ | High - Multi-agg | Low |
| **sortByKey()** | ✅ | Medium - Sort grouped | Low |
| **union / ++** | ✅ | Medium - Combine grouped | Low |

#### Medium Priority

| Feature | dwh-core | Impact | Complexity |
|---------|----------|--------|------------|
| **mapValuesWithKey** | ✅ | Low - Key access convenience | Low |

---

## Missing Industry-Standard Patterns

Based on Spark DataFrame, Pandas, Polars:

### ❌ Not in Strongbow or dwh-core

#### High Impact

| Feature | Spark | Pandas | Polars | Impact |
|---------|-------|--------|--------|--------|
| **Window Functions** | ✅ | ✅ | ✅ | High - rank(), row_number(), lag(), lead() |
| **Pivot / Unpivot** | ✅ | ✅ | ✅ | Medium - Reshape operations |
| **Set Operations** | ✅ | ✅ | ✅ | Medium - intersect(), except() |
| **Cross Join** | ✅ | ✅ | ✅ | Low - Cartesian product |
| **Coalesce** | ✅ | - | ✅ | Low - Reduce partitions |
| **Broadcast Hint** | ✅ | - | - | Low - Performance optimization |

#### Medium Impact

| Feature | Spark | Pandas | Polars | Impact |
|---------|-------|--------|--------|--------|
| **fillna / dropna** | ✅ | ✅ | ✅ | Medium - Null handling |
| **replace** | ✅ | ✅ | ✅ | Low - Value replacement |
| **cast / astype** | ✅ | ✅ | ✅ | Low - Type conversion |
| **withColumn** | ✅ | ✅ | ✅ | Medium - Add/replace column |
| **drop** | ✅ | ✅ | ✅ | Low - Remove columns |
| **rename** | ✅ | ✅ | ✅ | Low - Rename columns |

---

## Recommendations for Strongbow Pre-Spark

### Priority 1: Complete Core Dataset API

**Add these methods for basic feature parity:**

```scala
// Dataset[T]
def sample(fraction: Double, seed: Long = randomSeed, withReplacement: Boolean = false): Dataset[T]
def partition(predicate: Expr[T, Boolean]): (Dataset[T], Dataset[T])
def zipWithIndex: Dataset[(T, Long)]
def ++(other: Dataset[T]): Dataset[T]  // Alias for union

// Actions (return values, not AST)
def collect: Either[ExecutionError, Vector[T]]
def count: Either[ExecutionError, Long]
def isEmpty: Either[ExecutionError, Boolean]
def take(n: Int): Either[ExecutionError, Vector[T]]  // Convenience for limit(n).collect

// Debugging
def explain: String
def show(n: Int = 20): String
```

### Priority 2: Complete Grouped API

**Add missing Grouped methods:**

```scala
// Grouped[K, V]
def leftAntiJoin[U](other: Grouped[K, U]): Grouped[K, V]
def sortByKey(using Ordering[K]): Grouped[K, V]
def union(other: Grouped[K, V]): Grouped[K, V]
def ++(other: Grouped[K, V]): Grouped[K, V]  // Alias

// Multi-arity aggregations
def aggregateByKey[R1, R2](
  agg1: Aggregate[V, R1],
  agg2: Aggregate[V, R2]
): Grouped[K, (R1, R2)]

// (Add 3-7 arity overloads as needed)
```

### Priority 3: Aggregation System

**Design decision needed:** Expression-based vs dedicated Aggregate type

**Option A: Keep expression-based (current approach)**
```scala
// Use Expr aggregations in select
dataset.select(
  "total" -> Sum(Expr.Cell("amount", ...)),
  "count" -> Count(),
  "avg" -> Avg(Expr.Cell("value", ...))
)
```

**Option B: Add dedicated Aggregate abstraction (dwh-core style)**
```scala
import net.ghoula.strongbow.aggregations._

dataset.aggregate(
  sum(_.amount),
  count,
  avg(_.value)
)
```

**Recommendation:** Start with Option A (expression-based) for consistency, add Option B convenience layer later if needed.

### Priority 4: Extended Aggregations

**Add these aggregation expressions:**

```scala
// In Expr enum
case CountDistinct[Row, A](expr: Expr[Row, A]) extends Expr[Row, Long]
case CountIf[Row](predicate: Expr[Row, Boolean]) extends Expr[Row, Long]
case StdDev[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
case StdDevPop[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

// In companion object helpers
def countDistinct[Row, A](expr: Expr[Row, A]): Expr[Row, Long]
def countIf[Row](predicate: Expr[Row, Boolean]): Expr[Row, Long]
def stddev[Row](expr: Expr[Row, Double]): Expr[Row, Double]
```

### Defer to Spark Interpreter

**These can wait for Spark implementation:**
- Window functions (require partitioning + ordering)
- Pivot/unpivot (complex reshaping)
- Broadcast hints (execution-specific)
- I/O operations (writeTo, etc.)
- persist/checkpoint (cache management)
- rebalance/coalesce (partitioning control)

---

## Implementation Roadmap

### Phase 1: Core Completeness (Before Spark)

**Goal:** Feature parity with dwh-core for common operations

1. **Dataset actions** (3-5 methods, ~1 hour)
   - collect, count, isEmpty, take
   - Update tests

2. **Dataset transformations** (3-4 methods, ~2 hours)
   - sample, partition, zipWithIndex
   - ++operator alias
   - Update tests

3. **Grouped completeness** (4-5 methods, ~2 hours)
   - leftAntiJoin, sortByKey, union, ++
   - Update tests

4. **Aggregations expansion** (3-4 expressions, ~1 hour)
   - CountDistinct, CountIf, StdDev
   - Helper functions
   - Update tests

5. **Debug/introspection** (2 methods, ~1 hour)
   - explain, show
   - Pretty-print AST

**Total estimate:** ~7-9 hours to complete core API

### Phase 2: Spark Interpreter (Next)

With complete columnar API, implement Spark interpreter knowing the AST surface is stable.

---

## What We Have That dwh-core Doesn't

### ✅ Strongbow Unique Features

1. **Type-specialized columns** - `Array[Int]`, `Array[String]` vs `Array[Any]`
2. **Zero-cast GADT expressions** - Compile-time type safety
3. **Schema evidence propagation** - Type-safe schema composition
4. **Functional error handling** - `Either[ExecutionError, T]` throughout
5. **Expression-based DSL** - First-class expressions for select/filter
6. **Performance** - 18x faster joins, 2-3x faster sorts vs Crossbow
7. **Multiple interpreters** - Pluggable backends (columnar now, Spark next)

### Design Philosophy Differences

| Aspect | Strongbow | dwh-core |
|--------|-----------|----------|
| **Type Safety** | Compile-time via GADTs | Compile-time via macros |
| **Performance** | Zero-allocation critical path | Macro-optimized but Array[Any] |
| **Execution** | Multiple interpreters | Single Spark backend |
| **Expressions** | First-class Expr[Row, T] | Macro-extracted lambdas |
| **Error Handling** | Functional Either | Exceptions |
| **Schema** | Evidence-based with given/using | Codec-based implicits |

---

## Summary

### Current State
- **Dataset:** 11/12 methods (92% complete)
- **Grouped:** 11/16 methods (69% complete)
- **Aggregations:** 5/36 expressions (14% complete)

### Pre-Spark Gaps (High Priority)
1. **Actions:** collect, count, isEmpty (~30 min)
2. **Sampling:** sample, partition (~1 hour)
3. **Grouped joins:** leftAntiJoin, sortByKey (~1 hour)
4. **Aggregations:** countDistinct, countIf, stddev (~1 hour)
5. **Debug:** explain, show (~1 hour)

### Total Work
**~5-7 hours** to achieve feature parity with dwh-core core functionality.

### Strategic Decision
**Question:** Should we complete these ~5-7 hours of work now, or proceed with Spark interpreter with current API and backfill later?

**Recommendation:** Complete Priority 1 & 2 now (~4 hours) for core stability, defer Priority 3 & 4 aggregations to discover during Spark implementation.
