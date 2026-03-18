# Strongbow Spark Parity Roadmap

Goal: Make strongbow the FOSS Scala 3 alternative to Databricks for typed Spark
pipelines. Every Expr case maps to a native Spark SQL function via ExprToColumn —
no UDFs, no RDDs, no serialization surprises.

## Current State (March 2026)

Spark version: 4.1.1 (latest stable)

### What strongbow already covers

**Arithmetic**: Int/Long/Double — Add, Sub, Mul, Div (type-matched)
**Comparisons**: Gt, Gte, Lt, Lte, Eq, Neq (with Ordering evidence)
**Boolean**: And, Or, Not
**Conditional**: When (if/then/else)
**String**: Concat, Length, Like
**Option**: IsDefined, GetOrElse, Option2Iterable
**Date**: DateAddDays, DateSubDays, DateAddMonths, DateDiff, ExtractYear/Month/Day
**Aggregations**: Sum(Int/Long/Double), Count, CountDistinct, CountIf, Avg,
  Max, Min, StdDev, StdDevPop, First, Collect, PercentileApprox,
  MaxBy, MinBy, MaxN, MinN, MaxByN, MinByN
**Window**: RowNumber, Rank, DenseRank, Lag, Lead
**Dataset ops**: Filter, Map, FlatMap, Select, SelectExprs, Distinct, Limit,
  Union, Intersect, Except, Sample, ZipWithIndex, ZipWithUniqueId,
  Persist, Checkpoint, Rebalance, all Join variants (predicate + Expr-based),
  SemiJoin, GroupByAgg, Aggregate, SortByExpr, SortByExprs, WithWindow,
  ReduceByKey, AggregateByKey
**Macros**: where (filter), withFields (copy), project, sortByColumn, groupByColumn

---

## Phase 1: Close Real Pipeline Gaps

Operations found in actual production Spark pipelines (dwh repo) that strongbow
cannot currently push to Catalyst. These block adoption.

### 1a. String Operations

Spark SQL has ~50 string functions. We need the core ones that show up in real
ETL normalization pipelines.

| Expr case | Spark SQL function | Priority |
|---|---|---|
| `Lower[Row](expr)` | `lower()` | Must have |
| `Upper[Row](expr)` | `upper()` | Must have |
| `Trim[Row](expr)` | `trim()` | Must have |
| `LTrim[Row](expr)` | `ltrim()` | Nice to have |
| `RTrim[Row](expr)` | `rtrim()` | Nice to have |
| `Substring[Row](expr, pos, len)` | `substring()` | Must have |
| `Replace[Row](expr, search, replacement)` | `replace()` | Must have |
| `RegexpReplace[Row](expr, pattern, replacement)` | `regexp_replace()` | Must have |
| `RegexpExtract[Row](expr, pattern, groupIdx)` | `regexp_extract()` | Must have |
| `Split[Row](expr, delimiter)` | `split()` | Must have |
| `StartsWith[Row](expr, prefix)` | `startswith()` | Must have |
| `EndsWith[Row](expr, suffix)` | `endswith()` | Must have |
| `Contains[Row](expr, substr)` | `contains()` | Must have |
| `Initcap[Row](expr)` | `initcap()` | Nice to have |
| `Lpad[Row](expr, len, pad)` | `lpad()` | Nice to have |
| `Rpad[Row](expr, len, pad)` | `rpad()` | Nice to have |
| `Reverse[Row](expr)` | `reverse()` | Nice to have |
| `ConcatWs[Row](sep, exprs*)` | `concat_ws()` | Nice to have |

Implementation: each is one enum case + ExprInterpreter clause + ExprToColumn clause.

### 1b. Null/Conditional Operations

| Expr case | Spark SQL function | Priority |
|---|---|---|
| `Coalesce[Row, A](exprs: Vector[Expr])` | `coalesce()` | Must have |
| `IsNull[Row, A](expr)` | `isnull()` | Must have |
| `IsNotNull[Row, A](expr)` | `isnotnull()` | Must have |
| `IsNaN[Row](expr)` | `isnan()` | Nice to have |
| `NullIf[Row, A](left, right)` | `nullif()` | Nice to have |
| `Nvl[Row, A](expr, default)` | `nvl()` | Nice to have |
| `Greatest[Row, A](exprs*)` | `greatest()` | Nice to have |
| `Least[Row, A](exprs*)` | `least()` | Nice to have |
| `In[Row, A](expr, values)` | `in()` | Must have |
| `Between[Row, A](expr, lower, upper)` | `between()` | Nice to have |

Note: `IsNull`/`IsNotNull` differ from `IsDefined` — they work on any type
at the SQL level, not just `Option[A]` at the Scala level.

### 1c. Arithmetic Gaps

| Expr case | Spark SQL function | Priority |
|---|---|---|
| `Mod[Row](left, right)` | `mod()` / `%` | Must have |
| `ModLong[Row](left, right)` | `mod()` | Must have |
| `Abs[Row](expr)` | `abs()` | Must have |
| `AbsLong[Row](expr)` | `abs()` | Must have |
| `AbsDouble[Row](expr)` | `abs()` | Must have |
| `Negate[Row](expr)` | `negative()` / unary `-` | Must have |
| `NegateLong[Row](expr)` | `negative()` | Must have |
| `NegateDouble[Row](expr)` | `negative()` | Must have |
| `Round[Row](expr, scale)` | `round()` | Must have |
| `Floor[Row](expr)` | `floor()` | Must have |
| `Ceil[Row](expr)` | `ceil()` | Must have |

### 1d. Type Casting

| Expr case | Spark SQL function | Priority |
|---|---|---|
| `CastToLong[Row](expr: Expr[Row, Int])` | `.cast("long")` | Must have |
| `CastToDouble[Row](expr: Expr[Row, Int])` | `.cast("double")` | Must have |
| `CastLongToDouble[Row](expr: Expr[Row, Long])` | `.cast("double")` | Must have |
| `CastToString[Row, A](expr)` | `.cast("string")` | Must have |
| `CastStringToInt[Row](expr)` | `.cast("int")` | Nice to have |
| `CastStringToLong[Row](expr)` | `.cast("long")` | Nice to have |
| `CastStringToDouble[Row](expr)` | `.cast("double")` | Nice to have |
| `CastStringToDate[Row](expr)` | `to_date()` | Nice to have |

Design choice: explicit named casts (CastToLong, CastToDouble) rather than a
generic `Cast[Row, A, B]` — preserves GADT type refinement and avoids runtime
type evidence.

---

## Phase 2: Math & Extended Date/Time

Operations common in analytics, ML feature engineering, and time-series work.

### 2a. Math Functions

| Expr case | Spark SQL function |
|---|---|
| `Sqrt[Row](expr)` | `sqrt()` |
| `Pow[Row](base, exponent)` | `pow()` |
| `Log[Row](expr)` | `ln()` |
| `Log10[Row](expr)` | `log10()` |
| `Log2[Row](expr)` | `log2()` |
| `Exp[Row](expr)` | `exp()` |
| `Sin[Row](expr)` | `sin()` |
| `Cos[Row](expr)` | `cos()` |
| `Tan[Row](expr)` | `tan()` |
| `Asin[Row](expr)` | `asin()` |
| `Acos[Row](expr)` | `acos()` |
| `Atan[Row](expr)` | `atan()` |
| `Atan2[Row](y, x)` | `atan2()` |
| `Signum[Row](expr)` | `signum()` |
| `Rand[Row](seed)` | `rand()` |

### 2b. Extended Date/Time

| Expr case | Spark SQL function |
|---|---|
| `DayOfWeek[Row](date)` | `dayofweek()` |
| `DayOfYear[Row](date)` | `dayofyear()` |
| `WeekOfYear[Row](date)` | `weekofyear()` |
| `Quarter[Row](date)` | `quarter()` |
| `LastDay[Row](date)` | `last_day()` |
| `NextDay[Row](date, dayOfWeek)` | `next_day()` |
| `MonthsBetween[Row](end, start)` | `months_between()` |
| `DateTrunc[Row](unit, date)` | `date_trunc()` |
| `DateFormat[Row](date, format)` | `date_format()` |
| `MakeDate[Row](year, month, day)` | `make_date()` |

### 2c. Additional Aggregations

| Expr case | Spark SQL function |
|---|---|
| `Variance[Row](expr)` | `var_samp()` |
| `VariancePop[Row](expr)` | `var_pop()` |
| `ApproxCountDistinct[Row, A](expr)` | `approx_count_distinct()` |
| `CollectSet[Row, A](expr)` | `collect_set()` |
| `Last[Row, A](expr)` | `last()` |
| `AnyValue[Row, A](expr)` | `any_value()` |
| `BoolAnd[Row](expr)` | `bool_and()` |
| `BoolOr[Row](expr)` | `bool_or()` |
| `Corr[Row](left, right)` | `corr()` |
| `CovarSamp[Row](left, right)` | `covar_samp()` |
| `CovarPop[Row](left, right)` | `covar_pop()` |
| `Median[Row](expr)` | `median()` |
| `Mode[Row, A](expr)` | `mode()` |

### 2d. Additional Window Functions

| Expr case | Spark SQL function |
|---|---|
| `NTile[Row](n)` | `ntile()` |
| `CumeDist[Row]()` | `cume_dist()` |
| `PercentRank[Row]()` | `percent_rank()` |
| `NthValue[Row, A](expr, n)` | `nth_value()` |
| `FirstValue[Row, A](expr)` | `first_value()` |
| `LastValue[Row, A](expr)` | `last_value()` |

---

## Phase 3: Collection Types & Operations

This requires extending Schema and Column to support array and map column types
natively, not just via AnyColumn.

### 3a. Schema & Column Extensions

- `ArrayType(elementType: ColumnType)` — new ColumnType variant
- `MapType(keyType: ColumnType, valueType: ColumnType)` — new ColumnType variant
- `Column.ArrayColumn[A](data: Array[Array[A]], nulls: BitSet)` — or store as
  AnyColumn with typed accessors
- Schema support for `Seq[A]`, `List[A]`, `Set[A]`, `Map[K, V]` where
  A/K/V have Schema instances

### 3b. Array Expressions

| Expr case | Spark SQL function |
|---|---|
| `ArraySize[Row, A](expr)` | `size()` |
| `ArrayContains[Row, A](expr, value)` | `array_contains()` |
| `Explode[Row, A](expr)` | `explode()` |
| `ArraySort[Row, A](expr)` | `sort_array()` |
| `ArrayDistinct[Row, A](expr)` | `array_distinct()` |
| `ArrayUnion[Row, A](left, right)` | `array_union()` |
| `ArrayIntersect[Row, A](left, right)` | `array_intersect()` |
| `ArrayExcept[Row, A](left, right)` | `array_except()` |
| `Flatten[Row, A](expr)` | `flatten()` |
| `ElementAt[Row, A](expr, index)` | `element_at()` |
| `Slice[Row, A](expr, start, length)` | `slice()` |

### 3c. Map Expressions

| Expr case | Spark SQL function |
|---|---|
| `MapKeys[Row, K, V](expr)` | `map_keys()` |
| `MapValues[Row, K, V](expr)` | `map_values()` |
| `MapContainsKey[Row, K, V](expr, key)` | `map_contains_key()` |
| `MapEntries[Row, K, V](expr)` | `map_entries()` |
| `MapFromArrays[Row, K, V](keys, values)` | `map_from_arrays()` |
| `MapConcat[Row, K, V](left, right)` | `map_concat()` |

---

## Phase 4: JSON, Hashing & Encoding

Functions used in data lake ETL, deduplication, and interchange formats.

### 4a. JSON

| Expr case | Spark SQL function |
|---|---|
| `FromJson[Row](expr, schema)` | `from_json()` |
| `ToJson[Row](expr)` | `to_json()` |
| `GetJsonObject[Row](expr, path)` | `get_json_object()` |
| `JsonArrayLength[Row](expr)` | `json_array_length()` |

### 4b. Hashing & Encoding

| Expr case | Spark SQL function |
|---|---|
| `Md5[Row](expr)` | `md5()` |
| `Sha1[Row](expr)` | `sha1()` |
| `Sha2[Row](expr, bitLength)` | `sha2()` |
| `Hash[Row](exprs*)` | `hash()` |
| `Xxhash64[Row](exprs*)` | `xxhash64()` |
| `Base64Encode[Row](expr)` | `base64()` |
| `Base64Decode[Row](expr)` | `unbase64()` |
| `Hex[Row](expr)` | `hex()` |
| `Unhex[Row](expr)` | `unhex()` |
| `UrlEncode[Row](expr)` | `url_encode()` |
| `UrlDecode[Row](expr)` | `url_decode()` |

---

## Phase 5: Distributed Opaque Fallback

After Phases 1-4, the need for opaque functions on large data should be rare.
But for completeness and pragmatism:

**Decision point**: If real-world adoption shows pipelines that genuinely cannot
express their logic as Expr (truly custom business logic, not missing SQL
functions), then add a distributed path via `DataFrame.mapPartitions` with
`RowEncoder`. This is a last resort, not a design goal.

Until then, every opaque `map`/`flatMap` runs via collect-to-driver. This is
explicitly documented: "use Expr for distributed, use map for small transforms."

---

## Implementation Notes

### Each new Expr case requires exactly 3 changes:

1. **Expr.scala** — new enum case
2. **ExprInterpreter.scala** — in-memory evaluation clause
3. **ExprToColumn.scala** — Spark SQL translation clause

Plus tests in:
- **NewExprSpec.scala** or relevant test file — in-memory correctness
- **SparkInterpreterSpec.scala** — Spark round-trip correctness

### Design principles:

- **Explicit over implicit**: Named casts (CastToLong) not generic Cast[A, B]
- **Type-matched arithmetic**: Keep Int/Long/Double separate, add promotion casts
  in Phase 1d rather than implicit coercion in arithmetic ops
- **No UDFs ever**: Every Expr maps to a native Spark function or it doesn't exist
- **In-memory interpreter is the spec**: ExprInterpreter defines correctness,
  ExprToColumn must produce equivalent results
- **GADT refinement**: New cases must preserve type evidence through pattern
  matching — no asInstanceOf in the interpreter hot path

### Estimated scope:

| Phase | New Expr cases | Effort |
|---|---|---|
| Phase 1 | ~35 cases | Core work — blocks adoption |
| Phase 2 | ~30 cases | Analytics completeness |
| Phase 3 | ~15 cases + Schema changes | Collection support |
| Phase 4 | ~15 cases | ETL/interchange |
| Phase 5 | 0 Expr cases, SparkInterpreter change | Safety net |

Phase 1 is the gate to presenting strongbow as a viable Spark alternative.
Phases 2-4 build toward Databricks parity. Phase 5 is insurance.
