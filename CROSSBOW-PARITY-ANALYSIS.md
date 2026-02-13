# Crossbow Parity Analysis

## Operation Comparison Matrix

### ✅ Operations We Have (Parity Achieved)

| Operation | Strongbow | Crossbow | Notes |
|-----------|---------|----------|-------|
| **Dataset Construction** |
| fromColumns | ✅ Dataset.fromColumns | ✅ DataFrame.fromColumns | Identical |
| **Row Selection** |
| filter | ✅ Dataset.Filter | ✅ DataFrame.filter | Identical |
| limit | ✅ Dataset.Limit | ✅ DataFrame.take | Same concept |
| distinct | ✅ Dataset.Distinct | ❌ Missing | We have it! |
| **Sorting** |
| sort | ✅ Dataset.Sort | ✅ DataFrame.sortBy | Identical |
| sortBy | ✅ Dataset.SortBy | ✅ DataFrame.sortBy | Identical |
| **Combining** |
| union | ✅ Dataset.Union | ✅ DataFrame.union | Identical |
| **Grouping** |
| groupBy | ✅ Dataset.groupBy | ✅ DataFrame.groupBy | Identical |
| **Joins** |
| innerJoin | ✅ Grouped.InnerJoin | ✅ DataFrame.join(Inner) | Both have |
| leftJoin | ✅ Grouped.LeftJoin | ✅ DataFrame.join(LeftOuter) | Both have |
| rightJoin | ✅ Grouped.RightJoin | ✅ DataFrame.join(RightOuter) | Both have |
| fullJoin | ✅ Grouped.FullJoin | ✅ DataFrame.join(FullOuter) | Both have |
| **Expressions - Arithmetic** |
| Add (+) | ✅ Expr.Add | ✅ ArithmeticOps.+ | Identical |
| Sub (-) | ✅ Expr.Sub | ✅ ArithmeticOps.- | Identical |
| Mul (*) | ✅ Expr.Mul | ✅ ArithmeticOps.* | Identical |
| Div (/) | ✅ Expr.Div | ✅ ArithmeticOps./ | Identical |
| **Expressions - Comparisons** |
| Equal (==) | ✅ Expr.Eq | ✅ BaseOps.=== | Identical |
| Greater (>) | ✅ Expr.Gt | ✅ ComparisonOps.> | Identical |
| Less (<) | ✅ Expr.Lt | ✅ ComparisonOps.< | Identical |
| **Expressions - Boolean** |
| And (&&) | ✅ Expr.And | ✅ BooleanOps.&& | Identical |
| Or (||) | ✅ Expr.Or | ✅ BooleanOps.\|\| | Identical |
| Not (!) | ✅ Expr.Not | ✅ BooleanOps.not | Identical |
| **Aggregations** |
| sum | ✅ Expr.Sum | ✅ dsl.sum | Identical |
| count | ✅ Expr.Count | ✅ dsl.count | Identical |
| max | ✅ Expr.Max | ❌ Missing | We have it! |
| min | ✅ Expr.Min | ❌ Missing | We have it! |
| avg | ✅ Expr.Avg | ❌ Missing | We have it! |
| **Grouped Operations** |
| reduceByKey | ✅ Grouped.ReduceByKey | ✅ GroupedView.agg(reducer) | Similar |
| mapValues | ✅ Grouped.MapValues | ❌ Missing | We have it! |
| flatMapValues | ✅ Grouped.FlatMapValues | ❌ Missing | We have it! |
| filterKeys | ✅ Grouped.FilterKeys | ❌ Missing | We have it! |

### ❌ Operations Crossbow Has That We're Missing

| Operation | Crossbow | Priority | Notes |
|-----------|----------|----------|-------|
| **Column Operations** |
| select | ✅ DataFrame.select | ✅ DONE | SelectExprs implemented |
| addColumn | ✅ DataFrame.addColumn | 🟡 MEDIUM | Add computed column |
| removeColumns | ✅ DataFrame.removeColumns | 🟡 MEDIUM | Drop columns by name |
| renameColumns | ✅ DataFrame.renameColumns | 🟡 MEDIUM | Rename columns |
| **Advanced Operations** |
| explode | ✅ DataFrame.explode | 🟠 LOW | Flatten list column |
| partition | ✅ DataFrame.partition | 🟠 LOW | Split into (matching, non-matching) |
| **Join on DataFrame** |
| join (on Dataset) | ⚠️ Via Grouped | 🟡 MEDIUM | Crossbow has df.join(), we delegate to groupBy().join() - functionally equivalent |
| **Expressions - Arithmetic** |
| modulo (%) | ✅ ArithmeticOps.% | 🟡 MEDIUM | Remainder operation |
| abs | ✅ ArithmeticOps.abs | 🟡 MEDIUM | Absolute value |
| negate | ✅ ArithmeticOps.negate | 🟠 LOW | Unary negation |
| **Expressions - Comparison** |
| greaterEq (>=) | ✅ ComparisonOps.>= | 🟡 MEDIUM | Greater or equal |
| lessEq (<=) | ✅ ComparisonOps.<= | 🟡 MEDIUM | Less or equal |
| notEqual (!=) | ✅ BaseOps.=!= | 🟡 MEDIUM | Not equal |
| **Expressions - String** |
| concat | ✅ Expr.Concat | ✅ (we have it) | String concatenation |
| length | ✅ Expr.Length | ✅ (we have it) | String length |
| **Expressions - Option** |
| isDefined | ✅ Expr.IsDefined | ✅ (we have it) | Check if option has value |
| getOrElse | ✅ Expr.GetOrElse | ✅ (we have it) | Get value or default |
| **Expressions - Advanced** |
| as (rename) | ✅ Expr.Named | 🔴 HIGH | Rename expressions for output columns |
| index | ✅ Expr.Index | 🟠 LOW | Current row index |
| lambda | ✅ Expr.Unary | 🟠 LOW | Custom unary functions |
| seq | ✅ Expr.List | 🟠 LOW | Create list expressions |
| **Aggregations** |
| collect | ✅ dsl.collect | 🟡 MEDIUM | Collect all values into list |
| one | ✅ dsl.one | 🟠 LOW | Get arbitrary value from group |
| custom reducer | ✅ dsl.reducer | 🟡 MEDIUM | User-defined aggregation function |
| **Helpers** |
| lit | ✅ dsl.lit | 🔴 HIGH | Create literal expressions (we have Const but no helper) |
| $ interpolation | ✅ dsl.$ | 🟠 LOW | String interpolation for columns |

### ✨ Operations We Have That Crossbow Doesn't

| Operation | Strongbow | Advantage |
|-----------|---------|-----------|
| **Aggregations** |
| max | ✅ Expr.Max | Built-in with ordering |
| min | ✅ Expr.Min | Built-in with ordering |
| avg | ✅ Expr.Avg | Built-in average |
| **Grouped Operations** |
| mapValues | ✅ Grouped.MapValues | Transform values without regrouping |
| flatMapValues | ✅ Grouped.FlatMapValues | Expand grouped values |
| filterKeys | ✅ Grouped.FilterKeys | Filter groups by key |
| keys | ✅ Grouped.keys | Extract just keys |
| values | ✅ Grouped.values | Extract just values |
| toPairs | ✅ Grouped.toPairs | Convert to key-value pairs |
| **Dataset Operations** |
| distinct | ✅ Dataset.Distinct | Remove duplicates |
| **Architecture** |
| GADT expressions | ✅ Expr[Row, A] | Zero-cast evaluation |
| Separate interpreters | ✅ Multiple backends | Columnar + Spark from one plan |

### 🚫 Neither Has (But Should Consider)

| Operation | Description | Priority |
|-----------|-------------|----------|
| **String Operations** |
| substring | Extract substring | 🟡 MEDIUM |
| toLowerCase/toUpperCase | Case conversion | 🟡 MEDIUM |
| trim/ltrim/rtrim | Whitespace trimming | 🟡 MEDIUM |
| contains/startsWith/endsWith | String matching | 🟡 MEDIUM |
| split | Split string into array | 🟡 MEDIUM |
| regex_extract | Regular expression extraction | 🟠 LOW |
| **Date/Time** |
| date functions | Parse, format, extract | 🟠 LOW |
| timestamp operations | Time arithmetic | 🟠 LOW |
| **Window Functions** |
| row_number | Row numbering within window | 🟠 LOW |
| rank/dense_rank | Ranking functions | 🟠 LOW |
| lag/lead | Access previous/next row | 🟠 LOW |
| cumulative aggregations | Running sum/avg/etc | 🟠 LOW |
| **Null Handling** |
| coalesce | First non-null value | 🟡 MEDIUM |
| nvl/nvl2 | Null value replacement | 🟡 MEDIUM |
| isNull/isNotNull | Null checking | 🟡 MEDIUM |
| nullif | Conditional null | 🟠 LOW |
| **Type Conversions** |
| cast | Explicit type conversion | 🟡 MEDIUM |
| tryParse | Safe parsing | 🟡 MEDIUM |
| **Math Functions** |
| sqrt/pow | Math operations | 🟠 LOW |
| round/floor/ceil | Rounding | 🟡 MEDIUM |
| **Conditionals** |
| when/otherwise | CASE expressions | 🔴 HIGH |
| if/then/else | Conditional expression | 🔴 HIGH |
| **Collections** |
| array operations | Index, slice, contains | 🟠 LOW |
| map operations | Key access for maps | 🟠 LOW |

## Priority Recommendations

### 🔴 CRITICAL (Before Spark)

1. **lit helper** - Essential for creating literal expressions ergonomically
2. **as (rename)** - Essential for renaming output columns
3. **select** - Core operation for column selection
4. **join on Dataset** - Currently only on Grouped, need DataFrame-level joins
5. **when/otherwise** - Conditional logic is fundamental

### 🟡 HIGH PRIORITY (Phase 3.5 - Complete Columnar)

1. **>=, <=, !=** - Complete comparison operators
2. **modulo (%)** - Common arithmetic operation
3. **abs** - Common math operation
4. **addColumn** - Useful for adding computed columns
5. **collect aggregation** - Useful for grouping values into lists
6. **custom reducer** - Flexibility for user-defined aggregations
7. **coalesce/isNull** - Null handling is important

### 🟠 LOWER PRIORITY (Post-Spark)

1. **String operations** (substring, contains, etc.)
2. **explode** - Advanced flattening
3. **partition** - Nice-to-have split operation
4. **Window functions** - Advanced analytics
5. **Date/time** - Can be added incrementally

## Recommendations

**Before Phase 4 (Spark):**
1. Add `lit` helper for ergonomic literal creation
2. Add `as` (Expr.Named) for column renaming
3. Implement `select` operation
4. Add comparison operators: `>=`, `<=`, `!=`
5. Add `when/otherwise` for conditional logic
6. Consider Dataset-level joins (not just Grouped)

**Why these are critical:**
- Spark will need `lit()` and column naming heavily
- `select()` is fundamental to any DataFrame API
- Comparison operators are expected in any query language
- Conditional logic (when/otherwise) is core functionality

**Can wait until after Spark:**
- String operations (Spark has these built-in anyway)
- Math functions beyond basics
- Window functions
- Advanced aggregations

This gives us a solid, complete columnar interpreter that will make Spark integration cleaner.
