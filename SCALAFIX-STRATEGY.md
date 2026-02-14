# Scalafix Enforcement Strategy

## Goal
Enforce strict rules globally while allowing architecturally necessary violations with explicit annotations and justification.

## Current Status

### ✅ Vars - Resolved
- **Only 1 var** in entire codebase: `buildNullSet` in `Column.scala`
- **Justification**: Contained mutability for performance (145x memory savings)
- **Pattern**: mutable.BitSet + var, converted to immutable result
- **Annotation**: `// scalafix:ok DisableSyntax.noVars`
- **Benchmarked**: All functional alternatives cause unacceptable memory overhead

### ⚠️ Remaining Violations to Annotate

#### 1. **asInstanceOf** - One-Cast-at-Boundary Pattern
**Location**: `ExprInterpreter.scala` lines 48-54

**Architectural necessity**: Core zero-cast GADT pattern
```scala
case cell: Expr.Cell[Row, a] =>
  val value: a = column.columnType match {
    case ColumnType.IntType => column.getInt(idx).asInstanceOf[a]  // NECESSARY
    // ...
  }
```

**Why necessary**: Type system limitation - we need to bridge from typed column accessors to GADT's generic type `a`. This is the "one cast" in our "one-cast-at-boundary" architecture.

**Action**: Add `// scalafix:ok DisableSyntax.noAsInstanceOf` with documentation

#### 2. **asInstanceOf** - Schema Type Conversions
**Location**: `DatasetInterpreter.scala` lines 80, 85, 107

**Issue**: Schema conversions for tuple unpacking
```scala
Right((values(0).asInstanceOf[k], values(1).asInstanceOf[v]))
tupleSchema.asInstanceOf[Schema[T]]
```

**Why needed**: Type system limitation with Schema[T] conversions

**Action**: Need to evaluate if these can be eliminated or must be annotated

#### 3. **null** - Nullable Column Values
**Locations**:
- `Column.scala`: slice methods (lines 114, 118) - default values for nullable types
- `Column.scala`: fromValues (line 260) - representing SQL NULL

**Architectural choice**: Columnar storage uses `null` for:
1. Default values for nullable reference types (String, Any)
2. Representing SQL NULL in data

**Alternative considered**: Option[T] everywhere - rejected for performance and Spark compatibility

**Action**: Need to decide strategy:
- Option A: Annotate all with justification
- Option B: Refactor to eliminate (may not be possible)
- Option C: Design safer null handling API

#### 4. **throw** - Unimplemented Features & Validation
**Locations**:
- `Dataset.scala` line 113 - unimplemented joins
- `DatasetInterpreter.scala` lines 26, 31, 36 - unsupported operations
- `Column.scala` lines 66, 73, 80, 87, 94 - type mismatch errors
- `Column.scala` lines 232, 242, 252, 262, 272 - fromValues validation
- `MaterializedDataset.scala` line 42 - decode errors
- `NonEmptyList.scala` line 24 - precondition violation

**Categories**:
1. **Unimplemented features** - temporary, should become proper errors
2. **Type safety violations** - should never happen if used correctly
3. **Validation errors** - should be Either/Result

**Action**: Need strategy:
- Phase 1: Annotate existing throws with plan to eliminate
- Phase 2: Replace with proper error types in Either/Result

## Recommended Approach

### Phase 1: Document & Annotate (Now)
1. Add `scalafix:ok` annotations to all architecturally necessary violations
2. Each annotation must have:
   - Comment explaining why it's necessary
   - Link to benchmark results if performance-related
   - TODO if we plan to eliminate it later

### Phase 2: Eliminate Non-Essential (Next)
1. **null in tests**: Can likely be eliminated or annotated as test-only
2. **throw for unimplemented**: Convert to proper error types
3. **Schema asInstanceOf**: Investigate if type-level solution exists

### Phase 3: Null Handling Strategy (Future)
1. Design safe null handling that works for both:
   - Columnar interpreter (performance critical)
   - Spark interpreter (needs null for SQL NULL compatibility)
2. Possible approach: Separate nullable/non-nullable column types at type level

## Scalafix Configuration

Current (`.scalafix.conf`):
```scala
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

// No exceptions - enforce everywhere
// Use scalafix:ok annotations for architecturally necessary violations
```

This enforces strict rules globally. All violations must be explicitly annotated.

## Next Steps

1. ✅ Vars eliminated (except 1 with contained mutability pattern)
2. ⏳ Add prepare command to build.sbt
3. ⏳ Annotate all asInstanceOf in ExprInterpreter (architectural)
4. ⏳ Decide strategy for null handling
5. ⏳ Decide strategy for throw statements
6. ⏳ Run `sbt prepare` successfully with all violations documented

## Benchmark Results Summary

All benchmarks in `strongbow-bench/`:
- `VarEliminationBenchmark.scala` - Comprehensive var elimination analysis
- `ArrayVsVectorBench.scala` - Array vs Vector for indices (2x speedup)
- `MutableBitSetBench.scala` - mutable.BitSet vs Builder (145x memory with foreach)

**Key finding**: Contained mutability (mutable.BitSet + var) is necessary for performance.
**Pattern**: Matches industry practice - mutability as implementation detail, pure public API.
