# Strongbow Final Cleanup Plan

## Completed

### Phase 6: Delete eval — DONE
eval method deleted (~650 lines, 7 Cell boundary casts). evalAggregation
rewritten with Column[A] GADT refinement. evalAt deleted (test-only, Any
return type). Tests rewritten to use evalColumn directly.

### Phase 7: throw/return/var cleanup — DONE
ExprInterpreter: 172 var → 0, 6 return → 0, 0 throw.
DatasetInterpreter: 14 var → 0, 4 return → 0, 3 throw → 0.
Column: 6 throw → deleted (dead typed accessors removed).
SparkInterpreter: 11 throw → 0, 1 var → 0, 9 asInstanceOf → 0.

### Phase 8: vars — DONE (core)
Core module: 0 var. Remaining 11 in Spark interop + Column internals.

### Phase 10: asInstanceOf audit — DONE
ExprInterpreter: 0. DatasetInterpreter: 0. SparkInterpreter: 0.
Remaining 11 all in macros (10 ExprMacro + 1 Schema derivation) —
genuinely unavoidable at the macro quote/splice type erasure boundary.

## Remaining

### Phase 9: Null audit

41 null suppressions across the codebase. Each must be individually
justified as SQL NULL storage boundary or eliminated.

### Remaining var (11)

7 in RowConverter, 2 in DataFrameBuilder, 2 in Column. Each must be
individually justified or rewritten.

### Remaining throw (1)

1 in RowConverter. Must be replaced with proper error handling or
justified.

## Current Suppression Count

| Category | Count | Location |
|---|---|---|
| asInstanceOf | 11 | ExprMacro (10) + Schema derivation (1) — macro boundary |
| var | 11 | RowConverter (7) + DataFrameBuilder (2) + Column (2) |
| throw | 1 | RowConverter |
| return | 0 | Clean |
| null | 41 | ExprInterpreter (26) + Column (10) + DatasetInterpreter (2) + Spark (2) + Schema (1) |
