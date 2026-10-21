# Backend capability claims and their test coverage

This document lists every expression or dataset-transformation rejection in `vaire-core/src/main`
and `vaire-spark/src/main` that tells the caller a backend cannot provide a construct, and names
the test that pins it. It is the coverage ledger; the XPath divergence itself stays in
[`xpath-divergence.md`](xpath-divergence.md).

## Boundary

A site is in the class when it returns a typed error to say that the executing backend does not
provide an **expression or dataset transformation**: an expression case, a generator, a window
form, a Spark-only function family, or a conversion the other backend cannot express.

The audit searched both modules for `UnsupportedOperation` and `UnsupportedExpression`, then
classified every hit. The following uses of the same error type are outside the class, with the
reason:

- **Execution-policy controls** (`DatasetInterpreter.scala:47`, `:53`, `:203`;
  `SparkInterpreter.scala:34`, `:270`). These reject a policy-scoped plan or an unsupported
  policy entry point, not an expression or transformation. They are covered by
  `ErrorPolicySpec` and `docs/error-policy-design.md`.
- **Data-source plumbing** (`DatasetInterpreter.scala:38`, `SparkInterpreter.scala:64`). These
  reject an internal `DataSource` implementation the interpreter does not know. The trait is
  open for backend modules, not a user-facing transformation.
- **Malformed expression validation** (`ExprInterpreter.scala:556`, `Coalesce` with no
  expressions). The expression is invalid on every backend.
- **Invalid argument values** (`ExprToColumn.scala:955`, unsupported `timestampdiff` unit). The
  argument is outside the API's accepted set on every backend.
- **Schema-shape preconditions** (`Column.scala:867`, `MaterializedDataset.scala:124`,
  `RowConverter.scala:320`, `StructType` without a nested schema). The caller must supply the
  nested schema on either backend.
- **Generic conversion wrappers** (`SparkInterpreter.scala:634`, `:646`,
  `UnsupportedExpression`) and **structural plan errors** (`InvalidPlan` at
  `DatasetInterpreter.scala:307`, `SparkInterpreter.scala:55`, `:291`, `:301`). These restate an
  inner failure or a violated plan precondition; they do not name a backend capability.

## Claims

Twenty sites, seventeen claims: three messages are emitted from both branches of
`evalAggregation` (empty and non-empty input).

| Claim | Site | Entry point | Test | Status |
|---|---|---|---|---|
| `Between not supported for untyped columns` | `ExprInterpreter.scala:622` | `evalColumn` | `BackendClaimsSpec` — “untyped column comparisons should be rejected” | pinned, message |
| `Comparison not supported for untyped columns` | `ExprInterpreter.scala:3566` | `evalColumn` | same test | pinned, message |
| `Explode requires Dataset-level handling` | `ExprInterpreter.scala:1121` | `evalColumn` | `ExprArrayMapSpec` — “Explode should return UnsupportedOperation” | pinned, message |
| `ArraySort on untyped columns not supported` | `ExprInterpreter.scala:1124` | `evalColumn` | `ExprArrayMapSpec` — “ArraySort should return unsupported…” | pinned, message |
| `VariantExplode requires Dataset-level handling` | `ExprInterpreter.scala:2011` | `evalColumn` | `VariantSpec` — “variant_explode should be unsupported in-memory” | pinned, message |
| `Generator functions require Dataset-level handling` | `ExprInterpreter.scala:3051` | `evalColumn` | `BackendClaimsSpec` — “generator expressions should require Dataset-level handling” | pinned, message |
| `Aggregations not supported in columnar evalColumn` | `ExprInterpreter.scala:3062` | `evalColumn` | `BackendClaimsSpec` — “aggregation expressions should be rejected…” | pinned, all 33 members |
| `Window functions not supported in columnar evalColumn` | `ExprInterpreter.scala:3067` | `evalColumn` | `BackendClaimsSpec` — “window expressions should be rejected…” | pinned, all 11 members |
| `Datasketches and current_path are Spark-only; use SparkInterpreter` | `ExprInterpreter.scala:3073` | `evalColumn` | `BackendClaimsSpec` — “datasketches and current_path should be rejected…” | pinned, all 10 members |
| `AES functions use Spark's key-derivation and cipher format; use SparkInterpreter` | `ExprInterpreter.scala:3079` | `evalColumn` | `BackendClaimsSpec` — “AES expressions should be rejected in memory” | pinned, all 3 members |
| `HistogramNumeric is Spark-only` | `ExprInterpreter.scala:3685`, `:4003` | `evalAggregation`, both branches | `BackendClaimsSpec` — “Spark-only aggregates…” plus `SparkOnlyAggSpec` | pinned, message |
| `Grouping requires a grouping-sets context (Spark-only)` | `ExprInterpreter.scala:3687`, `:4005` | `evalAggregation`, both branches | same | pinned, message |
| `GroupingId requires a grouping-sets context (Spark-only)` | `ExprInterpreter.scala:3689`, `:4007` | `evalAggregation`, both branches | same | pinned, message |
| `<fn> requires the in-memory interpreter` (eight `TryXpath*` variants) | `ExprToColumn.scala:42` | `ExprToColumn.convert` | `BackendClaimsSparkSpec` — “try-xpath variants should require the in-memory interpreter” | pinned, all 8 |
| `json_tuple key cannot be addressed by a Spark get_json_object path: <key>` | `ExprToColumn.scala:53` | `ExprToColumn.convert` | `JsonTupleParitySpec` — “fail Spark conversion for keys mixing…” | pinned, message |
| `schema_of_json requires a constant JSON string on Spark (foldable input)` | `ExprToColumn.scala:1059` | `ExprToColumn.convert` | `BackendClaimsSparkSpec` — “schema_of_json should require a constant input on Spark” | pinned, message |
| `Unsupported window expr: <expr>` | `SparkInterpreter.scala:786` | `Dataset.WithWindow` | `BackendClaimsSparkSpec` — “window expressions outside the Dataset window path…” | pinned, message |

No claim in this table is untestable by construction. Every row has a construction that reaches
the rejecting arm without asserting something else.

The `TryXpath*` row rejects all eight variants because a per-row XML parse failure throws inside
Spark's `xpath` implementation and no `Column` combinator can catch it. The keep-or-fallback
decision is recorded in [`xpath-divergence.md`](xpath-divergence.md#tryxpath-open-question).

## Drift risk

The class has twenty sites across two modules, and the previous increment found it by reading one
block in one file. A check that every rejection message in `src/main` appears in this table — in
the shape of the `doc-coverage.json` ratchet — would catch a new site at the gate. It is proposed,
not built: the check needs a stable message identity for interpolated messages, and that design is
its own increment.
