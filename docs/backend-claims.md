# Backend capability claims and their test coverage

Status: current
Audience: user for “Rejected constructs”; contributor for the Internal sections.

Vairë's two backends do not provide the same set of expressions and dataset transformations.
This document lists the constructs one backend rejects and the other evaluates. The coverage
ledger, the boundary and the exclusion rationale are internal sections below.

## Rejected constructs

| ID | Construct | In-memory | Spark |
|---|---|---|---|
| C1 | `Between` on an untyped column | rejects: `Between not supported for untyped columns` | evaluates |
| C2 | Comparison on an untyped column | rejects: `Comparison not supported for untyped columns` | evaluates |
| C3 | `Explode` in a column expression | requires the Dataset-level form | evaluates |
| C4 | `ArraySort` on an untyped column | rejects: `ArraySort on untyped columns not supported` | evaluates |
| C5 | `VariantExplode` in a column expression | requires the Dataset-level form | evaluates |
| C6 | `Posexplode`, `ExplodeOuter`, `Inline` | require the Dataset-level form | evaluate |
| C7 | Aggregations in columnar `evalColumn` | reject; aggregates run through `evalAggregation` | evaluate |
| C8 | Window functions in columnar `evalColumn` | reject; windows run through the window path | evaluate |
| C9 | Datasketches and `current_path` | rejects: `Datasketches and current_path are Spark-only` | evaluates |
| C10 | AES encrypt/decrypt | rejects: `AES functions use Spark's key-derivation and cipher format` | evaluates |
| C11 | `HistogramNumeric` | rejects: `HistogramNumeric is Spark-only` | evaluates |
| C12 | `Grouping` | rejects: requires a grouping-sets context | evaluates |
| C13 | `GroupingId` | rejects: requires a grouping-sets context | evaluates |
| C14 | Eight `TryXpath*` variants | evaluates | rejects: `<fn> requires the in-memory interpreter` |
| C15 | `json_tuple` key mixing a quote with a dot or bracket | evaluates | conversion rejects: `cannot be addressed by a Spark get_json_object path` |
| C16 | `schema_of_json` on a non-constant input | evaluates | rejects: requires a constant JSON string |
| C17 | Some window forms in a Dataset window (for example `NTile`) | evaluates | rejects: `Unsupported window expr` |

## Internal: coverage ledger

Twenty sites, seventeen claims. Three messages (C11–C13) are emitted from both branches of
`evalAggregation`, empty and non-empty input.

| ID | Site | Entry point | Test | Status |
|---|---|---|---|---|
| C1 | `ExprInterpreter.scala:622` | `evalColumn` | `BackendClaimsSpec` — untyped comparisons | pinned, message |
| C2 | `ExprInterpreter.scala:3566` | `evalColumn` | same | pinned, message |
| C3 | `ExprInterpreter.scala:1121` | `evalColumn` | `ExprArrayMapSpec` — Explode | pinned, message |
| C4 | `ExprInterpreter.scala:1124` | `evalColumn` | `ExprArrayMapSpec` — ArraySort | pinned, message |
| C5 | `ExprInterpreter.scala:2011` | `evalColumn` | `VariantSpec` — variant_explode | pinned, message |
| C6 | `ExprInterpreter.scala:3051` | `evalColumn` | `BackendClaimsSpec` — generators | pinned, all 3 members |
| C7 | `ExprInterpreter.scala:3062` | `evalColumn` | `BackendClaimsSpec` — aggregations | pinned, all 33 members |
| C8 | `ExprInterpreter.scala:3067` | `evalColumn` | `BackendClaimsSpec` — window expressions | pinned, all 11 members |
| C9 | `ExprInterpreter.scala:3073` | `evalColumn` | `BackendClaimsSpec` — datasketches | pinned, all 10 members |
| C10 | `ExprInterpreter.scala:3079` | `evalColumn` | `BackendClaimsSpec` — AES | pinned, all 3 members |
| C11 | `ExprInterpreter.scala:3685`, `:4003` | `evalAggregation`, both branches | `BackendClaimsSpec` — Spark-only aggregates; `SparkOnlyAggSpec` | pinned, message |
| C12 | `ExprInterpreter.scala:3687`, `:4005` | `evalAggregation`, both branches | same | pinned, message |
| C13 | `ExprInterpreter.scala:3689`, `:4007` | `evalAggregation`, both branches | same | pinned, message |
| C14 | `ExprToColumn.scala:42` | `ExprToColumn.convert` | `BackendClaimsSparkSpec` — try-xpath | pinned, all 8 variants |
| C15 | `ExprToColumn.scala:53` | `ExprToColumn.convert` | `JsonTupleParitySpec` — mixed key | pinned, message |
| C16 | `ExprToColumn.scala:1059` | `ExprToColumn.convert` | `BackendClaimsSparkSpec` — schema_of_json | pinned, message |
| C17 | `SparkInterpreter.scala:786` | `Dataset.WithWindow` | `BackendClaimsSparkSpec` — Dataset window | pinned, message |

No claim is untestable by construction. Every row has a construction that reaches the rejecting
arm without asserting something else.

C14 rejects all eight variants because a per-row XML parse failure throws inside Spark's `xpath`
implementation and no `Column` combinator can catch it. The keep-or-fallback decision is recorded
in [`xpath-divergence.md`](xpath-divergence.md#internal-tryxpath-open-question).

## Internal: boundary and exclusions

A site is in the class when it returns a typed error to say that the executing backend does not
provide an **expression or dataset transformation**: an expression case, a generator, a window
form, a Spark-only function family, or a conversion the other backend cannot express.

The audit searched both modules for `UnsupportedOperation` and `UnsupportedExpression`, then
classified every hit. These uses of the same error type are outside the class, with the reason:

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

## Internal: drift risk

The class has twenty sites across two modules, and an earlier increment found it by reading one
block in one file. A check that every rejection message in `src/main` appears in this table — in
the shape of the `doc-coverage.json` ratchet — would catch a new site at the gate. It is proposed,
not built: the check needs a stable message identity for interpolated messages, and that design is
its own increment.
