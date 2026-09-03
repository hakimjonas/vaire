# Strongbow error policy — fail-fast, Collect, and quarantine

Design document for per-row error handling policy in the in-memory interpreter.
Companion to the parity plan (`strongbow-spark-4.2-parity-plan.md`) and the handover
(`strongbow-parity-handover-2.md`); grew out of the error-model discussion on the
`xpath*` work (PR #8), where the strict/try split raised the general question:
**who decides what a single bad row costs?**

> **Decisions recorded (2026-09-01, operator):** result shape = separate
> `evalColumnCollect` entry point; quarantine = separate method (E3); `maxErrors`
> default = 100; input previews = `Off` (Collect) / `Truncated(120)` (quarantine).
> Status: E1 shipped (`ErrorPolicy`, `evalColumnCollect`, `RowErrors`,
> `ErrorPolicySpec`); E2 shipped (`Dataset.WithPolicy` via `ds.withErrorPolicy`,
> `executeCollect` on `Interpreter`/`DatasetInterpreter`/`DatasetActions`,
> `CollectedDataset`, dataset-level specs — the plain `execute` path and Spark
> reject policy-scoped plans, nested scopes are rejected); E3 shipped
> (`evalColumnWithErrors`, `InputPreview`/`QuarantinedRow`/`Quarantined`, the
> `RowErrors.quarantining` recorder, composition specs — the dense error column is
> complete by contract; dataset-level quarantine is deferred until a shape for
> per-column error columns in multi-expression scopes is decided).

---

## 1. The problem

Strongbow's evaluation is fail-fast with first-error-only: `evalColumn` returns
`Either[ExecutionError, Column[?]]`, and a per-row failure in any arm aborts the whole
column with the first error encountered. `ExecutionError` has no shape for "several rows
failed" (`DivisionByZero(row)` and `IndexOutOfBounds(index, size)` carry row positions,
but there is no way to return more than one).

Both halves of that behavior are wrong for someone:

- **Fail-fast wastes diagnostics**: one malformed XML row among a million fails the
  pipeline and the operator learns about one error — row 4123 — with no idea whether
  rows 5000 and 900000 are also broken. Fixing one row and re-running a 30-minute job
  to find the next error is the failure mode users of batch systems know best.
- **Always-on accumulation wastes compute** in the opposite direction: a pipeline
  feeding an expensive stage should stop at the first bad row rather than spend an
  hour collecting 10M errors that will all be fixed by the same one-line change.

Both are legitimate pipeline-design preferences, in the same sense that Spark's own
parsers offer three modes: `FAILFAST` (fail), `DROPMALFORMED` (silently skip), and
`PERMISSIVE` + `_corrupt_record` (proceed and carry the reason). Strongbow currently
offers only the first. The design question: what does the other half look like without
breaking the default contract?

---

## 2. Scenarios (what the policies are for)

1. **Fail-fast protecting expensive compute** — a 20-stage pipeline with a costly final
   write: stop at the first bad row in stage 1 rather than paying for downstream stages
   on data that is already known-broken.
2. **Diagnosing complex failure states** — a run fails somewhere unexpected; the
   operator wants *all* offending rows in one pass to see the full shape of the breakage
   (is it one row or a systematic corruption? which columns?) before touching code.
3. **Dead-letter routing** — keep the pipeline running; good rows go downstream, bad
   rows go to a quarantine table with the reason. The Spark permissive-mode
   `_corrupt_record` pattern, applied to arbitrary expressions instead of just source
   parsing.
4. **Data-quality gate reporting** — error counts by kind as a first-class result
   (the Deequ/dbt-test shape): "column X produced 412 unparseable values since 03:00" as
   a metric, not an exception.
5. **Backfill / replay work-lists** — the accumulated error list from run N becomes the
   work-list for run N+1: reprocess only failed rows after the fix. Requires row
   identity in the payload.
6. **Debugging cascades** — in exploratory use the first error is usually the root cause
   and later errors are noise; fail-fast is the right default there, which is why it
   stays the default.

Scenarios 1 and 6 favor fail-fast; 2, 4, and 5 favor accumulation; 3 favors a
quarantine view. No single policy serves all six, which is why this is a policy knob
and not a behavior change.

---

## 3. The three policies

### 3.1 `FailFast` (default — today's behavior, the Spark contract)

Unchanged: `Left(ExecutionError)` on the first per-row failure. On the Spark backend,
strict semantics are Spark's own (its query fails); nothing changes there. Every
convention established so far — the strict `xpath*` family, `Div`, invalid-XML
handling — stays exactly as shipped.

### 3.2 `Collect(maxErrors)` — bounded accumulation

Evaluate all rows; per-row failures are recorded instead of aborting. The result
carries the **value column** (failed rows null-marked, like the try-family) plus a
bounded list of per-row errors:

```scala
final case class Collected(
  values: Column[?],
  errors: Vector[(Int, ExecutionError)], // row index -> cause, bounded
  truncated: Boolean,                    // true when maxErrors was hit
  byKind: Map[ExecutionError.Kind, Int]  // summary: kind -> count
)
```

The **by-kind summary** exists because the diagnostic gold in a 10M-row run with 3
distinct error kinds is the 3-line summary, not 10M entries — the raw list is bounded
(`maxErrors`, defaulting to a small constant) and `truncated` says whether the summary
is the complete picture.

This mirrors the Arda stack's own three-state result: Rumil/Sarati's
`Result[E, A] = Success | Partial(value, errors, consumed) | Failure(errors, furthest)`
accumulates parse errors into `Partial` while keeping the value. `Collected` is the
evaluation-layer `Partial`. The symmetry is deliberate: parse-side and eval-side error
handling should tell the same story.

### 3.3 `Quarantine` — the dead-letter view

A dense per-row **error column** (`Option[ExecutionError]` boxed, null where the row
succeeded) alongside the value column. It composes with everything Strongbow does —
filters, joins, selects — because it is just a column:

```scala
val (values, errs) = ... // error column: null = row succeeded
values.filter(errs.isNull)       // good rows -> downstream
values.filter(errs.isNotNull)    // bad rows  -> dead-letter table, reason included
```

Why a dedicated view instead of deriving it from `Collected`'s sparse list: the sparse
list is bounded and may be truncated, while quarantine's contract is *exactly one error
entry per failed row* — the routing use-case cannot tolerate "we stopped collecting at
1000". The two shapes have different contracts, so both exist; whether `Quarantine` is
a policy on the same knob or a separate method is an open decision (§7).

Note what quarantine is **not**: it is not the try-family. `tryXpath*` collapses every
failure reason into one null — a null result can mean "input was null" (legitimate),
"XML was malformed" (a row you need to see), or "path matched nothing" (legitimate).
Quarantine preserves the *reason* as data. Both exist because they answer different
questions.

---

## 4. The payload: two channels, log-safe by construction

Any error design that ships input data inside error messages creates a leak channel:
exception messages are passively harvested (log aggregation, job drivers, alerting),
and "error message contains sensitive information" is CWE-209. This is not hypothetical
for the functions that motivated the design — Spark's own `UDFXPathUtil` embeds the
entire XML document in its RuntimeException message, so the strict contract itself
inherits the leak. Strongbow can be better than the contract here without changing
which rows fail.

Two channels, never mixed:

- **Message channel (log-safe by construction)**: `ExecutionError` messages carry row
  index + reason + error kind, never input data. Already the convention
  (`DivisionByZero(row)`); the `xpath*` arms follow it. This is what `toString`
  produces, so passive harvesting is safe by default.
- **Payload channel (opt-in)**: per-row entries in `Collected`/quarantine may carry an
  `InputPreview` — a separate field, not part of `toString`, with policy knobs:
  `Off | Truncated(n) | Full` and an optional user-supplied `redact: String => String`
  (the team knows their data; the library does not guess). A developer gets input
  values only by explicitly reaching into the field — leakage becomes a routing
  decision rather than an infrastructure accident.

Default: `Truncated(120)` for quarantine (the reason usually lives in the visible
prefix — an unclosed tag, a bad number), `Off` for `Collected` summaries. Both
overridable.

---

## 5. Layer analysis: where the knob lives

### 5.1 `evalColumn` primitive

A defaulted parameter (implicit, like `LambdaScope`):

```scala
def evalColumn[Row, A](expr, columns, columnType)(
  using lambdaScope: LambdaScope = LambdaScope.empty,
        errorPolicy: ErrorPolicy = ErrorPolicy.FailFast
): Either[ExecutionError, Column[?]]  // FailFast
// or Either[ExecutionError, EvalOutcome]  — see open decision (a)
```

- Ordinary arms pass the policy through unchanged; per-row fallible arms
  (`xpathArm`, `try_variant_get`, division, XML parsing) consult it at the failure
  point: fail-fast aborts (today's code path), Collect records and continues, and the
  arm supplies the per-row error entry.
- Cost: every fallible arm gains a policy branch. Mechanical but wide — roughly the
  arms that currently construct `ExecutionError` from row-level conditions (division,
  casts, xpath, variant, XML parsing). The HOF arms' `cmpError`-style
  `AtomicReference` slots generalize into the collection mechanism.

### 5.2 Dataset-level surface

Pipelines live at the `Dataset` level, so the ergonomic API belongs there:
`ds.withErrorPolicy(...)` or an interpreter setting. The Dataset layer decides what
"continue" means for *whole operations* (a select with a failing expr under Collect
produces the null-marked column plus the error result; under FailFast it throws, as
today).

### 5.2.1 The structural/per-row boundary — normative

`Collect` only ever suppresses **per-row data errors**: errors whose occurrence depends
on the row's values (division by zero, malformed XML, unparseable timestamps, comparator
failures). **Structural errors** — wrong column type for an arm, unbound lambda
variables, untyped-column misuse — fail under *every* policy, including Collect: they
are usage errors that Spark rejects at analysis time, and letting Collect swallow them
would silently turn broken plans into null columns. Concretely: an arm's
type/coercion `Left`s and pre-condition checks stay `Left`; only error sites carrying a
row index (or created inside a per-row loop) consult the policy. This boundary is
enforced by construction in the implementation — structural failures never reach the
collector.

### 5.3 The Spark asymmetry — stated honestly

Policies are an **in-memory interpreter** feature. On Spark, per-row failures throw
inside Spark's own evaluation and no Column-level combinator can catch them — the
strict family's failure semantics are Spark's, the try-family reports
`UnsupportedOperation` on Spark, and Collect/Quarantine would be unrepresentable for
the same reason. This asymmetry strengthens the in-memory value proposition (full
policy support where Spark offers only fail-fast) but it must be documented plainly:
the knob does nothing on the Spark backend. Teams needing lenient Spark-side behavior
compose the try-family with filters (§3.3 style) or handle Spark's failures upstream.

---

## 6. Interaction with existing conventions

- **The try-family** stays as shipped: it is the per-expression, no-policy-object way
  to be lenient, and it maps to Spark where the policy cannot. Collect/Quarantine are
  the pipeline-wide story; try-cases are the single-expression story.
- **`Result.Partial` symmetry** (§3.2) argues for the evaluation result to eventually
  be Rumil/Sarati's `Result`-shaped rather than `Either` — but that is a breaking API
  change across every `evalColumn` caller; the doc recommends introducing `Collected`
  as a *separate return channel* first (an overloaded entry point) and treating the
  Either→Result migration as its own future phase.
- **`ExecutionError` stays an enum** with log-safe payloads; no input data ever enters
  it (§4). Accumulation shape (`ExecutionError.Accumulated(rows)`) vs a new `Collected`
  result type — open decision (a).

---

## 7. Open decisions

1. **Result shape**: `Either[ExecutionError, Column[?]]` with a new
   `ExecutionError.Accumulated(rows, truncated, byKind)` case (smallest change; the
   value column and the errors share the Either awkwardly) vs a three-state
   `EvalResult` return type mirroring `Result.Partial` (cleaner; wider blast radius).
   Lean: separate `evalColumnCollect` entry point returning `Collected`, leaving
   `evalColumn`'s signature untouched.
2. **Quarantine API**: a third policy on the same knob vs a separate
   `evalColumnWithErrors` returning the dense error column. Lean: separate method —
   its contract (complete, dense) differs from Collect's (bounded, sparse) and mixing
   them on one knob invites misconfiguration.
3. **`maxErrors` default**: small (100) to keep Collect memory-safe under error storms,
   with the by-kind summary always complete (counts are cheap even when raw entries are
   dropped early).
4. **Input previews**: default `Off` for Collect, `Truncated(120)` for quarantine — or
   uniformly `Off` with an explicit opt-in at the call site.

---

## 8. Phasing and effort

| Phase | Content | Deliverable | Estimate |
|---|---|---|---|
| E1 | `ErrorPolicy` + `evalColumnCollect` primitive; arm updates for division, casts, xpath, variant, XML parsing; core specs | in-memory Collect | 2–3 days |
| E2 | Dataset-level surface + `Collected` summary/by-kind; docs | ergonomic API | 1 day |
| E3 | Quarantine (dense error column) + composition tests | dead-letter view | 1–2 days |

Single PR per phase, each gated (`check` + both `testFull`s). No library releases —
everything lives in Strongbow core.

---

## 9. Non-goals

- Changing the default (FailFast) or any shipped strict semantics.
- Per-row recovery inside expressions (that is the try-family's job — try cases map
  failures to null rows by contract).
- Policy support on the Spark backend (§5.3).
- Accumulating errors across *operations* (a policy scopes to one evaluation; pipeline
  orchestration composes results itself).
