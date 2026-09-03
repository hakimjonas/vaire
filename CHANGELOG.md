# Changelog

All notable changes to Strongbow. Versions follow the automated release pipeline: every
PR merge to `main` cuts the next patch tag and publishes to the registry.

## Unreleased

- **Scaladoc coverage gate (pana-style ratchet)** — new `docTool` module checks that every
  public member of `src/main/scala` carries a non-empty `/** */` doc (defs, vals, types,
  classes/objects/enums, enum cases, extension methods; overrides, givens, exports, and
  private/protected members exempt). The `check` gate compares per-file counts against the
  checked-in `doc-coverage.json` and fails when undocumented counts grow; `sbt
  docCoverageSnapshot` regenerates the baseline after doc work. Baseline: 278/1138 (24.4%) —
  ratcheting to 100% happens family by family.
- **Error-policy E3 — quarantine (dead-letter view)** — `evalColumnWithErrors` evaluates under
  quarantine: per-row failures null-mark the value column and are recorded in a dense error
  column (an `AnyColumn` boxing `QuarantinedRow` — log-safe reason plus opt-in input preview — at
  every failed row, null where the row succeeded). Unlike the Collect list, the error column is
  complete (exactly one entry per failed row, no bound), so `values.filter(errs.isNull)` /
  `filter(errs.isNotNull)` routes good rows and dead-letter rows apart without dropping reasons.
  Input previews default to `Truncated(120)` and are knobbed via `InputPreview`
  (`Off | Truncated(n) | Full`) with a user-supplied redactor; the preview is the payload channel
  from design §4 and never enters the log-safe `ExecutionError` messages.
- **Error-policy E2 — dataset-level Collect surface** — `ds.withErrorPolicy(policy)`
  scopes a plan's expression evaluation under a per-row error policy; `ds.executeCollect`
  runs the plan and returns `CollectedDataset` (materialized values with failed rows
  null-marked, bounded per-row error list, `truncated` flag, complete by-kind summary).
  Policy-scoped plans are rejected on the plain `execute`/`collect` path, nested scopes
  are rejected, and the Spark backend rejects the feature outright (per-row error policies
  are unrepresentable there; see `docs/error-policy-design.md` §5.3). Structural errors
  and aggregation expressions keep fail-fast semantics under every policy.

## 0.0.7 (2026-09-01)

- **xpath\* family** — 16 new expression cases: `Xpath`, `XpathString`, `XpathBoolean`,
  `XpathShort/Int/Long/Float/Double` plus in-memory-only `TryXpath*` variants (per-row
  failures yield null rows). Backed by rumil's XPath 1.0 grammar and sarati's XPath
  evaluator; Spark maps to the native `functions.xpath*` family. Documented divergences:
  DTD documents, prefixed name tests, CDATA in `text()` (see `XpathDivergenceSpec`).
- Dependencies: sarati 0.3.9 (XPath evaluator + `xpathXmlConfig`), rumil-parsers 0.3.11
  (XPath grammar, XML end-tag fix, TOML tables).

## 0.0.6 (2026-08-30)

- **Higher-order array lambdas** — `transform`, `filter`, `exists`, `forall`,
  `aggregate`/`reduce`, `zip_with`, `map_filter`, `map_zip_with`, `transform_keys`,
  `transform_values`, `array_sort` comparator: dual backend, type-safe lambda variables
  via phantom-typed binders, body evaluation over the flat element dimension in-memory
  and native `functions.*` lambdas on Spark.

## 0.0.5 (2026-08-28)

- **`json_tuple`** — multi-key JSON object extraction returning an array of values
  (element-level nulls for absent keys); representation chosen by measurement
  (bench-verified against `get_json_object`-based alternatives on both backends).

## 0.0.4 (2026-08-25)

- **Spark 4.2 parity, Phases 1–3**: typed nested columns (`ArrayColumn`, `MapColumn`,
  `StructColumn`), `TimeType` and TIME functions, variant functions, Datasketches
  (Spark-only), JSON codec functions, crypto family, and the aggregate/string/math/
  date function backfill (~100 expression cases).

## 0.0.3 (2026-08-29)

- Release publishing pinned to the tag (prevents double-publish from dynver ambiguity).

## 0.0.2 (2026-08-29)

- Forgejo CI/release workflows and the three-tier test strategy (fast gate, slow stress,
  local benchmarks); squash-merge auto-tagging.

## 0.0.1 (2026-08-29)

- Initial release: `Dataset[T]`/`Expr[Row, A]`/`Column[+A]` GADTs, in-memory columnar
  interpreter, Spark backend via Catalyst translation, compile-time `Schema` derivation.
