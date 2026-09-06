# Changelog

All notable changes to Vairë. Versions follow the automated release pipeline: every
PR merge to `main` cuts the next alpha tag and publishes to Maven Central; the first
release tag (`v1.0.0-alpha`) is cut manually.

## Unreleased

- **License** — moved from GPL-3.0-or-later to LGPL-3.0-or-later across the LICENSE file,
  build metadata, and documentation, matching the rest of the Arda stack. LGPL keeps the
  copyleft protection of the source while permitting applications to link and use the
  library without license obligations on application code.
- **GitHub release pipeline** — publishing moves from the Forgejo/Codeberg Maven registry
  to Maven Central under `net.ghoula` (Central Portal staging, sbt-pgp signing), with the
  same SHA-pinned CI / auto-tag / release workflows the other Arda libraries run; the
  Forgejo workflow remnants (`.forgejo/`) are removed. `rumil` moves to 1.0.0-alpha.5.

## 0.0.20

- **Renamed from strongbow to Vairë** — the crossbow lineage (Arrow → Crossbow → strongbow)
  named the inspiration, not the library. Vairë the Weaver weaves all things that have been
  in Time into her storied webs: threads are columns, the weaving is execution, the web is
  the record. Packages (`net.ghoula.vaire`), artifactIds (`vaire-core_3`, `vaire-spark_3`),
  directories, and the repository move with the name; releases 0.0.1–0.0.19 were published
  under the old coordinates and remain on the registry as history. Repository home moves to
  `github.com/hakimjonas/vaire`, matching the ecosystem convention.

## 0.0.19 (2026-09-03)

- **License** — the POM license declaration was corrected from MIT (which the build metadata
  had claimed since 0.0.1) to GPL-3.0-or-later, matching the rest of the Arda stack
  (eru, sarati, rumil); the repository now carries the full license text. Previously
  published 0.0.x artifacts carry the stale MIT claim in their POMs.

## 0.0.18 (2026-09-03)

- **Scaladoc backfill to 100%** — every public member of core and spark carries a scaladoc
  (1133/1133): all 349 `Expr` enum cases and DSL methods anchored to their Spark SQL
  functions, plus the Dataset, Column, ColumnType, errors, params, types, interpreter and
  spark modules. At the 100% baseline the coverage ratchet acts as a hard gate: any new
  undocumented member fails `check`.

## 0.0.17 (2026-09-03)

- **Scaladoc coverage gate (pana-style ratchet)** — new `docTool` module checks that every
  public member of `src/main/scala` carries a non-empty `/** */` doc (defs, vals, types,
  classes/objects/enums, enum cases, extension methods; overrides, givens, exports, and
  private/protected members exempt). The `check` gate compares per-file counts against the
  checked-in `doc-coverage.json` and fails when undocumented counts grow; `sbt
  docCoverageSnapshot` regenerates the baseline after doc work.

## 0.0.16 (2026-09-03)

- **Error-policy E3 — quarantine (dead-letter view)** — `evalColumnWithErrors` evaluates under
  quarantine: per-row failures null-mark the value column and are recorded in a dense error
  column (an `AnyColumn` boxing `QuarantinedRow` — log-safe reason plus opt-in input preview — at
  every failed row, null where the row succeeded). Unlike the Collect list, the error column is
  complete (exactly one entry per failed row, no bound), so `values.filter(errs.isNull)` /
  `filter(errs.isNotNull)` routes good rows and dead-letter rows apart without dropping reasons.
  Input previews default to `Truncated(120)` and are knobbed via `InputPreview`
  (`Off | Truncated(n) | Full`) with a user-supplied redactor; the preview is the payload channel
  from design §4 and never enters the log-safe `ExecutionError` messages.

## 0.0.15 (2026-09-02)

- **Error-policy E2 — dataset-level Collect surface** — `ds.withErrorPolicy(policy)`
  scopes a plan's expression evaluation under a per-row error policy; `ds.executeCollect`
  runs the plan and returns `CollectedDataset` (materialized values with failed rows
  null-marked, bounded per-row error list, `truncated` flag, complete by-kind summary).
  Policy-scoped plans are rejected on the plain `execute`/`collect` path, nested scopes
  are rejected, and the Spark backend rejects the feature outright (per-row error policies
  are unrepresentable there; see `docs/error-policy-design.md` §5.3). Structural errors
  and aggregation expressions keep fail-fast semantics under every policy.

## 0.0.14 (2026-09-02)

- **DTD divergence resolved** — rumil-parsers 1.0.0-alpha.3 parses `<!DOCTYPE>` declarations
  with internal-subset entity expansion (consulting sarati's `resolveDtd` config); both
  backends now expand internal entities identically. `XpathDivergenceSpec` pins only the
  prefixed-name divergence (deliberate, permanent — vaire is more conformant per
  XPath 1.0). Dependencies: sarati 1.0.0-alpha.2 (`resolveDtd` config), rumil-parsers
  1.0.0-alpha.3 (DTD parser).

## 0.0.13 (2026-09-02)

- **Error-policy design document** — `docs/error-policy-design.md`: the three policies
  (FailFast default, bounded Collect, quarantine), the log-safe/payload channel split,
  the structural/per-row boundary (§5.2.1), the Spark asymmetry (§5.3), and the recorded
  operator decisions.

## 0.0.12 (2026-09-02)

- **Error-policy E1 — Collect primitive** — `ErrorPolicy` (`FailFast` / `Collect(maxErrors)`,
  default 100), `evalColumnCollect` returning `Collected` (value column with failed rows
  null-marked, bounded per-row error list, `truncated` flag, complete by-kind summary), the
  implicitly-threaded `RowErrors` collector, and per-row error-site updates (division, xpath
  parse). Structural errors fail under every policy by construction.

## 0.0.11 (2026-09-01)

- Doc-audit follow-up: ecosystem section states the actual dependencies, `.jvmopts.example`
  wording.

## 0.0.10 (2026-09-01)

- Dependencies resolve from Maven Central: `net.ghoula:sarati_3` and
  `net.ghoula:rumil-parsers_3` at 1.0.0-alpha.

## 0.0.9 (2026-09-01)

- **Number rendering matches Spark** — Jackson node-type semantics via preserved raw tokens
  (sarati 0.3.12 + rumil 0.3.12); the four number-formatting divergence rows flipped to
  parity in `JsonTupleParitySpec`.

## 0.0.8 (2026-09-01)

- Documentation audit per `DOC-AUDIT-POLICY`: verified claims (349 expression cases, 20
  Column variants), installation section citing the registry, this CHANGELOG created,
  `SourcePolicySpec` pinning the stated purity guarantees.

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
