# Changelog

All notable changes to Strongbow. Versions follow the automated release pipeline: every
PR merge to `main` cuts the next patch tag and publishes to the registry.

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
