@MANIFESTO.md

# Code Standards

These standards are non-negotiable. Every line of code must conform.
Violations are not fixed later — they are prevented.

## Type Safety

- **Zero asInstanceOf** in expression evaluation and dataset interpretation.
  The only acceptable boundary is AnyColumn storage access where JVM type
  erasure makes pattern matching impossible. Every other cast indicates a
  design error.
- **GADT refinement over casting.** Pattern match sealed enum cases to
  refine types. If the compiler can't prove a type, restructure the code
  rather than casting.
- **Invariant GADTs.** Covariant type parameters prevent GADT equality
  refinement (scala/scala3#11956). Use invariant type parameters on
  Dataset and other GADTs where pattern matching needs type equality.
- **Column[+A] GADT.** Pattern match Column variants for typed array
  access. Never use getValue + asInstanceOf — use typed accessors
  (getInt, getLong, etc.) or pattern match the Column case directly.

## FP Style

- **No var.** Use Array.tabulate for element-wise column construction.
  Use foldLeft for accumulations. The only acceptable var is in a
  performance-critical loop where profiling shows measurable difference
  AND the var is encapsulated (not visible in any API).
- **No throw.** Every error path returns Left(ExecutionError). The
  codebase uses Either[ExecutionError, A] consistently — throwing
  bypasses this.
- **No return.** Restructure control flow. Early termination uses
  foldLeft with short-circuit, or Iterator with takeWhile/find.
- **No null in value paths.** Nulls exist only as SQL NULL metadata
  in Column BitSet. Array placeholders at null positions are never
  read. If a function needs to represent absence, use Option.
- **No @unchecked on pattern matches** unless the sealed enum's
  exhaustiveness genuinely cannot be verified by the compiler.

## Code Cleanliness

- **No inline comments.** Code should be self-documenting through
  types and names. The only acceptable annotations are scalafix
  suppression comments (scalafix:ok), which should be rare.
- **No section separator comments** (no `// Phase X`, no `// =====`).
- **Scaladocs are user-facing.** Describe what a function does for
  the caller, not how it's implemented.

## Compiler Settings

Scala 3.8.2 with -Werror, -Wunused:all, -language:strictEquality.
Code must compile with zero warnings. Do not add scalafix suppression
annotations to make warnings go away — fix the underlying issue.

## Testing

- **Test assertions must never be weakened** to accommodate
  implementation changes. If the implementation changes the behavior
  (e.g., error vs null), that's a design decision that must be
  discussed, not silently adjusted in tests.
- **Tests use evalColumn for typed results.** evalAt is a convenience
  that returns Any — prefer evalColumn with typed Column extraction.

## Architecture

- **Columnar evaluation.** evalColumn is the primary evaluation path.
  All expression evaluation produces typed Columns, not per-row values.
- **Column[+A] GADT.** The storage layer carries type information.
  IntColumn extends Column[Int], etc. Pattern matching gives typed
  array access.
- **Expr GADT.** 167 expression cases, each mapping to a native
  Spark SQL function via ExprToColumn. No UDFs.
- **Dataset[T] (invariant).** Immutable transformation AST.
  Interpreted by DatasetInterpreter (columnar) or SparkInterpreter.

## Build

- `sbt prepare` before committing (scalafmt + scalafix + compile)
- `sbt core/test` must pass
- `sbt spark/compile` must pass
- Dependencies: Rumil (parser), Sarati (codec) from local Forgejo
