package net.ghoula.strongbow

/** Unified public prelude for Longbow.
  *
  * Usage: import net.ghoula.strongbow.prelude.*
  *
  * This prelude re-exports the complete public surface so users get a single canonical import with
  * no exposure of internal packages.
  *
  * @example
  *   {{{
  * import net.ghoula.strongbow.prelude.*
  *
  * // All types available
  * val plan: Dataset[Int] = Dataset.fromColumns(cols, schema) match {
  *   case Right(ds) => ds.filter(Expr.cell("value", ColumnIndex(0)) > Expr.const(18))
  *   case Left(errors) => ???
  * }
  *
  * // Expression DSL
  * val expr: Expr[Int, Boolean] =
  *   Expr.cell("age", ColumnIndex(0)) > Expr.const(18)
  *
  * // Grouped operations
  * val grouped: Grouped[String, Int] =
  *   dataset.groupBy(identity)
  *   }}}
  */
object prelude {
  // Core types and companion objects
  export net.ghoula.strongbow.{Dataset, Grouped, Expr, Column, ColumnType, Schema}

  // Materialized result
  export net.ghoula.strongbow.MaterializedDataset

  // Interpreters
  export net.ghoula.strongbow.{Interpreter, DatasetInterpreter, ExprInterpreter, GroupByInterpreter}

  // Opaque types
  export net.ghoula.strongbow.types.{ColumnIndex, RowIndex}

  // Error types
  export net.ghoula.strongbow.errors.{SchemaError, DecodeError, ExecutionError, NonEmptyList}

  // Action methods
  export net.ghoula.strongbow.DatasetActions.*

  // Debug/explain utilities
  export net.ghoula.strongbow.DatasetExplainer

  // Compile-time expression macro
  export net.ghoula.strongbow.ExprMacro
}
