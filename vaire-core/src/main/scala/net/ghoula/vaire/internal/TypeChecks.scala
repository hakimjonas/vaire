package net.ghoula.vaire.internal

import net.ghoula.vaire.column.{Column, ColumnType}
import net.ghoula.vaire.errors.ExecutionError

/** Checks that a declared `ColumnType` matches the column an expression resolves to.
  *
  * `ExprInterpreter.evalColumn` treats its `columnType` argument as a hint: it is used for
  * constants and empty columns, but a `Cell` resolves to the stored column as-is. Callers that
  * carry a declared type (keyed joins, `groupByAgg` keys, `sortByExpr`, window keys) must therefore
  * verify it against the resolved column, or a caller can label a `Long` column as `Int` and the
  * declared type is a lie.
  *
  * `OptionType(inner)` is unwrapped before comparison: under the single-column null model an
  * optional field is the inner column with nulls, so the column's own type is the inner type.
  */
private[vaire] object TypeChecks {

  /** The column-level type behind a declared type: `OptionType(inner)` unwraps to `inner`. */
  def underlying(ct: ColumnType): ColumnType = ct match {
    case ColumnType.OptionType(inner) => underlying(inner)
    case other => other
  }

  /** Whether a schema type is optional (`OptionType`). */
  def isOptional(ct: ColumnType): Boolean = ct match {
    case ColumnType.OptionType(_) => true
    case _ => false
  }

  /** `Right(())` when the column's type matches the declared type, else the error to surface. */
  def resolvedColumnType(column: Column[?], declared: ColumnType, context: String): Either[ExecutionError, Unit] =
    resolvedType(column.columnType, declared, context)

  /** `Right(())` when the resolved type matches the declared type, else the error to surface. */
  def resolvedType(actual: ColumnType, declared: ColumnType, context: String): Either[ExecutionError, Unit] =
    if (underlying(actual) == underlying(declared)) Right(())
    else
      Left(
        ExecutionError.TypeMismatch(
          expected = declared.toString,
          actual = actual.toString,
          context = s"$context: resolved column type differs from the declared type"
        )
      )
}
