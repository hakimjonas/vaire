package net.ghoula.vaire.errors

import net.ghoula.vaire.column.ColumnType

/** Errors that occur during schema validation.
  *
  * Uses enum for modern Scala 3 style error modeling.
  */
enum SchemaError {

  /** The columns do not match the schema's column count. */
  case ColumnCountMismatch(expected: Int, actual: Int)

  /** A column's type does not match the schema's declared type. */
  case ColumnTypeMismatch(columnIndex: Int, expected: ColumnType, actual: ColumnType)

  /** A column's length does not match the other columns'. */
  case ColumnLengthMismatch(columnIndex: Int, expected: Int, actual: Int)

  /** A schema field name is not a valid column name. */
  case InvalidColumnName(name: String)

  /** A schema field has no corresponding column. */
  case MissingColumn(name: String)
}
