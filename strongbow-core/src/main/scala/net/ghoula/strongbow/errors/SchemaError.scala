package net.ghoula.strongbow.errors

import net.ghoula.strongbow.ColumnType

/** Errors that occur during schema validation.
  *
  * Uses enum for modern Scala 3 style error modeling.
  */
enum SchemaError {
  case ColumnCountMismatch(expected: Int, actual: Int)
  case ColumnTypeMismatch(columnIndex: Int, expected: ColumnType, actual: ColumnType)
  case ColumnLengthMismatch(columnIndex: Int, expected: Int, actual: Int)
  case InvalidColumnName(name: String)
  case MissingColumn(name: String)
}
