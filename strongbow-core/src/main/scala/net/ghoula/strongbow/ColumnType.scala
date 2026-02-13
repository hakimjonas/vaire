package net.ghoula.strongbow

/** Column type enumeration for schema tracking.
  *
  * Represents the types that can be stored in columnar format.
  */
enum ColumnType derives CanEqual {
  case IntType
  case LongType
  case DoubleType
  case StringType
  case BooleanType
  case OptionType(inner: ColumnType)
  case AnyType
}
