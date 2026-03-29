package net.ghoula.strongbow.column

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
  case DateType
  case OptionType(inner: ColumnType)
  case ArrayType(elementType: ColumnType)
  case MapType(keyType: ColumnType, valueType: ColumnType)
  case AnyType
}
