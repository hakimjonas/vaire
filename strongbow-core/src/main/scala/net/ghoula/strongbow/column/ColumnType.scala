package net.ghoula.strongbow.column

/** Column type enumeration for schema tracking.
  *
  * Represents the types that can be stored in columnar format.
  */
enum ColumnType derives CanEqual {
  case IntType
  case LongType
  case DoubleType
  case FloatType
  case ShortType
  case ByteType
  case StringType
  case BooleanType
  case DateType
  case TimestampType
  case TimestampNTZType
  case YearMonthIntervalType
  case DayTimeIntervalType
  case BinaryType
  case CharType(length: Int)
  case VarcharType(maxLength: Int)
  case OptionType(inner: ColumnType)
  case ArrayType(elementType: ColumnType)
  case MapType(keyType: ColumnType, valueType: ColumnType)
  case AnyType
}
