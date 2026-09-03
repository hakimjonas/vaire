package net.ghoula.strongbow.column

/** Column type enumeration for schema tracking.
  *
  * Represents the types that can be stored in columnar format.
  */
enum ColumnType derives CanEqual {

  /** The Int logical type. */
  case IntType

  /** The Long logical type. */
  case LongType

  /** The Double logical type. */
  case DoubleType

  /** The Float logical type. */
  case FloatType

  /** The Short logical type. */
  case ShortType

  /** The Byte logical type. */
  case ByteType

  /** The String logical type. */
  case StringType

  /** The Boolean logical type. */
  case BooleanType

  /** The Date logical type. */
  case DateType

  /** The Time logical type. */
  case TimeType

  /** The Timestamp logical type. */
  case TimestampType

  /** The TimestampNTZ logical type. */
  case TimestampNTZType

  /** The YearMonthInterval logical type. */
  case YearMonthIntervalType

  /** The DayTimeInterval logical type. */
  case DayTimeIntervalType

  /** The Binary logical type. */
  case BinaryType

  /** Fixed-point decimal with the given precision and scale. */
  case DecimalType(precision: Int, scale: Int)

  /** Fixed-length character type. */
  case CharType(length: Int)

  /** Variable-length character type with a maximum length. */
  case VarcharType(maxLength: Int)

  /** Struct with named, typed fields. */
  case StructType(fields: Vector[(String, ColumnType)])

  /** Optionality wrapper over an inner type. */
  case OptionType(inner: ColumnType)

  /** Array of an element type. */
  case ArrayType(elementType: ColumnType)

  /** Map with key and value types. */
  case MapType(keyType: ColumnType, valueType: ColumnType)

  /** Semi-structured variant value. */
  case VariantType

  /** Type-erased storage for values without a more specific type. */
  case AnyType
}
