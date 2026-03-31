package net.ghoula.strongbow.spark

import org.apache.spark.sql.types.{
  ArrayType => SparkArrayType,
  BooleanType => SparkBooleanType,
  ByteType => SparkByteType,
  DataType => SparkDataType,
  DateType => SparkDateType,
  BinaryType => SparkBinaryType,
  CharType => SparkCharType,
  VarcharType => SparkVarcharType,
  DayTimeIntervalType => SparkDayTimeIntervalType,
  DecimalType => SparkDecimalType,
  DoubleType => SparkDoubleType,
  FloatType => SparkFloatType,
  IntegerType => SparkIntegerType,
  LongType => SparkLongType,
  MapType => SparkMapType,
  ShortType => SparkShortType,
  StringType => SparkStringType,
  StructField,
  StructType,
  TimestampNTZType => SparkTimestampNTZType,
  TimestampType => SparkTimestampType,
  YearMonthIntervalType => SparkYearMonthIntervalType
}

import net.ghoula.strongbow.Schema
import net.ghoula.strongbow.column.ColumnType

/** Bidirectional conversion between Strongbow Schema/ColumnType and Spark StructType/DataType. */
object SchemaConverter {

  /** Convert a Strongbow Schema[T] to a Spark StructType. */
  def toStructType[T](schema: Schema[T]): StructType = {
    val fields = schema.columnNames.zip(schema.columnTypes).map { case (name, ct) =>
      ct match {
        case ColumnType.OptionType(inner) =>
          StructField(name, toSparkType(inner), nullable = true)
        case _ =>
          StructField(name, toSparkType(ct), nullable = false)
      }
    }
    StructType(fields.toArray)
  }

  /** Convert a Strongbow ColumnType to a Spark DataType. */
  def toSparkType(ct: ColumnType): SparkDataType = ct match {
    case ColumnType.IntType => SparkIntegerType
    case ColumnType.LongType => SparkLongType
    case ColumnType.DoubleType => SparkDoubleType
    case ColumnType.ShortType => SparkShortType
    case ColumnType.ByteType => SparkByteType
    case ColumnType.FloatType => SparkFloatType
    case ColumnType.StringType => SparkStringType
    case ColumnType.BooleanType => SparkBooleanType
    case ColumnType.DateType => SparkDateType
    case ColumnType.TimestampType => SparkTimestampType
    case ColumnType.TimestampNTZType => SparkTimestampNTZType
    case ColumnType.YearMonthIntervalType => SparkYearMonthIntervalType()
    case ColumnType.DayTimeIntervalType => SparkDayTimeIntervalType()
    case ColumnType.BinaryType => SparkBinaryType
    case ColumnType.DecimalType(p, s) => SparkDecimalType(p, s)
    case ColumnType.CharType(n) => SparkCharType(n)
    case ColumnType.VarcharType(n) => SparkVarcharType(n)
    case ColumnType.OptionType(inner) => toSparkType(inner)
    case ColumnType.ArrayType(elem) => SparkArrayType(toSparkType(elem), containsNull = true)
    case ColumnType.MapType(key, value) => SparkMapType(toSparkType(key), toSparkType(value), valueContainsNull = true)
    case ColumnType.AnyType => SparkStringType
  }

  /** Convert a Spark DataType to a Strongbow ColumnType. */
  def fromSparkType(dt: SparkDataType): ColumnType = dt match {
    case SparkIntegerType => ColumnType.IntType
    case SparkLongType => ColumnType.LongType
    case SparkDoubleType => ColumnType.DoubleType
    case SparkShortType => ColumnType.ShortType
    case SparkByteType => ColumnType.ByteType
    case SparkFloatType => ColumnType.FloatType
    case SparkStringType => ColumnType.StringType
    case SparkBooleanType => ColumnType.BooleanType
    case SparkDateType => ColumnType.DateType
    case SparkTimestampType => ColumnType.TimestampType
    case _: SparkTimestampNTZType => ColumnType.TimestampNTZType
    case _: SparkYearMonthIntervalType => ColumnType.YearMonthIntervalType
    case _: SparkDayTimeIntervalType => ColumnType.DayTimeIntervalType
    case SparkBinaryType => ColumnType.BinaryType
    case dt: SparkDecimalType => ColumnType.DecimalType(dt.precision, dt.scale)
    case ct: SparkCharType => ColumnType.CharType(ct.length)
    case vt: SparkVarcharType => ColumnType.VarcharType(vt.length)
    case at: SparkArrayType => ColumnType.ArrayType(fromSparkType(at.elementType))
    case mt: SparkMapType => ColumnType.MapType(fromSparkType(mt.keyType), fromSparkType(mt.valueType))
    case _ => ColumnType.AnyType
  }
}
