package net.ghoula.vaire.spark

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
  VariantType => SparkVariantType,
  DoubleType => SparkDoubleType,
  FloatType => SparkFloatType,
  IntegerType => SparkIntegerType,
  LongType => SparkLongType,
  MapType => SparkMapType,
  ShortType => SparkShortType,
  StringType => SparkStringType,
  StructField,
  StructType => SparkStructType,
  TimeType => SparkTimeType,
  TimestampNTZType => SparkTimestampNTZType,
  TimestampType => SparkTimestampType,
  YearMonthIntervalType => SparkYearMonthIntervalType
}

import net.ghoula.vaire.Schema
import net.ghoula.vaire.column.ColumnType

/** Bidirectional conversion between Vairë Schema/ColumnType and Spark StructType/DataType. */
object SchemaConverter {

  /** Convert a Vairë Schema[T] to a Spark StructType.
    *
    * An optional field (`OptionType(inner)`) becomes `inner` with `nullable = true`; a non-optional
    * field becomes `nullable = false`. Nullability is part of the Vairë schema, and `fromColumns`
    * guarantees a non-optional field holds no nulls, so the flag round-trips through Spark.
    */
  def toStructType[T](schema: Schema[T]): SparkStructType =
    SparkStructType(schema.columnNames.zip(schema.columnTypes).map { case (name, ct) => sparkField(name, ct) }.toArray)

  /** A Spark field for a named Vairë type: `OptionType(inner)` is nullable, anything else is not.
    */
  private def sparkField(name: String, ct: ColumnType): StructField = ct match {
    case ColumnType.OptionType(inner) => StructField(name, toSparkType(inner), nullable = true)
    case _ => StructField(name, toSparkType(ct), nullable = false)
  }

  /** Validate a Vairë schema against a Spark `StructType`.
    *
    * Field types must match, and nullability must agree: a non-optional Vairë field requires a
    * non-nullable Spark field, and a nullable Spark field requires an `Option` Vairë field. Structs
    * recurse. Returns the first mismatch, or `None` when the schema is consistent.
    */
  def validateSchema(schema: Schema[?], structType: SparkStructType): Option[String] =
    validateFields(schema.columnNames.zip(schema.columnTypes), structType.fields.toVector)

  private def validateFields(
    vaire: Vector[(String, ColumnType)],
    spark: Vector[StructField]
  ): Option[String] =
    if (vaire.length != spark.length)
      Some(s"field count differs: Vairë has ${vaire.length}, Spark has ${spark.length}")
    else
      vaire.zip(spark).collectFirst {
        case ((name, vt), sf) if validateField(vt, sf).isDefined =>
          s"$name: ${validateField(vt, sf).get}"
      }

  private def validateField(vt: ColumnType, sf: StructField): Option[String] = {
    val (inner, optional) = vt match {
      case ColumnType.OptionType(in) => (in, true)
      case other => (other, false)
    }
    if (!optional && sf.nullable)
      Some(
        "non-optional Vairë field maps to a nullable Spark column; declare it Option or make the Spark column non-nullable"
      )
    else validateType(inner, sf.dataType)
  }

  private def validateType(vt: ColumnType, dt: SparkDataType): Option[String] = vt match {
    case ColumnType.OptionType(inner) => validateType(inner, dt)
    case ColumnType.StructType(fields) =>
      dt match {
        case st: SparkStructType => validateFields(fields, st.fields.toVector)
        case other => Some(s"expected a struct, found ${other.catalogString}")
      }
    case ColumnType.ArrayType(elem) =>
      dt match {
        case at: SparkArrayType => validateType(elem, at.elementType)
        case other => Some(s"expected an array, found ${other.catalogString}")
      }
    case ColumnType.MapType(key, value) =>
      dt match {
        case mt: SparkMapType => validateType(key, mt.keyType).orElse(validateType(value, mt.valueType))
        case other => Some(s"expected a map, found ${other.catalogString}")
      }
    case _ =>
      val expected = toSparkType(vt)
      if (expected == dt) None
      else Some(s"expected ${expected.catalogString}, found ${dt.catalogString}")
  }

  /** Convert a Vairë ColumnType to a Spark DataType. */
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
    case ColumnType.TimeType => SparkTimeType()
    case ColumnType.TimestampType => SparkTimestampType
    case ColumnType.TimestampNTZType => SparkTimestampNTZType
    case ColumnType.YearMonthIntervalType => SparkYearMonthIntervalType()
    case ColumnType.DayTimeIntervalType => SparkDayTimeIntervalType()
    case ColumnType.BinaryType => SparkBinaryType
    case ColumnType.DecimalType(p, s) => SparkDecimalType(p, s)
    case ColumnType.CharType(n) => SparkCharType(n)
    case ColumnType.VarcharType(n) => SparkVarcharType(n)
    case ColumnType.StructType(fields) =>
      SparkStructType(fields.map { case (name, ft) => sparkField(name, ft) }.toArray)
    case ColumnType.VariantType => SparkVariantType
    case ColumnType.OptionType(inner) => toSparkType(inner)
    case ColumnType.ArrayType(elem) => SparkArrayType(toSparkType(elem), containsNull = true)
    case ColumnType.MapType(key, value) => SparkMapType(toSparkType(key), toSparkType(value), valueContainsNull = true)
    case ColumnType.AnyType => SparkStringType
  }

  /** Convert a Spark DataType to a Vairë ColumnType. */
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
    case _: SparkTimeType => ColumnType.TimeType
    case SparkTimestampType => ColumnType.TimestampType
    case _: SparkTimestampNTZType => ColumnType.TimestampNTZType
    case _: SparkYearMonthIntervalType => ColumnType.YearMonthIntervalType
    case _: SparkDayTimeIntervalType => ColumnType.DayTimeIntervalType
    case SparkBinaryType => ColumnType.BinaryType
    case dt: SparkDecimalType => ColumnType.DecimalType(dt.precision, dt.scale)
    case ct: SparkCharType => ColumnType.CharType(ct.length)
    case vt: SparkVarcharType => ColumnType.VarcharType(vt.length)
    case SparkVariantType => ColumnType.VariantType
    case st: SparkStructType =>
      ColumnType.StructType(st.fields.map(f => (f.name, fromSparkField(f))).toVector)
    case at: SparkArrayType => ColumnType.ArrayType(fromSparkType(at.elementType))
    case mt: SparkMapType => ColumnType.MapType(fromSparkType(mt.keyType), fromSparkType(mt.valueType))
    case _ => ColumnType.AnyType
  }

  /** A Vairë type for a Spark struct field: a nullable field becomes `OptionType(inner)`. */
  private def fromSparkField(f: StructField): ColumnType =
    if (f.nullable) ColumnType.OptionType(fromSparkType(f.dataType))
    else fromSparkType(f.dataType)
}
