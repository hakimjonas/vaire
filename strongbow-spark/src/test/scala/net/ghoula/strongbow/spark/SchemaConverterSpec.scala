package net.ghoula.strongbow.spark

import org.apache.spark.sql.types.{
  BinaryType => SparkBinaryType,
  BooleanType => SparkBooleanType,
  ByteType => SparkByteType,
  DayTimeIntervalType => SparkDayTimeIntervalType,
  DecimalType => SparkDecimalType,
  DoubleType => SparkDoubleType,
  VariantType => SparkVariantType,
  FloatType => SparkFloatType,
  IntegerType => SparkIntegerType,
  LongType => SparkLongType,
  ShortType => SparkShortType,
  StringType => SparkStringType,
  TimestampNTZType => SparkTimestampNTZType,
  TimestampType => SparkTimestampType,
  YearMonthIntervalType => SparkYearMonthIntervalType
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.prelude.*

class SchemaConverterSpec extends AnyFlatSpec with Matchers {

  "toSparkType" should "convert IntType to IntegerType" in {
    SchemaConverter.toSparkType(ColumnType.IntType) shouldBe SparkIntegerType
  }

  it should "convert LongType to LongType" in {
    SchemaConverter.toSparkType(ColumnType.LongType) shouldBe SparkLongType
  }

  it should "convert DoubleType to DoubleType" in {
    SchemaConverter.toSparkType(ColumnType.DoubleType) shouldBe SparkDoubleType
  }

  it should "convert StringType to StringType" in {
    SchemaConverter.toSparkType(ColumnType.StringType) shouldBe SparkStringType
  }

  it should "convert BooleanType to BooleanType" in {
    SchemaConverter.toSparkType(ColumnType.BooleanType) shouldBe SparkBooleanType
  }

  it should "convert FloatType to FloatType" in {
    SchemaConverter.toSparkType(ColumnType.FloatType) shouldBe SparkFloatType
  }

  it should "convert ShortType to ShortType" in {
    SchemaConverter.toSparkType(ColumnType.ShortType) shouldBe SparkShortType
  }

  it should "convert ByteType to ByteType" in {
    SchemaConverter.toSparkType(ColumnType.ByteType) shouldBe SparkByteType
  }

  it should "convert TimestampType to TimestampType" in {
    SchemaConverter.toSparkType(ColumnType.TimestampType) shouldBe SparkTimestampType
  }

  it should "convert TimestampNTZType to TimestampNTZType" in {
    SchemaConverter.toSparkType(ColumnType.TimestampNTZType) shouldBe SparkTimestampNTZType
  }

  it should "convert YearMonthIntervalType" in {
    val sparkType = SchemaConverter.toSparkType(ColumnType.YearMonthIntervalType)
    sparkType shouldBe a[SparkYearMonthIntervalType]
  }

  it should "convert DayTimeIntervalType" in {
    val sparkType = SchemaConverter.toSparkType(ColumnType.DayTimeIntervalType)
    sparkType shouldBe a[SparkDayTimeIntervalType]
  }

  it should "convert BinaryType to BinaryType" in {
    SchemaConverter.toSparkType(ColumnType.BinaryType) shouldBe SparkBinaryType
  }

  it should "convert DecimalType with precision and scale" in {
    val sparkType = SchemaConverter.toSparkType(ColumnType.DecimalType(18, 4))
    sparkType shouldBe SparkDecimalType(18, 4)
  }

  it should "convert VariantType to VariantType" in {
    SchemaConverter.toSparkType(ColumnType.VariantType) shouldBe SparkVariantType
  }

  it should "convert OptionType to inner type" in {
    SchemaConverter.toSparkType(ColumnType.OptionType(ColumnType.IntType)) shouldBe SparkIntegerType
  }

  it should "convert AnyType to StringType fallback" in {
    SchemaConverter.toSparkType(ColumnType.AnyType) shouldBe SparkStringType
  }

  "fromSparkType" should "reverse-map all basic types" in {
    SchemaConverter.fromSparkType(SparkIntegerType) shouldBe ColumnType.IntType
    SchemaConverter.fromSparkType(SparkLongType) shouldBe ColumnType.LongType
    SchemaConverter.fromSparkType(SparkDoubleType) shouldBe ColumnType.DoubleType
    SchemaConverter.fromSparkType(SparkStringType) shouldBe ColumnType.StringType
    SchemaConverter.fromSparkType(SparkBooleanType) shouldBe ColumnType.BooleanType
  }

  it should "reverse-map new numeric types" in {
    SchemaConverter.fromSparkType(SparkFloatType) shouldBe ColumnType.FloatType
    SchemaConverter.fromSparkType(SparkShortType) shouldBe ColumnType.ShortType
    SchemaConverter.fromSparkType(SparkByteType) shouldBe ColumnType.ByteType
  }

  it should "reverse-map temporal types" in {
    SchemaConverter.fromSparkType(SparkTimestampType) shouldBe ColumnType.TimestampType
    SchemaConverter.fromSparkType(SparkTimestampNTZType) shouldBe ColumnType.TimestampNTZType
    SchemaConverter.fromSparkType(SparkYearMonthIntervalType()) shouldBe ColumnType.YearMonthIntervalType
    SchemaConverter.fromSparkType(SparkDayTimeIntervalType()) shouldBe ColumnType.DayTimeIntervalType
  }

  it should "reverse-map BinaryType" in {
    SchemaConverter.fromSparkType(SparkBinaryType) shouldBe ColumnType.BinaryType
  }

  it should "reverse-map DecimalType with precision and scale" in {
    SchemaConverter.fromSparkType(SparkDecimalType(10, 2)) shouldBe ColumnType.DecimalType(10, 2)
    SchemaConverter.fromSparkType(SparkDecimalType(38, 18)) shouldBe ColumnType.DecimalType(38, 18)
  }

  it should "reverse-map VariantType" in {
    SchemaConverter.fromSparkType(SparkVariantType) shouldBe ColumnType.VariantType
  }

  "toStructType" should "convert Int schema" in {
    val struct = SchemaConverter.toStructType(Schema.intSchema)
    struct.fields.length shouldBe 1
    struct.fields(0).dataType shouldBe SparkIntegerType
  }

  it should "convert tuple schema" in {
    given Schema[(Int, String)] = Schema.tuple2Schema[Int, String]
    val struct = SchemaConverter.toStructType(summon[Schema[(Int, String)]])
    struct.fields.length shouldBe 2
    struct.fields(0).dataType shouldBe SparkIntegerType
    struct.fields(1).dataType shouldBe SparkStringType
  }
}
