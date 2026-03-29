package net.ghoula.strongbow.spark

import org.apache.spark.sql.types.{
  BooleanType => SparkBooleanType,
  DoubleType => SparkDoubleType,
  IntegerType => SparkIntegerType,
  LongType => SparkLongType,
  StringType => SparkStringType
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
