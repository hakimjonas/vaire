package net.ghoula.strongbow.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.prelude.*

class RowConverterSpec extends AnyFlatSpec with Matchers {

  "toRow and fromRow" should "round-trip Int" in {
    val schema = Schema.intSchema
    val row = RowConverter.toRow(42, schema)
    row.getInt(0) shouldBe 42
    RowConverter.fromRow(row, schema) shouldBe Right(42)
  }

  it should "round-trip String" in {
    val schema = Schema.stringSchema
    val row = RowConverter.toRow("hello", schema)
    row.getString(0) shouldBe "hello"
    RowConverter.fromRow(row, schema) shouldBe Right("hello")
  }

  it should "round-trip Long" in {
    val schema = Schema.longSchema
    val row = RowConverter.toRow(123L, schema)
    row.getLong(0) shouldBe 123L
    RowConverter.fromRow(row, schema) shouldBe Right(123L)
  }

  it should "round-trip Double" in {
    val schema = Schema.doubleSchema
    val row = RowConverter.toRow(3.14, schema)
    row.getDouble(0) shouldBe 3.14
    RowConverter.fromRow(row, schema) shouldBe Right(3.14)
  }

  it should "round-trip Boolean" in {
    val schema = Schema.booleanSchema
    val row = RowConverter.toRow(true, schema)
    row.getBoolean(0) shouldBe true
    RowConverter.fromRow(row, schema) shouldBe Right(true)
  }

  it should "round-trip tuples" in {
    given Schema[(Int, String)] = Schema.tuple2Schema[Int, String]
    val schema = summon[Schema[(Int, String)]]
    val row = RowConverter.toRow((1, "a"), schema)
    RowConverter.fromRow(row, schema) shouldBe Right((1, "a"))
  }

  it should "round-trip Option[Int] Some" in {
    given Schema[Option[Int]] = Schema.optionSchema[Int]
    val schema = summon[Schema[Option[Int]]]
    val row = RowConverter.toRow(Some(42), schema)
    RowConverter.fromRow(row, schema) shouldBe Right(Some(42))
  }

  it should "round-trip Option[Int] None" in {
    given Schema[Option[Int]] = Schema.optionSchema[Int]
    val schema = summon[Schema[Option[Int]]]
    val row = RowConverter.toRow(None, schema)
    RowConverter.fromRow(row, schema) shouldBe Right(None)
  }

  "fromRowUnsafe" should "return value directly" in {
    val schema = Schema.intSchema
    val row = RowConverter.toRow(99, schema)
    RowConverter.fromRowUnsafe(row, schema) shouldBe 99
  }
}
