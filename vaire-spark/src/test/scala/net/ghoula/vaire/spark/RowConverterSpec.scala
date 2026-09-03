package net.ghoula.vaire.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.prelude.*
import net.ghoula.vaire.types

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

  it should "round-trip Float" in {
    val schema = Schema.floatSchema
    val row = RowConverter.toRow(2.5f, schema)
    row.getFloat(0) shouldBe 2.5f
    RowConverter.fromRow(row, schema) shouldBe Right(2.5f)
  }

  it should "round-trip Short" in {
    val schema = Schema.shortSchema
    val row = RowConverter.toRow(42: Short, schema)
    row.getShort(0) shouldBe (42: Short)
    RowConverter.fromRow(row, schema) shouldBe Right(42: Short)
  }

  it should "round-trip Byte" in {
    val schema = Schema.byteSchema
    val row = RowConverter.toRow(7: Byte, schema)
    row.getByte(0) shouldBe (7: Byte)
    RowConverter.fromRow(row, schema) shouldBe Right(7: Byte)
  }

  it should "round-trip Timestamp" in {
    val schema = Schema.timestampSchema
    val ts = types.Timestamp.ofEpochMicro(1000000L)
    val row = RowConverter.toRow(ts, schema)
    row.getLong(0) shouldBe 1000000L
    val decoded = RowConverter.fromRow(row, schema)
    decoded.isRight shouldBe true
    decoded.toOption.get.toEpochMicro shouldBe 1000000L
  }

  it should "round-trip TimestampNTZ" in {
    val schema = Schema.timestampNTZSchema
    val ts = types.TimestampNTZ.ofEpochMicro(2000000L)
    val row = RowConverter.toRow(ts, schema)
    row.getLong(0) shouldBe 2000000L
    val decoded = RowConverter.fromRow(row, schema)
    decoded.isRight shouldBe true
    decoded.toOption.get.toEpochMicro shouldBe 2000000L
  }

  it should "round-trip YearMonthInterval" in {
    val schema = Schema.yearMonthIntervalSchema
    val ymi = types.YearMonthInterval.ofMonths(18)
    val row = RowConverter.toRow(ymi, schema)
    row.getInt(0) shouldBe 18
    val decoded = RowConverter.fromRow(row, schema)
    decoded.isRight shouldBe true
    decoded.toOption.get.toMonths shouldBe 18
  }

  it should "round-trip DayTimeInterval" in {
    val schema = Schema.dayTimeIntervalSchema
    val dti = types.DayTimeInterval.ofMicros(86400000000L)
    val row = RowConverter.toRow(dti, schema)
    row.getLong(0) shouldBe 86400000000L
    val decoded = RowConverter.fromRow(row, schema)
    decoded.isRight shouldBe true
    decoded.toOption.get.toMicros shouldBe 86400000000L
  }

  it should "round-trip Binary" in {
    val schema = Schema.binarySchema
    val bin = types.Binary(Array[Byte](1, 2, 3))
    val row = RowConverter.toRow(bin, schema)
    row.get(0) match {
      case ba: Array[Byte @unchecked] => ba shouldBe Array[Byte](1, 2, 3)
      case other => fail(s"Expected Array[Byte], got ${other.getClass}")
    }
    val decoded = RowConverter.fromRow(row, schema)
    decoded.isRight shouldBe true
    decoded.toOption.get.toBytes shouldBe Array[Byte](1, 2, 3)
  }

  it should "round-trip Decimal" in {
    val schema = Schema.decimalSchema
    val dec = types.Decimal.ofUnscaled(42L)
    val row = RowConverter.toRow(dec, schema)
    val decoded = RowConverter.fromRow(row, schema)
    decoded.isRight shouldBe true
    decoded.toOption.get.toUnscaled shouldBe 42L
  }

  it should "round-trip Decimal with custom precision and scale" in {
    val schema = Schema.decimalWith(18, 4)
    val dec = types.Decimal.ofUnscaled(12345L)
    val row = RowConverter.toRow(dec, schema)
    row.get(0) match {
      case bd: java.math.BigDecimal => bd shouldBe java.math.BigDecimal.valueOf(12345L, 4)
      case other => fail(s"Expected BigDecimal, got ${other.getClass}")
    }
    val decoded = RowConverter.fromRow(row, schema)
    decoded.isRight shouldBe true
    decoded.toOption.get.toUnscaled shouldBe 12345L
  }

  "fromRowUnsafe" should "return value directly" in {
    val schema = Schema.intSchema
    val row = RowConverter.toRow(99, schema)
    RowConverter.fromRowUnsafe(row, schema) shouldBe 99
  }
}
