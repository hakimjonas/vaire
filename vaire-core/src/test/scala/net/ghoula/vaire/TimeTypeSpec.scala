package net.ghoula.vaire

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.BitSet
import scala.language.strictEquality

import net.ghoula.vaire.prelude.*
import net.ghoula.vaire.types.Time

class TimeTypeSpec extends AnyFlatSpec with Matchers {

  "Time" should "store microseconds since midnight" in {
    val t = Time.ofMicros(3661000000L)
    t.toMicros shouldBe 3661000000L
    Time.MIN.toMicros shouldBe 0L
    Time.MAX.toMicros shouldBe 86399999999L
  }

  it should "convert to and from LocalTime" in {
    val lt = java.time.LocalTime.of(10, 15, 30, 123456000)
    val t = Time.fromLocalTime(lt)
    t.toMicros shouldBe (10 * 3600 + 15 * 60 + 30) * 1000000L + 123456L
    t.toLocalTime shouldBe lt
  }

  it should "order times" in {
    summon[Ordering[Time]].lt(Time.ofMicros(1L), Time.ofMicros(2L)) shouldBe true
  }

  "TimeColumn" should "construct via factory" in {
    val col = Column.time(Array(0L, 3600000000L))
    col.length shouldBe 2
    col.columnType shouldBe ColumnType.TimeType
    col.getValue(0) shouldBe Time.ofMicros(0L)
    col.getValue(1) shouldBe Time.ofMicros(3600000000L)
  }

  it should "track nulls" in {
    val col = Column.time(Array(0L, 3600000000L, 7200000000L), BitSet(1))
    col.nullSet shouldBe BitSet(1)
    col.nullSet.contains(0) shouldBe false
  }

  it should "round-trip through fromValues" in {
    val result =
      Column.fromValues(
        Vector(3600000000L, SqlNull.value, 1L),
        ColumnType.TimeType
      )
    result match {
      case Right(col) =>
        col.columnType shouldBe ColumnType.TimeType
        col.nullSet shouldBe BitSet(1)
        col.getValue(0) shouldBe Time.ofMicros(3600000000L)
        col.getValue(2) shouldBe Time.ofMicros(1L)
      case other => fail(s"Expected time column, got: $other")
    }
  }

  it should "accept LocalTime values in fromValues" in {
    val lt = java.time.LocalTime.of(6, 30)
    val col = Column.fromValues(Vector(lt), ColumnType.TimeType).toOption.get
    col.getValue(0) shouldBe Time.fromLocalTime(lt)
  }

  it should "take a prefix" in {
    val col = Column.time(Array(1L, 2L, 3L, 4L), BitSet(2))
    val taken = col.take(3)
    taken.length shouldBe 3
    taken.nullSet shouldBe BitSet(2)
    taken.columnType shouldBe ColumnType.TimeType
  }

  it should "slice rows" in {
    val col = Column.time(Array(1L, 2L, 3L))
    val sliced = col.slice(Array(2, 0))
    sliced.getValue(0) shouldBe Time.ofMicros(3L)
    sliced.getValue(1) shouldBe Time.ofMicros(1L)
  }

  it should "concat with another TimeColumn" in {
    val left = Column.time(Array(1L, 2L))
    val right = Column.time(Array(3L), BitSet(0))
    left.concat(right) match {
      case Right(merged) =>
        merged.length shouldBe 3
        merged.nullSet shouldBe BitSet(2)
        merged.getValue(0) shouldBe Time.ofMicros(1L)
      case other => fail(s"Expected merged column, got: $other")
    }
  }

  it should "create an empty column for TimeType" in {
    val col = Column.empty(ColumnType.TimeType)
    col.length shouldBe 0
    col.columnType shouldBe ColumnType.TimeType
  }

  "timeSchema" should "round-trip through Schema encode/decode" in {
    val schema = Schema.timeSchema
    val encoded = schema.encode(Time.ofMicros(3600000000L))
    schema.decode(encoded) shouldBe Right(Time.ofMicros(3600000000L))
    schema.columnTypes shouldBe Vector(ColumnType.TimeType)
  }

  it should "round-trip through MaterializedDataset" in {
    val times = Vector(Time.ofMicros(0L), Time.ofMicros(3600000000L), Time.ofMicros(86399999999L))
    MaterializedDataset.fromVector(times)(using Schema.timeSchema) match {
      case Right(md) =>
        md.rowCount shouldBe 3
        md.columns.head.columnType shouldBe ColumnType.TimeType
        md.toVectorUnsafe shouldBe times
      case other => fail(s"Expected materialized dataset, got: $other")
    }
  }
}
