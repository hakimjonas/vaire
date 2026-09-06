package net.ghoula.vaire

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.BitSet

import net.ghoula.vaire.column.Column.*
import net.ghoula.vaire.column.{Column, ColumnType}

given anyCanEqual: CanEqual[Any, Any] = CanEqual.derived

class ColumnTypesSpec extends AnyFlatSpec with Matchers {

  // ── FloatColumn ──────────────────────────────────────────────────────

  "FloatColumn" should "construct via factory" in {
    val col = Column.float(Array(1.0f, 2.5f, 3.7f))
    col.length shouldBe 3
    col.columnType shouldBe ColumnType.FloatType
    col.getValue(0) shouldBe 1.0f
    col.getValue(2) shouldBe 3.7f
  }

  it should "track nulls" in {
    val col = Column.float(Array(1.0f, 0.0f, 3.0f), BitSet(1))
    col.nullSet shouldBe BitSet(1)
    col.length shouldBe 3
  }

  it should "round-trip through fromValues" in {
    val result =
      Column.fromValues(Vector(1.5f, SqlNull.value, 3.5f), ColumnType.FloatType)
    result.isRight shouldBe true
    val col = result.toOption.get
    col.length shouldBe 3
    col.nullSet shouldBe BitSet(1)
    col.getValue(0) shouldBe 1.5f
    col.getValue(2) shouldBe 3.5f
  }

  it should "take prefix" in {
    val col = Column.float(Array(1.0f, 2.0f, 3.0f, 4.0f), BitSet(2))
    val taken = col.take(2)
    taken.length shouldBe 2
    taken.nullSet shouldBe BitSet.empty
    taken.getValue(0) shouldBe 1.0f
  }

  it should "slice by indices" in {
    val col = Column.float(Array(10.0f, 20.0f, 30.0f, 40.0f), BitSet(1))
    val sliced = col.slice(Array(3, 1, 0))
    sliced.length shouldBe 3
    sliced.getValue(0) shouldBe 40.0f
    sliced.nullSet shouldBe BitSet(1)
  }

  it should "concat two columns" in {
    val a = Column.float(Array(1.0f, 2.0f), BitSet(0))
    val b = Column.float(Array(3.0f, 4.0f), BitSet(1))
    val result = a.concat(b)
    result.isRight shouldBe true
    val col = result.toOption.get
    col.length shouldBe 4
    col.nullSet shouldBe BitSet(0, 3)
  }

  it should "reject concat with mismatched type" in {
    val a = Column.float(Array(1.0f))
    val b = Column.int(Array(1))
    a.concat(b).isLeft shouldBe true
  }

  it should "compareAt correctly" in {
    val col = Column.float(Array(1.0f, 3.0f, 2.0f))
    Column.compareAt(col, 0, 1) should be < 0
    Column.compareAt(col, 1, 2) should be > 0
    Column.compareAt(col, 0, 0) shouldBe 0
  }

  it should "compareAt with nulls" in {
    val col = Column.float(Array(1.0f, 0.0f, 2.0f), BitSet(1))
    Column.compareAt(col, 1, 0) should be < 0
    Column.compareAt(col, 0, 1) should be > 0
    Column.compareAt(col, 1, 1) shouldBe 0
  }

  it should "sortIndicesByColumn" in {
    val col = Column.float(Array(3.0f, 1.0f, 2.0f))
    val indices = Column.sortIndicesByColumn(col, 3)
    indices shouldBe Array(1, 2, 0)
  }

  // ── ShortColumn ──────────────────────────────────────────────────────

  "ShortColumn" should "construct via factory" in {
    val col = Column.short(Array[Short](1, 2, 3))
    col.length shouldBe 3
    col.columnType shouldBe ColumnType.ShortType
    col.getValue(0) shouldBe (1: Short)
  }

  it should "round-trip through fromValues" in {
    val result = Column.fromValues(
      Vector[Any](1: Short, SqlNull.value, 3: Short),
      ColumnType.ShortType
    )
    result.isRight shouldBe true
    val col = result.toOption.get
    col.length shouldBe 3
    col.nullSet shouldBe BitSet(1)
    col.getValue(0) shouldBe (1: Short)
  }

  it should "take prefix" in {
    val col = Column.short(Array[Short](10, 20, 30), BitSet(2))
    val taken = col.take(2)
    taken.length shouldBe 2
    taken.nullSet shouldBe BitSet.empty
  }

  it should "slice by indices" in {
    val col = Column.short(Array[Short](10, 20, 30, 40), BitSet(0))
    val sliced = col.slice(Array(2, 0))
    sliced.length shouldBe 2
    sliced.getValue(0) shouldBe (30: Short)
    sliced.nullSet shouldBe BitSet(1)
  }

  it should "concat two columns" in {
    val a = Column.short(Array[Short](1, 2))
    val b = Column.short(Array[Short](3, 4), BitSet(0))
    val result = a.concat(b)
    result.isRight shouldBe true
    val col = result.toOption.get
    col.length shouldBe 4
    col.nullSet shouldBe BitSet(2)
  }

  it should "compareAt correctly" in {
    val col = Column.short(Array[Short](5, 10, 3))
    Column.compareAt(col, 0, 1) should be < 0
    Column.compareAt(col, 1, 2) should be > 0
  }

  it should "sortIndicesByColumn" in {
    val col = Column.short(Array[Short](30, 10, 20))
    Column.sortIndicesByColumn(col, 3) shouldBe Array(1, 2, 0)
  }

  // ── ByteColumn ───────────────────────────────────────────────────────

  "ByteColumn" should "construct via factory" in {
    val col = Column.byte(Array[Byte](1, 2, 3))
    col.length shouldBe 3
    col.columnType shouldBe ColumnType.ByteType
    col.getValue(0) shouldBe (1: Byte)
  }

  it should "round-trip through fromValues" in {
    val result = Column.fromValues(
      Vector[Any](1: Byte, SqlNull.value, 3: Byte),
      ColumnType.ByteType
    )
    result.isRight shouldBe true
    val col = result.toOption.get
    col.nullSet shouldBe BitSet(1)
    col.getValue(2) shouldBe (3: Byte)
  }

  it should "take prefix" in {
    val col = Column.byte(Array[Byte](10, 20, 30))
    col.take(2).length shouldBe 2
  }

  it should "slice by indices" in {
    val col = Column.byte(Array[Byte](10, 20, 30, 40), BitSet(1))
    val sliced = col.slice(Array(3, 1))
    sliced.length shouldBe 2
    sliced.getValue(0) shouldBe (40: Byte)
    sliced.nullSet shouldBe BitSet(1)
  }

  it should "concat two columns" in {
    val a = Column.byte(Array[Byte](1, 2))
    val b = Column.byte(Array[Byte](3, 4))
    val result = a.concat(b)
    result.isRight shouldBe true
    result.toOption.get.length shouldBe 4
  }

  it should "compareAt correctly" in {
    val col = Column.byte(Array[Byte](1, 5, 3))
    Column.compareAt(col, 0, 1) should be < 0
    Column.compareAt(col, 1, 2) should be > 0
  }

  it should "sortIndicesByColumn" in {
    val col = Column.byte(Array[Byte](30, 10, 20))
    Column.sortIndicesByColumn(col, 3) shouldBe Array(1, 2, 0)
  }

  // ── TimestampColumn ──────────────────────────────────────────────────

  "TimestampColumn" should "construct via factory with epoch micros" in {
    val col = Column.timestamp(Array(1000000L, 2000000L, 3000000L))
    col.length shouldBe 3
    col.columnType shouldBe ColumnType.TimestampType
  }

  it should "round-trip through fromValues" in {
    val result = Column.fromValues(
      Vector[Any](1000000L, SqlNull.value, 3000000L),
      ColumnType.TimestampType
    )
    result.isRight shouldBe true
    val col = result.toOption.get
    col.nullSet shouldBe BitSet(1)
    col.length shouldBe 3
  }

  it should "take prefix preserving nulls" in {
    val col = Column.timestamp(Array(100L, 200L, 300L), BitSet(0))
    val taken = col.take(2)
    taken.length shouldBe 2
    taken.nullSet shouldBe BitSet(0)
  }

  it should "slice by indices" in {
    val col = Column.timestamp(Array(100L, 200L, 300L, 400L))
    val sliced = col.slice(Array(3, 0))
    sliced.length shouldBe 2
  }

  it should "concat two columns" in {
    val a = Column.timestamp(Array(100L, 200L))
    val b = Column.timestamp(Array(300L, 400L), BitSet(0))
    val result = a.concat(b)
    result.isRight shouldBe true
    val col = result.toOption.get
    col.length shouldBe 4
    col.nullSet shouldBe BitSet(2)
  }

  it should "compareAt correctly" in {
    val col = Column.timestamp(Array(100L, 300L, 200L))
    Column.compareAt(col, 0, 1) should be < 0
    Column.compareAt(col, 1, 2) should be > 0
    Column.compareAt(col, 0, 0) shouldBe 0
  }

  it should "sortIndicesByColumn" in {
    val col = Column.timestamp(Array(300L, 100L, 200L))
    Column.sortIndicesByColumn(col, 3) shouldBe Array(1, 2, 0)
  }

  // ── TimestampNTZColumn ───────────────────────────────────────────────

  "TimestampNTZColumn" should "construct via factory" in {
    val col = Column.timestampNTZ(Array(1000L, 2000L))
    col.length shouldBe 2
    col.columnType shouldBe ColumnType.TimestampNTZType
  }

  it should "round-trip through fromValues" in {
    val result = Column.fromValues(
      Vector[Any](500L, SqlNull.value, 700L),
      ColumnType.TimestampNTZType
    )
    result.isRight shouldBe true
    val col = result.toOption.get
    col.nullSet shouldBe BitSet(1)
  }

  it should "take prefix" in {
    val col = Column.timestampNTZ(Array(10L, 20L, 30L), BitSet(1))
    val taken = col.take(2)
    taken.length shouldBe 2
    taken.nullSet shouldBe BitSet(1)
  }

  it should "slice by indices" in {
    val col = Column.timestampNTZ(Array(10L, 20L, 30L))
    val sliced = col.slice(Array(2, 0))
    sliced.length shouldBe 2
  }

  it should "concat two columns" in {
    val a = Column.timestampNTZ(Array(10L))
    val b = Column.timestampNTZ(Array(20L), BitSet(0))
    val result = a.concat(b)
    result.isRight shouldBe true
    result.toOption.get.nullSet shouldBe BitSet(1)
  }

  it should "compareAt correctly" in {
    val col = Column.timestampNTZ(Array(100L, 50L, 200L))
    Column.compareAt(col, 0, 1) should be > 0
    Column.compareAt(col, 1, 2) should be < 0
  }

  it should "sortIndicesByColumn" in {
    val col = Column.timestampNTZ(Array(300L, 100L, 200L))
    Column.sortIndicesByColumn(col, 3) shouldBe Array(1, 2, 0)
  }

  // ── YearMonthIntervalColumn ──────────────────────────────────────────

  "YearMonthIntervalColumn" should "construct via factory" in {
    val col = Column.yearMonthInterval(Array(12, 24, 6))
    col.length shouldBe 3
    col.columnType shouldBe ColumnType.YearMonthIntervalType
  }

  it should "round-trip through fromValues" in {
    val result = Column.fromValues(
      Vector[Any](12, SqlNull.value, 36),
      ColumnType.YearMonthIntervalType
    )
    result.isRight shouldBe true
    val col = result.toOption.get
    col.nullSet shouldBe BitSet(1)
  }

  it should "take prefix" in {
    val col = Column.yearMonthInterval(Array(12, 24, 36), BitSet(2))
    val taken = col.take(2)
    taken.length shouldBe 2
    taken.nullSet shouldBe BitSet.empty
  }

  it should "slice by indices" in {
    val col = Column.yearMonthInterval(Array(12, 24, 36, 48), BitSet(0))
    val sliced = col.slice(Array(3, 0, 1))
    sliced.length shouldBe 3
    sliced.nullSet shouldBe BitSet(1)
  }

  it should "concat two columns" in {
    val a = Column.yearMonthInterval(Array(12, 24))
    val b = Column.yearMonthInterval(Array(36))
    val result = a.concat(b)
    result.isRight shouldBe true
    result.toOption.get.length shouldBe 3
  }

  it should "compareAt correctly" in {
    val col = Column.yearMonthInterval(Array(6, 24, 12))
    Column.compareAt(col, 0, 1) should be < 0
    Column.compareAt(col, 1, 2) should be > 0
  }

  it should "sortIndicesByColumn" in {
    val col = Column.yearMonthInterval(Array(36, 12, 24))
    Column.sortIndicesByColumn(col, 3) shouldBe Array(1, 2, 0)
  }

  // ── DayTimeIntervalColumn ────────────────────────────────────────────

  "DayTimeIntervalColumn" should "construct via factory" in {
    val col = Column.dayTimeInterval(Array(86400000000L, 172800000000L))
    col.length shouldBe 2
    col.columnType shouldBe ColumnType.DayTimeIntervalType
  }

  it should "round-trip through fromValues" in {
    val result = Column.fromValues(
      Vector[Any](86400000000L, SqlNull.value, 259200000000L),
      ColumnType.DayTimeIntervalType
    )
    result.isRight shouldBe true
    val col = result.toOption.get
    col.nullSet shouldBe BitSet(1)
  }

  it should "take prefix" in {
    val col = Column.dayTimeInterval(Array(100L, 200L, 300L), BitSet(0))
    val taken = col.take(2)
    taken.length shouldBe 2
    taken.nullSet shouldBe BitSet(0)
  }

  it should "slice by indices" in {
    val col = Column.dayTimeInterval(Array(100L, 200L, 300L))
    val sliced = col.slice(Array(2, 0))
    sliced.length shouldBe 2
  }

  it should "concat two columns" in {
    val a = Column.dayTimeInterval(Array(100L, 200L), BitSet(1))
    val b = Column.dayTimeInterval(Array(300L))
    val result = a.concat(b)
    result.isRight shouldBe true
    val col = result.toOption.get
    col.length shouldBe 3
    col.nullSet shouldBe BitSet(1)
  }

  it should "compareAt correctly" in {
    val col = Column.dayTimeInterval(Array(300L, 100L, 200L))
    Column.compareAt(col, 0, 1) should be > 0
    Column.compareAt(col, 1, 2) should be < 0
  }

  it should "sortIndicesByColumn" in {
    val col = Column.dayTimeInterval(Array(300L, 100L, 200L))
    Column.sortIndicesByColumn(col, 3) shouldBe Array(1, 2, 0)
  }

  // ── BinaryColumn ─────────────────────────────────────────────────────

  "BinaryColumn" should "construct via binaryFromArrays" in {
    val col = Column.binaryFromArrays(
      Array(Array[Byte](1, 2, 3), Array[Byte](4, 5), Array[Byte](6)),
      BitSet.empty
    )
    col.length shouldBe 3
    col.columnType shouldBe ColumnType.BinaryType
  }

  it should "construct via flat layout factory" in {
    val col = Column.binary(Array[Byte](1, 2, 3, 4, 5), Array(0, 3, 5), BitSet.empty)
    col.length shouldBe 2
    col.columnType shouldBe ColumnType.BinaryType
  }

  it should "round-trip through fromValues" in {
    val result = Column.fromValues(
      Vector[Any](Array[Byte](1, 2), SqlNull.value, Array[Byte](3, 4, 5)),
      ColumnType.BinaryType
    )
    result.isRight shouldBe true
    val col = result.toOption.get
    col.length shouldBe 3
    col.nullSet shouldBe BitSet(1)
  }

  it should "take prefix" in {
    val col = Column.binaryFromArrays(
      Array(Array[Byte](1, 2), Array[Byte](3), Array[Byte](4, 5, 6)),
      BitSet.empty
    )
    val taken = col.take(2)
    taken.length shouldBe 2
    taken match {
      case BinaryColumn(data, offsets, _) =>
        offsets.length shouldBe 3
        data.length shouldBe 3
      case _ => fail("Expected BinaryColumn")
    }
  }

  it should "take prefix with nulls" in {
    val col = Column.binaryFromArrays(
      Array(Array[Byte](1, 2), Array.empty[Byte], Array[Byte](3)),
      BitSet(1)
    )
    val taken = col.take(3)
    taken.length shouldBe 3
    taken.nullSet shouldBe BitSet(1)
  }

  it should "slice by indices" in {
    val col = Column.binaryFromArrays(
      Array(Array[Byte](10, 20), Array[Byte](30), Array[Byte](40, 50, 60)),
      BitSet.empty
    )
    val sliced = col.slice(Array(2, 0))
    sliced.length shouldBe 2
    sliced match {
      case BinaryColumn(data, offsets, _) =>
        offsets shouldBe Array(0, 3, 5)
        data shouldBe Array[Byte](40, 50, 60, 10, 20)
      case _ => fail("Expected BinaryColumn")
    }
  }

  it should "slice with nulls" in {
    val col = Column.binaryFromArrays(
      Array(Array[Byte](1), Array.empty[Byte], Array[Byte](2, 3)),
      BitSet(1)
    )
    val sliced = col.slice(Array(1, 2))
    sliced.length shouldBe 2
    sliced.nullSet shouldBe BitSet(0)
  }

  it should "concat two columns" in {
    val a = Column.binaryFromArrays(Array(Array[Byte](1, 2)), BitSet.empty)
    val b = Column.binaryFromArrays(Array(Array[Byte](3, 4, 5)), BitSet.empty)
    val result = a.concat(b)
    result.isRight shouldBe true
    val col = result.toOption.get
    col.length shouldBe 2
    col match {
      case BinaryColumn(data, offsets, _) =>
        data shouldBe Array[Byte](1, 2, 3, 4, 5)
        offsets shouldBe Array(0, 2, 5)
      case _ => fail("Expected BinaryColumn")
    }
  }

  it should "concat with nulls" in {
    val a = Column.binaryFromArrays(Array(Array[Byte](1)), BitSet(0))
    val b = Column.binaryFromArrays(Array(Array[Byte](2)), BitSet.empty)
    val result = a.concat(b)
    result.isRight shouldBe true
    result.toOption.get.nullSet shouldBe BitSet(0)
  }

  it should "compareAt correctly" in {
    val col = Column.binaryFromArrays(
      Array(Array[Byte](1, 2), Array[Byte](1, 3), Array[Byte](1, 2)),
      BitSet.empty
    )
    Column.compareAt(col, 0, 1) should be < 0
    Column.compareAt(col, 1, 0) should be > 0
    Column.compareAt(col, 0, 2) shouldBe 0
  }

  it should "compareAt with different lengths" in {
    val col = Column.binaryFromArrays(
      Array(Array[Byte](1, 2), Array[Byte](1, 2, 3)),
      BitSet.empty
    )
    Column.compareAt(col, 0, 1) should be < 0
    Column.compareAt(col, 1, 0) should be > 0
  }

  it should "compareAt with nulls" in {
    val col = Column.binaryFromArrays(
      Array(Array[Byte](1), Array.empty[Byte]),
      BitSet(1)
    )
    Column.compareAt(col, 1, 0) should be < 0
    Column.compareAt(col, 0, 1) should be > 0
    Column.compareAt(col, 1, 1) shouldBe 0
  }

  it should "sortIndicesByColumn" in {
    val col = Column.binaryFromArrays(
      Array(Array[Byte](3), Array[Byte](1), Array[Byte](2)),
      BitSet.empty
    )
    Column.sortIndicesByColumn(col, 3) shouldBe Array(1, 2, 0)
  }

  it should "create empty via Column.empty" in {
    val col = Column.empty(ColumnType.BinaryType)
    col.length shouldBe 0
    col.columnType shouldBe ColumnType.BinaryType
  }

  // ── CharType / VarcharType ───────────────────────────────────────────

  "CharType" should "map to StringColumn via fromValues" in {
    val result = Column.fromValues(Vector("abc", "def"), ColumnType.CharType(3))
    result.isRight shouldBe true
    val col = result.toOption.get
    col match {
      case StringColumn(_, _) => succeed
      case _ => fail("Expected StringColumn")
    }
    col.getValue(0) shouldBe "abc"
  }

  it should "create empty column" in {
    val col = Column.empty(ColumnType.CharType(10))
    col.length shouldBe 0
    col match {
      case StringColumn(_, _) => succeed
      case _ => fail("Expected StringColumn")
    }
  }

  "VarcharType" should "map to StringColumn via fromValues" in {
    val result = Column.fromValues(
      Vector("hello", SqlNull.value, "world"),
      ColumnType.VarcharType(10)
    )
    result.isRight shouldBe true
    val col = result.toOption.get
    col match {
      case StringColumn(_, _) => succeed
      case _ => fail("Expected StringColumn")
    }
    col.nullSet shouldBe BitSet(1)
  }

  it should "create empty column" in {
    val col = Column.empty(ColumnType.VarcharType(20))
    col.length shouldBe 0
    col match {
      case StringColumn(_, _) => succeed
      case _ => fail("Expected StringColumn")
    }
  }

  // ── Schema encode/decode ─────────────────────────────────────────────

  "Schema[Float]" should "encode and decode" in {
    val schema = summon[Schema[Float]]
    schema.columnTypes shouldBe Vector(ColumnType.FloatType)
    schema.encode(1.5f) shouldBe Vector(1.5f)
    schema.decode(Vector(1.5f)) shouldBe Right(1.5f)
  }

  "Schema[Short]" should "encode and decode" in {
    val schema = summon[Schema[Short]]
    schema.columnTypes shouldBe Vector(ColumnType.ShortType)
    schema.encode(42: Short) shouldBe Vector(42: Short)
    schema.decode(Vector(42: Short)) shouldBe Right(42: Short)
  }

  "Schema[Byte]" should "encode and decode" in {
    val schema = summon[Schema[Byte]]
    schema.columnTypes shouldBe Vector(ColumnType.ByteType)
    schema.encode(7: Byte) shouldBe Vector(7: Byte)
    schema.decode(Vector(7: Byte)) shouldBe Right(7: Byte)
  }

  "Schema[Timestamp]" should "encode and decode" in {
    val schema = summon[Schema[types.Timestamp]]
    val ts = types.Timestamp.ofEpochMicro(1000000L)
    schema.columnTypes shouldBe Vector(ColumnType.TimestampType)
    schema.encode(ts) shouldBe Vector(1000000L)
    val decoded = schema.decode(Vector(1000000L))
    decoded.isRight shouldBe true
    decoded.toOption.get.toEpochMicro shouldBe 1000000L
  }

  "Schema[TimestampNTZ]" should "encode and decode" in {
    val schema = summon[Schema[types.TimestampNTZ]]
    val ts = types.TimestampNTZ.ofEpochMicro(2000000L)
    schema.columnTypes shouldBe Vector(ColumnType.TimestampNTZType)
    schema.encode(ts) shouldBe Vector(2000000L)
    val decoded = schema.decode(Vector(2000000L))
    decoded.isRight shouldBe true
    decoded.toOption.get.toEpochMicro shouldBe 2000000L
  }

  "Schema[YearMonthInterval]" should "encode and decode" in {
    val schema = summon[Schema[types.YearMonthInterval]]
    val ymi = types.YearMonthInterval.ofMonths(18)
    schema.columnTypes shouldBe Vector(ColumnType.YearMonthIntervalType)
    schema.encode(ymi) shouldBe Vector(18)
    val decoded = schema.decode(Vector(18))
    decoded.isRight shouldBe true
    decoded.toOption.get.toMonths shouldBe 18
  }

  "Schema[DayTimeInterval]" should "encode and decode" in {
    val schema = summon[Schema[types.DayTimeInterval]]
    val dti = types.DayTimeInterval.ofMicros(86400000000L)
    schema.columnTypes shouldBe Vector(ColumnType.DayTimeIntervalType)
    schema.encode(dti) shouldBe Vector(86400000000L)
    val decoded = schema.decode(Vector(86400000000L))
    decoded.isRight shouldBe true
    decoded.toOption.get.toMicros shouldBe 86400000000L
  }

  "Schema[Binary]" should "encode and decode" in {
    val schema = summon[Schema[types.Binary]]
    val bin = types.Binary(Array[Byte](1, 2, 3))
    schema.columnTypes shouldBe Vector(ColumnType.BinaryType)
    val encoded = schema.encode(bin)
    encoded.length shouldBe 1
    encoded.head match {
      case ba: Array[Byte @unchecked] => ba shouldBe Array[Byte](1, 2, 3)
      case other => fail(s"Expected Array[Byte], got ${other.getClass}")
    }
    val decoded = schema.decode(Vector(Array[Byte](1, 2, 3)))
    decoded.isRight shouldBe true
    decoded.toOption.get.toBytes shouldBe Array[Byte](1, 2, 3)
  }

  // ── Column.empty for all new types ───────────────────────────────────

  "Column.empty" should "create empty FloatColumn" in {
    val col = Column.empty(ColumnType.FloatType)
    col.length shouldBe 0
    col.columnType shouldBe ColumnType.FloatType
  }

  it should "create empty ShortColumn" in {
    val col = Column.empty(ColumnType.ShortType)
    col.length shouldBe 0
    col.columnType shouldBe ColumnType.ShortType
  }

  it should "create empty ByteColumn" in {
    val col = Column.empty(ColumnType.ByteType)
    col.length shouldBe 0
    col.columnType shouldBe ColumnType.ByteType
  }

  it should "create empty TimestampColumn" in {
    val col = Column.empty(ColumnType.TimestampType)
    col.length shouldBe 0
    col.columnType shouldBe ColumnType.TimestampType
  }

  it should "create empty TimestampNTZColumn" in {
    val col = Column.empty(ColumnType.TimestampNTZType)
    col.length shouldBe 0
    col.columnType shouldBe ColumnType.TimestampNTZType
  }

  it should "create empty YearMonthIntervalColumn" in {
    val col = Column.empty(ColumnType.YearMonthIntervalType)
    col.length shouldBe 0
    col.columnType shouldBe ColumnType.YearMonthIntervalType
  }

  it should "create empty DayTimeIntervalColumn" in {
    val col = Column.empty(ColumnType.DayTimeIntervalType)
    col.length shouldBe 0
    col.columnType shouldBe ColumnType.DayTimeIntervalType
  }

  it should "create empty BinaryColumn" in {
    val col = Column.empty(ColumnType.BinaryType)
    col.length shouldBe 0
    col.columnType shouldBe ColumnType.BinaryType
  }

  // ── VariantType ───────────────────────────────────────────────────────

  "VariantType" should "map to AnyColumn via Column.empty" in {
    val col = Column.empty(ColumnType.VariantType)
    col.length shouldBe 0
    col match {
      case AnyColumn(_, _) => succeed
      case _ => fail("Expected AnyColumn")
    }
  }

  it should "map to AnyColumn via fromValues" in {
    val result = Column.fromValues(Vector("hello", 42, true), ColumnType.VariantType)
    result.isRight shouldBe true
    val col = result.toOption.get
    col.length shouldBe 3
    col match {
      case AnyColumn(_, _) => succeed
      case _ => fail("Expected AnyColumn")
    }
  }

  // ── fromValues type mismatch errors ──────────────────────────────────

  "Column.fromValues" should "reject wrong types for FloatType" in {
    val result = Column.fromValues(Vector("not a float"), ColumnType.FloatType)
    result.isLeft shouldBe true
  }

  it should "reject wrong types for ShortType" in {
    val result = Column.fromValues(Vector(1), ColumnType.ShortType)
    result.isLeft shouldBe true
  }

  it should "reject wrong types for ByteType" in {
    val result = Column.fromValues(Vector(1), ColumnType.ByteType)
    result.isLeft shouldBe true
  }

  it should "reject wrong types for TimestampType" in {
    val result = Column.fromValues(Vector("not a long"), ColumnType.TimestampType)
    result.isLeft shouldBe true
  }

  it should "reject wrong types for BinaryType" in {
    val result = Column.fromValues(Vector("not bytes"), ColumnType.BinaryType)
    result.isLeft shouldBe true
  }

  // ── DecimalColumn ────────────────────────────────────────────────────

  "DecimalColumn" should "construct via factory" in {
    val col = Column.decimal(Array(1500L, 2500L, 3500L), 10, 2)
    col.length shouldBe 3
    col.columnType shouldBe ColumnType.DecimalType(10, 2)
  }

  it should "return types.Decimal from getValue" in {
    val col = Column.decimal(Array(150L), 10, 2)
    val value = col.getValue(0)
    value shouldBe 150L
  }

  it should "return SqlNull.value from getValue for SqlNull.value index" in {
    val col = Column.decimal(Array(0L, 200L), 10, 2, BitSet(0))
    (col.getValue(0) == SqlNull.value) shouldBe true
    col.getValue(1) shouldBe 200L
  }

  it should "track nulls" in {
    val col = Column.decimal(Array(100L, 0L, 300L), 10, 2, BitSet(1))
    col.nullSet shouldBe BitSet(1)
    col.length shouldBe 3
  }

  it should "round-trip through fromValues with Long input" in {
    val result = Column.fromValues(
      Vector[Any](150L, SqlNull.value, 350L),
      ColumnType.DecimalType(10, 2)
    )
    result.isRight shouldBe true
    val col = result.toOption.get
    col.length shouldBe 3
    col.nullSet shouldBe BitSet(1)
    col.getValue(0) shouldBe 150L
    col.getValue(2) shouldBe 350L
  }

  it should "round-trip through fromValues with BigDecimal input" in {
    val result = Column.fromValues(
      Vector[Any](java.math.BigDecimal.valueOf(150, 2), java.math.BigDecimal.valueOf(350, 2)),
      ColumnType.DecimalType(10, 2)
    )
    result.isRight shouldBe true
    val col = result.toOption.get
    col.getValue(0) shouldBe 150L
    col.getValue(1) shouldBe 350L
  }

  it should "reject BigDecimal that overflows Long" in {
    val huge = new java.math.BigDecimal("99999999999999999999")
    val result = Column.fromValues(Vector[Any](huge), ColumnType.DecimalType(10, 0))
    result.isLeft shouldBe true
  }

  it should "reject wrong types for DecimalType" in {
    val result = Column.fromValues(Vector("not a decimal"), ColumnType.DecimalType(10, 2))
    result.isLeft shouldBe true
  }

  it should "fall to AnyColumn for precision > 18" in {
    val col = Column.empty(ColumnType.DecimalType(38, 10))
    col match {
      case AnyColumn(_, _) => succeed
      case _ => fail("Expected AnyColumn for precision > 18")
    }
  }

  it should "fromValues falls to AnyColumn for precision > 18" in {
    val result = Column.fromValues(Vector(1L), ColumnType.DecimalType(20, 5))
    result.isRight shouldBe true
    result.toOption.get match {
      case AnyColumn(_, _) => succeed
      case _ => fail("Expected AnyColumn for precision > 18")
    }
  }

  it should "take prefix" in {
    val col = Column.decimal(Array(100L, 200L, 300L, 400L), 10, 2, BitSet(2))
    val taken = col.take(2)
    taken.length shouldBe 2
    taken.nullSet shouldBe BitSet.empty
    taken.getValue(0) shouldBe 100L
    taken.columnType shouldBe ColumnType.DecimalType(10, 2)
  }

  it should "slice by indices" in {
    val col = Column.decimal(Array(100L, 200L, 300L, 400L), 10, 2, BitSet(1))
    val sliced = col.slice(Array(3, 1, 0))
    sliced.length shouldBe 3
    sliced.getValue(0) shouldBe 400L
    sliced.nullSet shouldBe BitSet(1)
    sliced.columnType shouldBe ColumnType.DecimalType(10, 2)
  }

  it should "concat two columns with same precision/scale" in {
    val a = Column.decimal(Array(100L, 200L), 10, 2, BitSet(0))
    val b = Column.decimal(Array(300L, 400L), 10, 2, BitSet(1))
    val result = a.concat(b)
    result.isRight shouldBe true
    val col = result.toOption.get
    col.length shouldBe 4
    col.nullSet shouldBe BitSet(0, 3)
    col.columnType shouldBe ColumnType.DecimalType(10, 2)
  }

  it should "reject concat with different precision" in {
    val a = Column.decimal(Array(100L), 10, 2)
    val b = Column.decimal(Array(200L), 18, 2)
    a.concat(b).isLeft shouldBe true
  }

  it should "reject concat with different scale" in {
    val a = Column.decimal(Array(100L), 10, 2)
    val b = Column.decimal(Array(200L), 10, 4)
    a.concat(b).isLeft shouldBe true
  }

  it should "reject concat with non-decimal column" in {
    val a = Column.decimal(Array(100L), 10, 2)
    val b = Column.long(Array(200L))
    a.concat(b).isLeft shouldBe true
  }

  it should "compareAt correctly" in {
    val col = Column.decimal(Array(100L, 300L, 200L), 10, 2)
    Column.compareAt(col, 0, 1) should be < 0
    Column.compareAt(col, 1, 2) should be > 0
    Column.compareAt(col, 0, 0) shouldBe 0
  }

  it should "compareAt with nulls" in {
    val col = Column.decimal(Array(100L, 0L, 200L), 10, 2, BitSet(1))
    Column.compareAt(col, 1, 0) should be < 0
    Column.compareAt(col, 0, 1) should be > 0
    Column.compareAt(col, 1, 1) shouldBe 0
  }

  it should "sortIndicesByColumn" in {
    val col = Column.decimal(Array(300L, 100L, 200L), 10, 2)
    Column.sortIndicesByColumn(col, 3) shouldBe Array(1, 2, 0)
  }

  it should "create empty via Column.empty" in {
    val col = Column.empty(ColumnType.DecimalType(10, 2))
    col.length shouldBe 0
    col.columnType shouldBe ColumnType.DecimalType(10, 2)
  }

  // ── Schema[Decimal] ─────────────────────────────────────────────────

  "Schema[Decimal]" should "encode and decode with default precision" in {
    val schema = summon[Schema[types.Decimal]]
    schema.columnTypes shouldBe Vector(ColumnType.DecimalType(10, 0))
    val dec = types.Decimal.ofUnscaled(42L)
    schema.encode(dec) shouldBe Vector(42L)
    val decoded = schema.decode(Vector(42L))
    decoded.isRight shouldBe true
    decoded.toOption.get.toUnscaled shouldBe 42L
  }

  it should "decode from BigDecimal" in {
    val schema = summon[Schema[types.Decimal]]
    val decoded = schema.decode(Vector(java.math.BigDecimal.valueOf(42)))
    decoded.isRight shouldBe true
    decoded.toOption.get.toUnscaled shouldBe 42L
  }

  it should "return Left for BigDecimal that overflows Long" in {
    val schema = summon[Schema[types.Decimal]]
    val huge = new java.math.BigDecimal("99999999999999999999")
    val decoded = schema.decode(Vector(huge))
    decoded.isLeft shouldBe true
  }

  "Schema.decimalWith" should "use custom precision and scale" in {
    val schema = Schema.decimalWith(18, 4)
    schema.columnTypes shouldBe Vector(ColumnType.DecimalType(18, 4))
    val dec = types.Decimal.ofUnscaled(12345L)
    schema.encode(dec) shouldBe Vector(12345L)
    schema.decode(Vector(12345L)).toOption.get.toUnscaled shouldBe 12345L
  }

  // ── Decimal opaque type ──────────────────────────────────────────────

  "types.Decimal" should "convert to BigDecimal with scale" in {
    val dec = types.Decimal.ofUnscaled(150L)
    val bd = dec.toBigDecimal(2)
    bd shouldBe java.math.BigDecimal.valueOf(150, 2)
  }

  it should "round-trip through toUnscaled" in {
    val dec = types.Decimal.ofUnscaled(12345L)
    dec.toUnscaled shouldBe 12345L
  }
}
