package net.ghoula.strongbow

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.BitSet
import scala.language.strictEquality

import net.ghoula.strongbow.prelude.*

class NestedColumnSpec extends AnyFlatSpec with Matchers {

  "ArrayColumn" should "construct via fromValues with typed elements" in {
    val result = Column.fromValues(
      Vector(Seq(1, 2), SqlNull.value, Seq(3)),
      ColumnType.ArrayType(ColumnType.IntType)
    )
    result match {
      case Right(col) =>
        col.columnType shouldBe ColumnType.ArrayType(ColumnType.IntType)
        col.length shouldBe 3
        col.nullSet shouldBe BitSet(1)
        col.getValue(0) shouldBe Seq(1, 2)
        col.getValue(2) shouldBe Seq(3)
      case other => fail(s"Expected array column, got: $other")
    }
  }

  it should "debox into ArrayColumn, never AnyColumn" in {
    val col = Column.fromValues(Vector(Seq("a"), Seq("b", "c")), ColumnType.ArrayType(ColumnType.StringType))
    col match {
      case Right(_: Column.ArrayColumn[?]) => succeed
      case other => fail(s"Expected ArrayColumn, got: ${other.map(_.getClass.getSimpleName)}")
    }
  }

  it should "keep empty arrays distinct from nulls" in {
    val col = Column.fromValues(
      Vector(Seq.empty[Int], SqlNull.value, Seq(1)),
      ColumnType.ArrayType(ColumnType.IntType)
    )
    col match {
      case Right(col) =>
        col.nullSet shouldBe BitSet(1)
        col.getValue(0) shouldBe Seq.empty[Int]
        col.getValue(2) shouldBe Seq(1)
      case other => fail(s"Expected array column, got: $other")
    }
  }

  it should "take a typed prefix without re-boxing" in {
    val col = Column
      .fromValues(
        Vector(Seq(1, 2), Seq(3), SqlNull.value, Seq(4, 5)),
        ColumnType.ArrayType(ColumnType.IntType)
      )
      .toOption
      .get
    val taken = col.take(2)
    taken.length shouldBe 2
    taken.getValue(0) shouldBe Seq(1, 2)
    taken.getValue(1) shouldBe Seq(3)
    taken.columnType shouldBe ColumnType.ArrayType(ColumnType.IntType)
  }

  it should "slice rows preserving offsets" in {
    val col = Column
      .fromValues(Vector(Seq(1, 2), Seq(3), Seq(4, 5, 6)), ColumnType.ArrayType(ColumnType.IntType))
      .toOption
      .get
    val sliced = col.slice(Array(2, 0))
    sliced.length shouldBe 2
    sliced.getValue(0) shouldBe Seq(4, 5, 6)
    sliced.getValue(1) shouldBe Seq(1, 2)
  }

  it should "concat arrays of the same type" in {
    val left = Column.fromValues(Vector(Seq(1, 2)), ColumnType.ArrayType(ColumnType.IntType)).toOption.get
    val right = Column.fromValues(Vector(Seq(3), Seq(4, 5)), ColumnType.ArrayType(ColumnType.IntType)).toOption.get
    left.concat(right) match {
      case Right(merged) =>
        merged.length shouldBe 3
        merged.getValue(0) shouldBe Seq(1, 2)
        merged.getValue(2) shouldBe Seq(4, 5)
      case other => fail(s"Expected concatenated column, got: $other")
    }
  }

  it should "create an empty column for ArrayType" in {
    val col = Column.empty(ColumnType.ArrayType(ColumnType.DoubleType))
    col.length shouldBe 0
    col.columnType shouldBe ColumnType.ArrayType(ColumnType.DoubleType)
  }

  "ArrayColumn GADT refinement" should "refine elements to Column[A] when matched against Column[Seq[A]]" in {
    def elementsOf[A](col: Column[Seq[A]]): Option[Column[A]] = col match {
      case Column.ArrayColumn(elements, _, _) => Some(elements)
      case _ => None
    }

    val col: Column[Seq[Int]] = Column.array(Column.int(Array(1, 2, 3)), Array(0, 3))
    elementsOf(col) match {
      case Some(elements) =>
        elements match {
          case Column.IntColumn(data, _) => assert(data.sameElements(Array(1, 2, 3)))
          case other => fail(s"Expected typed IntColumn refinement, got: $other")
        }
      case None => fail("ArrayColumn pattern did not refine against Column[Seq[Int]]")
    }
  }

  "MapColumn" should "construct via fromValues with typed keys and values" in {
    val col = Column
      .fromValues(
        Vector(Map("a" -> 1L, "b" -> 2L), SqlNull.value, Map("c" -> 3L)),
        ColumnType.MapType(ColumnType.StringType, ColumnType.LongType)
      )
      .toOption
      .get
    col.columnType shouldBe ColumnType.MapType(ColumnType.StringType, ColumnType.LongType)
    col.length shouldBe 3
    col.nullSet shouldBe BitSet(1)
    col.getValue(0) shouldBe Map("a" -> 1L, "b" -> 2L)
    col.getValue(2) shouldBe Map("c" -> 3L)
  }

  it should "debox into MapColumn, never AnyColumn" in {
    val col = Column.fromValues(Vector(Map(1 -> "x")), ColumnType.MapType(ColumnType.IntType, ColumnType.StringType))
    col match {
      case Right(_: Column.MapColumn[?, ?]) => succeed
      case other => fail(s"Expected MapColumn, got: ${other.map(_.getClass.getSimpleName)}")
    }
  }

  it should "take and slice rows" in {
    val col = Column
      .fromValues(
        Vector(Map(1 -> "a"), Map(2 -> "b", 3 -> "c"), Map(4 -> "d")),
        ColumnType.MapType(ColumnType.IntType, ColumnType.StringType)
      )
      .toOption
      .get
    val taken = col.take(2)
    taken.getValue(0) shouldBe Map(1 -> "a")
    taken.getValue(1) shouldBe Map(2 -> "b", 3 -> "c")
    val sliced = col.slice(Array(1))
    sliced.getValue(0) shouldBe Map(2 -> "b", 3 -> "c")
  }

  "StructColumn" should "hold one child column per flattened schema field" in {
    case class Point(x: Int, y: String)
    given Schema[Point] = Schema.derived

    val xCol = Column.int(Array(1, 2))
    val yCol = Column.string(Array("a", "b"))
    val col = Column.struct[Point](Vector(xCol, yCol), summon[Schema[Point]])

    col.columnType shouldBe ColumnType.StructType(
      Vector(("x_value", ColumnType.IntType), ("y_value", ColumnType.StringType))
    )
    col.length shouldBe 2
    col.getValue(0) shouldBe Point(1, "a")
    col.getValue(1) shouldBe Point(2, "b")
  }

  it should "track row-level nulls" in {
    case class Point(x: Int, y: String)
    given Schema[Point] = Schema.derived

    val col = Column.struct[Point](
      Vector(Column.int(Array(1, 0)), Column.string(Array("a", "b"))),
      summon[Schema[Point]],
      BitSet(1)
    )
    col.nullSet shouldBe BitSet(1)
    col.nullSet.contains(0) shouldBe false
  }

  it should "build from values via structFromValues" in {
    case class Point(x: Int, y: String)
    given Schema[Point] = Schema.derived

    val result = Column.structFromValues[Point](
      Vector(Point(1, "a"), Point(2, "b")),
      summon[Schema[Point]],
      Vector(("x_value", ColumnType.IntType), ("y_value", ColumnType.StringType))
    )
    result match {
      case Right(col) =>
        col.columnType shouldBe ColumnType.StructType(
          Vector(("x_value", ColumnType.IntType), ("y_value", ColumnType.StringType))
        )
        col.getValue(0) shouldBe Point(1, "a")
        col.getValue(1) shouldBe Point(2, "b")
      case other => fail(s"Expected struct column, got: $other")
    }
  }

  it should "take and concat rows" in {
    case class Point(x: Int, y: String)
    given Schema[Point] = Schema.derived

    def pointCol(xs: Seq[Point]): Column[Point] =
      Column
        .structFromValues[Point](
          xs.toVector,
          summon[Schema[Point]],
          Vector(("x_value", ColumnType.IntType), ("y_value", ColumnType.StringType))
        )
        .toOption
        .get

    val left = pointCol(Seq(Point(1, "a"), Point(2, "b")))
    val right = pointCol(Seq(Point(3, "c")))
    left.take(1).getValue(0) shouldBe Point(1, "a")
    left.concat(right) match {
      case Right(merged) =>
        merged.length shouldBe 3
        merged.getValue(2) shouldBe Point(3, "c")
        merged.columnType shouldBe left.columnType
      case other => fail(s"Expected concatenated struct column, got: $other")
    }
  }

  "nested arrays" should "recurse into typed child columns" in {
    val col = Column
      .fromValues(
        Vector(Seq(Seq(1, 2), Seq(3)), Seq.empty),
        ColumnType.ArrayType(ColumnType.ArrayType(ColumnType.IntType))
      )
      .toOption
      .get
    col.columnType shouldBe ColumnType.ArrayType(ColumnType.ArrayType(ColumnType.IntType))
    col.getValue(0) shouldBe Seq(Seq(1, 2), Seq(3))
    col.getValue(1) shouldBe Seq.empty[Seq[Int]]
  }

  "sortIndicesByColumn" should "order array rows element-wise" in {
    val col = Column
      .fromValues(Vector(Seq(5), Seq(1, 9), Seq(1, 2)), ColumnType.ArrayType(ColumnType.IntType))
      .toOption
      .get
    val indices = Column.sortIndicesByColumn(col, 3)
    indices shouldBe Array(2, 1, 0)
  }
}
