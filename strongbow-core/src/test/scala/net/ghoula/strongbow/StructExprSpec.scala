package net.ghoula.strongbow

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.language.strictEquality

import net.ghoula.strongbow.prelude.*

class StructExprSpec extends AnyFlatSpec with Matchers {

  case class Row(id: Int, score: Int, label: String)
  given Schema[Row] = Schema.derived

  case class Point(x: Int, y: String)
  given Schema[Point] = Schema.derived

  case class OnlyX(x: Int)
  given Schema[OnlyX] = Schema.derived

  val rows: Vector[Row] = Vector(
    Row(1, 10, "a"),
    Row(2, 20, "b"),
    Row(3, 30, "c")
  )

  lazy val dataset: Dataset[Row] = MaterializedDataset.fromVector(rows) match {
    case Right(md) => Dataset.Root(InMemorySource(md.columns), summon[Schema[Row]])
    case Left(err) => fail(s"Dataset creation failed: $err")
  }

  val scoreCell: Expr[Row, Int] = Expr.cell("score_value", ColumnIndex(1))
  val labelCell: Expr[Row, String] = Expr.cell("label_value", ColumnIndex(2))

  val structExpr: Expr[Row, Point] =
    Expr.struct[Row, Point](
      ("x_value", scoreCell, ColumnType.IntType),
      ("y_value", labelCell, ColumnType.StringType)
    )

  val structColumnType: ColumnType = ColumnType.StructType(
    Vector(("x_value", ColumnType.IntType), ("y_value", ColumnType.StringType))
  )

  "Expr.Struct" should "report the struct's flattened column type" in {
    structExpr.outputType shouldBe Some(structColumnType)
  }

  it should "evaluate to a StructColumn in-memory" in {
    val projected = dataset.selectAs[Point](("p", structExpr, structColumnType))(using Schema.structColumn[Point])
    DatasetInterpreter.execute(projected) match {
      case Right(md) =>
        md.columnCount shouldBe 1
        md.columns.head.columnType shouldBe structColumnType
        md.columns.head match {
          case _: Column.StructColumn[?] => succeed
          case other => fail(s"Expected StructColumn, got: ${other.getClass.getSimpleName}")
        }
        md.toVectorUnsafe shouldBe Vector(Point(10, "a"), Point(20, "b"), Point(30, "c"))
      case other => fail(s"Execution failed: $other")
    }
  }

  "Expr.GetField" should "extract a field column in-memory" in {
    val getFieldExpr: Expr[Row, Int] = structExpr.getField[Int](ColumnIndex(0), "x_value")
    val projected = dataset.selectAs[OnlyX](("x", getFieldExpr, ColumnType.IntType))
    DatasetInterpreter.execute(projected) match {
      case Right(md) =>
        md.columns.head.columnType shouldBe ColumnType.IntType
        md.toVectorUnsafe shouldBe Vector(OnlyX(10), OnlyX(20), OnlyX(30))
      case other => fail(s"Execution failed: $other")
    }
  }

  it should "extract by field position, not global position" in {
    val getFieldExpr: Expr[Row, String] = structExpr.getField[String](ColumnIndex(1), "y_value")
    val projected = dataset.selectAs[OnlyY](("y", getFieldExpr, ColumnType.StringType))
    DatasetInterpreter.execute(projected) match {
      case Right(md) =>
        md.toVectorUnsafe shouldBe Vector(OnlyY("a"), OnlyY("b"), OnlyY("c"))
      case other => fail(s"Execution failed: $other")
    }
  }

  it should "reject out-of-bounds field indexes" in {
    val getFieldExpr: Expr[Row, Int] = structExpr.getField[Int](ColumnIndex(7), "x_value")
    val projected = dataset.selectAs[OnlyX](("x", getFieldExpr, ColumnType.IntType))
    DatasetInterpreter.execute(projected).isLeft shouldBe true
  }

  case class OnlyY(y: String)
  given Schema[OnlyY] = Schema.derived
}
