package net.ghoula.strongbow

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.types.ColumnIndex

class SemiJoinSpec extends AnyFlatSpec with Matchers {

  private def makeLeft: Dataset[Int] = {
    val col = Column.int(Array(1, 2, 3, 4, 5))
    Dataset.fromColumns(Vector(col), Schema.intSchema).toOption.get
  }

  private def makeRight: Dataset[Int] = {
    val col = Column.int(Array(2, 4, 6))
    Dataset.fromColumns(Vector(col), Schema.intSchema).toOption.get
  }

  "LeftSemiJoinOn" should "implement IN via semi-join" in {
    val left = makeLeft
    val right = makeRight

    val result = left.semiJoinOn(
      right,
      Expr.Cell[Int, Int]("value", ColumnIndex(0)),
      Expr.Cell[Int, Int]("value", ColumnIndex(0)),
      ColumnType.IntType,
      ColumnType.IntType
    )

    val values = DatasetInterpreter.execute(result).toOption.get.toVectorUnsafe.sorted
    values shouldBe Vector(2, 4)
  }

  it should "return empty when right side is empty" in {
    val left = makeLeft
    val emptyCol = Column.int(Array.empty[Int])
    val right = Dataset.fromColumns(Vector(emptyCol), Schema.intSchema).toOption.get

    val result = left.semiJoinOn(
      right,
      Expr.Cell[Int, Int]("value", ColumnIndex(0)),
      Expr.Cell[Int, Int]("value", ColumnIndex(0)),
      ColumnType.IntType,
      ColumnType.IntType
    )

    val values = DatasetInterpreter.execute(result).toOption.get.toVectorUnsafe
    values shouldBe empty
  }

  it should "return all rows when all match" in {
    val left = makeLeft
    val rightCol = Column.int(Array(1, 2, 3, 4, 5))
    val right = Dataset.fromColumns(Vector(rightCol), Schema.intSchema).toOption.get

    val result = left.semiJoinOn(
      right,
      Expr.Cell[Int, Int]("value", ColumnIndex(0)),
      Expr.Cell[Int, Int]("value", ColumnIndex(0)),
      ColumnType.IntType,
      ColumnType.IntType
    )

    val values = DatasetInterpreter.execute(result).toOption.get.toVectorUnsafe.sorted
    values shouldBe Vector(1, 2, 3, 4, 5)
  }

  it should "return empty when no rows match" in {
    val left = makeLeft
    val rightCol = Column.int(Array(10, 20, 30))
    val right = Dataset.fromColumns(Vector(rightCol), Schema.intSchema).toOption.get

    val result = left.semiJoinOn(
      right,
      Expr.Cell[Int, Int]("value", ColumnIndex(0)),
      Expr.Cell[Int, Int]("value", ColumnIndex(0)),
      ColumnType.IntType,
      ColumnType.IntType
    )

    val values = DatasetInterpreter.execute(result).toOption.get.toVectorUnsafe
    values shouldBe empty
  }

  "NOT IN via anti-join" should "return rows not in right" in {
    val left = makeLeft
    val right = makeRight

    val result = left.antiJoinOn(
      right,
      Expr.Cell[Int, Int]("value", ColumnIndex(0)),
      Expr.Cell[Int, Int]("value", ColumnIndex(0)),
      ColumnType.IntType,
      ColumnType.IntType
    )

    val values = DatasetInterpreter.execute(result).toOption.get.toVectorUnsafe.sorted
    values shouldBe Vector(1, 3, 5)
  }

  "Semi-join with case class" should "work with multi-column types" in {
    case class Order(orderId: Int, status: String)
    given Schema[Order] = Schema.derived

    case class LineItem(orderId: Int, amount: Double)
    given Schema[LineItem] = Schema.derived

    val orderIdCol = Column.int(Array(1, 2, 3, 4))
    val statusCol = Column.string(Array("open", "closed", "open", "closed"))
    val orders = Dataset.fromColumns(Vector(orderIdCol, statusCol), summon[Schema[Order]]).toOption.get

    val liOrderIdCol = Column.int(Array(2, 3, 3))
    val liAmountCol = Column.double(Array(100.0, 200.0, 300.0))
    val lineItems = Dataset.fromColumns(Vector(liOrderIdCol, liAmountCol), summon[Schema[LineItem]]).toOption.get

    // Orders that have at least one line item (EXISTS)
    val result = orders.semiJoinOn(
      lineItems,
      Expr.Cell[Order, Int]("orderId", ColumnIndex(0)),
      Expr.Cell[LineItem, Int]("orderId", ColumnIndex(0)),
      ColumnType.IntType,
      ColumnType.IntType
    )

    val values = DatasetInterpreter.execute(result).toOption.get.toVectorUnsafe
    values.map(_.orderId).sorted shouldBe Vector(2, 3)
  }
}
