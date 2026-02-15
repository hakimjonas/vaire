package net.ghoula.strongbow.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.{Column, ColumnType, Dataset, DatasetInterpreter, Expr, Schema}
import net.ghoula.strongbow.types.ColumnIndex

class SparkSemiJoinSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  // Schema[Int] has column name "value", so Cell needs "value"
  "SparkInterpreter LeftSemiJoinOn" should "produce same results as in-memory" in {
    val leftCol = Column.int(Array(1, 2, 3, 4, 5))
    val left = Dataset.fromColumns(Vector(leftCol), Schema.intSchema).toOption.get

    val rightCol = Column.int(Array(2, 4, 6))
    val right = Dataset.fromColumns(Vector(rightCol), Schema.intSchema).toOption.get

    val result = left.semiJoinOn(
      right,
      Expr.Cell[Int, Int]("value", ColumnIndex(0)),
      Expr.Cell[Int, Int]("value", ColumnIndex(0)),
      ColumnType.IntType,
      ColumnType.IntType
    )

    val inMemory = DatasetInterpreter.execute(result).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(result).map(_.toVectorUnsafe.sorted)

    sparkResult shouldBe inMemory
  }

  it should "handle empty right side" in {
    val leftCol = Column.int(Array(1, 2, 3))
    val left = Dataset.fromColumns(Vector(leftCol), Schema.intSchema).toOption.get

    val rightCol = Column.int(Array.empty[Int])
    val right = Dataset.fromColumns(Vector(rightCol), Schema.intSchema).toOption.get

    val result = left.semiJoinOn(
      right,
      Expr.Cell[Int, Int]("value", ColumnIndex(0)),
      Expr.Cell[Int, Int]("value", ColumnIndex(0)),
      ColumnType.IntType,
      ColumnType.IntType
    )

    val inMemory = DatasetInterpreter.execute(result).map(_.toVectorUnsafe)
    val sparkResult = sparkInterpreter.execute(result).map(_.toVectorUnsafe)

    sparkResult shouldBe inMemory
  }
}
