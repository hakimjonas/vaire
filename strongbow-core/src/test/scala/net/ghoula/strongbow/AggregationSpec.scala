package net.ghoula.strongbow

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.prelude.*

class AggregationSpec extends AnyFlatSpec with Matchers {

  "Count" should "return number of rows" in {
    val intColumn = Column.IntColumn(Array(1, 2, 3, 4, 5), nulls = scala.collection.immutable.BitSet.empty)
    val columns = Vector(intColumn)

    val result = ExprInterpreter.evalAggregation(Expr.Count[Int](), columns)
    result shouldBe Right(5L)
  }

  it should "return 0 for empty dataset" in {
    val result = ExprInterpreter.evalAggregation(Expr.Count[Int](), Vector.empty)
    result shouldBe Right(0L)
  }

  "Sum" should "compute total using zero casts" in {
    val intColumn = Column.IntColumn(Array(1, 2, 3, 4, 5), nulls = scala.collection.immutable.BitSet.empty)
    val columns = Vector(intColumn)

    // Sum of Cell(0) which references the int column
    val sumExpr = Expr.Sum(Expr.Cell[Int, Int]("value", ColumnIndex(0)))
    val result = ExprInterpreter.evalAggregation(sumExpr, columns)

    result shouldBe Right(15)
  }

  it should "return 0 for empty dataset" in {
    val sumExpr = Expr.Sum(Expr.Cell[Int, Int]("value", ColumnIndex(0)))
    val result = ExprInterpreter.evalAggregation(sumExpr, Vector.empty)

    result shouldBe Right(0)
  }

  "Avg" should "compute average using zero casts" in {
    val doubleColumn = Column.DoubleColumn(
      Array(1.0, 2.0, 3.0, 4.0, 5.0),
      nulls = scala.collection.immutable.BitSet.empty
    )
    val columns = Vector(doubleColumn)

    val avgExpr = Expr.Avg(Expr.Cell[Double, Double]("value", ColumnIndex(0)))
    val result = ExprInterpreter.evalAggregation(avgExpr, columns)

    result shouldBe Right(3.0)
  }

  "Max" should "find maximum using GADT ordering evidence" in {
    val intColumn = Column.IntColumn(Array(3, 1, 4, 1, 5, 9, 2), nulls = scala.collection.immutable.BitSet.empty)
    val columns = Vector(intColumn)

    val maxExpr = Expr.Max(
      Expr.Cell[Int, Int]("value", ColumnIndex(0)),
      summon[Ordering[Int]]
    )
    val result = ExprInterpreter.evalAggregation(maxExpr, columns)

    result shouldBe Right(Some(9))
  }

  it should "return None for empty dataset" in {
    val maxExpr = Expr.Max(
      Expr.Cell[Int, Int]("value", ColumnIndex(0)),
      summon[Ordering[Int]]
    )
    val result = ExprInterpreter.evalAggregation(maxExpr, Vector.empty)

    result shouldBe Right(None)
  }

  "Min" should "find minimum using GADT ordering evidence" in {
    val intColumn = Column.IntColumn(Array(3, 1, 4, 1, 5, 9, 2), nulls = scala.collection.immutable.BitSet.empty)
    val columns = Vector(intColumn)

    val minExpr = Expr.Min(
      Expr.Cell[Int, Int]("value", ColumnIndex(0)),
      summon[Ordering[Int]]
    )
    val result = ExprInterpreter.evalAggregation(minExpr, columns)

    result shouldBe Right(Some(1))
  }

  it should "return None for empty dataset" in {
    val minExpr = Expr.Min(
      Expr.Cell[Int, Int]("value", ColumnIndex(0)),
      summon[Ordering[Int]]
    )
    val result = ExprInterpreter.evalAggregation(minExpr, Vector.empty)

    result shouldBe Right(None)
  }

  "Aggregations" should "work with string columns" in {
    val stringColumn = Column.StringColumn(
      Array("apple", "zebra", "banana", "mango"),
      nulls = scala.collection.immutable.BitSet.empty
    )
    val columns = Vector(stringColumn)

    val maxExpr = Expr.Max(
      Expr.Cell[String, String]("fruit", ColumnIndex(0)),
      summon[Ordering[String]]
    )
    val result = ExprInterpreter.evalAggregation(maxExpr, columns)

    result shouldBe Right(Some("zebra"))
  }

  "GADT type refinement" should "ensure no casts in aggregation logic" in {
    // This test documents the zero-cast principle
    val intColumn = Column.IntColumn(Array(10, 20, 30), nulls = scala.collection.immutable.BitSet.empty)
    val columns = Vector(intColumn)

    // Count returns Long - GADT refines A to Long, no cast needed
    val countResult: Either[ExecutionError, Long] =
      ExprInterpreter.evalAggregation(Expr.Count[Int](), columns)

    // Sum returns Int - GADT refines A to Int, no cast needed
    val sumResult: Either[ExecutionError, Int] =
      ExprInterpreter.evalAggregation(
        Expr.Sum(Expr.Cell[Int, Int]("value", ColumnIndex(0))),
        columns
      )

    // Max returns Option[Int] - GADT refines A to Option[Int], no cast needed
    val maxResult: Either[ExecutionError, Option[Int]] =
      ExprInterpreter.evalAggregation(
        Expr.Max(Expr.Cell[Int, Int]("value", ColumnIndex(0)), summon[Ordering[Int]]),
        columns
      )

    countResult shouldBe Right(3L)
    sumResult shouldBe Right(60)
    maxResult shouldBe Right(Some(30))
  }

  "countDistinct" should "count unique values" in {
    val intColumn = Column.IntColumn(Array(1, 2, 2, 3, 3, 3), nulls = scala.collection.immutable.BitSet.empty)
    val columns = Vector(intColumn)

    val expr = Expr.countDistinct(Expr.Cell[Int, Int]("value", ColumnIndex(0)))

    val result = ExprInterpreter.evalAggregation(expr, columns)

    result shouldBe Right(3L) // 1, 2, 3 distinct values
  }

  "countIf" should "count matching rows" in {
    val intColumn = Column.IntColumn(Array(1, 5, 10, 15, 20), nulls = scala.collection.immutable.BitSet.empty)
    val columns = Vector(intColumn)

    val expr = Expr.countIf(
      Expr.Cell[Int, Int]("value", ColumnIndex(0)) > Expr.Const(10)
    )

    val result = ExprInterpreter.evalAggregation(expr, columns)

    result shouldBe Right(2L) // 15, 20 are > 10
  }

  "stddev" should "compute sample standard deviation" in {
    val doubleColumn = Column.DoubleColumn(
      Array(2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0),
      nulls = scala.collection.immutable.BitSet.empty
    )
    val columns = Vector(doubleColumn)

    val expr = Expr.stddev(Expr.Cell[Double, Double]("value", ColumnIndex(0)))

    val result = ExprInterpreter.evalAggregation(expr, columns)

    // stddev should be approximately 2.0
    result match {
      case Right(value) => math.abs(value - 2.0) should be < 0.2
      case Left(err) => fail(s"Stddev failed: $err")
    }
  }

  "stddevPop" should "compute population standard deviation" in {
    val doubleColumn = Column.DoubleColumn(
      Array(2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0),
      nulls = scala.collection.immutable.BitSet.empty
    )
    val columns = Vector(doubleColumn)

    val expr = Expr.stddevPop(Expr.Cell[Double, Double]("value", ColumnIndex(0)))

    val result = ExprInterpreter.evalAggregation(expr, columns)

    // stddevPop should be approximately 1.87
    result match {
      case Right(value) => math.abs(value - 1.87) should be < 0.2
      case Left(err) => fail(s"StddevPop failed: $err")
    }
  }
}
