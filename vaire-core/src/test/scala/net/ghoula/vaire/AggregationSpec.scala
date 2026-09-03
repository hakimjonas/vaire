package net.ghoula.vaire

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.prelude.*

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

    result shouldBe Right(15L)
  }

  it should "return 0 for empty dataset" in {
    val sumExpr = Expr.Sum(Expr.Cell[Int, Int]("value", ColumnIndex(0)))
    val result = ExprInterpreter.evalAggregation(sumExpr, Vector.empty)

    result shouldBe Right(0L)
  }

  "SumDouble" should "compute total of doubles" in {
    val doubleColumn = Column.DoubleColumn(
      Array(1.5, 2.5, 3.0, 4.0, 5.0),
      nulls = scala.collection.immutable.BitSet.empty
    )
    val columns = Vector(doubleColumn)

    val sumExpr = Expr.SumDouble(Expr.Cell[Double, Double]("value", ColumnIndex(0)))
    val result = ExprInterpreter.evalAggregation(sumExpr, columns)

    result shouldBe Right(16.0)
  }

  it should "return 0.0 for empty dataset" in {
    val sumExpr = Expr.SumDouble(Expr.Cell[Double, Double]("value", ColumnIndex(0)))
    val result = ExprInterpreter.evalAggregation(sumExpr, Vector.empty)

    result shouldBe Right(0.0)
  }

  "SumLong" should "compute total of longs" in {
    val longColumn = Column.LongColumn(
      Array(100L, 200L, 300L, 400L, 500L),
      nulls = scala.collection.immutable.BitSet.empty
    )
    val columns = Vector(longColumn)

    val sumExpr = Expr.SumLong(Expr.Cell[Long, Long]("value", ColumnIndex(0)))
    val result = ExprInterpreter.evalAggregation(sumExpr, columns)

    result shouldBe Right(1500L)
  }

  it should "return 0L for empty dataset" in {
    val sumExpr = Expr.SumLong(Expr.Cell[Long, Long]("value", ColumnIndex(0)))
    val result = ExprInterpreter.evalAggregation(sumExpr, Vector.empty)

    result shouldBe Right(0L)
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

    val countResult = ExprInterpreter.evalAggregation(Expr.Count[Int](), columns)

    val sumResult = ExprInterpreter.evalAggregation(
      Expr.Sum(Expr.Cell[Int, Int]("value", ColumnIndex(0))),
      columns
    )

    val maxResult = ExprInterpreter.evalAggregation(
      Expr.Max(Expr.Cell[Int, Int]("value", ColumnIndex(0)), summon[Ordering[Int]]),
      columns
    )

    countResult shouldBe Right(3L)
    sumResult shouldBe Right(60L)
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
      case Right(value: Double) => math.abs(value - 2.0) should be < 0.2
      case other => fail(s"Stddev failed: $other")
    }
  }

  "First" should "return first value" in {
    val intColumn = Column.IntColumn(Array(10, 20, 30), nulls = scala.collection.immutable.BitSet.empty)
    val columns = Vector(intColumn)

    val expr = Expr.First(Expr.Cell[Int, Int]("value", ColumnIndex(0)))
    val result = ExprInterpreter.evalAggregation(expr, columns)

    result shouldBe Right(Some(10))
  }

  it should "return None for empty dataset" in {
    val expr = Expr.First(Expr.Cell[Int, Int]("value", ColumnIndex(0)))
    val result = ExprInterpreter.evalAggregation(expr, Vector.empty)

    result shouldBe Right(None)
  }

  "Collect" should "accumulate all values into a Seq" in {
    val intColumn = Column.IntColumn(Array(3, 1, 4, 1, 5), nulls = scala.collection.immutable.BitSet.empty)
    val columns = Vector(intColumn)

    val expr = Expr.Collect(Expr.Cell[Int, Int]("value", ColumnIndex(0)))
    val result = ExprInterpreter.evalAggregation(expr, columns)

    result shouldBe Right(Seq(3, 1, 4, 1, 5))
  }

  it should "return empty Seq for empty dataset" in {
    val expr = Expr.Collect(Expr.Cell[Int, Int]("value", ColumnIndex(0)))
    val result = ExprInterpreter.evalAggregation(expr, Vector.empty)

    result shouldBe Right(Seq.empty)
  }

  "Dataset.aggregate" should "compute global aggregations without grouping keys" in {
    case class Stats(totalCount: Long, totalSum: Long)
    given Schema[Stats] = Schema.derived

    val intColumn = Column.IntColumn(Array(10, 20, 30, 40, 50), nulls = scala.collection.immutable.BitSet.empty)
    val columns = Vector(intColumn)
    val dataset = Dataset.fromColumns(columns, Schema.intSchema).toOption.get

    val result = dataset
      .aggregate[Stats](
        Vector(
          AggSpec("totalCount", Expr.Count[Int](), ColumnType.LongType),
          AggSpec("totalSum", Expr.Sum(Expr.Cell[Int, Int]("value", ColumnIndex(0))), ColumnType.LongType)
        )
      )
      .collect
      .toOption
      .get

    result should have length 1
    result.head shouldBe Stats(5L, 150L)
  }

  "PercentileApprox" should "compute approximate percentile" in {
    val doubleColumn = Column.DoubleColumn(
      Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0),
      nulls = scala.collection.immutable.BitSet.empty
    )
    val columns = Vector(doubleColumn)

    // Median (0.5)
    val medianExpr = Expr.percentileApprox(Expr.Cell[Double, Double]("value", ColumnIndex(0)), 0.5)
    val median = ExprInterpreter.evalAggregation(medianExpr, columns)
    median match {
      case Right(v: Double) => v should (be >= 5.0 and be <= 6.0)
      case other => fail(s"Percentile failed: $other")
    }

    // p0 (min)
    val p0Expr = Expr.percentileApprox(Expr.Cell[Double, Double]("value", ColumnIndex(0)), 0.0)
    ExprInterpreter.evalAggregation(p0Expr, columns) shouldBe Right(1.0)
  }

  it should "return 0.0 for empty dataset" in {
    val expr = Expr.percentileApprox(Expr.Cell[Double, Double]("value", ColumnIndex(0)), 0.5)
    ExprInterpreter.evalAggregation(expr, Vector.empty) shouldBe Right(0.0)
  }

  "medianApprox" should "be a convenience for percentile 0.5" in {
    val doubleColumn = Column.DoubleColumn(
      Array(1.0, 2.0, 3.0, 4.0, 5.0),
      nulls = scala.collection.immutable.BitSet.empty
    )
    val columns = Vector(doubleColumn)

    val expr = Expr.medianApprox(Expr.Cell[Double, Double]("value", ColumnIndex(0)))
    val result = ExprInterpreter.evalAggregation(expr, columns)
    result shouldBe Right(3.0)
  }

  "MaxBy" should "find value at row with maximum key" in {
    // Two columns: name (string) and salary (int)
    val nameCol = Column.StringColumn(Array("Alice", "Bob", "Carol"), nulls = scala.collection.immutable.BitSet.empty)
    val salaryCol = Column.IntColumn(Array(50, 90, 70), nulls = scala.collection.immutable.BitSet.empty)
    val columns = Vector(nameCol, salaryCol)

    val expr = Expr.maxBy[Any, String, Int](
      Expr.Cell("name", ColumnIndex(0)),
      Expr.Cell("salary", ColumnIndex(1))
    )
    val result = ExprInterpreter.evalAggregation(expr, columns)
    result shouldBe Right(Some("Bob"))
  }

  it should "return None for empty dataset" in {
    val expr = Expr.maxBy[Any, String, Int](
      Expr.Cell("name", ColumnIndex(0)),
      Expr.Cell("salary", ColumnIndex(1))
    )
    ExprInterpreter.evalAggregation(expr, Vector.empty) shouldBe Right(None)
  }

  "MinBy" should "find value at row with minimum key" in {
    val nameCol = Column.StringColumn(Array("Alice", "Bob", "Carol"), nulls = scala.collection.immutable.BitSet.empty)
    val salaryCol = Column.IntColumn(Array(50, 90, 70), nulls = scala.collection.immutable.BitSet.empty)
    val columns = Vector(nameCol, salaryCol)

    val expr = Expr.minBy[Any, String, Int](
      Expr.Cell("name", ColumnIndex(0)),
      Expr.Cell("salary", ColumnIndex(1))
    )
    val result = ExprInterpreter.evalAggregation(expr, columns)
    result shouldBe Right(Some("Alice"))
  }

  "MaxN" should "return top N values" in {
    val intColumn = Column.IntColumn(Array(3, 1, 4, 1, 5, 9, 2), nulls = scala.collection.immutable.BitSet.empty)
    val columns = Vector(intColumn)

    val expr = Expr.maxN(Expr.Cell[Int, Int]("value", ColumnIndex(0)), 3)
    val result = ExprInterpreter.evalAggregation(expr, columns)
    result shouldBe Right(Seq(9, 5, 4))
  }

  it should "return empty Seq for empty dataset" in {
    val expr = Expr.maxN(Expr.Cell[Int, Int]("value", ColumnIndex(0)), 3)
    ExprInterpreter.evalAggregation(expr, Vector.empty) shouldBe Right(Seq.empty)
  }

  "MinN" should "return bottom N values" in {
    val intColumn = Column.IntColumn(Array(3, 1, 4, 1, 5, 9, 2), nulls = scala.collection.immutable.BitSet.empty)
    val columns = Vector(intColumn)

    val expr = Expr.minN(Expr.Cell[Int, Int]("value", ColumnIndex(0)), 3)
    val result = ExprInterpreter.evalAggregation(expr, columns)
    result shouldBe Right(Seq(1, 1, 2))
  }

  "MaxByN" should "return top N values ordered by key" in {
    val nameCol = Column.StringColumn(
      Array("Alice", "Bob", "Carol", "Dave"),
      nulls = scala.collection.immutable.BitSet.empty
    )
    val salaryCol = Column.IntColumn(Array(50, 90, 70, 80), nulls = scala.collection.immutable.BitSet.empty)
    val columns = Vector(nameCol, salaryCol)

    val expr = Expr.maxByN[Any, String, Int](
      Expr.Cell("name", ColumnIndex(0)),
      Expr.Cell("salary", ColumnIndex(1)),
      2
    )
    val result = ExprInterpreter.evalAggregation(expr, columns)
    result shouldBe Right(Seq("Bob", "Dave"))
  }

  it should "return empty Seq for empty dataset" in {
    val expr = Expr.maxByN[Any, String, Int](
      Expr.Cell("name", ColumnIndex(0)),
      Expr.Cell("salary", ColumnIndex(1)),
      2
    )
    ExprInterpreter.evalAggregation(expr, Vector.empty) shouldBe Right(Seq.empty)
  }

  "MinByN" should "return bottom N values ordered by key" in {
    val nameCol = Column.StringColumn(
      Array("Alice", "Bob", "Carol", "Dave"),
      nulls = scala.collection.immutable.BitSet.empty
    )
    val salaryCol = Column.IntColumn(Array(50, 90, 70, 80), nulls = scala.collection.immutable.BitSet.empty)
    val columns = Vector(nameCol, salaryCol)

    val expr = Expr.minByN[Any, String, Int](
      Expr.Cell("name", ColumnIndex(0)),
      Expr.Cell("salary", ColumnIndex(1)),
      2
    )
    val result = ExprInterpreter.evalAggregation(expr, columns)
    result shouldBe Right(Seq("Alice", "Carol"))
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
      case Right(value: Double) => math.abs(value - 1.87) should be < 0.2
      case other => fail(s"StddevPop failed: $other")
    }
  }
}
