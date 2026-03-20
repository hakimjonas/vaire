package net.ghoula.strongbow

import org.scalatest.Inside.inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.prelude.*
import net.ghoula.strongbow.types.{ColumnIndex, Date, RowIndex}

class Phase2ExprSpec extends AnyFlatSpec with Matchers {

  private val dblCol = Column.double(Array(4.0, 9.0, 16.0))
  private val dblColumns = Vector(dblCol)
  private val dblCell = Expr.Cell[Double, Double]("v", ColumnIndex(0))

  "sqrt" should "compute square root" in {
    val expr = dblCell.sqrt
    val results = (0 until 3).map(i => ExprInterpreter.evalAt(expr, dblColumns, RowIndex(i)))
    results shouldBe Seq(Right(2.0), Right(3.0), Right(4.0))
  }

  "pow" should "compute exponentiation" in {
    val base = Column.double(Array(2.0, 3.0))
    val exp = Column.double(Array(3.0, 2.0))
    val columns = Vector(base, exp)
    val b = Expr.Cell[Double, Double]("b", ColumnIndex(0))
    val e = Expr.Cell[Double, Double]("e", ColumnIndex(1))
    val expr = b.pow(e)

    val results = (0 until 2).map(i => ExprInterpreter.evalAt(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(8.0), Right(9.0))
  }

  "log" should "compute natural logarithm" in {
    val col = Column.double(Array(1.0, Math.E))
    val columns = Vector(col)
    val cell = Expr.Cell[Double, Double]("v", ColumnIndex(0))
    val expr = cell.log

    val results = (0 until 2).map(i => ExprInterpreter.evalAt(expr, columns, RowIndex(i)))
    results(0) shouldBe Right(0.0)
    inside(results(1)) { case Right(v: Double) => v shouldBe 1.0 +- 1e-10 }
  }

  "log10" should "compute base-10 logarithm" in {
    val col = Column.double(Array(1.0, 100.0, 1000.0))
    val columns = Vector(col)
    val cell = Expr.Cell[Double, Double]("v", ColumnIndex(0))
    val expr = cell.log10

    val results = (0 until 3).map(i => ExprInterpreter.evalAt(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(0.0), Right(2.0), Right(3.0))
  }

  "log2" should "compute base-2 logarithm" in {
    val col = Column.double(Array(1.0, 2.0, 8.0))
    val columns = Vector(col)
    val cell = Expr.Cell[Double, Double]("v", ColumnIndex(0))
    val expr = cell.log2

    val results = (0 until 3).map(i => ExprInterpreter.evalAt(expr, columns, RowIndex(i)))
    results(0) shouldBe Right(0.0)
    inside(results(1)) { case Right(v: Double) => v shouldBe 1.0 +- 1e-10 }
    inside(results(2)) { case Right(v: Double) => v shouldBe 3.0 +- 1e-10 }
  }

  "exp" should "compute e^x" in {
    val col = Column.double(Array(0.0, 1.0))
    val columns = Vector(col)
    val cell = Expr.Cell[Double, Double]("v", ColumnIndex(0))
    val expr = cell.exp

    val results = (0 until 2).map(i => ExprInterpreter.evalAt(expr, columns, RowIndex(i)))
    results(0) shouldBe Right(1.0)
    inside(results(1)) { case Right(v: Double) => v shouldBe Math.E +- 1e-10 }
  }

  "sin" should "compute sine" in {
    val col = Column.double(Array(0.0, Math.PI / 2))
    val columns = Vector(col)
    val cell = Expr.Cell[Double, Double]("v", ColumnIndex(0))
    val expr = cell.sin

    val results = (0 until 2).map(i => ExprInterpreter.evalAt(expr, columns, RowIndex(i)))
    results(0) shouldBe Right(0.0)
    inside(results(1)) { case Right(v: Double) => v shouldBe 1.0 +- 1e-10 }
  }

  "cos" should "compute cosine" in {
    val col = Column.double(Array(0.0, Math.PI))
    val columns = Vector(col)
    val cell = Expr.Cell[Double, Double]("v", ColumnIndex(0))
    val expr = cell.cos

    val results = (0 until 2).map(i => ExprInterpreter.evalAt(expr, columns, RowIndex(i)))
    results(0) shouldBe Right(1.0)
    inside(results(1)) { case Right(v: Double) => v shouldBe -1.0 +- 1e-10 }
  }

  "tan" should "compute tangent" in {
    val col = Column.double(Array(0.0, Math.PI / 4))
    val columns = Vector(col)
    val cell = Expr.Cell[Double, Double]("v", ColumnIndex(0))
    val expr = cell.tan

    val results = (0 until 2).map(i => ExprInterpreter.evalAt(expr, columns, RowIndex(i)))
    results(0) shouldBe Right(0.0)
    inside(results(1)) { case Right(v: Double) => v shouldBe 1.0 +- 1e-10 }
  }

  "asin" should "compute arc sine" in {
    val col = Column.double(Array(0.0, 1.0))
    val columns = Vector(col)
    val cell = Expr.Cell[Double, Double]("v", ColumnIndex(0))
    val expr = cell.asin

    val results = (0 until 2).map(i => ExprInterpreter.evalAt(expr, columns, RowIndex(i)))
    results(0) shouldBe Right(0.0)
    inside(results(1)) { case Right(v: Double) => v shouldBe (Math.PI / 2) +- 1e-10 }
  }

  "acos" should "compute arc cosine" in {
    val col = Column.double(Array(1.0, 0.0))
    val columns = Vector(col)
    val cell = Expr.Cell[Double, Double]("v", ColumnIndex(0))
    val expr = cell.acos

    val results = (0 until 2).map(i => ExprInterpreter.evalAt(expr, columns, RowIndex(i)))
    results(0) shouldBe Right(0.0)
    inside(results(1)) { case Right(v: Double) => v shouldBe (Math.PI / 2) +- 1e-10 }
  }

  "atan" should "compute arc tangent" in {
    val col = Column.double(Array(0.0, 1.0))
    val columns = Vector(col)
    val cell = Expr.Cell[Double, Double]("v", ColumnIndex(0))
    val expr = cell.atan

    val results = (0 until 2).map(i => ExprInterpreter.evalAt(expr, columns, RowIndex(i)))
    results(0) shouldBe Right(0.0)
    inside(results(1)) { case Right(v: Double) => v shouldBe (Math.PI / 4) +- 1e-10 }
  }

  "atan2" should "compute two-argument arc tangent" in {
    val yCol = Column.double(Array(1.0, 0.0))
    val xCol = Column.double(Array(0.0, 1.0))
    val columns = Vector(yCol, xCol)
    val y = Expr.Cell[Double, Double]("y", ColumnIndex(0))
    val x = Expr.Cell[Double, Double]("x", ColumnIndex(1))
    val expr = y.atan2(x)

    val results = (0 until 2).map(i => ExprInterpreter.evalAt(expr, columns, RowIndex(i)))
    inside(results(0)) { case Right(v: Double) => v shouldBe (Math.PI / 2) +- 1e-10 }
    results(1) shouldBe Right(0.0)
  }

  "signum" should "compute sign" in {
    val col = Column.double(Array(-5.0, 0.0, 3.0))
    val columns = Vector(col)
    val cell = Expr.Cell[Double, Double]("v", ColumnIndex(0))
    val expr = cell.signum

    val results = (0 until 3).map(i => ExprInterpreter.evalAt(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(-1.0), Right(0.0), Right(1.0))
  }

  "rand" should "produce deterministic double with seed" in {
    val col = Column.double(Array(0.0))
    val columns = Vector(col)
    val expr = Expr.rand[Double](42L)

    val r1 = ExprInterpreter.evalAt(expr, columns, RowIndex(0))
    val r2 = ExprInterpreter.evalAt(expr, columns, RowIndex(0))
    r1.isRight shouldBe true
    r1 shouldBe r2
  }

  "math outputType" should "return DoubleType for all math functions" in {
    dblCell.sqrt.outputType shouldBe Some(ColumnType.DoubleType)
    dblCell.pow(dblCell).outputType shouldBe Some(ColumnType.DoubleType)
    dblCell.log.outputType shouldBe Some(ColumnType.DoubleType)
    dblCell.log10.outputType shouldBe Some(ColumnType.DoubleType)
    dblCell.log2.outputType shouldBe Some(ColumnType.DoubleType)
    dblCell.exp.outputType shouldBe Some(ColumnType.DoubleType)
    dblCell.sin.outputType shouldBe Some(ColumnType.DoubleType)
    dblCell.cos.outputType shouldBe Some(ColumnType.DoubleType)
    dblCell.tan.outputType shouldBe Some(ColumnType.DoubleType)
    dblCell.asin.outputType shouldBe Some(ColumnType.DoubleType)
    dblCell.acos.outputType shouldBe Some(ColumnType.DoubleType)
    dblCell.atan.outputType shouldBe Some(ColumnType.DoubleType)
    dblCell.atan2(dblCell).outputType shouldBe Some(ColumnType.DoubleType)
    dblCell.signum.outputType shouldBe Some(ColumnType.DoubleType)
    Expr.rand[Double](42L).outputType shouldBe Some(ColumnType.DoubleType)
  }

  private val dateCol = Column.date(
    Array(
      Date(2026, 3, 19).toEpochDay.toInt,
      Date(2026, 1, 1).toEpochDay.toInt,
      Date(2025, 12, 31).toEpochDay.toInt
    )
  )
  private val dateColumns = Vector(dateCol)
  private val dateCell = Expr.Cell[Date, Date]("d", ColumnIndex(0))

  "dayOfWeek" should "return day of week (Sunday=1, Saturday=7)" in {
    val expr = dateCell.dayOfWeek
    val results = (0 until 3).map(i => ExprInterpreter.evalAt(expr, dateColumns, RowIndex(i)))
    results(0) shouldBe Right(5)
    results(1) shouldBe Right(5)
    results(2) shouldBe Right(4)
  }

  "dayOfYear" should "return day of year" in {
    val expr = dateCell.dayOfYear
    val results = (0 until 3).map(i => ExprInterpreter.evalAt(expr, dateColumns, RowIndex(i)))
    results(0) shouldBe Right(78)
    results(1) shouldBe Right(1)
    results(2) shouldBe Right(365)
  }

  "weekOfYear" should "return ISO week of year" in {
    val expr = dateCell.weekOfYear
    val r = ExprInterpreter.evalAt(expr, dateColumns, RowIndex(0))
    inside(r) { case Right(v: Int) =>
      v should be > 0
      v should be <= 53
    }
  }

  "quarter" should "return quarter of year" in {
    val expr = dateCell.quarter
    val results = (0 until 3).map(i => ExprInterpreter.evalAt(expr, dateColumns, RowIndex(i)))
    results(0) shouldBe Right(1)
    results(1) shouldBe Right(1)
    results(2) shouldBe Right(4)
  }

  "lastDay" should "return last day of month" in {
    val expr = dateCell.lastDay
    val r = ExprInterpreter.evalAt(expr, dateColumns, RowIndex(0))
    r shouldBe Right(Date(2026, 3, 31))
  }

  "nextDay" should "return next occurrence of given day of week" in {
    val expr = dateCell.nextDay("MONDAY")
    val r = ExprInterpreter.evalAt(expr, dateColumns, RowIndex(0))
    inside(r) { case Right(v: java.time.LocalDate) =>
      val d = Date.fromLocalDate(v)
      d.getDayOfWeek.getValue shouldBe 1
      d.isAfter(Date(2026, 3, 19)) shouldBe true
    }
  }

  "monthsBetween" should "compute approximate months between two dates" in {
    val dateCol2 = Column.date(
      Array(
        Date(2026, 6, 19).toEpochDay.toInt,
        Date(2026, 6, 19).toEpochDay.toInt,
        Date(2026, 6, 19).toEpochDay.toInt
      )
    )
    val columns = Vector(dateCol, dateCol2)
    val cell2 = Expr.Cell[Date, Date]("d2", ColumnIndex(1))
    val expr = cell2.monthsBetween(dateCell)

    val r = ExprInterpreter.evalAt(expr, columns, RowIndex(0))
    inside(r) { case Right(v: Double) => v shouldBe 3.0 +- 0.1 }
  }

  "dateTrunc" should "truncate to year" in {
    val expr = dateCell.dateTrunc("year")
    val r = ExprInterpreter.evalAt(expr, dateColumns, RowIndex(0))
    r shouldBe Right(Date(2026, 1, 1))
  }

  it should "truncate to month" in {
    val expr = dateCell.dateTrunc("month")
    val r = ExprInterpreter.evalAt(expr, dateColumns, RowIndex(0))
    r shouldBe Right(Date(2026, 3, 1))
  }

  it should "truncate to quarter" in {
    val expr = dateCell.dateTrunc("quarter")
    val r = ExprInterpreter.evalAt(expr, dateColumns, RowIndex(0))
    r shouldBe Right(Date(2026, 1, 1))
  }

  "dateFormat" should "format date as string" in {
    val expr = dateCell.dateFormat("yyyy/MM/dd")
    val r = ExprInterpreter.evalAt(expr, dateColumns, RowIndex(0))
    r shouldBe Right("2026/03/19")
  }

  "makeDate" should "construct date from year, month, day" in {
    val yearCol = Column.int(Array(2026))
    val monthCol = Column.int(Array(3))
    val dayCol = Column.int(Array(19))
    val columns = Vector(yearCol, monthCol, dayCol)
    val y = Expr.Cell[Int, Int]("y", ColumnIndex(0))
    val m = Expr.Cell[Int, Int]("m", ColumnIndex(1))
    val d = Expr.Cell[Int, Int]("d", ColumnIndex(2))
    val expr = Expr.makeDate[Int](y, m, d)

    val r = ExprInterpreter.evalAt(expr, columns, RowIndex(0))
    r shouldBe Right(Date(2026, 3, 19))
  }

  "date outputType" should "return correct types" in {
    dateCell.dayOfWeek.outputType shouldBe Some(ColumnType.IntType)
    dateCell.dayOfYear.outputType shouldBe Some(ColumnType.IntType)
    dateCell.weekOfYear.outputType shouldBe Some(ColumnType.IntType)
    dateCell.quarter.outputType shouldBe Some(ColumnType.IntType)
    dateCell.lastDay.outputType shouldBe Some(ColumnType.DateType)
    dateCell.nextDay("MONDAY").outputType shouldBe Some(ColumnType.DateType)
    dateCell.monthsBetween(dateCell).outputType shouldBe Some(ColumnType.DoubleType)
    dateCell.dateTrunc("year").outputType shouldBe Some(ColumnType.DateType)
    dateCell.dateFormat("yyyy").outputType shouldBe Some(ColumnType.StringType)
  }

  "aggregation expressions" should "return unsupported in row-level eval" in {
    val col = Column.double(Array(1.0))
    val columns = Vector(col)
    val cell = Expr.Cell[Double, Double]("v", ColumnIndex(0))

    Expr.variance[Double](cell).outputType shouldBe Some(ColumnType.DoubleType)
    Expr.variancePop[Double](cell).outputType shouldBe Some(ColumnType.DoubleType)
    Expr.approxCountDistinct[Double, Double](cell).outputType shouldBe Some(ColumnType.LongType)
    Expr.collectSet[Double, Double](cell).outputType shouldBe Some(ColumnType.AnyType)
    Expr.median[Double](cell).outputType shouldBe Some(ColumnType.DoubleType)

    ExprInterpreter.evalAt(Expr.variance[Double](cell), columns, RowIndex(0)).isLeft shouldBe true
    ExprInterpreter.evalAt(Expr.variancePop[Double](cell), columns, RowIndex(0)).isLeft shouldBe true
  }

  "boolAnd/boolOr outputType" should "return BooleanType" in {
    val boolCell = Expr.Cell[Boolean, Boolean]("b", ColumnIndex(0))
    Expr.boolAnd[Boolean](boolCell).outputType shouldBe Some(ColumnType.BooleanType)
    Expr.boolOr[Boolean](boolCell).outputType shouldBe Some(ColumnType.BooleanType)
  }

  "corr/covar outputType" should "return DoubleType" in {
    Expr.corr[Double](dblCell, dblCell).outputType shouldBe Some(ColumnType.DoubleType)
    Expr.covarSamp[Double](dblCell, dblCell).outputType shouldBe Some(ColumnType.DoubleType)
    Expr.covarPop[Double](dblCell, dblCell).outputType shouldBe Some(ColumnType.DoubleType)
  }

  "window function expressions" should "return unsupported in row-level eval" in {
    val col = Column.int(Array(1))
    val columns = Vector(col)

    Expr.NTile[Int](4).outputType shouldBe Some(ColumnType.IntType)
    Expr.CumeDist[Int]().outputType shouldBe Some(ColumnType.DoubleType)
    Expr.PercentRank[Int]().outputType shouldBe Some(ColumnType.DoubleType)

    ExprInterpreter.evalAt(Expr.NTile[Int](4), columns, RowIndex(0)).isLeft shouldBe true
    ExprInterpreter.evalAt(Expr.CumeDist[Int](), columns, RowIndex(0)).isLeft shouldBe true
    ExprInterpreter.evalAt(Expr.PercentRank[Int](), columns, RowIndex(0)).isLeft shouldBe true
  }

  "math functions" should "compose with each other" in {
    val col = Column.double(Array(1.0))
    val columns = Vector(col)
    val cell = Expr.Cell[Double, Double]("v", ColumnIndex(0))
    val expr = cell.exp.log

    val r = ExprInterpreter.evalAt(expr, columns, RowIndex(0))
    inside(r) { case Right(v: Double) => v shouldBe 1.0 +- 1e-10 }
  }

  "date functions" should "compose with each other" in {
    val expr = dateCell.lastDay.day
    val r = ExprInterpreter.evalAt(expr, dateColumns, RowIndex(0))
    r shouldBe Right(31)
  }
}
