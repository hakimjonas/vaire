package net.ghoula.strongbow

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.prelude.*
import net.ghoula.strongbow.types.{ColumnIndex, Date}

class DateExprSpec extends AnyFlatSpec with Matchers {

  private def eval[Row, A](
    expr: Expr[Row, A],
    columns: Vector[Column[?]],
    idx: Int
  ): Either[errors.ExecutionError, Any] = {
    val effectiveColumns =
      if (columns.isEmpty || columns.head.length == 0) Vector(Column.int(Array(0)))
      else columns
    val colType = ExprInterpreter.inferExprColumnType(expr, effectiveColumns)
    ExprInterpreter.evalColumn(expr, effectiveColumns, colType).map(_.getValue(idx))
  }

  private def makeDateColumn(dates: Date*): Column[Date] = {
    Column.date(dates.map(_.toEpochDay.toInt).toArray)
  }

  "DateColumn" should "store and retrieve dates correctly" in {
    val d1 = Date(2024, 1, 15)
    val d2 = Date(2024, 6, 30)
    val col = makeDateColumn(d1, d2)
    col.length shouldBe 2
    col.columnType shouldBe ColumnType.DateType
    col.getValue(0) shouldBe d1
    col.getValue(1) shouldBe d2
  }

  "DateAddDays" should "add days to a date" in {
    val d = Date(2024, 1, 15)
    val dateCol = makeDateColumn(d)
    val daysCol = Column.int(Array(10))
    val columns = Vector(dateCol, daysCol)

    val expr = Expr.DateAddDays(
      Expr.Cell[Any, Date]("date", ColumnIndex(0)),
      Expr.Cell[Any, Int]("days", ColumnIndex(1))
    )
    val result = eval(expr, columns, 0)
    result shouldBe Right(Date(2024, 1, 25))
  }

  "DateSubDays" should "subtract days from a date" in {
    val d = Date(2024, 1, 15)
    val dateCol = makeDateColumn(d)
    val daysCol = Column.int(Array(5))
    val columns = Vector(dateCol, daysCol)

    val expr = Expr.DateSubDays(
      Expr.Cell[Any, Date]("date", ColumnIndex(0)),
      Expr.Cell[Any, Int]("days", ColumnIndex(1))
    )
    val result = eval(expr, columns, 0)
    result shouldBe Right(Date(2024, 1, 10))
  }

  "DateAddMonths" should "add months to a date" in {
    val d = Date(2024, 1, 31)
    val dateCol = makeDateColumn(d)
    val monthsCol = Column.int(Array(1))
    val columns = Vector(dateCol, monthsCol)

    val expr = Expr.DateAddMonths(
      Expr.Cell[Any, Date]("date", ColumnIndex(0)),
      Expr.Cell[Any, Int]("months", ColumnIndex(1))
    )
    val result = eval(expr, columns, 0)
    result shouldBe Right(Date(2024, 2, 29))
  }

  "DateDiff" should "compute difference in days" in {
    val d1 = Date(2024, 1, 15)
    val d2 = Date(2024, 1, 10)
    val col1 = makeDateColumn(d1)
    val col2 = makeDateColumn(d2)
    val columns = Vector(col1, col2)

    val expr = Expr.DateDiff(
      Expr.Cell[Any, Date]("d1", ColumnIndex(0)),
      Expr.Cell[Any, Date]("d2", ColumnIndex(1))
    )
    val result = eval(expr, columns, 0)
    result shouldBe Right(5)
  }

  "ExtractYear" should "extract year from date" in {
    val d = Date(2024, 6, 15)
    val dateCol = makeDateColumn(d)
    val columns = Vector(dateCol)

    val expr = Expr.ExtractYear(Expr.Cell[Any, Date]("date", ColumnIndex(0)))
    val result = eval(expr, columns, 0)
    result shouldBe Right(2024)
  }

  "ExtractMonth" should "extract month from date" in {
    val d = Date(2024, 6, 15)
    val dateCol = makeDateColumn(d)
    val columns = Vector(dateCol)

    val expr = Expr.ExtractMonth(Expr.Cell[Any, Date]("date", ColumnIndex(0)))
    val result = eval(expr, columns, 0)
    result shouldBe Right(6)
  }

  "ExtractDay" should "extract day from date" in {
    val d = Date(2024, 6, 15)
    val dateCol = makeDateColumn(d)
    val columns = Vector(dateCol)

    val expr = Expr.ExtractDay(Expr.Cell[Any, Date]("date", ColumnIndex(0)))
    val result = eval(expr, columns, 0)
    result shouldBe Right(15)
  }

  "Date comparisons" should "work with Gt/Lt/Gte/Lte" in {
    val d1 = Date(2024, 6, 15)
    val d2 = Date(2024, 1, 10)
    val col1 = makeDateColumn(d1)
    val col2 = makeDateColumn(d2)
    val columns = Vector(col1, col2)

    val cell1 = Expr.Cell[Any, Date]("d1", ColumnIndex(0))
    val cell2 = Expr.Cell[Any, Date]("d2", ColumnIndex(1))

    val gtExpr = Expr.Gt(cell1, cell2, summon[Ordering[Date]])
    eval(gtExpr, columns, 0) shouldBe Right(true)

    val ltExpr = Expr.Lt(cell1, cell2, summon[Ordering[Date]])
    eval(ltExpr, columns, 0) shouldBe Right(false)
  }

  "Date extension methods" should "work on Expr[Row, Date]" in {
    val d = Date(2024, 3, 15)
    val dateCol = makeDateColumn(d)
    val daysCol = Column.int(Array(7))
    val columns = Vector(dateCol, daysCol)

    val dateExpr = Expr.Cell[Any, Date]("date", ColumnIndex(0))
    val daysExpr = Expr.Cell[Any, Int]("days", ColumnIndex(1))

    val addResult = eval(dateExpr.addDays(daysExpr), columns, 0)
    addResult shouldBe Right(Date(2024, 3, 22))

    val yearResult = eval(dateExpr.year, columns, 0)
    yearResult shouldBe Right(2024)

    val monthResult = eval(dateExpr.month, columns, 0)
    monthResult shouldBe Right(3)
  }

  "DateColumn" should "support slice operations" in {
    val d1 = Date(2024, 1, 1)
    val d2 = Date(2024, 6, 15)
    val d3 = Date(2024, 12, 31)
    val col = makeDateColumn(d1, d2, d3)

    val sliced = col.slice(Array(0, 2))
    sliced.length shouldBe 2
    sliced.getValue(0) shouldBe d1
    sliced.getValue(1) shouldBe d3
  }

  it should "support concat operations" in {
    val d1 = Date(2024, 1, 1)
    val d2 = Date(2024, 6, 15)
    val col1 = makeDateColumn(d1)
    val col2 = makeDateColumn(d2)

    val result = col1.concat(col2)
    result match {
      case Right(combined) =>
        combined.length shouldBe 2
        combined.getValue(0) shouldBe d1
        combined.getValue(1) shouldBe d2
      case Left(err) => fail(s"concat failed: $err")
    }
  }

  it should "support Column.fromValues" in {
    val d1 = Date(2024, 1, 1)
    val d2 = Date(2024, 6, 15)

    val col = Column.fromValues(Vector(d1, d2), ColumnType.DateType)
    col match {
      case Right(c) =>
        c.length shouldBe 2
        c.getValue(0) shouldBe d1
        c.getValue(1) shouldBe d2
      case Left(err) => fail(s"fromValues failed: $err")
    }
  }

  "Schema[Date]" should "encode and decode correctly" in {
    val d = Date(2024, 3, 15)
    val schema = Schema.dateSchema
    val encoded = schema.encode(d)
    encoded.length shouldBe 1
    val decoded = schema.decode(encoded)
    decoded shouldBe Right(d)
  }
}
