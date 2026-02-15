package net.ghoula.strongbow

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.prelude.*
import net.ghoula.strongbow.types.{ColumnIndex, RowIndex}

/** Tests for new expression operations (>=, <=, !=, as, lit, when). */
class NewExprSpec extends AnyFlatSpec with Matchers {

  "lit helper" should "create literal expressions ergonomically" in {
    val expr = Expr.lit[Int, Int](42)

    val result = ExprInterpreter.eval(expr, Vector.empty, RowIndex(0))
    result shouldBe Right(42)
  }

  "as (rename)" should "wrap expression for column naming" in {
    val expr = Expr.lit[Int, Int](42).as("answer")

    expr match {
      case Expr.Named(_, name) => name shouldBe "answer"
      case _ => fail("Expected Named expression")
    }

    // Named should evaluate to inner expression
    val result = ExprInterpreter.eval(expr, Vector.empty, RowIndex(0))
    result shouldBe Right(42)
  }

  ">= operator" should "evaluate greater-than-or-equal" in {
    val col = Column.int(Array(5, 10, 15, 20))
    val columns = Vector(col)

    val expr = Expr.Cell[Int, Int]("value", ColumnIndex(0)) >= Expr.lit(10)

    val results = (0 until 4).map { idx =>
      ExprInterpreter.eval(expr, columns, RowIndex(idx))
    }

    results shouldBe Seq(Right(false), Right(true), Right(true), Right(true))
  }

  "<= operator" should "evaluate less-than-or-equal" in {
    val col = Column.int(Array(5, 10, 15, 20))
    val columns = Vector(col)

    val expr = Expr.Cell[Int, Int]("value", ColumnIndex(0)) <= Expr.lit(10)

    val results = (0 until 4).map { idx =>
      ExprInterpreter.eval(expr, columns, RowIndex(idx))
    }

    results shouldBe Seq(Right(true), Right(true), Right(false), Right(false))
  }

  "Neq (not-equal)" should "evaluate not-equal" in {
    val col = Column.int(Array(5, 10, 15, 10))
    val columns = Vector(col)

    val cellExpr = Expr.Cell[Int, Int]("value", ColumnIndex(0))
    val litExpr = Expr.lit[Int, Int](10)
    val expr = Expr.Neq(cellExpr, litExpr)

    val results = (0 until 4).map { idx =>
      ExprInterpreter.eval(expr, columns, RowIndex(idx))
    }

    results shouldBe Seq(Right(true), Right(false), Right(true), Right(false))
  }

  "when expression" should "evaluate conditional logic" in {
    val col = Column.int(Array(5, 15, 25, 35))
    val columns = Vector(col)

    // when(age < 18, "minor", "adult")
    val condition = Expr.Cell[Int, Int]("age", ColumnIndex(0)) < Expr.lit(18)
    val thenExpr = Expr.lit[Int, String]("minor")
    val elseExpr = Expr.lit[Int, String]("adult")
    val expr = Expr.when(condition, thenExpr, elseExpr)

    val results = (0 until 4).map { idx =>
      ExprInterpreter.eval(expr, columns, RowIndex(idx))
    }

    results shouldBe Seq(Right("minor"), Right("minor"), Right("adult"), Right("adult"))
  }

  "when expression" should "support nested conditionals" in {
    val col = Column.int(Array(5, 15, 25, 65))
    val columns = Vector(col)

    // when(age < 18, "minor", when(age < 65, "adult", "senior"))
    val age = Expr.Cell[Int, Int]("age", ColumnIndex(0))
    val expr = Expr.when(
      age < Expr.lit(18),
      Expr.lit[Int, String]("minor"),
      Expr.when(
        age < Expr.lit(65),
        Expr.lit[Int, String]("adult"),
        Expr.lit[Int, String]("senior")
      )
    )

    val results = (0 until 4).map { idx =>
      ExprInterpreter.eval(expr, columns, RowIndex(idx))
    }

    results shouldBe Seq(Right("minor"), Right("minor"), Right("adult"), Right("senior"))
  }

  "when expression" should "work with numeric results" in {
    val col = Column.int(Array(-5, 0, 5))
    val columns = Vector(col)

    // Absolute value using when: when(x < 0, -x, x)
    val x = Expr.Cell[Int, Int]("x", ColumnIndex(0))
    val expr = Expr.when(
      x < Expr.lit(0),
      Expr.lit[Int, Int](0) - x, // -x
      x
    )

    val results = (0 until 3).map { idx =>
      ExprInterpreter.eval(expr, columns, RowIndex(idx))
    }

    results shouldBe Seq(Right(5), Right(0), Right(5))
  }

  // --- Like ---

  "like" should "match with % wildcard" in {
    val col = Column.string(Array("PROMO STEEL", "STANDARD COPPER", "PROMO BRASS", "ECONOMY TIN"))
    val columns = Vector(col)

    val strCell = Expr.Cell[String, String]("type", ColumnIndex(0))
    val expr = strCell.like("PROMO%")

    val results = (0 until 4).map { idx =>
      ExprInterpreter.eval(expr, columns, RowIndex(idx))
    }

    results shouldBe Seq(Right(true), Right(false), Right(true), Right(false))
  }

  it should "match with _ wildcard" in {
    val col = Column.string(Array("abc", "aXc", "abcd", "ac"))
    val columns = Vector(col)

    val strCell = Expr.Cell[String, String]("value", ColumnIndex(0))
    val expr = strCell.like("a_c")

    val results = (0 until 4).map { idx =>
      ExprInterpreter.eval(expr, columns, RowIndex(idx))
    }

    results shouldBe Seq(Right(true), Right(true), Right(false), Right(false))
  }

  it should "match exact strings without wildcards" in {
    val col = Column.string(Array("hello", "world"))
    val columns = Vector(col)

    val strCell = Expr.Cell[String, String]("value", ColumnIndex(0))
    val expr = strCell.like("hello")

    val results = (0 until 2).map { idx =>
      ExprInterpreter.eval(expr, columns, RowIndex(idx))
    }

    results shouldBe Seq(Right(true), Right(false))
  }

  it should "match with % in the middle" in {
    val col = Column.string(Array("abcdef", "aef", "aXYZef", "xyz"))
    val columns = Vector(col)

    val strCell = Expr.Cell[String, String]("value", ColumnIndex(0))
    val expr = strCell.like("a%ef")

    val results = (0 until 4).map { idx =>
      ExprInterpreter.eval(expr, columns, RowIndex(idx))
    }

    results shouldBe Seq(Right(true), Right(true), Right(true), Right(false))
  }

  "Like outputType" should "return BooleanType" in {
    val strCell = Expr.Cell[String, String]("value", ColumnIndex(0))
    strCell.like("PROMO%").outputType shouldBe Some(ColumnType.BooleanType)
  }

  // --- outputType ---

  "outputType" should "return IntType for arithmetic expressions" in {
    val cell = Expr.Cell[Int, Int]("value", ColumnIndex(0))
    (cell + Expr.const(1)).outputType shouldBe Some(ColumnType.IntType)
    (cell - Expr.const(1)).outputType shouldBe Some(ColumnType.IntType)
    (cell * Expr.const(2)).outputType shouldBe Some(ColumnType.IntType)
    (cell / Expr.const(2)).outputType shouldBe Some(ColumnType.IntType)
  }

  "outputType" should "return BooleanType for comparison expressions" in {
    val cell = Expr.Cell[Int, Int]("value", ColumnIndex(0))
    (cell > Expr.const(1)).outputType shouldBe Some(ColumnType.BooleanType)
    (cell >= Expr.const(1)).outputType shouldBe Some(ColumnType.BooleanType)
    (cell < Expr.const(1)).outputType shouldBe Some(ColumnType.BooleanType)
    (cell <= Expr.const(1)).outputType shouldBe Some(ColumnType.BooleanType)
    Expr.Eq(cell, Expr.const(1)).outputType shouldBe Some(ColumnType.BooleanType)
    Expr.Neq(cell, Expr.const(1)).outputType shouldBe Some(ColumnType.BooleanType)
  }

  "outputType" should "return StringType for Concat and IntType for Length" in {
    val cell = Expr.Cell[String, String]("value", ColumnIndex(0))
    (cell ++ Expr.const("!")).outputType shouldBe Some(ColumnType.StringType)
    cell.length.outputType shouldBe Some(ColumnType.IntType)
  }

  "outputType" should "return None for Cell and Const (erased types)" in {
    Expr.Cell[Int, Int]("value", ColumnIndex(0)).outputType shouldBe None
    Expr.Const[Int, Int](42).outputType shouldBe None
  }

  "outputType" should "return LongType for count expressions" in {
    Expr.Count[Int]().outputType shouldBe Some(ColumnType.LongType)
    Expr.CountDistinct(Expr.Cell[Int, Int]("value", ColumnIndex(0))).outputType shouldBe Some(ColumnType.LongType)
  }

  "outputType" should "return DoubleType for Avg, StdDev, and SumDouble" in {
    val cell = Expr.Cell[Double, Double]("value", ColumnIndex(0))
    Expr.Avg(cell).outputType shouldBe Some(ColumnType.DoubleType)
    Expr.StdDev(cell).outputType shouldBe Some(ColumnType.DoubleType)
    Expr.StdDevPop(cell).outputType shouldBe Some(ColumnType.DoubleType)
    Expr.SumDouble(cell).outputType shouldBe Some(ColumnType.DoubleType)
  }

  "outputType" should "return LongType for SumLong" in {
    val cell = Expr.Cell[Long, Long]("value", ColumnIndex(0))
    Expr.SumLong(cell).outputType shouldBe Some(ColumnType.LongType)
  }

  "comparison operators" should "maintain zero-cast architecture" in {
    val col = Column.int(Array(10))
    val columns = Vector(col)

    val x = Expr.Cell[Int, Int]("x", ColumnIndex(0))
    val ten = Expr.lit[Int, Int](10)

    // All comparison operations should work without casts
    val gteExpr = x >= ten
    val lteExpr = x <= ten
    val neqExpr = Expr.Neq(x, ten) // Use constructor directly to avoid conflict with Scala's !=

    val gteResult: Either[ExecutionError, Boolean] = ExprInterpreter.eval(gteExpr, columns, RowIndex(0))
    val lteResult: Either[ExecutionError, Boolean] = ExprInterpreter.eval(lteExpr, columns, RowIndex(0))
    val neqResult: Either[ExecutionError, Boolean] = ExprInterpreter.eval(neqExpr, columns, RowIndex(0))

    gteResult shouldBe Right(true)
    lteResult shouldBe Right(true)
    neqResult shouldBe Right(false)
  }
}
