package net.ghoula.strongbow

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.prelude.*
import net.ghoula.strongbow.types.{ColumnIndex, RowIndex}

/** Tests for Phase 1 Spark Parity expressions. */
class Phase1ExprSpec extends AnyFlatSpec with Matchers {

  "lower" should "convert string to lowercase" in {
    val col = Column.string(Array("HELLO", "World", "test"))
    val columns = Vector(col)
    val cell = Expr.Cell[String, String]("s", ColumnIndex(0))
    val expr = cell.lower

    val results = (0 until 3).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right("hello"), Right("world"), Right("test"))
  }

  "upper" should "convert string to uppercase" in {
    val col = Column.string(Array("hello", "World", "TEST"))
    val columns = Vector(col)
    val cell = Expr.Cell[String, String]("s", ColumnIndex(0))
    val expr = cell.upper

    val results = (0 until 3).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right("HELLO"), Right("WORLD"), Right("TEST"))
  }

  "trim" should "remove leading and trailing whitespace" in {
    val col = Column.string(Array("  hello  ", " world", "test  "))
    val columns = Vector(col)
    val cell = Expr.Cell[String, String]("s", ColumnIndex(0))
    val expr = cell.trim

    val results = (0 until 3).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right("hello"), Right("world"), Right("test"))
  }

  "ltrim" should "remove leading whitespace only" in {
    val col = Column.string(Array("  hello  ", " world"))
    val columns = Vector(col)
    val cell = Expr.Cell[String, String]("s", ColumnIndex(0))
    val expr = cell.ltrim

    val results = (0 until 2).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right("hello  "), Right("world"))
  }

  "rtrim" should "remove trailing whitespace only" in {
    val col = Column.string(Array("  hello  ", "world  "))
    val columns = Vector(col)
    val cell = Expr.Cell[String, String]("s", ColumnIndex(0))
    val expr = cell.rtrim

    val results = (0 until 2).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right("  hello"), Right("world"))
  }

  "substring" should "extract substring with 1-based position" in {
    val col = Column.string(Array("hello world", "abcdef"))
    val columns = Vector(col)
    val cell = Expr.Cell[String, String]("s", ColumnIndex(0))
    val expr = cell.substring(1, 5)

    val results = (0 until 2).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right("hello"), Right("abcde"))
  }

  it should "handle position beyond string length" in {
    val col = Column.string(Array("hi"))
    val columns = Vector(col)
    val cell = Expr.Cell[String, String]("s", ColumnIndex(0))
    val expr = cell.substring(10, 5)

    ExprInterpreter.eval(expr, columns, RowIndex(0)) shouldBe Right("")
  }

  "replace" should "replace all occurrences of search string" in {
    val col = Column.string(Array("hello world hello", "foo bar"))
    val columns = Vector(col)
    val cell = Expr.Cell[String, String]("s", ColumnIndex(0))
    val expr = cell.replace("hello", "hi")

    val results = (0 until 2).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right("hi world hi"), Right("foo bar"))
  }

  "regexpReplace" should "replace regex matches" in {
    val col = Column.string(Array("abc123def456", "no digits"))
    val columns = Vector(col)
    val cell = Expr.Cell[String, String]("s", ColumnIndex(0))
    val expr = cell.regexpReplace("[0-9]+", "NUM")

    val results = (0 until 2).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right("abcNUMdefNUM"), Right("no digits"))
  }

  "regexpExtract" should "extract regex group" in {
    val col = Column.string(Array("abc123def", "no match"))
    val columns = Vector(col)
    val cell = Expr.Cell[String, String]("s", ColumnIndex(0))
    val expr = cell.regexpExtract("([0-9]+)", 1)

    val results = (0 until 2).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right("123"), Right(""))
  }

  "split" should "split string by delimiter" in {
    val col = Column.string(Array("a,b,c", "x"))
    val columns = Vector(col)
    val cell = Expr.Cell[String, String]("s", ColumnIndex(0))
    val expr = cell.split(",")

    val results = (0 until 2).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(Seq("a", "b", "c")), Right(Seq("x")))
  }

  "startsWith" should "check string prefix" in {
    val col = Column.string(Array("hello world", "goodbye"))
    val columns = Vector(col)
    val cell = Expr.Cell[String, String]("s", ColumnIndex(0))
    val prefix = Expr.lit[String, String]("hello")
    val expr = cell.startsWith(prefix)

    val results = (0 until 2).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(true), Right(false))
  }

  "endsWith" should "check string suffix" in {
    val col = Column.string(Array("hello world", "goodbye"))
    val columns = Vector(col)
    val cell = Expr.Cell[String, String]("s", ColumnIndex(0))
    val suffix = Expr.lit[String, String]("world")
    val expr = cell.endsWith(suffix)

    val results = (0 until 2).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(true), Right(false))
  }

  "contains" should "check if string contains substring" in {
    val col = Column.string(Array("hello world", "goodbye"))
    val columns = Vector(col)
    val cell = Expr.Cell[String, String]("s", ColumnIndex(0))
    val substr = Expr.lit[String, String]("lo wo")
    val expr = cell.contains(substr)

    val results = (0 until 2).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(true), Right(false))
  }

  "concatWs" should "concatenate strings with separator" in {
    val col1 = Column.string(Array("a", "d"))
    val col2 = Column.string(Array("b", "e"))
    val col3 = Column.string(Array("c", "f"))
    val columns = Vector(col1, col2, col3)
    val c1 = Expr.Cell[String, String]("c1", ColumnIndex(0))
    val c2 = Expr.Cell[String, String]("c2", ColumnIndex(1))
    val c3 = Expr.Cell[String, String]("c3", ColumnIndex(2))
    val expr = Expr.concatWs[String]("-", c1, c2, c3)

    val results = (0 until 2).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right("a-b-c"), Right("d-e-f"))
  }

  "string ops outputType" should "return correct types" in {
    val cell = Expr.Cell[String, String]("s", ColumnIndex(0))
    cell.lower.outputType shouldBe Some(ColumnType.StringType)
    cell.upper.outputType shouldBe Some(ColumnType.StringType)
    cell.trim.outputType shouldBe Some(ColumnType.StringType)
    cell.ltrim.outputType shouldBe Some(ColumnType.StringType)
    cell.rtrim.outputType shouldBe Some(ColumnType.StringType)
    cell.substring(1, 5).outputType shouldBe Some(ColumnType.StringType)
    cell.replace("a", "b").outputType shouldBe Some(ColumnType.StringType)
    cell.regexpReplace("a", "b").outputType shouldBe Some(ColumnType.StringType)
    cell.regexpExtract("a", 0).outputType shouldBe Some(ColumnType.StringType)
    cell.split(",").outputType shouldBe Some(ColumnType.AnyType)
    cell.startsWith(Expr.lit("x")).outputType shouldBe Some(ColumnType.BooleanType)
    cell.endsWith(Expr.lit("x")).outputType shouldBe Some(ColumnType.BooleanType)
    cell.contains(Expr.lit("x")).outputType shouldBe Some(ColumnType.BooleanType)
  }

  "isNull" should "detect null values" in {
    val col = Column.any(
      Array("hello", null, "world"), // scalafix:ok DisableSyntax.null
      scala.collection.immutable.BitSet(1)
    )
    val columns = Vector(col)
    val cell = Expr.Cell[Any, Any]("v", ColumnIndex(0))
    val expr = cell.isNull

    val results = (0 until 3).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(false), Right(true), Right(false))
  }

  "isNotNull" should "detect non-null values" in {
    val col = Column.any(
      Array("hello", null, "world"), // scalafix:ok DisableSyntax.null
      scala.collection.immutable.BitSet(1)
    )
    val columns = Vector(col)
    val cell = Expr.Cell[Any, Any]("v", ColumnIndex(0))
    val expr = cell.isNotNull

    val results = (0 until 3).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(true), Right(false), Right(true))
  }

  "in" should "check membership in value list" in {
    val col = Column.int(Array(1, 2, 3, 4, 5))
    val columns = Vector(col)
    val cell = Expr.Cell[Int, Int]("v", ColumnIndex(0))
    val expr = cell.in(Vector(2, 4))

    val results = (0 until 5).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(false), Right(true), Right(false), Right(true), Right(false))
  }

  "between" should "check value is within range inclusive" in {
    val col = Column.int(Array(1, 5, 10, 15, 20))
    val columns = Vector(col)
    val cell = Expr.Cell[Int, Int]("v", ColumnIndex(0))
    val lo = Expr.lit[Int, Int](5)
    val hi = Expr.lit[Int, Int](15)
    val expr = cell.between(lo, hi)

    val results = (0 until 5).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(false), Right(true), Right(true), Right(true), Right(false))
  }

  "coalesce" should "return first non-null value" in {
    val col1 = Column.any(
      Array(null, "b", null), // scalafix:ok DisableSyntax.null
      scala.collection.immutable.BitSet(0, 2)
    )
    val col2 = Column.any(
      Array("x", null, null), // scalafix:ok DisableSyntax.null
      scala.collection.immutable.BitSet(1, 2)
    )
    val columns = Vector(col1, col2)
    val c1 = Expr.Cell[Any, Any]("c1", ColumnIndex(0))
    val c2 = Expr.Cell[Any, Any]("c2", ColumnIndex(1))
    val expr = Expr.coalesce[Any, Any](c1, c2)

    ExprInterpreter.eval(expr, columns, RowIndex(0)) shouldBe Right("x")
    ExprInterpreter.eval(expr, columns, RowIndex(1)) shouldBe Right("b")
  }

  "null/conditional outputType" should "return correct types" in {
    val cell = Expr.Cell[Int, Int]("v", ColumnIndex(0))
    cell.isNull.outputType shouldBe Some(ColumnType.BooleanType)
    cell.isNotNull.outputType shouldBe Some(ColumnType.BooleanType)
    cell.in(Vector(1, 2)).outputType shouldBe Some(ColumnType.BooleanType)
  }

  "mod (Int)" should "compute modulus" in {
    val col = Column.int(Array(10, 7, 15))
    val columns = Vector(col)
    val cell = Expr.Cell[Int, Int]("v", ColumnIndex(0))
    val expr = cell % Expr.lit[Int, Int](3)

    val results = (0 until 3).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(1), Right(1), Right(0))
  }

  "mod (Int)" should "return error on division by zero" in {
    val col = Column.int(Array(10))
    val columns = Vector(col)
    val cell = Expr.Cell[Int, Int]("v", ColumnIndex(0))
    val expr = cell % Expr.lit[Int, Int](0)

    ExprInterpreter.eval(expr, columns, RowIndex(0)).isLeft shouldBe true
  }

  "mod (Long)" should "compute modulus" in {
    val col = Column.long(Array(10L, 7L, 15L))
    val columns = Vector(col)
    val cell = Expr.Cell[Long, Long]("v", ColumnIndex(0))
    val expr = cell % Expr.lit[Long, Long](3L)

    val results = (0 until 3).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(1L), Right(1L), Right(0L))
  }

  "abs (Int)" should "compute absolute value" in {
    val col = Column.int(Array(-5, 0, 5))
    val columns = Vector(col)
    val cell = Expr.Cell[Int, Int]("v", ColumnIndex(0))
    val expr = cell.abs

    val results = (0 until 3).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(5), Right(0), Right(5))
  }

  "abs (Long)" should "compute absolute value" in {
    val col = Column.long(Array(-5L, 0L, 5L))
    val columns = Vector(col)
    val cell = Expr.Cell[Long, Long]("v", ColumnIndex(0))
    val expr = cell.abs

    val results = (0 until 3).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(5L), Right(0L), Right(5L))
  }

  "abs (Double)" should "compute absolute value" in {
    val col = Column.double(Array(-5.5, 0.0, 5.5))
    val columns = Vector(col)
    val cell = Expr.Cell[Double, Double]("v", ColumnIndex(0))
    val expr = cell.abs

    val results = (0 until 3).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(5.5), Right(0.0), Right(5.5))
  }

  "negate (Int)" should "negate values" in {
    val col = Column.int(Array(-5, 0, 5))
    val columns = Vector(col)
    val cell = Expr.Cell[Int, Int]("v", ColumnIndex(0))
    val expr = cell.negate

    val results = (0 until 3).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(5), Right(0), Right(-5))
  }

  "negate (Long)" should "negate values" in {
    val col = Column.long(Array(-5L, 0L, 5L))
    val columns = Vector(col)
    val cell = Expr.Cell[Long, Long]("v", ColumnIndex(0))
    val expr = cell.negate

    val results = (0 until 3).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(5L), Right(0L), Right(-5L))
  }

  "negate (Double)" should "negate values" in {
    val col = Column.double(Array(-5.5, 0.0, 5.5))
    val columns = Vector(col)
    val cell = Expr.Cell[Double, Double]("v", ColumnIndex(0))
    val expr = cell.negate

    val results = (0 until 3).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(5.5), Right(-0.0), Right(-5.5))
  }

  "round" should "round to given scale" in {
    val col = Column.double(Array(3.14159, 2.71828, -1.555))
    val columns = Vector(col)
    val cell = Expr.Cell[Double, Double]("v", ColumnIndex(0))
    val expr = cell.round(2)

    val results = (0 until 3).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(3.14), Right(2.72), Right(-1.56))
  }

  "floor" should "round down" in {
    val col = Column.double(Array(3.7, -2.3, 5.0))
    val columns = Vector(col)
    val cell = Expr.Cell[Double, Double]("v", ColumnIndex(0))
    val expr = cell.floor

    val results = (0 until 3).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(3.0), Right(-3.0), Right(5.0))
  }

  "ceil" should "round up" in {
    val col = Column.double(Array(3.2, -2.7, 5.0))
    val columns = Vector(col)
    val cell = Expr.Cell[Double, Double]("v", ColumnIndex(0))
    val expr = cell.ceil

    val results = (0 until 3).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(4.0), Right(-2.0), Right(5.0))
  }

  "arithmetic outputType" should "return correct types" in {
    val intCell = Expr.Cell[Int, Int]("v", ColumnIndex(0))
    val longCell = Expr.Cell[Long, Long]("v", ColumnIndex(0))
    val dblCell = Expr.Cell[Double, Double]("v", ColumnIndex(0))

    (intCell % Expr.lit(1)).outputType shouldBe Some(ColumnType.IntType)
    (longCell % Expr.lit(1L)).outputType shouldBe Some(ColumnType.LongType)
    intCell.abs.outputType shouldBe Some(ColumnType.IntType)
    longCell.abs.outputType shouldBe Some(ColumnType.LongType)
    dblCell.abs.outputType shouldBe Some(ColumnType.DoubleType)
    intCell.negate.outputType shouldBe Some(ColumnType.IntType)
    longCell.negate.outputType shouldBe Some(ColumnType.LongType)
    dblCell.negate.outputType shouldBe Some(ColumnType.DoubleType)
    dblCell.round(2).outputType shouldBe Some(ColumnType.DoubleType)
    dblCell.floor.outputType shouldBe Some(ColumnType.DoubleType)
    dblCell.ceil.outputType shouldBe Some(ColumnType.DoubleType)
  }

  "castToLong" should "cast Int to Long" in {
    val col = Column.int(Array(1, 2, 3))
    val columns = Vector(col)
    val cell = Expr.Cell[Int, Int]("v", ColumnIndex(0))
    val expr = cell.castToLong

    val results = (0 until 3).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(1L), Right(2L), Right(3L))
  }

  "castToDouble (Int)" should "cast Int to Double" in {
    val col = Column.int(Array(1, 2, 3))
    val columns = Vector(col)
    val cell = Expr.Cell[Int, Int]("v", ColumnIndex(0))
    val expr = cell.castToDouble

    val results = (0 until 3).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(1.0), Right(2.0), Right(3.0))
  }

  "castToDouble (Long)" should "cast Long to Double" in {
    val col = Column.long(Array(1L, 2L, 3L))
    val columns = Vector(col)
    val cell = Expr.Cell[Long, Long]("v", ColumnIndex(0))
    val expr = cell.castToDouble

    val results = (0 until 3).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(1.0), Right(2.0), Right(3.0))
  }

  "castToString" should "cast any value to String" in {
    val col = Column.int(Array(42, -1, 0))
    val columns = Vector(col)
    val cell = Expr.Cell[Int, Int]("v", ColumnIndex(0))
    val expr = cell.castToString

    val results = (0 until 3).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right("42"), Right("-1"), Right("0"))
  }

  "cast outputType" should "return correct types" in {
    val intCell = Expr.Cell[Int, Int]("v", ColumnIndex(0))
    val longCell = Expr.Cell[Long, Long]("v", ColumnIndex(0))

    intCell.castToLong.outputType shouldBe Some(ColumnType.LongType)
    intCell.castToDouble.outputType shouldBe Some(ColumnType.DoubleType)
    longCell.castToDouble.outputType shouldBe Some(ColumnType.DoubleType)
    intCell.castToString.outputType shouldBe Some(ColumnType.StringType)
  }

  "string ops" should "compose with other expressions" in {
    val col = Column.string(Array("  Hello World  ", "  foo bar  "))
    val columns = Vector(col)
    val cell = Expr.Cell[String, String]("s", ColumnIndex(0))
    val expr = cell.trim.lower

    val results = (0 until 2).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right("hello world"), Right("foo bar"))
  }

  "arithmetic ops" should "compose with casts" in {
    val col = Column.int(Array(-5, 10))
    val columns = Vector(col)
    val cell = Expr.Cell[Int, Int]("v", ColumnIndex(0))
    val expr = cell.abs.castToDouble

    val results = (0 until 2).map(i => ExprInterpreter.eval(expr, columns, RowIndex(i)))
    results shouldBe Seq(Right(5.0), Right(10.0))
  }
}
