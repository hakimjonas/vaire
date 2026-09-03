package net.ghoula.vaire.spark

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.column.ColumnType
import net.ghoula.vaire.expr.Expr
import net.ghoula.vaire.interpreter.ExprInterpreter
import net.ghoula.vaire.prelude.*
import net.ghoula.vaire.types.Binary

class StringMathParitySpec extends AnyFlatSpec with Matchers with SparkTestBase {

  case class Row2(id: Int, txt: String, num: Double)
  given Schema[Row2] = Schema.derived

  private lazy val base: DataFrame = spark.sql("""
      SELECT inline(array(
        struct(5, 'hello world', cast(2.0 as double)),
        struct(10, 'scala-spark', cast(9.0 as double)),
        struct(-8, 'ABC def', cast(0.5 as double))
      )) AS (id_value, txt_value, num_value)
    """)

  private lazy val materialized = {
    val rows = base.collect()
    RowConverter.toMaterialized(rows, summon[Schema[Row2]]).fold(err => fail(s"$err"), identity)
  }

  private def idCell: Expr[Row2, Int] = Expr.cell("id_value", ColumnIndex(0))
  private def txtCell: Expr[Row2, String] = Expr.cell("txt_value", ColumnIndex(1))
  private def numCell: Expr[Row2, Double] = Expr.cell("num_value", ColumnIndex(2))

  private def evalBoth[A](
    expr: Expr[Row2, A],
    columnType: ColumnType
  ): (Vector[Any | Null], Vector[Any | Null]) = {
    val inMemory = ExprInterpreter.evalColumn(expr, materialized.columns, columnType) match {
      case Right(col) => (0 until col.length).toVector.map(col.getValue)
      case other => fail(s"In-memory eval failed: $other")
    }
    val (sparkCol, _) = ExprToColumn.convert(expr) match {
      case Right(converted) => converted
      case other => fail(s"Spark conversion failed: $other")
    }
    val sparkValues = base.select(sparkCol).collect().map(r => r.get(0): Any | Null).toVector
    (inMemory, sparkValues)
  }

  /** Backend parity is the primary assertion; `expected` is a hand-verifiable anchor. */
  private def checkParity[A](expr: Expr[Row2, A], columnType: ColumnType, expected: Vector[Any | Null]): Unit = {
    val (inMemory, sparkValues) = evalBoth(expr, columnType)
    inMemory shouldBe sparkValues
    sparkValues shouldBe expected
  }

  "string functions" should "agree on both backends" in {
    checkParity(
      txtCell.initcap,
      ColumnType.StringType,
      Vector[Any | Null]("Hello World", "Scala-spark", "Abc Def")
    )
    checkParity(
      txtCell.instr(Expr.const[Row2, String]("a")),
      ColumnType.IntType,
      Vector[Any | Null](0, 3, 0)
    )
    checkParity(
      txtCell.substringIndex("-", 1),
      ColumnType.StringType,
      Vector[Any | Null]("hello world", "scala", "ABC def")
    )
    checkParity(
      txtCell.leftStr(Expr.const[Row2, Int](5)),
      ColumnType.StringType,
      Vector[Any | Null]("hello", "scala", "ABC d")
    )
    checkParity(
      txtCell.rightStr(Expr.const[Row2, Int](5)),
      ColumnType.StringType,
      Vector[Any | Null]("world", "spark", "C def")
    )
    checkParity(
      txtCell.repeat(Expr.const[Row2, Int](2)),
      ColumnType.StringType,
      Vector[Any | Null]("hello worldhello world", "scala-sparkscala-spark", "ABC defABC def")
    )
    checkParity(txtCell.reverse, ColumnType.StringType, Vector[Any | Null]("dlrow olleh", "kraps-alacs", "fed CBA"))
    checkParity(
      txtCell.lpad(Expr.const[Row2, Int](8), "x"),
      ColumnType.StringType,
      Vector[Any | Null]("hello wo", "scala-sp", "xABC def")
    )
    checkParity(
      txtCell.rpad(Expr.const[Row2, Int](8), "x"),
      ColumnType.StringType,
      Vector[Any | Null]("hello wo", "scala-sp", "ABC defx")
    )
    checkParity(
      txtCell.translate("lo", "10"),
      ColumnType.StringType,
      Vector[Any | Null]("he110 w0r1d", "sca1a-spark", "ABC def")
    )
    checkParity(txtCell.ascii, ColumnType.IntType, Vector[Any | Null](104, 115, 65))
    checkParity(
      Expr.chr[Row2](Expr.const[Row2, Int](97)),
      ColumnType.StringType,
      Vector[Any | Null]("a", "a", "a")
    )
    checkParity(
      txtCell.levenshtein(Expr.const[Row2, String]("hello")),
      ColumnType.IntType,
      Vector[Any | Null](6, 10, 7)
    )
    checkParity(
      txtCell.rlike("^[A-Za-z ]+$"),
      ColumnType.BooleanType,
      Vector[Any | Null](true, false, true)
    )
    checkParity(
      txtCell.splitPart("-", Expr.const[Row2, Int](2)),
      ColumnType.StringType,
      Vector[Any | Null]("", "spark", "")
    )
    checkParity(
      Expr.formatString[Row2]("<%s>", txtCell),
      ColumnType.StringType,
      Vector[Any | Null]("<hello world>", "<scala-spark>", "<ABC def>")
    )
  }

  "math functions" should "agree on both backends" in {
    checkParity(
      numCell.cbrt,
      ColumnType.DoubleType,
      Vector[Any | Null](1.2599210498948732, 2.0800838230519041, 0.7937005259840998)
    )
    checkParity(numCell.bround(1), ColumnType.DoubleType, Vector[Any | Null](2.0, 9.0, 0.5))
    checkParity(
      Expr.factorial[Row2](idCell),
      ColumnType.LongType,
      Vector[Any | Null](120L, 3628800L, SqlNull.value)
    )
    checkParity(
      numCell.degrees,
      ColumnType.DoubleType,
      Vector[Any | Null](114.59155902616465, 515.662015617741, 28.64788975654116)
    )
    checkParity(
      numCell.radians,
      ColumnType.DoubleType,
      Vector[Any | Null](0.03490658503988659, 0.15707963267948966, 0.008726646259971648)
    )
    checkParity(
      numCell.sinh,
      ColumnType.DoubleType,
      Vector[Any | Null](3.626860407847019, 4051.54190208279, 0.5210953054937474)
    )
    checkParity(
      numCell.log1p,
      ColumnType.DoubleType,
      Vector[Any | Null](1.0986122886681096, 2.302585092994046, 0.4054651081081644)
    )
    checkParity(
      numCell.expm1,
      ColumnType.DoubleType,
      Vector[Any | Null](6.38905609893065, 8102.083927575384, 0.6487212707001282)
    )
    checkParity(
      idCell.castToLong.pmod(Expr.const[Row2, Long](3L)),
      ColumnType.LongType,
      Vector[Any | Null](2L, 1L, 1L)
    )
  }

  it should "compute pi, e, bin, and conversions on both backends" in {
    checkParity(Expr.pi[Row2], ColumnType.DoubleType, Vector[Any | Null](Math.PI, Math.PI, Math.PI))
    checkParity(Expr.euler[Row2], ColumnType.DoubleType, Vector[Any | Null](Math.E, Math.E, Math.E))
    checkParity(
      Expr.bin[Row2](idCell.castToLong),
      ColumnType.StringType,
      Vector[Any | Null]("101", "1010", "1111111111111111111111111111111111111111111111111111111111111000")
    )
    checkParity(
      Expr.conv[Row2](Expr.const[Row2, String]("ff"), 16, 10),
      ColumnType.StringType,
      Vector[Any | Null]("255", "255", "255")
    )
    checkParity(
      numCell.hypot(Expr.const[Row2, Double](4.0)),
      ColumnType.DoubleType,
      Vector[Any | Null](Math.hypot(2.0, 4.0), Math.hypot(9.0, 4.0), Math.hypot(0.5, 4.0))
    )
  }

  it should "handle try_divide SqlNull.value semantics on both backends" in {
    checkParity(
      numCell.tryDivide(Expr.const[Row2, Double](0.0)),
      ColumnType.DoubleType,
      Vector[Any | Null](SqlNull.value, SqlNull.value, SqlNull.value)
    )
    checkParity(
      idCell.castToLong.tryDivide(Expr.const[Row2, Long](2L)),
      ColumnType.DoubleType,
      Vector[Any | Null](2.5, 5.0, -4.0)
    )
  }

  "unhex" should "decode hex strings on both backends" in {
    val expr = Expr.unhex[Row2](Expr.const[Row2, String]("537061726b"))
    val (inMemory, sparkValues) = evalBoth(expr, ColumnType.BinaryType)
    inMemory.map { case b: Array[Byte] => new String(b); case other => fail(s"Expected bytes: $other") }
      .shouldBe(Vector("Spark", "Spark", "Spark").map(s => s: Any | Null))
    sparkValues.map { case b: Array[Byte] => new String(b); case other => fail(s"Expected bytes: $other") }
      .shouldBe(Vector("Spark", "Spark", "Spark").map(s => s: Any | Null))
  }
}
