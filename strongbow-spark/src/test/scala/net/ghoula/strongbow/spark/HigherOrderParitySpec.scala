package net.ghoula.strongbow.spark

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.column.ColumnType
import net.ghoula.strongbow.expr.Expr
import net.ghoula.strongbow.interpreter.ExprInterpreter
import net.ghoula.strongbow.prelude.*

/** Parity for the higher-order lambda family (transform, filter, exists, forall, aggregate,
  * zip_with, map_*).
  *
  * In-memory binds lambda variables to typed element columns sliced from the array layout and
  * evaluates the body once per higher-order expression over the flat element dimension; Spark maps
  * to the native `functions.*` lambda builders.
  */
class HigherOrderParitySpec extends AnyFlatSpec with Matchers with SparkTestBase {

  case class Doc(xs: Seq[Int], c: Int)
  given Schema[Doc] = Schema.derived

  private lazy val base: DataFrame = spark.sql("""
      SELECT inline(array(
        struct(array(1, 2, 3), 10),
        struct(array(4, 5), 20),
        struct(array(), 30),
        struct(null, 40)
      )) AS (xs_value, c_value)
    """)

  private lazy val materialized = {
    val rows = base.collect()
    RowConverter.toMaterialized(rows, summon[Schema[Doc]]).fold(err => fail(s"$err"), identity)
  }

  private def xsCell: Expr[Doc, Seq[Int]] = Expr.cell("xs_value", ColumnIndex(0))
  private def cCell: Expr[Doc, Int] = Expr.cell("c_value", ColumnIndex(1))

  private def evalBoth[A](
    expr: Expr[Doc, A],
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

  private def checkParity[A](expr: Expr[Doc, A], columnType: ColumnType, expected: Vector[Any | Null]): Unit = {
    val (inMemory, sparkValues) = evalBoth(expr, columnType)
    inMemory shouldBe sparkValues
    sparkValues shouldBe expected
  }

  "transform" should "apply a lambda to every element on both backends" in {
    checkParity(
      xsCell.transform(x => x + Expr.const(1)),
      ColumnType.AnyType,
      Vector(
        Seq(2, 3, 4),
        Seq(5, 6),
        Seq.empty,
        null // scalafix:ok DisableSyntax.null
      )
    )
  }

  it should "bind element and index in the two-argument form" in {
    checkParity(
      xsCell.transform((x, i) => x * i),
      ColumnType.AnyType,
      Vector(
        Seq(0, 2, 6),
        Seq(0, 5),
        Seq.empty,
        null // scalafix:ok DisableSyntax.null
      )
    )
  }

  it should "let the body reference outer columns" in {
    checkParity(
      xsCell.transform(x => x + cCell),
      ColumnType.AnyType,
      Vector(
        Seq(11, 12, 13),
        Seq(24, 25),
        Seq.empty,
        null // scalafix:ok DisableSyntax.null
      )
    )
  }

  "filter" should "keep elements whose predicate holds on both backends" in {
    checkParity(
      xsCell.filter(x => x > Expr.const(1)),
      ColumnType.AnyType,
      Vector(
        Seq(2, 3),
        Seq(4, 5),
        Seq.empty,
        null // scalafix:ok DisableSyntax.null
      )
    )
  }

  it should "bind the element index in the two-argument form" in {
    checkParity(
      xsCell.filter((x, i) => x > i),
      ColumnType.AnyType,
      Vector(
        Seq(1, 2, 3),
        Seq(4, 5),
        Seq.empty,
        null // scalafix:ok DisableSyntax.null
      )
    )
  }

  "exists" should "follow Spark's three-valued logic on both backends" in {
    checkParity(
      xsCell.exists(x => x > Expr.const(1)),
      ColumnType.BooleanType,
      Vector(true, true, false, null) // scalafix:ok DisableSyntax.null
    )
  }

  "forall" should "follow Spark's three-valued logic on both backends" in {
    checkParity(
      xsCell.forall(x => x > Expr.const(1)),
      ColumnType.BooleanType,
      Vector(false, true, true, null) // scalafix:ok DisableSyntax.null
    )
  }
}
