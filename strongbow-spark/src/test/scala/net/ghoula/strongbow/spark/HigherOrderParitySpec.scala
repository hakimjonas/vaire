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

  case class Doc(xs: Seq[Int], ys: Seq[Int], c: Int, m: Map[String, Int])
  given Schema[Doc] = Schema.derived

  private lazy val base: DataFrame = spark.sql("""
      SELECT inline(array(
        struct(array(1, 2, 3), array(10, 20), 10, map('a', 1, 'b', 2, 'c', 3)),
        struct(array(4, 5), array(30, 40, 50), 20, map('x', 10)),
        struct(array(), array(60), 30, map()),
        struct(null, array(70), 40, null)
      )) AS (xs_value, ys_value, c_value, m_value)
    """)

  private lazy val materialized = {
    val rows = base.collect()
    RowConverter.toMaterialized(rows, summon[Schema[Doc]]).fold(err => fail(s"$err"), identity)
  }

  private def xsCell: Expr[Doc, Seq[Int]] = Expr.cell("xs_value", ColumnIndex(0))
  private def ysCell: Expr[Doc, Seq[Int]] = Expr.cell("ys_value", ColumnIndex(1))
  private def cCell: Expr[Doc, Int] = Expr.cell("c_value", ColumnIndex(2))
  private def mCell: Expr[Doc, Map[String, Int]] = Expr.cell("m_value", ColumnIndex(3))

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

  "aggregate" should "fold with a merge lambda and apply finish on both backends" in {
    checkParity(
      xsCell.aggregate(Expr.const[Doc, Int](0))((acc, x) => acc + x, acc => acc * Expr.const(10)),
      ColumnType.AnyType,
      Vector(60, 90, 0, null) // scalafix:ok DisableSyntax.null
    )
  }

  it should "fold without finish (Spark reduce semantics) on both backends" in {
    checkParity(
      xsCell.aggregate(Expr.const[Doc, Int](0))((acc, x) => acc + x),
      ColumnType.AnyType,
      Vector(6, 9, 0, null) // scalafix:ok DisableSyntax.null
    )
  }

  "zip_with" should "pad the shorter array with null binders on both backends" in {
    checkParity(
      xsCell.zipWith(ysCell)((x, y) => x.getOrElse(0) + y.getOrElse(0)),
      ColumnType.AnyType,
      Vector(
        Seq(11, 22, 3),
        Seq(34, 45, 50),
        Seq(60),
        null // scalafix:ok DisableSyntax.null
      )
    )
  }

  "map_filter" should "keep entries whose predicate holds on both backends" in {
    checkParity(
      mCell.mapFilter((_, v) => v > Expr.const(1)),
      ColumnType.AnyType,
      Vector(
        Map("b" -> 2, "c" -> 3),
        Map("x" -> 10),
        Map.empty,
        null // scalafix:ok DisableSyntax.null
      )
    )
  }

  "transform_keys" should "transform map keys on both backends" in {
    checkParity(
      mCell.transformKeys((k, _) => k ++ Expr.const("_v")),
      ColumnType.AnyType,
      Vector(
        Map("a_v" -> 1, "b_v" -> 2, "c_v" -> 3),
        Map("x_v" -> 10),
        Map.empty,
        null // scalafix:ok DisableSyntax.null
      )
    )
  }

  "transform_values" should "transform map values on both backends" in {
    checkParity(
      mCell.transformValues((_, v) => v * Expr.const(10)),
      ColumnType.AnyType,
      Vector(
        Map("a" -> 10, "b" -> 20, "c" -> 30),
        Map("x" -> 100),
        Map.empty,
        null // scalafix:ok DisableSyntax.null
      )
    )
  }

  "map_zip_with" should "merge two maps with null binders for missing keys on both backends" in {
    checkParity(
      mCell.mapZipWith(mCell)((_, v1, v2) => v1.getOrElse(0) + v2.getOrElse(0)),
      ColumnType.AnyType,
      Vector(
        Map("a" -> 2, "b" -> 4, "c" -> 6),
        Map("x" -> 20),
        Map.empty,
        null // scalafix:ok DisableSyntax.null
      )
    )
  }

  it should "let the merge reference outer columns" in {
    checkParity(
      xsCell.aggregate(Expr.const[Doc, Int](0))((acc, x) => acc + x + cCell),
      ColumnType.AnyType,
      Vector(36, 49, 0, null) // scalafix:ok DisableSyntax.null
    )
  }
}
