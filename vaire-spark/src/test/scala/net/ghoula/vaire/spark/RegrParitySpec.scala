package net.ghoula.vaire.spark

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.expr.Expr
import net.ghoula.vaire.interpreter.ExprInterpreter
import net.ghoula.vaire.prelude.*

/** Parity for the regr_* aggregate family.
  *
  * Anchor data x = (1,2,3,4,5), y = (2,4,5,4,5) gives count=5, x̄=3, ȳ=4, sxx=10, syy=6, sxy=6
  * (verified independently), hence slope=0.6, intercept=2.2, r²=0.6. Float aggregates are compared
  * with a 1e-9 tolerance because Spark and Vairë accumulate in different orders (observed 2-ULP
  * difference on r²).
  */
class RegrParitySpec extends AnyFlatSpec with Matchers with SparkTestBase {

  case class Pts(x: Double, y: Double)
  given Schema[Pts] = Schema.derived

  private lazy val base: DataFrame = spark.sql("""
      SELECT inline(array(
        struct(cast(1.0 as double), cast(2.0 as double)),
        struct(cast(2.0 as double), cast(4.0 as double)),
        struct(cast(3.0 as double), cast(5.0 as double)),
        struct(cast(4.0 as double), cast(4.0 as double)),
        struct(cast(5.0 as double), cast(5.0 as double))
      )) AS (x_value, y_value)
    """)

  private lazy val materialized = {
    val rows = base.collect()
    RowConverter.toMaterialized(rows, summon[Schema[Pts]]).fold(err => fail(s"$err"), identity)
  }

  private def xCell: Expr[Pts, Double] = Expr.cell("x_value", ColumnIndex(0))
  private def yCell: Expr[Pts, Double] = Expr.cell("y_value", ColumnIndex(1))

  private def sparkValueOf(expr: Expr[Pts, ?]): Any | Null =
    base
      .agg(ExprToColumn.convert(expr) match {
        case Right((c, _)) => c
        case other => fail(s"Spark conversion failed: $other")
      })
      .collect()
      .head
      .get(0)

  private def assertDoubleOption(actual: Either[?, Any | Null], anchor: Double): Unit = {
    val value = actual match {
      case Right(Some(d: Double)) => d
      case other => fail(s"Expected Some(Double), got: $other")
    }
    value shouldBe (anchor +- 1e-9)
  }

  private def checkStats(expr: Expr[Pts, Option[Double]], anchor: Double): Unit = {
    assertDoubleOption(ExprInterpreter.evalAggregation(expr, materialized.columns), anchor)
    val sparkValue: Any | Null = sparkValueOf(expr)
    val value = sparkValue match {
      case d: Double => d
      case other => fail(s"Expected Double, got: $other")
    }
    value shouldBe (anchor +- 1e-9)
  }

  private def checkLong(expr: Expr[Pts, Option[Long]], anchor: Long): Unit = {
    ExprInterpreter.evalAggregation(expr, materialized.columns) shouldBe Right(Some(anchor))
    sparkValueOf(expr) shouldBe anchor
  }

  "regr_* aggregates" should "match the verified regression statistics on both backends" in {
    checkLong(Expr.regrCount[Pts](yCell, xCell), 5L)
    checkStats(Expr.regrAvgx[Pts](yCell, xCell), 3.0)
    checkStats(Expr.regrAvgy[Pts](yCell, xCell), 4.0)
    checkStats(Expr.regrSxx[Pts](yCell, xCell), 10.0)
    checkStats(Expr.regrSyy[Pts](yCell, xCell), 6.0)
    checkStats(Expr.regrSxy[Pts](yCell, xCell), 6.0)
    checkStats(Expr.regrSlope[Pts](yCell, xCell), 0.6)
    checkStats(Expr.regrIntercept[Pts](yCell, xCell), 2.2)
    checkStats(Expr.regrR2[Pts](yCell, xCell), 0.6)
  }

  it should "return None for a degenerate group where sxx is zero" in {
    val vertical = spark.sql("""
        SELECT inline(array(
          struct(cast(0.0 as double), cast(2.0 as double)),
          struct(cast(0.0 as double), cast(4.0 as double))
        )) AS (x_value, y_value)
      """)
    val md = RowConverter.toMaterialized(vertical.collect(), summon[Schema[Pts]]).fold(err => fail(s"$err"), identity)
    ExprInterpreter.evalAggregation(Expr.regrSlope[Pts](yCell, xCell), md.columns) shouldBe Right(None)
    val sparkSlope: Any | Null = vertical
      .agg(ExprToColumn.convert(Expr.regrSlope[Pts](yCell, xCell)) match {
        case Right((c, _)) => c
        case other => fail(s"$other")
      })
      .collect()
      .head
      .get(0)
    sparkSlope shouldBe SqlNull.value
  }
}
