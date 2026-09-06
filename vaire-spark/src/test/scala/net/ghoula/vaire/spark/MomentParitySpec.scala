package net.ghoula.vaire.spark

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.column.ColumnType
import net.ghoula.vaire.expr.Expr
import net.ghoula.vaire.interpreter.ExprInterpreter
import net.ghoula.vaire.prelude.*

/** Parity for kurtosis/skewness.
  *
  * Spark uses the biased moment estimators: skewness = m3/m2^1.5, kurtosis = m4/m2² − 3 (verified
  * directly against Spark). Float comparisons use a 1e-9 tolerance.
  */
class MomentParitySpec extends AnyFlatSpec with Matchers with SparkTestBase {

  case class Vals(y: Double)
  given Schema[Vals] = Schema.derived

  private lazy val base: DataFrame = spark.sql("""
      SELECT inline(array(
        struct(cast(2.0 as double)),
        struct(cast(4.0 as double)),
        struct(cast(5.0 as double)),
        struct(cast(4.0 as double)),
        struct(cast(5.0 as double))
      )) AS (y_value)
    """)

  private lazy val materialized = {
    val rows = base.collect()
    RowConverter.toMaterialized(rows, summon[Schema[Vals]]).fold(err => fail(s"$err"), identity)
  }

  private def yCell: Expr[Vals, Double] = Expr.cell("y_value", ColumnIndex(0))

  private def checkStats(expr: Expr[Vals, Option[Double]], anchor: Double): Unit = {
    val inMemory = ExprInterpreter.evalAggregation(expr, materialized.columns) match {
      case Right(Some(d: Double)) => d
      case other => fail(s"Expected Some(Double), got: $other")
    }
    inMemory shouldBe (anchor +- 1e-9)
    val sparkValue: Any | Null = base
      .agg(ExprToColumn.convert(expr) match {
        case Right((c, _)) => c
        case other => fail(s"Spark conversion failed: $other")
      })
      .collect()
      .head
      .get(0)
    val sparkD = sparkValue match {
      case d: Double => d
      case other => fail(s"Expected Double, got: $other")
    }
    sparkD shouldBe (anchor +- 1e-9)
  }

  "kurtosis and skewness" should "use Spark's biased moment estimators" in {
    checkStats(Expr.skewness[Vals](yCell), -0.9128709291752769)
    checkStats(Expr.kurtosis[Vals](yCell), -0.5)
  }

  it should "return None for symmetric data (skewness 0) on both backends" in {
    val symmetric = spark.sql("""
        SELECT inline(array(
          struct(cast(1.0 as double)), struct(cast(2.0 as double)), struct(cast(3.0 as double)),
          struct(cast(4.0 as double)), struct(cast(5.0 as double)), struct(cast(6.0 as double))
        )) AS (y_value)
      """)
    val md = RowConverter.toMaterialized(symmetric.collect(), summon[Schema[Vals]]).fold(err => fail(s"$err"), identity)
    val inMemory = ExprInterpreter.evalAggregation(Expr.skewness[Vals](yCell), md.columns) match {
      case Right(Some(d: Double)) => d
      case other => fail(s"Expected Some(Double), got: $other")
    }
    inMemory shouldBe (0.0 +- 1e-9)
  }
}
