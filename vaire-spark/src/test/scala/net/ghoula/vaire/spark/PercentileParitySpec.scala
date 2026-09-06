package net.ghoula.vaire.spark

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.column.ColumnType
import net.ghoula.vaire.expr.Expr
import net.ghoula.vaire.interpreter.ExprInterpreter
import net.ghoula.vaire.prelude.*

/** Parity for the exact percentile aggregate.
  *
  * Anchor: y = (2,4,5,4,5) sorted is (2,4,4,5,5); p=0.5 → index 2.0 → 4; p=0.25 → index 1.0 → 4;
  * p=0.625 → index 2.5 → 4 + 0.5·(5−4) = 4.5. Verified independently.
  */
class PercentileParitySpec extends AnyFlatSpec with Matchers with SparkTestBase {

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
  private def constD(v: Double): Expr[Vals, Double] = Expr.const[Vals, Double](v)

  private def checkPercentile(p: Double, anchor: Double): Unit = {
    val expr = Expr.percentile[Vals](yCell, constD(p))
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

  "percentile" should "interpolate exactly on both backends" in {
    checkPercentile(0.0, 2.0)
    checkPercentile(0.25, 4.0)
    checkPercentile(0.5, 4.0)
    checkPercentile(0.625, 4.5)
    checkPercentile(1.0, 5.0)
  }
}
