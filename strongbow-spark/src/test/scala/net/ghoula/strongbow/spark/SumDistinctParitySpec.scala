package net.ghoula.strongbow.spark

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.expr.Expr
import net.ghoula.strongbow.interpreter.ExprInterpreter
import net.ghoula.strongbow.prelude.*

/** Parity for sum_distinct. Anchor: values (1,2,2,3) → distinct sum 6 (verified independently). */
class SumDistinctParitySpec extends AnyFlatSpec with Matchers with SparkTestBase {

  case class N(n: Long)
  given Schema[N] = Schema.derived

  private lazy val base: DataFrame = spark.sql("""
      SELECT inline(array(
        struct(cast(1 as bigint)), struct(cast(2 as bigint)),
        struct(cast(2 as bigint)), struct(cast(3 as bigint))
      )) AS (n_value)
    """)

  private lazy val materialized = {
    val rows = base.collect()
    RowConverter.toMaterialized(rows, summon[Schema[N]]).fold(err => fail(s"$err"), identity)
  }

  "sum_distinct" should "sum unique values on both backends" in {
    val expr = Expr.sumDistinct[N](Expr.cell[N, Long]("n_value", ColumnIndex(0)))
    ExprInterpreter.evalAggregation(expr, materialized.columns) shouldBe Right(Some(6L))
    base
      .agg(ExprToColumn.convert(expr) match {
        case Right((c, _)) => c
        case other => fail(s"Spark conversion failed: $other")
      })
      .collect()
      .head
      .get(0) shouldBe 6L
  }
}
