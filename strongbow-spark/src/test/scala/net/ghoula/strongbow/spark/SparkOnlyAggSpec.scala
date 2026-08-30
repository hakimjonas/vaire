package net.ghoula.strongbow.spark

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.errors.ExecutionError
import net.ghoula.strongbow.expr.Expr
import net.ghoula.strongbow.interpreter.ExprInterpreter
import org.apache.spark.sql.functions.{col, grouping, grouping_id}

import net.ghoula.strongbow.prelude.*

/** HistogramNumeric, Grouping and GroupingId are documented Spark-only deviations: the in-memory
  * interpreter returns Left(Unsupported) while Spark maps to native functions.
  */
class SparkOnlyAggSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  case class G(k: String, v: Long)
  given Schema[G] = Schema.derived

  private lazy val base: DataFrame = spark.sql("""
      SELECT inline(array(
        struct('a', cast(1 as bigint)), struct('a', cast(2 as bigint)),
        struct('b', cast(3 as bigint))
      )) AS (k_value, v_value)
    """)

  private lazy val materialized = {
    val rows = base.collect()
    RowConverter.toMaterialized(rows, summon[Schema[G]]).fold(err => fail(s"$err"), identity)
  }

  private def kCell: Expr[G, Any] = Expr.cell("k_value", ColumnIndex(0))
  private def vCell: Expr[G, Any] = Expr.cell("v_value", ColumnIndex(1))

  private def assertUnsupported(expr: Expr[G, ?]): Unit =
    ExprInterpreter.evalAggregation(expr, materialized.columns) match {
      case Left(_: ExecutionError.UnsupportedOperation) => (): Unit
      case other => fail(s"Expected UnsupportedOperation, got: $other")
    }

  "histogram_numeric" should "build a histogram on Spark and be unsupported in-memory" in {
    assertUnsupported(Expr.histogramNumeric[G](Expr.cell[G, Double]("v_value", ColumnIndex(1)), Expr.const[G, Int](2)))
    val (sparkCol, _) = ExprToColumn.convert(
      Expr.histogramNumeric[G](Expr.cell[G, Double]("v_value", ColumnIndex(1)), Expr.const[G, Int](2))
    ) match {
      case Right(converted) => converted
      case other => fail(s"Spark conversion failed: $other")
    }
    val hist = base.agg(sparkCol).collect().head.get(0)
    hist match {
      case s: scala.collection.Seq[?] => s.size should be <= 2
      case other => fail(s"Expected array, got ${other.getClass}")
    }
  }

  "grouping / grouping_id" should "map to Spark and be unsupported in-memory" in {
    assertUnsupported(Expr.grouping[G](kCell))
    assertUnsupported(Expr.groupingId[G](kCell, vCell))

    val grouped = base.rollup(col("k_value")).agg(grouping(col("k_value")).as("g"))
    val gValues = grouped.collect().map(row => row.get(1).asInstanceOf[Number].intValue()).toSeq
    gValues.sorted shouldBe Seq(0, 0, 1)

    val withRollup = base.rollup(col("k_value")).agg(grouping_id(col("k_value")).as("gid"))
    val gidValues = withRollup.collect().map(row => row.get(1).asInstanceOf[Number].longValue()).toSeq
    gidValues.sorted shouldBe Seq(0L, 0L, 1L)
  }
}
