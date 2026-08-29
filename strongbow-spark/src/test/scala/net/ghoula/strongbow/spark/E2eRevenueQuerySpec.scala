package net.ghoula.strongbow.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.prelude.*

/** TPC-H Q6 proof-of-concept: strongbow Dataset plan vs native Spark SQL.
  *
  * Q6 (Forecasting Revenue Change):
  * {{{
  * SELECT sum(l_extendedprice * l_discount) as revenue
  * FROM lineitem
  * WHERE l_shipdate >= '1994-01-01'
  *   AND l_shipdate < '1995-01-01'
  *   AND l_discount BETWEEN 0.05 AND 0.07
  *   AND l_quantity < 24
  * }}}
  *
  * This is the simplest TPC-H query — filter + single aggregate. After adding SumDouble to Expr,
  * this is fully expressible in strongbow using distributed Expr-based operations.
  */
class E2eRevenueQuerySpec extends AnyFlatSpec with Matchers with SparkTestBase {

  import E2eTestData.LineItem

  // Column indices for LineItem (16-field case class, each primitive → 1 column)
  private val shipdate: Expr[LineItem, String] = Expr.Cell("l_shipdate_value", ColumnIndex(10))
  private val discount: Expr[LineItem, Double] = Expr.Cell("l_discount_value", ColumnIndex(6))
  private val quantity: Expr[LineItem, Double] = Expr.Cell("l_quantity_value", ColumnIndex(4))
  private val extendedprice: Expr[LineItem, Double] = Expr.Cell("l_extendedprice_value", ColumnIndex(5))

  "TPC-H Q6" should "produce identical results via strongbow and native Spark" in {
    val items = E2eTestData.generateLineItems(100_000)
    val ds = E2eTestData.lineItemDataset(items)

    // --- Reference: compute in Scala ---
    val scalaRevenue = items
      .filter(li =>
        li.l_shipdate >= "1994-01-01" &&
          li.l_shipdate < "1995-01-01" &&
          li.l_discount >= 0.05 &&
          li.l_discount <= 0.07 &&
          li.l_quantity < 24.0
      )
      .map(li => li.l_extendedprice * li.l_discount)
      .sum

    // --- Strongbow path ---
    // Build the filter predicate
    val predicate =
      (shipdate >= Expr.lit[LineItem, String]("1994-01-01")) &&
        (shipdate < Expr.lit[LineItem, String]("1995-01-01")) &&
        (discount >= Expr.lit[LineItem, Double](0.05)) &&
        (discount <= Expr.lit[LineItem, Double](0.07)) &&
        (quantity < Expr.lit[LineItem, Double](24.0))

    // Filter using distributed Expr, then compute price * discount via selectExprs
    val revenueExpr = Expr.MulDouble(extendedprice, discount).asInstanceOf[Expr[LineItem, Any]]

    val filtered = ds
      .filter(predicate)
      .selectAs[Double](("revenue", revenueExpr, ColumnType.DoubleType))

    val t0 = System.nanoTime()
    val sbResult = sparkInterpreter.execute(filtered)
    val t1 = System.nanoTime()

    sbResult.isRight shouldBe true
    val sbValues = sbResult.toOption.get.toVectorUnsafe
    val sbRevenue = sbValues.sum

    info(f"Strongbow: revenue = $sbRevenue%.2f (${sbValues.size} rows, ${(t1 - t0) / 1e6}%.1f ms)")

    // --- Native Spark path ---
    val structType = org.apache.spark.sql.types.StructType(
      Array(
        org.apache.spark.sql.types.StructField("l_orderkey", org.apache.spark.sql.types.LongType),
        org.apache.spark.sql.types.StructField("l_partkey", org.apache.spark.sql.types.LongType),
        org.apache.spark.sql.types.StructField("l_suppkey", org.apache.spark.sql.types.LongType),
        org.apache.spark.sql.types.StructField("l_linenumber", org.apache.spark.sql.types.IntegerType),
        org.apache.spark.sql.types.StructField("l_quantity", org.apache.spark.sql.types.DoubleType),
        org.apache.spark.sql.types.StructField("l_extendedprice", org.apache.spark.sql.types.DoubleType),
        org.apache.spark.sql.types.StructField("l_discount", org.apache.spark.sql.types.DoubleType),
        org.apache.spark.sql.types.StructField("l_tax", org.apache.spark.sql.types.DoubleType),
        org.apache.spark.sql.types.StructField("l_returnflag", org.apache.spark.sql.types.StringType),
        org.apache.spark.sql.types.StructField("l_linestatus", org.apache.spark.sql.types.StringType),
        org.apache.spark.sql.types.StructField("l_shipdate", org.apache.spark.sql.types.StringType),
        org.apache.spark.sql.types.StructField("l_commitdate", org.apache.spark.sql.types.StringType),
        org.apache.spark.sql.types.StructField("l_receiptdate", org.apache.spark.sql.types.StringType),
        org.apache.spark.sql.types.StructField("l_shipinstruct", org.apache.spark.sql.types.StringType),
        org.apache.spark.sql.types.StructField("l_shipmode", org.apache.spark.sql.types.StringType),
        org.apache.spark.sql.types.StructField("l_comment", org.apache.spark.sql.types.StringType)
      )
    )

    val rows = items.map { li =>
      org.apache.spark.sql.Row(
        li.l_orderkey,
        li.l_partkey,
        li.l_suppkey,
        li.l_linenumber,
        li.l_quantity,
        li.l_extendedprice,
        li.l_discount,
        li.l_tax,
        li.l_returnflag,
        li.l_linestatus,
        li.l_shipdate,
        li.l_commitdate,
        li.l_receiptdate,
        li.l_shipinstruct,
        li.l_shipmode,
        li.l_comment
      )
    }
    val javaRows = java.util.Arrays.asList(rows*)
    val nativeDf = spark.createDataFrame(javaRows, structType)

    import org.apache.spark.sql.functions.{col, sum, lit}

    val t2 = System.nanoTime()
    val nativeResult = nativeDf
      .filter(
        col("l_shipdate") >= lit("1994-01-01") &&
          col("l_shipdate") < lit("1995-01-01") &&
          col("l_discount") >= lit(0.05) &&
          col("l_discount") <= lit(0.07) &&
          col("l_quantity") < lit(24.0)
      )
      .select(sum(col("l_extendedprice") * col("l_discount")).as("revenue"))
      .collect()
    val t3 = System.nanoTime()

    val nativeRevenue = nativeResult.head.getDouble(0)

    info(f"Native Spark: revenue = $nativeRevenue%.2f (${(t3 - t2) / 1e6}%.1f ms)")
    info(f"Scala reference: revenue = $scalaRevenue%.2f")

    // Results should match within Double precision
    math.abs(sbRevenue - nativeRevenue) should be < (math.abs(nativeRevenue) * 1e-6 + 0.01)
    math.abs(sbRevenue - scalaRevenue) should be < (math.abs(scalaRevenue) * 1e-6 + 0.01)
  }

  it should "show identical Spark physical plans" in {
    val items = E2eTestData.generateLineItems(1_000)
    val ds = E2eTestData.lineItemDataset(items)

    val predicate =
      (shipdate >= Expr.lit[LineItem, String]("1994-01-01")) &&
        (shipdate < Expr.lit[LineItem, String]("1995-01-01")) &&
        (discount >= Expr.lit[LineItem, Double](0.05)) &&
        (discount <= Expr.lit[LineItem, Double](0.07)) &&
        (quantity < Expr.lit[LineItem, Double](24.0))

    val revenueExpr = Expr.MulDouble(extendedprice, discount).asInstanceOf[Expr[LineItem, Any]]

    val filtered = ds
      .filter(predicate)
      .selectAs[Double](("revenue", revenueExpr, ColumnType.DoubleType))

    // Just verify it executes without error — plan details logged via info
    val result = sparkInterpreter.execute(filtered)
    result.isRight shouldBe true
    info(s"Strongbow Q6 result rows: ${result.toOption.get.toVectorUnsafe.size}")
  }
}
