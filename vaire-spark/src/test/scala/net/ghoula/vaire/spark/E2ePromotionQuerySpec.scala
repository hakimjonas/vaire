package net.ghoula.vaire.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.prelude.*

/** TPC-H Q14 proof-of-concept: vaire Dataset plan vs native Spark SQL.
  *
  * Q14 (Promotion Effect):
  * {{{
  * SELECT 100.00 * sum(CASE WHEN p_type LIKE 'PROMO%'
  *   THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
  *   / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
  * FROM lineitem, part
  * WHERE l_partkey = p_partkey
  *   AND l_shipdate >= '1995-09-01'
  *   AND l_shipdate < '1995-10-01'
  * }}}
  *
  * Tests distributed equi-join + filter + CASE WHEN + LIKE + SumDouble.
  */
class E2ePromotionQuerySpec extends AnyFlatSpec with Matchers with SparkTestBase with SlowTests {

  import E2eTestData.{LineItem, Part, lineItemSchema, partSchema}

  "TPC-H Q14" should "produce identical results via vaire and native Spark" in {
    val items = E2eTestData.generateLineItems(100_000)
    val partCount = 200_000
    val parts = E2eTestData.generateParts(partCount)

    // --- Reference: compute in Scala ---
    val partMap = parts.map(p => p.p_partkey -> p).toMap
    val filteredItems = items.filter(li => li.l_shipdate >= "1995-09-01" && li.l_shipdate < "1995-10-01")
    val joinedPairs = filteredItems.flatMap { li =>
      partMap.get(li.l_partkey).map(p => (li, p))
    }
    val totalRevenue = joinedPairs.map { case (li, _) =>
      li.l_extendedprice * (1.0 - li.l_discount)
    }.sum
    val promoRevenue = joinedPairs.filter { case (_, p) => p.p_type.startsWith("PROMO") }.map { case (li, _) =>
      li.l_extendedprice * (1.0 - li.l_discount)
    }.sum
    val scalaPromoPercent = if (totalRevenue == 0.0) 0.0 else 100.0 * promoRevenue / totalRevenue

    // --- Vairë path ---
    // Use joinOn for distributed equi-join: lineitem.l_partkey = part.p_partkey
    // After join, the schema is (LineItem, Part) — a tuple2 with prefixed columns.
    //
    // For a tuple2 schema: left fields get "_1_fieldName_value", right get "_2_fieldName_value"
    // Left (LineItem): _1_l_orderkey_value(0) .. _1_l_comment_value(15)   (16 columns)
    // Right (Part): _2_p_partkey_value(16) .. _2_p_comment_value(24)      (9 columns)
    given Schema[(LineItem, Part)] = Schema.tuple2Schema[LineItem, Part]

    val liDs = E2eTestData.lineItemDataset(items)
    val partDs = E2eTestData.partDataset(parts)

    val liPartkey: Expr[LineItem, Long] = Expr.Cell("l_partkey_value", ColumnIndex(1))
    val pPartkey: Expr[Part, Long] = Expr.Cell("p_partkey_value", ColumnIndex(0))

    // Join
    val joined = liDs.joinOn(
      partDs,
      liPartkey,
      pPartkey,
      ColumnType.LongType,
      ColumnType.LongType
    )

    // After join, column indices shift. The Spark DataFrame has the left columns
    // followed by the right columns (using their original names without prefix):
    // Left (LineItem): indices 0-15, named l_orderkey_value, l_partkey_value, ...
    // Right (Part): indices 16-24, named p_partkey_value, p_name_value, ...
    //
    // Part fields: p_partkey(16), p_name(17), p_mfgr(18), p_brand(19), p_type(20),
    //              p_size(21), p_container(22), p_retailprice(23), p_comment(24)
    type LP = (LineItem, Part)
    val jShipdate: Expr[LP, String] = Expr.Cell("l_shipdate_value", ColumnIndex(10))
    val jExtPrice: Expr[LP, Double] = Expr.Cell("l_extendedprice_value", ColumnIndex(5))
    val jDiscount: Expr[LP, Double] = Expr.Cell("l_discount_value", ColumnIndex(6))
    val jPType: Expr[LP, String] = Expr.Cell("p_type_value", ColumnIndex(20))

    // Filter by date
    val datePred =
      (jShipdate >= Expr.lit[LP, String]("1995-09-01")) &&
        (jShipdate < Expr.lit[LP, String]("1995-10-01"))

    val filtered = joined.filter(datePred)

    // Compute l_extendedprice * (1 - l_discount) via selectExprs
    val revenueExpr: Expr[LP, Double] = Expr.MulDouble(
      jExtPrice,
      Expr.SubDouble(Expr.lit[LP, Double](1.0), jDiscount)
    )

    // Compute CASE WHEN p_type LIKE 'PROMO%' THEN revenue ELSE 0.0
    val promoExpr: Expr[LP, Double] = Expr.When(
      jPType.like("PROMO%"),
      revenueExpr,
      Expr.lit[LP, Double](0.0)
    )

    // Select both expressions
    val projected = filtered.selectAs[(Double, Double)](
      ("promo_rev", promoExpr.asInstanceOf[Expr[LP, Any]], ColumnType.DoubleType),
      ("total_rev", revenueExpr.asInstanceOf[Expr[LP, Any]], ColumnType.DoubleType)
    )

    val t0 = System.nanoTime()
    val sbResult = sparkInterpreter.execute(projected)
    val t1 = System.nanoTime()

    sbResult.isRight shouldBe true
    val sbValues = sbResult.toOption.get.toVectorUnsafe
    val sbTotalRev = sbValues.map(_._2).sum
    val sbPromoRev = sbValues.map(_._1).sum
    val sbPromoPercent = if (sbTotalRev == 0.0) 0.0 else 100.0 * sbPromoRev / sbTotalRev

    info(
      f"Vairë: promo%% = $sbPromoPercent%.4f (promo=$sbPromoRev%.2f, total=$sbTotalRev%.2f, ${sbValues.size} rows, ${(t1 - t0) / 1e6}%.1f ms)"
    )

    // --- Native Spark path ---
    val liStructType = org.apache.spark.sql.types.StructType(
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

    val partStructType = org.apache.spark.sql.types.StructType(
      Array(
        org.apache.spark.sql.types.StructField("p_partkey", org.apache.spark.sql.types.LongType),
        org.apache.spark.sql.types.StructField("p_name", org.apache.spark.sql.types.StringType),
        org.apache.spark.sql.types.StructField("p_mfgr", org.apache.spark.sql.types.StringType),
        org.apache.spark.sql.types.StructField("p_brand", org.apache.spark.sql.types.StringType),
        org.apache.spark.sql.types.StructField("p_type", org.apache.spark.sql.types.StringType),
        org.apache.spark.sql.types.StructField("p_size", org.apache.spark.sql.types.IntegerType),
        org.apache.spark.sql.types.StructField("p_container", org.apache.spark.sql.types.StringType),
        org.apache.spark.sql.types.StructField("p_retailprice", org.apache.spark.sql.types.DoubleType),
        org.apache.spark.sql.types.StructField("p_comment", org.apache.spark.sql.types.StringType)
      )
    )

    val liRows = items.map { li =>
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
    val partRows = parts.map { p =>
      org.apache.spark.sql.Row(
        p.p_partkey,
        p.p_name,
        p.p_mfgr,
        p.p_brand,
        p.p_type,
        p.p_size,
        p.p_container,
        p.p_retailprice,
        p.p_comment
      )
    }

    val liDf = spark.createDataFrame(java.util.Arrays.asList(liRows*), liStructType)
    val partDf = spark.createDataFrame(java.util.Arrays.asList(partRows*), partStructType)

    import org.apache.spark.sql.{functions => F}

    val t2 = System.nanoTime()
    val nativeResult = liDf
      .join(partDf, liDf("l_partkey") === partDf("p_partkey"))
      .filter(
        F.col("l_shipdate") >= F.lit("1995-09-01") &&
          F.col("l_shipdate") < F.lit("1995-10-01")
      )
      .select(
        F.sum(
          F.when(F.col("p_type").like("PROMO%"), F.col("l_extendedprice") * (F.lit(1.0) - F.col("l_discount")))
            .otherwise(F.lit(0.0))
        ).as("promo_rev"),
        F.sum(F.col("l_extendedprice") * (F.lit(1.0) - F.col("l_discount"))).as("total_rev")
      )
      .collect()
    val t3 = System.nanoTime()

    val nativePromoRev = nativeResult.head.getDouble(0)
    val nativeTotalRev = nativeResult.head.getDouble(1)
    val nativePromoPercent = if (nativeTotalRev == 0.0) 0.0 else 100.0 * nativePromoRev / nativeTotalRev

    info(
      f"Native Spark: promo%% = $nativePromoPercent%.4f (promo=$nativePromoRev%.2f, total=$nativeTotalRev%.2f, ${(t3 - t2) / 1e6}%.1f ms)"
    )
    info(f"Scala reference: promo%% = $scalaPromoPercent%.4f")

    // Results should match within Double precision
    // Note: vaire collects individual rows and sums on driver, native Spark aggregates
    // distributedly — ordering differences can cause small floating point drift.
    math.abs(sbPromoPercent - nativePromoPercent) should be < 0.01
    math.abs(sbPromoPercent - scalaPromoPercent) should be < 0.01
  }

  it should "correctly filter with LIKE in the CASE WHEN" in {
    // Small test: verify Like works in the join+filter+when pipeline
    val items = E2eTestData.generateLineItems(1_000)
    val parts = E2eTestData.generateParts(200_000)

    given Schema[(LineItem, Part)] = Schema.tuple2Schema[LineItem, Part]

    val liDs = E2eTestData.lineItemDataset(items)
    val partDs = E2eTestData.partDataset(parts)

    val liPartkey: Expr[LineItem, Long] = Expr.Cell("l_partkey_value", ColumnIndex(1))
    val pPartkey: Expr[Part, Long] = Expr.Cell("p_partkey_value", ColumnIndex(0))

    val joined = liDs.joinOn(
      partDs,
      liPartkey,
      pPartkey,
      ColumnType.LongType,
      ColumnType.LongType
    )

    type LP = (LineItem, Part)
    val jPType: Expr[LP, String] = Expr.Cell("p_type_value", ColumnIndex(20))

    // Filter to PROMO types via distributed Expr.Like
    val promoFilter = jPType.like("PROMO%")
    val promoJoined = joined.filter(promoFilter)

    val result = sparkInterpreter.execute(promoJoined)
    result.isRight shouldBe true

    val promoValues = result.toOption.get.toVectorUnsafe
    info(s"PROMO-filtered join: ${promoValues.size} rows")

    // Verify all matched rows have a PROMO type on the part side
    val partMapLocal = parts.map(p => p.p_partkey -> p).toMap
    promoValues.foreach { case (li, p) =>
      p.p_type should startWith("PROMO")
    }
  }
}
