package net.ghoula.vaire.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{functions => F}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.prelude.*

import scala.util.Random

class SparkPlanVsExecBench extends AnyFlatSpec with Matchers with SparkTestBase {

  override protected lazy val spark: SparkSession = {
    val session = SparkSession
      .builder()
      .master("local[2]")
      .appName("vaire-plan-vs-exec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.shuffle.compress", "false")
      .config("spark.shuffle.spill.compress", "false")
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    session
  }

  private val Warmup = 3
  private val Measured = 7
  private val Scales = Vector(100_000, 500_000, 1_000_000, 5_000_000)
  private val NumGroups = 1_000

  case class BenchRow(key: String, value: Int)
  given benchRowSchema: Schema[BenchRow] = Schema.derived

  case class AggResult(key: String, totalValue: Long, cnt: Long)
  given aggResultSchema: Schema[AggResult] = Schema.derived

  case class WindowResult(key: String, value: Int, rowNum: Int)
  given windowResultSchema: Schema[WindowResult] = Schema.derived

  private case class Timing(planNs: Long, execNs: Long) {
    def planMs: Double = planNs / 1e6
    def execMs: Double = execNs / 1e6
    def totalMs: Double = (planNs + execNs) / 1e6
  }

  private def median(values: Seq[Timing]): Timing = {
    val byTotal = values.sortBy(t => t.planNs + t.execNs)
    byTotal(byTotal.length / 2)
  }

  private def cachedDf(rows: Int, groups: Int): DataFrame = {
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{IntegerType, StringType => SparkStringType, StructField, StructType}

    val r = new Random(42)
    val javaRows = java.util.Arrays.asList(
      Array.tabulate(rows)(i => Row(s"group${r.nextInt(groups)}", r.nextInt(1000)))*
    )
    val structType = StructType(
      Array(
        StructField("key_value", SparkStringType, nullable = false),
        StructField("value_value", IntegerType, nullable = false)
      )
    )
    val df = spark.createDataFrame(javaRows, structType).cache()
    df.count()
    df
  }

  private def measure(warmup: Int, runs: Int)(planF: => DataFrame): Seq[Timing] =
    (0 until warmup + runs).map { _ =>
      val t0 = System.nanoTime()
      val df = planF
      val t1 = System.nanoTime()
      df.count()
      val t2 = System.nanoTime()
      Timing(t1 - t0, t2 - t1)
    }.drop(warmup)

  private def runOp(
    opName: String,
    scale: Int,
    groups: Int,
    sbPlan: Dataset[?] => DataFrame,
    sbDataset: DataFrame => Dataset[?],
    nativePlan: DataFrame => DataFrame
  ): (Timing, Timing) = {
    val df = cachedDf(scale, groups)
    val ds = SparkDatasets.fromDataFrame(df, benchRowSchema)

    val sbTimings = measure(Warmup, Measured) { sbPlan(sbDataset(df)) }
    val nativeTimings = measure(Warmup, Measured) { nativePlan(df) }

    val sb = median(sbTimings)
    val native = median(nativeTimings)
    (sb, native)
  }

  private def printHeader(scale: Int): Unit = {
    val label = scale match {
      case s if s >= 1_000_000 => s"${s / 1_000_000}M"
      case s => s"${s / 1_000}K"
    }
    info(f"\n${"=" * 85}")
    info(f"$label rows, $NumGroups groups (Spark 4.1.1)")
    info(f"${"=" * 85}")
    info(
      f"${"Operation"}%-16s  ${"Plan(SB)"}%10s ${"Plan(Nat)"}%10s ${"Exec(SB)"}%10s ${"Exec(Nat)"}%10s ${"Overhead"}%10s"
    )
    info("-" * 85)
  }

  private def printRow(op: String, sb: Timing, native: Timing): Unit = {
    val overhead = if (native.totalMs > 0) sb.totalMs / native.totalMs else 0.0
    info(
      f"$op%-16s  ${sb.planMs}%7.1f ms ${native.planMs}%7.1f ms ${sb.execMs}%7.1f ms ${native.execMs}%7.1f ms ${overhead}%9.2fx"
    )
  }

  "Plan vs Exec" should "show overhead amortizes with scale" taggedAs Benchmark in {
    val valCell = Expr.Cell[BenchRow, Int]("value_value", ColumnIndex(1))
    val keyCell: Expr[BenchRow, Any] = Expr.Cell("key_value", ColumnIndex(0))
    val keyStrCell: Expr[BenchRow, String] = Expr.Cell("key_value", ColumnIndex(0))

    Scales.foreach { scale =>
      printHeader(scale)

      val df = cachedDf(scale, NumGroups)
      val ds = SparkDatasets.fromDataFrame(df, benchRowSchema)

      val filterSb = measure(Warmup, Measured) {
        val filtered = ds.filter(valCell > Expr.lit(500))
        sparkInterpreter.toDataFrame(filtered).toOption.get
      }
      val filterNat = measure(Warmup, Measured) {
        df.filter(F.col("value_value") > F.lit(500))
      }
      printRow("Filter", median(filterSb), median(filterNat))

      val sortSb = measure(Warmup, Measured) {
        val sorted = ds.sortByExpr(valCell, ColumnType.IntType)(using Ordering[Int])
        sparkInterpreter.toDataFrame(sorted).toOption.get
      }
      val sortNat = measure(Warmup, Measured) {
        df.sort(F.col("value_value"))
      }
      printRow("Sort", median(sortSb), median(sortNat))

      val distinctSb = measure(Warmup, Measured) {
        sparkInterpreter.toDataFrame(ds.distinct).toOption.get
      }
      val distinctNat = measure(Warmup, Measured) {
        df.distinct()
      }
      printRow("Distinct", median(distinctSb), median(distinctNat))

      val aggKeys = Vector(KeySpec[BenchRow, Any]("key", keyCell, ColumnType.StringType))
      val aggs = Vector(
        AggSpec("totalValue", Expr.Sum(valCell), ColumnType.LongType),
        AggSpec("cnt", Expr.Count[BenchRow](), ColumnType.LongType)
      )
      val groupSb = measure(Warmup, Measured) {
        val grouped = ds.groupByAgg[AggResult](aggKeys, aggs)
        sparkInterpreter.toDataFrame(grouped).toOption.get
      }
      val groupNat = measure(Warmup, Measured) {
        df.groupBy("key_value").agg(F.sum("value_value").as("totalValue"), F.count("*").as("cnt"))
      }
      printRow("GroupByAgg", median(groupSb), median(groupNat))

      val sortKeys = Vector(
        SortSpec(valCell, summon[Ordering[Int]], ColumnType.IntType, true),
        SortSpec(keyStrCell, summon[Ordering[String]], ColumnType.StringType, false)
      )
      val sortExprsSb = measure(Warmup, Measured) {
        sparkInterpreter.toDataFrame(ds.sortByExprs(sortKeys)).toOption.get
      }
      val sortExprsNat = measure(Warmup, Measured) {
        df.sort(F.col("value_value").asc, F.col("key_value").desc)
      }
      printRow("SortByExprs", median(sortExprsSb), median(sortExprsNat))

      val ds2 = SparkDatasets.fromDataFrame(cachedDf(scale, NumGroups), benchRowSchema)
      val leftKey: Expr[BenchRow, String] = Expr.Cell("key_value", ColumnIndex(0))
      val rightKey: Expr[BenchRow, String] = Expr.Cell("key_value", ColumnIndex(0))
      val joinSb = measure(Warmup, Measured) {
        val joined = ds.joinOn(ds2, leftKey, rightKey, ColumnType.StringType, ColumnType.StringType)
        sparkInterpreter.toDataFrame(joined).toOption.get
      }
      val df2 = cachedDf(scale, NumGroups)
      val joinNat = measure(Warmup, Measured) {
        df.alias("_l").join(df2.alias("_r"), F.col("_l.key_value") === F.col("_r.key_value"))
      }
      printRow("JoinOn", median(joinSb), median(joinNat))

      val unionSb = measure(Warmup, Measured) {
        sparkInterpreter.toDataFrame(ds.union(ds)).toOption.get
      }
      val unionNat = measure(Warmup, Measured) {
        df.union(df)
      }
      printRow("Union", median(unionSb), median(unionNat))

      df.unpersist()
    }
  }
}
