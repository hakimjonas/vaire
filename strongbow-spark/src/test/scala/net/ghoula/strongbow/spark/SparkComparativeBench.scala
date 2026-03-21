package net.ghoula.strongbow.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.lang.management.ManagementFactory
import scala.jdk.CollectionConverters.*
import scala.util.Random

import net.ghoula.strongbow.{ColumnType, Dataset, Expr, Schema}
import net.ghoula.strongbow.specs.{AggSpec, KeySpec, SortSpec}
import net.ghoula.strongbow.types.ColumnIndex

/** Head-to-head Spark benchmark matching dwh-core's SparkComparativeBenchmark.
  *
  * Same operations, same scales, same deterministic seed (42/99). Run separately from dwh-core's
  * benchmark, compare wall clock times.
  *
  * Spark 4.1.1 | Scala 3.8.2 | Strongbow columnar Dataset -> SparkInterpreter
  */
class SparkComparativeBench extends AnyFlatSpec with Matchers with SparkTestBase {

  private val outputFile = java.nio.file.Paths.get("target", "spark-comparative-results.txt")

  override protected lazy val spark: SparkSession = {
    val session = SparkSession
      .builder()
      .master("local[2]")
      .appName("strongbow-comparative-bench")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.driver.maxResultSize", "1g")
      .config("spark.shuffle.compress", "false")
      .config("spark.shuffle.spill.compress", "false")
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    session
  }

  private def writeToFile(line: String): Unit = {
    java.nio.file.Files.write(
      outputFile,
      (line + "\n").getBytes,
      java.nio.file.StandardOpenOption.CREATE,
      java.nio.file.StandardOpenOption.APPEND
    )
  }

  private val gcBeans = ManagementFactory.getGarbageCollectorMXBeans.asScala.toList

  private def getGCStats(): (Long, Long) = {
    val totalCollections = gcBeans.map(_.getCollectionCount).sum
    val totalGCTime = gcBeans.map(_.getCollectionTime).sum
    (totalCollections, totalGCTime)
  }

  case class BenchRow(key: String, value: Int)
  given benchRowSchema: Schema[BenchRow] = Schema.derived

  case class AggResult(key: String, totalValue: Long, cnt: Long)
  given aggResultSchema: Schema[AggResult] = Schema.derived

  private def generateData(rows: Int, numGroups: Int): Dataset[BenchRow] = {
    import org.apache.spark.sql.types.{IntegerType, StringType => SparkStringType, StructField, StructType}
    import org.apache.spark.sql.Row

    val r = new Random(42)
    val javaRows =
      java.util.Arrays.asList(Array.tabulate(rows)(i => Row(s"group${r.nextInt(numGroups)}", r.nextInt(1000)))*)
    val structType = StructType(
      Array(
        StructField("key_value", SparkStringType, nullable = false),
        StructField("value_value", IntegerType, nullable = false)
      )
    )
    val df = spark.createDataFrame(javaRows, structType).cache()
    df.count()
    SparkDatasets.fromDataFrame(df, benchRowSchema)
  }

  private def generateHalfData(rows: Int, numGroups: Int, seed: Int): Dataset[BenchRow] = {
    import org.apache.spark.sql.types.{IntegerType, StringType => SparkStringType, StructField, StructType}
    import org.apache.spark.sql.Row

    val r = new Random(seed)
    val javaRows =
      java.util.Arrays.asList(Array.tabulate(rows)(i => Row(s"group${r.nextInt(numGroups)}", r.nextInt(1000)))*)
    val structType = StructType(
      Array(
        StructField("key_value", SparkStringType, nullable = false),
        StructField("value_value", IntegerType, nullable = false)
      )
    )
    val df = spark.createDataFrame(javaRows, structType).cache()
    df.count()
    SparkDatasets.fromDataFrame(df, benchRowSchema)
  }

  case class ScaleResult(
    operation: String,
    medianMs: Double,
    p95Ms: Double,
    gcCollections: Long,
    gcTimeMs: Long,
    gcOverhead: Double
  )

  private def benchmarkOp(operation: String, warmups: Int, runs: Int)(f: => Long): ScaleResult = {
    (0 until warmups).foreach(_ => f)

    val (gcBefore, gcTimeBefore) = getGCStats()
    val startWall = System.nanoTime()

    val times = (0 until runs).map { _ =>
      val start = System.nanoTime()
      f
      val end = System.nanoTime()
      (end - start) / 1_000_000.0
    }

    val endWall = System.nanoTime()
    val totalWallMs = (endWall - startWall) / 1_000_000.0
    val (gcAfter, gcTimeAfter) = getGCStats()

    val sorted = times.sorted
    val median = sorted(runs / 2)
    val p95 = sorted((runs * 0.95).toInt)
    val gcCollections = gcAfter - gcBefore
    val gcTimeMs = gcTimeAfter - gcTimeBefore
    val gcOverhead = if (totalWallMs > 0) (gcTimeMs.toDouble / totalWallMs) * 100.0 else 0.0

    ScaleResult(operation, median, p95, gcCollections, gcTimeMs, gcOverhead)
  }

  private def runScale(rows: Int, groups: Int): Unit = {
    val scale = rows match {
      case 10000 => "10K"
      case 100000 => "100K"
      case 200000 => "200K"
      case 400000 => "400K"
      case _ => s"${rows / 1000}K"
    }

    val dataset = generateData(rows, groups)
    val results = scala.collection.mutable.ArrayBuffer[ScaleResult]()
    val warmups = 2
    val runs = 5

    val filterResult = benchmarkOp("Filter", warmups, runs) {
      val valCell = Expr.Cell[BenchRow, Int]("value_value", ColumnIndex(1))
      val filtered = dataset.filter(valCell > Expr.lit(500))
      sparkInterpreter.toDataFrame(filtered).toOption.get.count()
    }
    results += filterResult

    val groupByResult = benchmarkOp("GroupBy", warmups, runs) {
      val keyCell: Expr[BenchRow, Any] = Expr.Cell("key_value", ColumnIndex(0))
      val valCell: Expr[BenchRow, Int] = Expr.Cell("value_value", ColumnIndex(1))
      val aggKeys = Vector(KeySpec[BenchRow, Any]("key", keyCell, ColumnType.StringType))
      val aggs = Vector(AggSpec("total", Expr.Sum(valCell), ColumnType.LongType))
      val grouped = dataset.groupByAgg[(String, Long)](aggKeys, aggs)
      sparkInterpreter.toDataFrame(grouped).toOption.get.count()
    }
    results += groupByResult

    val sortResult = benchmarkOp("Sort", warmups, runs) {
      val valCell = Expr.Cell[BenchRow, Int]("value_value", ColumnIndex(1))
      val sorted = dataset.sortByExpr(valCell, ColumnType.IntType)(using Ordering[Int])
      sparkInterpreter.toDataFrame(sorted).toOption.get.count()
    }
    results += sortResult

    val limitResult = benchmarkOp("Limit", warmups, runs) {
      val limited = dataset.limit(rows / 2)
      sparkInterpreter.toDataFrame(limited).toOption.get.count()
    }
    results += limitResult

    val unionResult = benchmarkOp("Union", warmups, runs) {
      val unioned = dataset.union(dataset)
      sparkInterpreter.toDataFrame(unioned).toOption.get.count()
    }
    results += unionResult

    val distinctResult = benchmarkOp("Distinct", warmups, runs) {
      val distincted = dataset.distinct
      sparkInterpreter.toDataFrame(distincted).toOption.get.count()
    }
    results += distinctResult

    val halfSize = rows / 2
    val dataset1 = generateHalfData(halfSize, groups / 2, 42)
    val dataset2 = generateHalfData(halfSize, groups / 2, 99)

    val joinResult = benchmarkOp("Join", warmups, runs) {
      val leftKey: Expr[BenchRow, String] = Expr.Cell("key_value", ColumnIndex(0))
      val rightKey: Expr[BenchRow, String] = Expr.Cell("key_value", ColumnIndex(0))
      val joined = dataset1.joinOn(dataset2, leftKey, rightKey, ColumnType.StringType, ColumnType.StringType)
      sparkInterpreter.toDataFrame(joined).toOption.get.count()
    }
    results += joinResult

    val groupByAggResult = benchmarkOp("GroupByAgg", warmups, runs) {
      val keyCell: Expr[BenchRow, Any] = Expr.Cell("key_value", ColumnIndex(0))
      val valCell: Expr[BenchRow, Int] = Expr.Cell("value_value", ColumnIndex(1))
      val aggKeys = Vector(KeySpec[BenchRow, Any]("key", keyCell, ColumnType.StringType))
      val aggs = Vector(
        AggSpec("totalValue", Expr.Sum(valCell), ColumnType.LongType),
        AggSpec("cnt", Expr.Count[BenchRow](), ColumnType.LongType)
      )
      val grouped = dataset.groupByAgg[AggResult](aggKeys, aggs)
      sparkInterpreter.toDataFrame(grouped).toOption.get.count()
    }
    results += groupByAggResult

    val sortByExprsResult = benchmarkOp("SortByExprs", warmups, runs) {
      val valCell: Expr[BenchRow, Int] = Expr.Cell("value_value", ColumnIndex(1))
      val keyCell: Expr[BenchRow, String] = Expr.Cell("key_value", ColumnIndex(0))
      val sortKeys = Vector(
        SortSpec(valCell, summon[Ordering[Int]], ColumnType.IntType, true),
        SortSpec(keyCell, summon[Ordering[String]], ColumnType.StringType, true)
      )
      val sorted = dataset.sortByExprs(sortKeys)
      sparkInterpreter.toDataFrame(sorted).toOption.get.count()
    }
    results += sortByExprsResult

    val header = s"STRONGBOW SPARK — $scale rows, $groups groups (Spark 4.1.1)"
    val colHeader = f"${"Operation"}%-14s ${"Median"}%12s ${"P95"}%12s ${"GC"}%8s ${"GC%"}%8s"
    val separator = "-" * 70
    val lines = results.map { r =>
      f"${r.operation}%-14s ${r.medianMs}%9.2f ms ${r.p95Ms}%9.2f ms ${r.gcCollections}%8d ${r.gcOverhead}%7.1f%%"
    }

    val block = (Vector(s"\n${"=" * 70}", header, "=" * 70, colHeader, separator) ++ lines).mkString("\n")
    info(block)
    writeToFile(block)
  }

  "Strongbow Spark comparative" should "benchmark at 10K rows" in { runScale(10000, 100) }
  it should "benchmark at 100K rows" in { runScale(100000, 1000) }
  it should "benchmark at 200K rows" in { runScale(200000, 2000) }
  it should "benchmark at 400K rows" in { runScale(400000, 4000) }
}
