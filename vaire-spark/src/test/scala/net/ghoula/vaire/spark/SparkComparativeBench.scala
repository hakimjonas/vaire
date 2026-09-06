package net.ghoula.vaire.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.lang.management.ManagementFactory
import scala.jdk.CollectionConverters.*
import scala.util.Random

import net.ghoula.vaire.prelude.*

/** Head-to-head Spark benchmark matching dwh-core's SparkComparativeBenchmark.
  *
  * Same operations, same scales, same deterministic seed (42/99). Run separately from dwh-core's
  * benchmark, compare wall clock times.
  *
  * Spark 4.1.1 | Scala 3.8.2 | Vairë columnar Dataset -> SparkInterpreter
  */
class SparkComparativeBench extends AnyFlatSpec with Matchers with SparkTestBase {

  private val outputFile = java.nio.file.Paths.get("target", "spark-comparative-results.txt")

  override protected lazy val spark: SparkSession = {
    val session = SparkSession
      .builder()
      .master("local[2]")
      .appName("vaire-comparative-bench")
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

  private val threadBean = ManagementFactory.getThreadMXBean.asInstanceOf[com.sun.management.ThreadMXBean]
  private val memoryBean = ManagementFactory.getMemoryMXBean
  private val gcBeans = ManagementFactory.getGarbageCollectorMXBeans.asScala.toList

  private def getGCStats(): (Long, Long) = {
    val totalCollections = gcBeans.map(_.getCollectionCount).sum
    val totalGCTime = gcBeans.map(_.getCollectionTime).sum
    (totalCollections, totalGCTime)
  }

  private def getHeapUsageMB(): Double =
    memoryBean.getHeapMemoryUsage.getUsed / (1024.0 * 1024.0)

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
    planAllocMB: Double,
    execAllocMB: Double,
    totalAllocMB: Double,
    peakHeapMB: Double,
    gcCollections: Long,
    gcTimeMs: Long,
    gcOverhead: Double
  )

  private def benchmarkOp(
    operation: String,
    warmups: Int,
    runs: Int
  )(planF: => org.apache.spark.sql.DataFrame): ScaleResult = {
    (0 until warmups).foreach(_ => planF.count())

    val threadId = Thread.currentThread().threadId()
    val (gcBefore, gcTimeBefore) = getGCStats()
    val allocBefore = threadBean.getThreadAllocatedBytes(threadId)
    val heapSamples = scala.collection.mutable.ArrayBuffer[Double]()
    var totalPlanAlloc = 0L
    var totalExecAlloc = 0L
    val startWall = System.nanoTime()

    val times = (0 until runs).map { _ =>
      heapSamples += getHeapUsageMB()

      val a0 = threadBean.getThreadAllocatedBytes(threadId)
      val start = System.nanoTime()
      val df = planF
      val a1 = threadBean.getThreadAllocatedBytes(threadId)
      df.count()
      val end = System.nanoTime()
      val a2 = threadBean.getThreadAllocatedBytes(threadId)

      totalPlanAlloc += (a1 - a0)
      totalExecAlloc += (a2 - a1)
      (end - start) / 1_000_000.0
    }

    val endWall = System.nanoTime()
    val totalWallMs = (endWall - startWall) / 1_000_000.0
    val allocAfter = threadBean.getThreadAllocatedBytes(threadId)
    val (gcAfter, gcTimeAfter) = getGCStats()

    val sorted = times.sorted
    val median = sorted(runs / 2)
    val p95 = sorted((runs * 0.95).toInt)
    val planAllocMB = totalPlanAlloc / (1024.0 * 1024.0) / runs
    val execAllocMB = totalExecAlloc / (1024.0 * 1024.0) / runs
    val totalAllocMB = (allocAfter - allocBefore) / (1024.0 * 1024.0) / runs
    val peakHeap = heapSamples.max
    val gcCollections = gcAfter - gcBefore
    val gcTimeMs = gcTimeAfter - gcTimeBefore
    val gcOverhead = if (totalWallMs > 0) (gcTimeMs.toDouble / totalWallMs) * 100.0 else 0.0

    ScaleResult(
      operation,
      median,
      p95,
      planAllocMB,
      execAllocMB,
      totalAllocMB,
      peakHeap,
      gcCollections,
      gcTimeMs,
      gcOverhead
    )
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
      sparkInterpreter.toDataFrame(filtered).toOption.get
    }
    results += filterResult

    val groupByResult = benchmarkOp("GroupBy", warmups, runs) {
      val keyCell: Expr[BenchRow, Any] = Expr.Cell("key_value", ColumnIndex(0))
      val valCell: Expr[BenchRow, Int] = Expr.Cell("value_value", ColumnIndex(1))
      val aggKeys = Vector(KeySpec[BenchRow, Any]("key", keyCell, ColumnType.StringType))
      val aggs = Vector(AggSpec("total", Expr.Sum(valCell), ColumnType.LongType))
      val grouped = dataset.groupByAgg[(String, Long)](aggKeys, aggs)
      sparkInterpreter.toDataFrame(grouped).toOption.get
    }
    results += groupByResult

    val sortResult = benchmarkOp("Sort", warmups, runs) {
      val valCell = Expr.Cell[BenchRow, Int]("value_value", ColumnIndex(1))
      val sorted = dataset.sortByExpr(valCell, ColumnType.IntType)(using Ordering[Int])
      sparkInterpreter.toDataFrame(sorted).toOption.get
    }
    results += sortResult

    val limitResult = benchmarkOp("Limit", warmups, runs) {
      val limited = dataset.limit(rows / 2)
      sparkInterpreter.toDataFrame(limited).toOption.get
    }
    results += limitResult

    val unionResult = benchmarkOp("Union", warmups, runs) {
      val unioned = dataset.union(dataset)
      sparkInterpreter.toDataFrame(unioned).toOption.get
    }
    results += unionResult

    val distinctResult = benchmarkOp("Distinct", warmups, runs) {
      val distincted = dataset.distinct
      sparkInterpreter.toDataFrame(distincted).toOption.get
    }
    results += distinctResult

    val halfSize = rows / 2
    val dataset1 = generateHalfData(halfSize, groups / 2, 42)
    val dataset2 = generateHalfData(halfSize, groups / 2, 99)

    val joinResult = benchmarkOp("Join", warmups, runs) {
      val leftKey: Expr[BenchRow, String] = Expr.Cell("key_value", ColumnIndex(0))
      val rightKey: Expr[BenchRow, String] = Expr.Cell("key_value", ColumnIndex(0))
      val joined = dataset1.joinOn(dataset2, leftKey, rightKey, ColumnType.StringType, ColumnType.StringType)
      sparkInterpreter.toDataFrame(joined).toOption.get
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
      sparkInterpreter.toDataFrame(grouped).toOption.get
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
      sparkInterpreter.toDataFrame(sorted).toOption.get
    }
    results += sortByExprsResult

    import org.apache.spark.sql.{functions => F}

    val sourceDf = dataset match {
      case Dataset.Root(src: SparkSource, _) => src.df
      case _ => throw new RuntimeException("Expected SparkSource") // scalafix:ok DisableSyntax.throw
    }
    val sourceDf1 = dataset1 match {
      case Dataset.Root(src: SparkSource, _) => src.df
      case _ => throw new RuntimeException("Expected SparkSource") // scalafix:ok DisableSyntax.throw
    }
    val sourceDf2 = dataset2 match {
      case Dataset.Root(src: SparkSource, _) => src.df
      case _ => throw new RuntimeException("Expected SparkSource") // scalafix:ok DisableSyntax.throw
    }

    val nativeResults = scala.collection.mutable.ArrayBuffer[ScaleResult]()

    nativeResults += benchmarkOp("Filter", warmups, runs) {
      sourceDf.filter(F.col("value_value") > F.lit(500))
    }
    nativeResults += benchmarkOp("GroupBy", warmups, runs) {
      sourceDf.groupBy("key_value").agg(F.sum("value_value").as("total"))
    }
    nativeResults += benchmarkOp("Sort", warmups, runs) {
      sourceDf.sort(F.col("value_value"))
    }
    nativeResults += benchmarkOp("Limit", warmups, runs) {
      sourceDf.limit(rows / 2)
    }
    nativeResults += benchmarkOp("Union", warmups, runs) {
      sourceDf.union(sourceDf)
    }
    nativeResults += benchmarkOp("Distinct", warmups, runs) {
      sourceDf.distinct()
    }
    nativeResults += benchmarkOp("Join", warmups, runs) {
      sourceDf1.alias("_l").join(sourceDf2.alias("_r"), F.col("_l.key_value") === F.col("_r.key_value"))
    }
    nativeResults += benchmarkOp("GroupByAgg", warmups, runs) {
      sourceDf.groupBy("key_value").agg(F.sum("value_value").as("totalValue"), F.count("*").as("cnt"))
    }
    nativeResults += benchmarkOp("SortByExprs", warmups, runs) {
      sourceDf.sort(F.col("value_value").asc, F.col("key_value").asc)
    }

    val header = s"VAIRË SPARK — $scale rows, $groups groups (Spark 4.1.1)"
    val colHeader =
      f"${"Operation"}%-14s ${"Median"}%10s ${"Plan"}%10s ${"Exec"}%10s ${"Total"}%10s ${"PeakHeap"}%10s ${"GC"}%6s"
    val separator = "-" * 90
    val lines = results.map { r =>
      f"${r.operation}%-14s ${r.medianMs}%7.2f ms ${r.planAllocMB}%7.1f MB ${r.execAllocMB}%7.1f MB ${r.totalAllocMB}%7.1f MB ${r.peakHeapMB}%7.0f MB ${r.gcCollections}%6d"
    }

    val nativeHeader = s"NATIVE SPARK CONTROL — $scale rows, $groups groups (Spark 4.1.1)"
    val nativeLines = nativeResults.map { r =>
      f"${r.operation}%-14s ${r.medianMs}%7.2f ms ${r.planAllocMB}%7.1f MB ${r.execAllocMB}%7.1f MB ${r.totalAllocMB}%7.1f MB ${r.peakHeapMB}%7.0f MB ${r.gcCollections}%6d"
    }

    val block = (
      Vector(s"\n${"=" * 70}", header, "=" * 70, colHeader, separator) ++ lines ++
        Vector(s"\n${"=" * 70}", nativeHeader, "=" * 70, colHeader, separator) ++ nativeLines
    ).mkString("\n")
    info(block)
    writeToFile(block)
  }

  "Vairë Spark comparative" should "benchmark at 10K rows" taggedAs Benchmark in { runScale(10000, 100) }
  it should "benchmark at 100K rows" taggedAs Benchmark in { runScale(100000, 1000) }
  it should "benchmark at 200K rows" taggedAs Benchmark in { runScale(200000, 2000) }
  it should "benchmark at 400K rows" taggedAs Benchmark in { runScale(400000, 4000) }
}
