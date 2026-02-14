package net.ghoula.strongbow.bench

import com.sun.management.ThreadMXBean

import java.lang.management.ManagementFactory
import scala.jdk.CollectionConverters.*
import scala.util.Random

import net.ghoula.strongbow.prelude.*
import net.ghoula.strongbow.types.ColumnIndex

/** Multi-scale benchmarks to find where Longbow's advantages matter.
  *
  * Tests at realistic single-machine scales:
  *   - 100K rows: 10x current baseline
  *   - 500K rows: 50x current baseline
  *   - 1M rows: 100x current baseline
  *
  * Goal: Show where memory efficiency and zero-GC architecture compound to deliver real value for
  * data science iteration.
  */
object ScalingBenchmark {

  val threadBean: ThreadMXBean = ManagementFactory.getThreadMXBean.asInstanceOf[ThreadMXBean]
  val memoryBean = ManagementFactory.getMemoryMXBean
  val gcBeans = ManagementFactory.getGarbageCollectorMXBeans.asScala.toList

  case class ScaleResult(
    scale: String,
    rows: Int,
    operation: String,
    medianMs: Double,
    p95Ms: Double,
    allocatedMB: Double,
    avgHeapMB: Double,
    maxHeapMB: Double,
    gcCollections: Long,
    gcTimeMs: Long,
    gcOverhead: Double
  )

  def getGCStats(): (Long, Long) = {
    val totalCollections = gcBeans.map(_.getCollectionCount).sum
    val totalGCTime = gcBeans.map(_.getCollectionTime).sum
    (totalCollections, totalGCTime)
  }

  def getHeapUsageMB(): Double = {
    memoryBean.getHeapMemoryUsage.getUsed / (1024.0 * 1024.0)
  }

  def generateData(rows: Int, numGroups: Int): (Vector[String], Vector[Int]) = {
    val keys = Vector.fill(rows)(s"group${Random.nextInt(numGroups)}")
    val values = Vector.fill(rows)(Random.nextInt(1000))
    (keys, values)
  }

  def createDataset(keys: Vector[String], values: Vector[Int]): Dataset[(String, Int)] = {
    val keyCol = Column.string(keys.toArray)
    val valCol = Column.int(values.toArray)

    given Schema[(String, Int)] with {
      def columnCount: Int = 2
      def columnNames: Vector[String] = Vector("key", "value")
      def columnTypes: Vector[ColumnType] = Vector(ColumnType.StringType, ColumnType.IntType)
      def encode(pair: (String, Int)): Vector[Any] = Vector(pair._1, pair._2)
      def decode(values: Vector[Any]): Either[DecodeError, (String, Int)] = {
        if (values.length != 2) Left(DecodeError.WrongArity(2, values.length))
        else
          (values(0), values(1)) match {
            case (k: String, v: Int) => Right((k, v))
            case _ => Left(DecodeError.TypeMismatch("(String, Int)", "unexpected"))
          }
      }
    }

    Dataset.fromColumns(Vector(keyCol, valCol), summon[Schema[(String, Int)]]) match {
      case Right(ds) => ds
      case Left(err) => throw new RuntimeException(s"Failed to create dataset: $err")
    }
  }

  def benchmarkOperation[T](
    scale: String,
    rows: Int,
    operation: String,
    warmups: Int,
    runs: Int
  )(f: => T): ScaleResult = {
    // Aggressive GC
    System.gc()
    System.gc()
    Thread.sleep(500)

    // Warmup
    (0 until warmups).foreach(_ => f)

    // Settle
    System.gc()
    Thread.sleep(200)

    // Time measurement
    val times = (0 until runs).map { _ =>
      val start = System.nanoTime()
      f
      val end = System.nanoTime()
      (end - start) / 1_000_000.0
    }

    val sorted = times.sorted
    val median = sorted(runs / 2)
    val p95 = sorted((runs * 0.95).toInt)

    // Memory and GC measurement
    val threadId = Thread.currentThread().getId
    System.gc()
    System.gc()
    Thread.sleep(100)

    val (gcBefore, gcTimeBefore) = getGCStats()
    val allocBefore = threadBean.getThreadAllocatedBytes(threadId)
    val heapSamples = scala.collection.mutable.ArrayBuffer[Double]()

    val sampleRuns = 10
    val startTime = System.nanoTime()
    for (_ <- 0 until sampleRuns) {
      heapSamples += getHeapUsageMB()
      f
    }
    val endTime = System.nanoTime()
    val executionTimeMs = (endTime - startTime) / 1_000_000.0

    val allocAfter = threadBean.getThreadAllocatedBytes(threadId)
    val (gcAfter, gcTimeAfter) = getGCStats()

    val allocMB = (allocAfter - allocBefore) / (1024.0 * 1024.0) / sampleRuns
    val avgHeap = heapSamples.sum / heapSamples.length
    val maxHeap = heapSamples.max
    val gcCollections = gcAfter - gcBefore
    val gcTimeMs = gcTimeAfter - gcTimeBefore
    val gcOverhead = (gcTimeMs.toDouble / executionTimeMs) * 100.0

    ScaleResult(
      scale,
      rows,
      operation,
      median,
      p95,
      allocMB,
      avgHeap,
      maxHeap,
      gcCollections,
      gcTimeMs,
      gcOverhead
    )
  }

  def benchmarkScale(rows: Int, groups: Int): Seq[ScaleResult] = {
    val scale = rows match {
      case 100000 => "100K"
      case 500000 => "500K"
      case 1000000 => "1M"
      case _ => s"${rows / 1000}K"
    }

    println(s"\n${"=".repeat(60)}")
    println(s"Scale: $scale rows, $groups groups")
    println(s"${"=".repeat(60)}")

    val (keys, values) = generateData(rows, groups)
    val dataset = createDataset(keys, values)

    val results = scala.collection.mutable.ArrayBuffer[ScaleResult]()

    // Filter
    print("  Filter... ")
    val filterResult = benchmarkOperation(scale, rows, "Filter", 20, 50) {
      val valCell = Expr.Cell[(String, Int), Int]("value", ColumnIndex(1))
      val filtered = dataset.filter(valCell > Expr.lit(500))
      val materialized = DatasetInterpreter
        .execute(filtered)
        .getOrElse(
          throw new RuntimeException("Benchmark execution failed") // scalafix:ok DisableSyntax.throw
        )
      materialized.rowCount
    }
    results += filterResult
    println(
      f"${filterResult.medianMs}%.2f ms (alloc: ${filterResult.allocatedMB}%.1f MB, heap: ${filterResult.maxHeapMB}%.0f MB, GC: ${filterResult.gcCollections})"
    )

    // GroupBy
    print("  GroupBy... ")
    val groupByResult = benchmarkOperation(scale, rows, "GroupBy", 20, 50) {
      val grouped = dataset.groupBy(_._1)
      val reduced = grouped.reduceByKey((a, b) => (a._1, a._2 + b._2))
      val pairs = GroupByInterpreter.execute(reduced)
      pairs.length
    }
    results += groupByResult
    println(
      f"${groupByResult.medianMs}%.2f ms (alloc: ${groupByResult.allocatedMB}%.1f MB, heap: ${groupByResult.maxHeapMB}%.0f MB, GC: ${groupByResult.gcCollections})"
    )

    // Sort
    print("  Sort... ")
    val sortResult = benchmarkOperation(scale, rows, "Sort", 20, 50) {
      val sorted = dataset.sortBy(_._2)(using Ordering[Int])
      val materialized = DatasetInterpreter
        .execute(sorted)
        .getOrElse(
          throw new RuntimeException("Benchmark execution failed") // scalafix:ok DisableSyntax.throw
        )
      materialized.rowCount
    }
    results += sortResult
    println(
      f"${sortResult.medianMs}%.2f ms (alloc: ${sortResult.allocatedMB}%.1f MB, heap: ${sortResult.maxHeapMB}%.0f MB, GC: ${sortResult.gcCollections})"
    )

    // Join
    print("  Join... ")
    val (keys1, values1) = generateData(rows / 2, groups / 2)
    val (keys2, values2) = generateData(rows / 2, groups / 2)
    val dataset1 = createDataset(keys1, values1)
    val dataset2 = createDataset(keys2, values2)

    val joinResult = benchmarkOperation(scale, rows / 2, "Join", 20, 50) {
      val grouped1 = dataset1.groupBy(_._1)
      val grouped2 = dataset2.groupBy(_._1)
      val joined = grouped1.join(grouped2)
      val pairs = GroupByInterpreter.execute(joined)
      pairs.length
    }
    results += joinResult
    println(
      f"${joinResult.medianMs}%.2f ms (alloc: ${joinResult.allocatedMB}%.1f MB, heap: ${joinResult.maxHeapMB}%.0f MB, GC: ${joinResult.gcCollections})"
    )

    results.toSeq
  }

  def printSummaryTable(allResults: Seq[ScaleResult]): Unit = {
    println("\n" + "=".repeat(80))
    println("SCALING SUMMARY")
    println("=".repeat(80))

    val operations = Seq("Filter", "GroupBy", "Sort", "Join")
    val scales = allResults.map(_.scale).distinct

    println()
    println("Time Performance (Median)")
    println("-".repeat(80))
    printf("%-10s", "Operation")
    scales.foreach(s => printf("%15s", s))
    println()
    println("-".repeat(80))

    operations.foreach { op =>
      printf("%-10s", op)
      scales.foreach { scale =>
        val result = allResults.find(r => r.scale == scale && r.operation == op)
        result match {
          case Some(r) => printf("%12.2f ms", r.medianMs)
          case None => printf("%15s", "-")
        }
      }
      println()
    }

    println()
    println("Memory Efficiency (Allocated per operation)")
    println("-".repeat(80))
    printf("%-10s", "Operation")
    scales.foreach(s => printf("%15s", s))
    println()
    println("-".repeat(80))

    operations.foreach { op =>
      printf("%-10s", op)
      scales.foreach { scale =>
        val result = allResults.find(r => r.scale == scale && r.operation == op)
        result match {
          case Some(r) => printf("%12.2f MB", r.allocatedMB)
          case None => printf("%15s", "-")
        }
      }
      println()
    }

    println()
    println("Peak Heap Usage")
    println("-".repeat(80))
    printf("%-10s", "Operation")
    scales.foreach(s => printf("%15s", s))
    println()
    println("-".repeat(80))

    operations.foreach { op =>
      printf("%-10s", op)
      scales.foreach { scale =>
        val result = allResults.find(r => r.scale == scale && r.operation == op)
        result match {
          case Some(r) => printf("%12.0f MB", r.maxHeapMB)
          case None => printf("%15s", "-")
        }
      }
      println()
    }

    println()
    println("GC Collections")
    println("-".repeat(80))
    printf("%-10s", "Operation")
    scales.foreach(s => printf("%15s", s))
    println()
    println("-".repeat(80))

    operations.foreach { op =>
      printf("%-10s", op)
      scales.foreach { scale =>
        val result = allResults.find(r => r.scale == scale && r.operation == op)
        result match {
          case Some(r) => printf("%15d", r.gcCollections)
          case None => printf("%15s", "-")
        }
      }
      println()
    }

    println()
    println("GC Overhead %")
    println("-".repeat(80))
    printf("%-10s", "Operation")
    scales.foreach(s => printf("%15s", s))
    println()
    println("-".repeat(80))

    operations.foreach { op =>
      printf("%-10s", op)
      scales.foreach { scale =>
        val result = allResults.find(r => r.scale == scale && r.operation == op)
        result match {
          case Some(r) => printf("%14.1f%%", r.gcOverhead)
          case None => printf("%15s", "-")
        }
      }
      println()
    }
  }

  def main(args: Array[String]): Unit = {
    println("Longbow Scaling Benchmark")
    println("=".repeat(80))
    println("Testing at realistic single-machine scales:")
    println("  - 100K rows: 10x baseline")
    println("  - 500K rows: 50x baseline")
    println("  - 1M rows: 100x baseline")
    println()
    println("Goal: Show where memory efficiency and zero-GC compound")
    println("=".repeat(80))

    val allResults = scala.collection.mutable.ArrayBuffer[ScaleResult]()

    // 100K rows
    allResults ++= benchmarkScale(100000, 1000)

    // 500K rows
    allResults ++= benchmarkScale(500000, 5000)

    // 1M rows
    allResults ++= benchmarkScale(1000000, 10000)

    printSummaryTable(allResults.toSeq)

    println("\n" + "=".repeat(80))
    println("Benchmark complete!")
    println("=".repeat(80))
  }
}
