package net.ghoula.strongbow.bench

import com.sun.management.ThreadMXBean

import java.lang.management.{ManagementFactory, MemoryMXBean}
import scala.jdk.CollectionConverters.*
import scala.util.Random
import net.ghoula.strongbow.prelude.*
import net.ghoula.strongbow.types.ColumnIndex

/** Benchmark Longbow vs Crossbow for comparable operations.
  *
  * Setup mirrors Crossbow's GroupByFairBenchmark: - 10K rows, 100 groups
  *   - ThreadMXBean allocation tracking
  *   - 20 warmup + 100 measurement runs
  */
object LongbowVsCrossbow {

  val threadBean: ThreadMXBean = ManagementFactory.getThreadMXBean.asInstanceOf[ThreadMXBean]
  val memoryBean: MemoryMXBean = ManagementFactory.getMemoryMXBean
  val gcBeans = ManagementFactory.getGarbageCollectorMXBeans.asScala.toList

  case class MemoryStats(
    heapUsedMB: Vector[Double],
    heapCommittedMB: Vector[Double],
    avgHeapUsedMB: Double,
    maxHeapUsedMB: Double,
    allocatedMB: Double
  )

  case class GCStats(
    totalCollections: Long,
    totalGCTimeMs: Long
  )

  case class BenchResult(
    medianMs: Double,
    p95Ms: Double,
    minMs: Double,
    maxMs: Double,
    stdDevMs: Double,
    memory: MemoryStats,
    gc: GCStats,
    gcOverheadPercent: Double
  )

  def getGCStats(): GCStats = {
    val totalCollections = gcBeans.map(_.getCollectionCount).sum
    val totalGCTime = gcBeans.map(_.getCollectionTime).sum
    GCStats(totalCollections, totalGCTime)
  }

  def getHeapUsageMB(): Double = {
    memoryBean.getHeapMemoryUsage.getUsed / (1024.0 * 1024.0)
  }

  def getHeapCommittedMB(): Double = {
    memoryBean.getHeapMemoryUsage.getCommitted / (1024.0 * 1024.0)
  }

  def measureTime[T](warmups: Int, runs: Int)(f: => T): BenchResult = {
    // Aggressive GC before benchmark
    System.gc()
    System.gc()
    Thread.sleep(500)

    // Warmup
    (0 until warmups).foreach(_ => f)

    // Settle after warmup
    System.gc()
    Thread.sleep(200)

    // Measurement
    val times = (0 until runs).map { _ =>
      val start = System.nanoTime()
      f
      val end = System.nanoTime()
      (end - start) / 1_000_000.0 // Convert to ms
    }

    val sorted = times.sorted
    val median = sorted(runs / 2)
    val p95 = sorted((runs * 0.95).toInt)
    val min = sorted.head
    val max = sorted.last
    val mean = times.sum / times.length
    val variance = times.map(t => Math.pow(t - mean, 2)).sum / times.length
    val stdDev = Math.sqrt(variance)

    // Detailed memory and GC measurement
    val threadId = Thread.currentThread().getId
    System.gc()
    System.gc()
    Thread.sleep(100)

    val gcBefore = getGCStats()
    val allocBefore = threadBean.getThreadAllocatedBytes(threadId)
    val heapSamples = scala.collection.mutable.ArrayBuffer[Double]()
    val heapCommittedSamples = scala.collection.mutable.ArrayBuffer[Double]()

    // Sample memory during execution (10 samples)
    val sampleRuns = 10
    val startTime = System.nanoTime()
    for (_ <- 0 until sampleRuns) {
      heapSamples += getHeapUsageMB()
      heapCommittedSamples += getHeapCommittedMB()
      f
    }
    val endTime = System.nanoTime()
    val executionTimeMs = (endTime - startTime) / 1_000_000.0

    val allocAfter = threadBean.getThreadAllocatedBytes(threadId)
    val gcAfter = getGCStats()

    val allocMB = (allocAfter - allocBefore) / (1024.0 * 1024.0)
    val gcCollections = gcAfter.totalCollections - gcBefore.totalCollections
    val gcTimeMs = gcAfter.totalGCTimeMs - gcBefore.totalGCTimeMs
    val gcOverhead = (gcTimeMs.toDouble / executionTimeMs) * 100.0

    val memStats = MemoryStats(
      heapUsedMB = heapSamples.toVector,
      heapCommittedMB = heapCommittedSamples.toVector,
      avgHeapUsedMB = heapSamples.sum / heapSamples.length,
      maxHeapUsedMB = heapSamples.max,
      allocatedMB = allocMB / sampleRuns // Average per run
    )

    val gcStats = GCStats(gcCollections, gcTimeMs)

    BenchResult(median, p95, min, max, stdDev, memStats, gcStats, gcOverhead)
  }

  /** Run a benchmark multiple times and compute statistics. */
  def measureMultipleRuns[T](iterations: Int, warmups: Int, runs: Int)(
    f: => T
  ): Seq[BenchResult] = {
    (0 until iterations).map { iteration =>
      println(s"  Run ${iteration + 1}/$iterations...")
      val result = measureTime(warmups, runs)(f)

      // Isolate runs with GC and sleep
      System.gc()
      System.gc()
      Thread.sleep(1000)

      result
    }
  }

  def reportStats(results: Seq[BenchResult]): Unit = {
    val medians = results.map(_.medianMs)
    val p95s = results.map(_.p95Ms)
    val allocations = results.map(_.memory.allocatedMB)
    val avgHeaps = results.map(_.memory.avgHeapUsedMB)
    val maxHeaps = results.map(_.memory.maxHeapUsedMB)
    val gcCounts = results.map(_.gc.totalCollections)
    val gcTimes = results.map(_.gc.totalGCTimeMs)
    val gcOverheads = results.map(_.gcOverheadPercent)

    val avgMedian = medians.sum / medians.length
    val avgP95 = p95s.sum / p95s.length
    val avgAllocation = allocations.sum / allocations.length
    val avgHeap = avgHeaps.sum / avgHeaps.length
    val avgMaxHeap = maxHeaps.sum / maxHeaps.length
    val avgGCCount = gcCounts.sum / gcCounts.length
    val avgGCTime = gcTimes.sum / gcTimes.length
    val avgGCOverhead = gcOverheads.sum / gcOverheads.length

    val medianStdDev = Math.sqrt(medians.map(m => Math.pow(m - avgMedian, 2)).sum / medians.length)

    println(f"  Median Time:      ${avgMedian}%.2f ms (±${medianStdDev}%.2f ms across runs)")
    println(f"  P95 Time:         ${avgP95}%.2f ms")
    println(f"  Range:            ${medians.min}%.2f - ${medians.max}%.2f ms")
    println(f"  Allocated:        ${avgAllocation}%.2f MB per operation")
    println(f"  Avg Heap Used:    ${avgHeap}%.2f MB")
    println(f"  Max Heap Used:    ${avgMaxHeap}%.2f MB")
    println(f"  GC Collections:   ${avgGCCount}%.0f")
    println(f"  GC Time:          ${avgGCTime}%.0f ms")
    println(f"  GC Overhead:      ${avgGCOverhead}%.1f%%")
  }

  // Test data generation (matches Crossbow)
  def generateData(rows: Int, numGroups: Int): (Vector[String], Vector[Int]) = {
    val keys = Vector.fill(rows)(s"group${Random.nextInt(numGroups)}")
    val values = Vector.fill(rows)(Random.nextInt(1000))
    (keys, values)
  }

  // Create Longbow dataset from data
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

  def benchmarkFilter(rows: Int): Unit = {
    println(s"\n=== FILTER BENCHMARK ($rows rows) ===")

    val (keys, values) = generateData(rows, 100)
    val dataset = createDataset(keys, values)

    val results = measureMultipleRuns(5, 50, 200) {
      val valCell = Expr.Cell[(String, Int), Int]("value", ColumnIndex(1))
      val filtered = dataset.filter(valCell > Expr.lit(500))
      val materialized = DatasetInterpreter
        .execute(filtered)
        .getOrElse(
          throw new RuntimeException("Benchmark execution failed") // scalafix:ok DisableSyntax.throw
        )
      materialized.rowCount // Force evaluation
    }

    reportStats(results)
  }

  def benchmarkGroupBy(rows: Int): Unit = {
    println(s"\n=== GROUPBY + SUM BENCHMARK ($rows rows, 100 groups) ===")

    val (keys, values) = generateData(rows, 100)
    val dataset = createDataset(keys, values)

    val results = measureMultipleRuns(5, 50, 200) {
      val grouped = dataset.groupBy(_._1) // Group by key
      val reduced = grouped.reduceByKey((a, b) => (a._1, a._2 + b._2)) // Sum values
      val pairs = GroupByInterpreter.execute(reduced)
      pairs.length // Force evaluation
    }

    reportStats(results)
  }

  def benchmarkSort(rows: Int): Unit = {
    println(s"\n=== SORT BENCHMARK ($rows rows) ===")

    val (keys, values) = generateData(rows, 100)
    val dataset = createDataset(keys, values)

    val results = measureMultipleRuns(5, 50, 200) {
      val sorted = dataset.sortBy(_._2)(using Ordering[Int])
      val materialized = DatasetInterpreter
        .execute(sorted)
        .getOrElse(
          throw new RuntimeException("Benchmark execution failed") // scalafix:ok DisableSyntax.throw
        )
      materialized.rowCount // Force evaluation
    }

    reportStats(results)
  }

  def benchmarkJoin(rows: Int): Unit = {
    println(s"\n=== JOIN BENCHMARK (${rows / 2} x ${rows / 2} rows) ===")

    val (keys1, values1) = generateData(rows / 2, 50)
    val (keys2, values2) = generateData(rows / 2, 50)
    val dataset1 = createDataset(keys1, values1)
    val dataset2 = createDataset(keys2, values2)

    val results = measureMultipleRuns(5, 50, 200) {
      val grouped1 = dataset1.groupBy(_._1)
      val grouped2 = dataset2.groupBy(_._1)
      val joined = grouped1.join(grouped2)
      val pairs = GroupByInterpreter.execute(joined)
      pairs.length // Force evaluation
    }

    reportStats(results)
  }

  def main(args: Array[String]): Unit = {
    println("Longbow vs Crossbow Benchmark")
    println("=" * 50)
    println("Configuration:")
    println("  - JVM warmup: 20 runs")
    println("  - Measurement: 100 runs")
    println("  - Data: 10K rows, 100 groups")
    println("  - Tracking: ThreadMXBean allocation")

    benchmarkFilter(10000)
    benchmarkGroupBy(10000)
    benchmarkSort(10000)
    benchmarkJoin(10000)

    println("\n" + "=" * 50)
    println("Benchmark complete!")
  }
}
