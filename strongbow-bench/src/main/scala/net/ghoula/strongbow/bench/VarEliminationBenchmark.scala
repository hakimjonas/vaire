package net.ghoula.strongbow.bench

import java.lang.management.ManagementFactory
import scala.collection.immutable.BitSet
import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag

/** Benchmark comparing var-based loops vs functional approaches for Column slicing operations.
  *
  * Tests both performance and memory characteristics across different data sizes to determine if we
  * can eliminate vars without meaningful performance regression.
  *
  * Measures:
  *   - Execution time (median over multiple runs)
  *   - Memory allocation (heap usage)
  *   - GC pressure
  */
object VarEliminationBenchmark {

  // Current implementation with vars
  object WithVars {
    def sliceArray[T: ClassTag](
      data: Array[T],
      indices: IndexedSeq[Int],
      nulls: BitSet,
      defaultValue: T
    ): Array[T] = {
      val newData = new Array[T](indices.size)
      var i = 0
      while (i < indices.size) {
        val srcIdx = indices(i)
        newData(i) = if (nulls.contains(srcIdx)) defaultValue else data(srcIdx)
        i += 1
      }
      newData
    }

    def buildNullSet(nulls: BitSet, indices: IndexedSeq[Int]): BitSet = {
      val builder = BitSet.newBuilder
      var i = 0
      while (i < indices.size) {
        if (nulls.contains(indices(i))) builder += i
        i += 1
      }
      builder.result()
    }
  }

  // Iterator approach
  object WithIterator {
    def sliceArray[T: ClassTag](
      data: Array[T],
      indices: IndexedSeq[Int],
      nulls: BitSet,
      defaultValue: T
    ): Array[T] =
      indices.iterator.map(srcIdx => if (nulls.contains(srcIdx)) defaultValue else data(srcIdx)).toArray

    def buildNullSet(nulls: BitSet, indices: IndexedSeq[Int]): BitSet =
      indices.iterator.zipWithIndex.collect { case (srcIdx, dstIdx) if nulls.contains(srcIdx) => dstIdx }
        .to(BitSet)
  }

  // View approach (Scala 3 lazy collections)
  object WithView {
    def sliceArray[T: ClassTag](
      data: Array[T],
      indices: IndexedSeq[Int],
      nulls: BitSet,
      defaultValue: T
    ): Array[T] =
      indices.view.map(srcIdx => if (nulls.contains(srcIdx)) defaultValue else data(srcIdx)).toArray

    def buildNullSet(nulls: BitSet, indices: IndexedSeq[Int]): BitSet =
      indices.view.zipWithIndex.collect { case (srcIdx, dstIdx) if nulls.contains(srcIdx) => dstIdx }
        .to(BitSet)
  }

  // Direct functional (allocates intermediate collection)
  object DirectFunctional {
    def sliceArray[T: ClassTag](
      data: Array[T],
      indices: IndexedSeq[Int],
      nulls: BitSet,
      defaultValue: T
    ): Array[T] =
      indices.map(srcIdx => if (nulls.contains(srcIdx)) defaultValue else data(srcIdx)).toArray

    def buildNullSet(nulls: BitSet, indices: IndexedSeq[Int]): BitSet =
      indices.zipWithIndex.collect { case (srcIdx, dstIdx) if nulls.contains(srcIdx) => dstIdx }.to(BitSet)
  }

  // FoldLeft approach (like Eru pattern)
  object WithFoldLeft {
    def sliceArray[T: ClassTag](
      data: Array[T],
      indices: IndexedSeq[Int],
      nulls: BitSet,
      defaultValue: T
    ): Array[T] = {
      val result = new Array[T](indices.size)
      indices.zipWithIndex.foldLeft(()) { case (_, (srcIdx, dstIdx)) =>
        result(dstIdx) = if (nulls.contains(srcIdx)) defaultValue else data(srcIdx)
      }
      result
    }

    def buildNullSet(nulls: BitSet, indices: IndexedSeq[Int]): BitSet =
      indices.zipWithIndex.foldLeft(BitSet.empty) { case (acc, (srcIdx, dstIdx)) =>
        if (nulls.contains(srcIdx)) acc + dstIdx else acc
      }
  }

  // Array.tabulate approach
  object WithTabulate {
    def sliceArray[T: ClassTag](
      data: Array[T],
      indices: IndexedSeq[Int],
      nulls: BitSet,
      defaultValue: T
    ): Array[T] =
      Array.tabulate(indices.size) { dstIdx =>
        val srcIdx = indices(dstIdx)
        if (nulls.contains(srcIdx)) defaultValue else data(srcIdx)
      }

    def buildNullSet(nulls: BitSet, indices: IndexedSeq[Int]): BitSet =
      (0 until indices.size).foldLeft(BitSet.empty) { (acc, dstIdx) =>
        if (nulls.contains(indices(dstIdx))) acc + dstIdx else acc
      }
  }

  // Builder with iterator (no intermediate IndexedSeq)
  object WithBuilderIterator {
    def sliceArray[T: ClassTag](
      data: Array[T],
      indices: IndexedSeq[Int],
      nulls: BitSet,
      defaultValue: T
    ): Array[T] =
      indices.iterator.map(srcIdx => if (nulls.contains(srcIdx)) defaultValue else data(srcIdx)).toArray

    def buildNullSet(nulls: BitSet, indices: IndexedSeq[Int]): BitSet = {
      val builder = BitSet.newBuilder
      indices.iterator.zipWithIndex.foreach { case (srcIdx, dstIdx) =>
        if (nulls.contains(srcIdx)) builder += dstIdx
      }
      builder.result()
    }
  }

  // Range-based (O(1) Range, no zipWithIndex allocation)
  object WithRange {
    def sliceArray[T: ClassTag](
      data: Array[T],
      indices: IndexedSeq[Int],
      nulls: BitSet,
      defaultValue: T
    ): Array[T] =
      indices.iterator.map(srcIdx => if (nulls.contains(srcIdx)) defaultValue else data(srcIdx)).toArray

    def buildNullSet(nulls: BitSet, indices: IndexedSeq[Int]): BitSet = {
      val builder = BitSet.newBuilder
      indices.indices.foreach { dstIdx => // Range, not zipWithIndex!
        if (nulls.contains(indices(dstIdx))) builder += dstIdx
      }
      builder.result()
    }
  }

  // Test data generator
  case class TestData(
    size: Int,
    data: Array[Int],
    indices: IndexedSeq[Int],
    nulls: BitSet,
    description: String
  )

  def generateTestData(totalSize: Int, sliceSize: Int, nullPercentage: Double): TestData = {
    val data = Array.fill(totalSize)(scala.util.Random.nextInt(1000))
    val indices = scala.util.Random.shuffle((0 until totalSize).toVector).take(sliceSize)
    val nullCount = (sliceSize * nullPercentage).toInt
    val nullIndices = scala.util.Random.shuffle(indices).take(nullCount)
    val nulls = BitSet(nullIndices*)

    TestData(
      sliceSize,
      data,
      indices,
      nulls,
      s"total=$totalSize, slice=$sliceSize, nulls=${nullPercentage * 100}%"
    )
  }

  // Benchmark runner
  case class BenchmarkResult(
    implementation: String,
    operation: String,
    dataSize: Int,
    medianMs: Double,
    memoryMB: Double,
    gcCount: Long
  )

  def benchmark[T](
    name: String,
    operation: String,
    testData: TestData,
    warmups: Int,
    iterations: Int
  )(f: => T): BenchmarkResult = {
    val runtime = Runtime.getRuntime
    val gcBeans = ManagementFactory.getGarbageCollectorMXBeans

    // Warmup
    for (_ <- 0 until warmups) {
      f
    }

    // Force GC before measurement
    System.gc()
    Thread.sleep(100)

    val gcCountBefore = gcBeans.iterator().asScala.map(_.getCollectionCount).sum
    val memBefore = runtime.totalMemory() - runtime.freeMemory()

    // Measure
    val times = (0 until iterations).map { _ =>
      val start = System.nanoTime()
      val _ = f
      val end = System.nanoTime()
      (end - start) / 1_000_000.0 // Convert to ms
    }

    val memAfter = runtime.totalMemory() - runtime.freeMemory()
    val gcCountAfter = gcBeans.iterator().asScala.map(_.getCollectionCount).sum

    val medianMs = times.sorted.apply(times.size / 2)
    val memoryMB = (memAfter - memBefore) / (1024.0 * 1024.0)
    val gcCount = gcCountAfter - gcCountBefore

    BenchmarkResult(name, operation, testData.size, medianMs, memoryMB, gcCount)
  }

  def runSliceArrayBenchmarks(testData: TestData): Seq[BenchmarkResult] = {
    val warmups = 20
    val iterations = 100

    Seq(
      benchmark("WithVars", "sliceArray", testData, warmups, iterations) {
        WithVars.sliceArray(testData.data, testData.indices, testData.nulls, 0)
      },
      benchmark("WithIterator", "sliceArray", testData, warmups, iterations) {
        WithIterator.sliceArray(testData.data, testData.indices, testData.nulls, 0)
      },
      benchmark("WithView", "sliceArray", testData, warmups, iterations) {
        WithView.sliceArray(testData.data, testData.indices, testData.nulls, 0)
      },
      benchmark("WithRange", "sliceArray", testData, warmups, iterations) {
        WithRange.sliceArray(testData.data, testData.indices, testData.nulls, 0)
      },
      benchmark("WithFoldLeft", "sliceArray", testData, warmups, iterations) {
        WithFoldLeft.sliceArray(testData.data, testData.indices, testData.nulls, 0)
      },
      benchmark("WithTabulate", "sliceArray", testData, warmups, iterations) {
        WithTabulate.sliceArray(testData.data, testData.indices, testData.nulls, 0)
      },
      benchmark("WithBuilderIterator", "sliceArray", testData, warmups, iterations) {
        WithBuilderIterator.sliceArray(testData.data, testData.indices, testData.nulls, 0)
      },
      benchmark("DirectFunctional", "sliceArray", testData, warmups, iterations) {
        DirectFunctional.sliceArray(testData.data, testData.indices, testData.nulls, 0)
      }
    )
  }

  def runBuildNullSetBenchmarks(testData: TestData): Seq[BenchmarkResult] = {
    val warmups = 20
    val iterations = 100

    Seq(
      benchmark("WithVars", "buildNullSet", testData, warmups, iterations) {
        WithVars.buildNullSet(testData.nulls, testData.indices)
      },
      benchmark("WithIterator", "buildNullSet", testData, warmups, iterations) {
        WithIterator.buildNullSet(testData.nulls, testData.indices)
      },
      benchmark("WithView", "buildNullSet", testData, warmups, iterations) {
        WithView.buildNullSet(testData.nulls, testData.indices)
      },
      benchmark("WithRange", "buildNullSet", testData, warmups, iterations) {
        WithRange.buildNullSet(testData.nulls, testData.indices)
      },
      benchmark("WithFoldLeft", "buildNullSet", testData, warmups, iterations) {
        WithFoldLeft.buildNullSet(testData.nulls, testData.indices)
      },
      benchmark("WithTabulate", "buildNullSet", testData, warmups, iterations) {
        WithTabulate.buildNullSet(testData.nulls, testData.indices)
      },
      benchmark("WithBuilderIterator", "buildNullSet", testData, warmups, iterations) {
        WithBuilderIterator.buildNullSet(testData.nulls, testData.indices)
      },
      benchmark("DirectFunctional", "buildNullSet", testData, warmups, iterations) {
        DirectFunctional.buildNullSet(testData.nulls, testData.indices)
      }
    )
  }

  def printResults(results: Seq[BenchmarkResult]): Unit = {
    println("\n" + "=" * 100)
    println(
      f"${"Implementation"}%-20s ${"Operation"}%-15s ${"Size"}%-10s ${"Median(ms)"}%-12s ${"Memory(MB)"}%-12s ${"GC"}%-8s"
    )
    println("=" * 100)

    results.foreach { r =>
      println(
        f"${r.implementation}%-20s ${r.operation}%-15s ${r.dataSize}%-10d ${r.medianMs}%-12.4f ${r.memoryMB}%-12.4f ${r.gcCount}%-8d"
      )
    }
    println("=" * 100)
  }

  def printComparison(results: Seq[BenchmarkResult]): Unit = {
    val grouped = results.groupBy(r => (r.operation, r.dataSize))

    println("\n" + "=" * 80)
    println("PERFORMANCE COMPARISON (vs WithVars)")
    println("=" * 80)

    grouped.foreach { case ((op, size), impls) =>
      val baseline = impls.find(_.implementation == "WithVars").get
      println(s"\n$op - Size: $size")
      println(f"${"Implementation"}%-20s ${"Time Ratio"}%-15s ${"Memory Ratio"}%-15s")
      println("-" * 80)

      impls.sortBy(_.medianMs).foreach { impl =>
        val timeRatio = impl.medianMs / baseline.medianMs
        val memRatio = impl.memoryMB / baseline.memoryMB
        val marker = if (impl.implementation == "WithVars") "*" else " "
        println(f"$marker ${impl.implementation}%-19s ${timeRatio}%-15.2fx ${memRatio}%-15.2fx")
      }
    }
    println("=" * 80)
  }

  def main(args: Array[String]): Unit = {
    println("Var Elimination Benchmark")
    println("Testing: Column.sliceArray and Column.buildNullSet")
    println("\nGoal: Determine if we can eliminate vars without performance regression\n")

    // Test at multiple scales
    val testSizes = Seq(
      (100, 50, "tiny"),
      (1_000, 500, "small"),
      (10_000, 5_000, "medium"),
      (100_000, 50_000, "large"),
      (1_000_000, 500_000, "xlarge")
    )

    val nullPercentages = Seq(0.0, 0.1, 0.5) // 0%, 10%, 50% nulls

    var allResults = Seq.empty[BenchmarkResult]

    for {
      (totalSize, sliceSize, label) <- testSizes
      nullPct <- nullPercentages
    } {
      println(s"\n>>> Running benchmarks for $label (slice=$sliceSize, nulls=${nullPct * 100}%)")

      val testData = generateTestData(totalSize, sliceSize, nullPct)

      println("  sliceArray benchmarks...")
      val sliceResults = runSliceArrayBenchmarks(testData)
      allResults ++= sliceResults

      println("  buildNullSet benchmarks...")
      val nullSetResults = runBuildNullSetBenchmarks(testData)
      allResults ++= nullSetResults

      // Print immediate results for this size
      printResults(sliceResults ++ nullSetResults)
    }

    // Final comparison
    println("\n\n")
    println("=" * 100)
    println("FINAL ANALYSIS")
    println("=" * 100)
    printComparison(allResults)

    println("\n\nRecommendation:")
    println("If Iterator or View shows <10% performance regression and similar memory,")
    println("we should eliminate vars for cleaner, more principled code.")
    println("If regression is significant (>20%), annotate while loops with scalafix:ok.")
  }
}
