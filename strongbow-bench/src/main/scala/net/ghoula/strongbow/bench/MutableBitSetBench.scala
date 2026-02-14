package net.ghoula.strongbow.bench

import java.lang.management.ManagementFactory
import scala.collection.immutable.BitSet
import scala.jdk.CollectionConverters.*

/** Benchmark different ways to build BitSet:
  *   1. BitSet.newBuilder with while loop + var
  *   2. mutable.BitSet with while loop + var
  *   3. mutable.BitSet with foreach (no var)
  *   4. mutable.BitSet with indices.foreach (no var, like Eru pattern)
  */
object MutableBitSetBench {

  // Current: Builder with while loop
  def withBuilder(nulls: BitSet, indices: Array[Int]): BitSet = {
    val builder = BitSet.newBuilder
    var i = 0
    while (i < indices.length) {
      if (nulls.contains(indices(i))) builder += i
      i += 1
    }
    builder.result()
  }

  // mutable.BitSet with while loop
  def withMutableWhile(nulls: BitSet, indices: Array[Int]): BitSet = {
    val mutableSet = scala.collection.mutable.BitSet.empty
    var i = 0
    while (i < indices.length) {
      if (nulls.contains(indices(i))) mutableSet += i
      i += 1
    }
    BitSet.empty ++ mutableSet // Convert to immutable
  }

  // mutable.BitSet with foreach (no var!)
  def withMutableForeach(nulls: BitSet, indices: Array[Int]): BitSet = {
    val mutableSet = scala.collection.mutable.BitSet.empty
    indices.indices.foreach { i =>
      if (nulls.contains(indices(i))) mutableSet += i
    }
    BitSet.empty ++ mutableSet
  }

  // mutable.BitSet with Array.indices.foreach (Eru-style)
  def withMutableIndicesForeach(nulls: BitSet, indices: Array[Int]): BitSet = {
    val mutableSet = scala.collection.mutable.BitSet.empty
    (0 until indices.length).foreach { i =>
      if (nulls.contains(indices(i))) mutableSet += i
    }
    BitSet.empty ++ mutableSet
  }

  case class BenchResult(
    impl: String,
    size: Int,
    medianMs: Double,
    memoryMB: Double,
    gcCount: Long
  )

  def benchmark(name: String, size: Int, warmups: Int, iterations: Int)(f: => BitSet): BenchResult = {
    val runtime = Runtime.getRuntime
    val gcBeans = ManagementFactory.getGarbageCollectorMXBeans

    // Warmup
    for (_ <- 0 until warmups) f

    // Measure
    System.gc()
    Thread.sleep(100)

    val gcBefore = gcBeans.iterator().asScala.map(_.getCollectionCount).sum
    val memBefore = runtime.totalMemory() - runtime.freeMemory()

    val times = (0 until iterations).map { _ =>
      val start = System.nanoTime()
      val _ = f
      val end = System.nanoTime()
      (end - start) / 1_000_000.0
    }

    val memAfter = runtime.totalMemory() - runtime.freeMemory()
    val gcAfter = gcBeans.iterator().asScala.map(_.getCollectionCount).sum

    val medianMs = times.sorted.apply(times.size / 2)
    val memoryMB = (memAfter - memBefore) / (1024.0 * 1024.0)
    val gcCount = gcAfter - gcBefore

    BenchResult(name, size, medianMs, memoryMB, gcCount)
  }

  def main(args: Array[String]): Unit = {
    println("mutable.BitSet vs Builder Benchmark")
    println("Question: Can we use mutable.BitSet to eliminate var?")
    println("=" * 90)

    val sizes = Seq(
      (50_000, 25_000),
      (100_000, 50_000),
      (500_000, 250_000)
    )

    val nullPercentages = Seq(0.0, 0.1, 0.5)

    println(
      f"\n${"Size"}%-10s ${"Nulls%"}%-8s ${"Implementation"}%-25s ${"Time(ms)"}%-12s ${"Mem(MB)"}%-10s ${"GC"}%-5s"
    )
    println("=" * 90)

    for {
      (totalSize, sliceSize) <- sizes
      nullPct <- nullPercentages
    } {
      // Generate test data
      val data = Array.fill(totalSize)(scala.util.Random.nextInt(1000))
      val indices = scala.util.Random.shuffle((0 until totalSize).toVector).take(sliceSize).toArray
      val nullCount = (sliceSize * nullPct).toInt
      val nullIndices = scala.util.Random.shuffle(indices.toVector).take(nullCount)
      val nulls = BitSet(nullIndices*)

      // Benchmark all approaches
      val builderResult = benchmark("Builder+while+var", sliceSize, 10, 50) {
        withBuilder(nulls, indices)
      }

      val mutableWhileResult = benchmark("mutable.BitSet+while+var", sliceSize, 10, 50) {
        withMutableWhile(nulls, indices)
      }

      val mutableForeachResult = benchmark("mutable.BitSet+foreach", sliceSize, 10, 50) {
        withMutableForeach(nulls, indices)
      }

      val mutableIndicesResult = benchmark("mutable.BitSet+indices.foreach", sliceSize, 10, 50) {
        withMutableIndicesForeach(nulls, indices)
      }

      println(
        f"${sliceSize}%-10d ${(nullPct * 100).toInt}%-8d ${builderResult.impl}%-25s ${builderResult.medianMs}%-12.4f ${builderResult.memoryMB}%-10.2f ${builderResult.gcCount}%-5d"
      )
      println(
        f"${sliceSize}%-10d ${(nullPct * 100).toInt}%-8d ${mutableWhileResult.impl}%-25s ${mutableWhileResult.medianMs}%-12.4f ${mutableWhileResult.memoryMB}%-10.2f ${mutableWhileResult.gcCount}%-5d"
      )
      println(
        f"${sliceSize}%-10d ${(nullPct * 100).toInt}%-8d ${mutableForeachResult.impl}%-25s ${mutableForeachResult.medianMs}%-12.4f ${mutableForeachResult.memoryMB}%-10.2f ${mutableForeachResult.gcCount}%-5d"
      )
      println(
        f"${sliceSize}%-10d ${(nullPct * 100).toInt}%-8d ${mutableIndicesResult.impl}%-25s ${mutableIndicesResult.medianMs}%-12.4f ${mutableIndicesResult.memoryMB}%-10.2f ${mutableIndicesResult.gcCount}%-5d"
      )

      // Show comparison
      val foreachVsBuilder = mutableForeachResult.medianMs / builderResult.medianMs
      val indicesVsBuilder = mutableIndicesResult.medianMs / builderResult.medianMs

      println(f"${""}%-10s ${""}%-8s   → foreach vs builder: ${foreachVsBuilder}%.2fx")
      println(f"${""}%-10s ${""}%-8s   → indices.foreach vs builder: ${indicesVsBuilder}%.2fx")
      println()
    }

    println("\n" + "=" * 90)
    println("CONCLUSION:")
    println("If mutable.BitSet + foreach is comparable, we can eliminate var!")
    println("Pattern would match Eru: contained mutability with no vars.")
    println("=" * 90)
  }
}
