package net.ghoula.strongbow.bench

import java.lang.management.ManagementFactory
import scala.collection.immutable.BitSet
import scala.jdk.CollectionConverters.*

/** Benchmark Array vs Vector for indices parameter in buildNullSet.
  *
  * Tests whether changing from IndexedSeq (Vector) to Array eliminates memory overhead.
  */
object ArrayVsVectorBench {

  // Current: accepts IndexedSeq (usually Vector from Range.filter)
  def buildNullSetVector(nulls: BitSet, indices: IndexedSeq[Int]): BitSet = {
    val builder = BitSet.newBuilder
    var i = 0
    while (i < indices.size) {
      if (nulls.contains(indices(i))) builder += i
      i += 1
    }
    builder.result()
  }

  // Proposed: accepts Array directly
  def buildNullSetArray(nulls: BitSet, indices: Array[Int]): BitSet = {
    val builder = BitSet.newBuilder
    var i = 0
    while (i < indices.length) {
      if (nulls.contains(indices(i))) builder += i
      i += 1
    }
    builder.result()
  }

  case class BenchResult(
    impl: String,
    size: Int,
    medianMs: Double,
    memoryMB: Double,
    gcCount: Long
  )

  def benchmark(name: String, size: Int, warmups: Int, iterations: Int)(f: => Unit): BenchResult = {
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
      f
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
    println("Array vs Vector Benchmark for buildNullSet")
    println("=" * 80)

    val sizes = Seq(
      (10_000, 5_000),
      (50_000, 25_000),
      (100_000, 50_000),
      (500_000, 250_000),
      (1_000_000, 500_000)
    )

    val nullPercentages = Seq(0.0, 0.1, 0.5)

    println(f"\n${"Size"}%-10s ${"Nulls%"}%-8s ${"Impl"}%-15s ${"Time(ms)"}%-12s ${"Memory(MB)"}%-12s ${"GC"}%-8s")
    println("=" * 80)

    for {
      (totalSize, sliceSize) <- sizes
      nullPct <- nullPercentages
    } {
      // Generate test data
      val data = Array.fill(totalSize)(scala.util.Random.nextInt(1000))
      val indicesVector = scala.util.Random.shuffle((0 until totalSize).toVector).take(sliceSize)
      val indicesArray = indicesVector.toArray
      val nullCount = (sliceSize * nullPct).toInt
      val nullIndices = scala.util.Random.shuffle(indicesVector).take(nullCount)
      val nulls = BitSet(nullIndices*)

      // Benchmark Vector version
      val vectorResult = benchmark("Vector", sliceSize, 10, 50) {
        buildNullSetVector(nulls, indicesVector)
      }

      // Benchmark Array version
      val arrayResult = benchmark("Array", sliceSize, 10, 50) {
        buildNullSetArray(nulls, indicesArray)
      }

      println(
        f"${sliceSize}%-10d ${(nullPct * 100).toInt}%-8d ${vectorResult.impl}%-15s ${vectorResult.medianMs}%-12.4f ${vectorResult.memoryMB}%-12.4f ${vectorResult.gcCount}%-8d"
      )
      println(
        f"${sliceSize}%-10d ${(nullPct * 100).toInt}%-8d ${arrayResult.impl}%-15s ${arrayResult.medianMs}%-12.4f ${arrayResult.memoryMB}%-12.4f ${arrayResult.gcCount}%-8d"
      )

      // Calculate ratios
      val timeRatio = arrayResult.medianMs / vectorResult.medianMs
      val memRatio = if (vectorResult.memoryMB != 0.0) arrayResult.memoryMB / vectorResult.memoryMB else 0.0

      println(f"${" "}%-10s ${" "}%-8s ${"→ Ratio"}%-15s ${timeRatio}%-12.2fx ${memRatio}%-12.2fx")
      println()
    }

    println("\n" + "=" * 80)
    println("KEY QUESTION: Does Array eliminate the memory overhead?")
    println("If Array shows similar memory to Vector, the problem is NOT Vector itself.")
    println("If Array shows much less memory, we should change the API to use Array.")
    println("=" * 80)
  }
}
