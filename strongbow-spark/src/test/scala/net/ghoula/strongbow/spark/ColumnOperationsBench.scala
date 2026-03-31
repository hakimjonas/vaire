package net.ghoula.strongbow.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.BitSet

import net.ghoula.strongbow.column.Column

/** Column-level operation benchmarks at 1M rows. No Spark dependency — pure Column operations.
  *
  * Diagnostic results (2026-03-31, JDK 21, ZGC, 16G heap, fat jar):
  *
  * Sort dispatch investigation — DecimalColumn/TimestampColumn show ~15% overhead vs LongColumn on
  * sortIndicesByColumn despite identical Array[Long] storage and comparison. Root cause is enum
  * pattern match dispatch cost, not the comparison function:
  * {{{
  * direct _ < _ (no dispatch)      9.9 ms
  * direct Long.compare            10.3 ms   comparison function is not the cause
  * LongColumn dispatch            16.8 ms   7ms match overhead (case 2 of 16)
  * DecimalColumn dispatch         19.3 ms   10ms match overhead (case 15 of 16)
  * TimestampColumn dispatch       19.9 ms   10ms match overhead (case 8 of 16)
  * }}}
  * The 3ms gap between LongColumn and Decimal/Timestamp is enum ordinal distance in the JVM's
  * compiled match. Not actionable without restructuring the enum. All types remain under 1.5x vs
  * IntColumn on sortIndicesByColumn.
  */
class ColumnOperationsBench extends AnyFlatSpec with Matchers {

  private val N = 1_000_000
  private val Warmup = 3
  private val Measured = 5

  private val randomIndices: Array[Int] = {
    val rng = new java.util.Random(42)
    (0 until N).filter(_ => rng.nextBoolean()).toArray
  }

  private val intCol = Column.int(Array.tabulate(N)(i => i))
  private val floatCol = Column.float(Array.tabulate(N)(i => i.toFloat))
  private val longCol = Column.long(Array.tabulate(N)(i => i.toLong))
  private val decimalCol = Column.decimal(Array.tabulate(N)(i => i.toLong), 10, 2)
  private val timestampCol = Column.timestamp(Array.tabulate(N)(i => i.toLong * 1000))
  private val stringCol = Column.string(Array.tabulate(N)(i => f"val$i%07d"))
  private val binaryCol = Column.binaryFromArrays(
    Array.tabulate(N)(i => {
      val buf = java.nio.ByteBuffer.allocate(8)
      buf.putLong(i.toLong)
      buf.array()
    }),
    BitSet.empty
  )

  private case class BenchResult(sortMs: Double, compareNs: Double, sliceMs: Double)

  private def medianMs(values: Seq[Long]): Double = {
    val sorted = values.sorted
    sorted(sorted.length / 2) / 1e6
  }

  private def medianNs(values: Seq[Long]): Double = {
    val sorted = values.sorted
    sorted(sorted.length / 2).toDouble
  }

  private def benchColumn(col: Column[?]): BenchResult = {
    val sortTimes = (0 until Warmup + Measured).map { _ =>
      val t0 = System.nanoTime()
      Column.sortIndicesByColumn(col, N)
      System.nanoTime() - t0
    }.drop(Warmup)

    val compareIters = 1_000_000
    val compareTimes = (0 until Warmup + Measured).map { _ =>
      val t0 = System.nanoTime()
      (0 until compareIters).foreach(i => Column.compareAt(col, i % N, (i + 1) % N))
      (System.nanoTime() - t0) / compareIters
    }.drop(Warmup)

    val sliceTimes = (0 until Warmup + Measured).map { _ =>
      val t0 = System.nanoTime()
      col.slice(randomIndices)
      System.nanoTime() - t0
    }.drop(Warmup)

    BenchResult(medianMs(sortTimes), medianNs(compareTimes), medianMs(sliceTimes))
  }

  "Column operations" should "benchmark all types at 1M rows" taggedAs Benchmark in {
    val types = Vector(
      ("IntColumn", intCol),
      ("FloatColumn", floatCol),
      ("LongColumn", longCol),
      ("DecimalColumn", decimalCol),
      ("TimestampColumn", timestampCol),
      ("StringColumn", stringCol),
      ("BinaryColumn", binaryCol)
    )

    info(f"\n${"=" * 70}")
    info(f"Column operations benchmark — ${N / 1_000_000}M rows, $Warmup warmup + $Measured measured")
    info(f"${"=" * 70}")
    info(f"${"Type"}%-18s ${"sortIndices"}%12s ${"compareAt"}%12s ${"slice(50%%)"}%12s")
    info("-" * 70)

    val results = types.map { case (name, col) =>
      val r = benchColumn(col)
      info(f"$name%-18s ${r.sortMs}%9.1f ms ${r.compareNs}%8.1f ns ${r.sliceMs}%9.1f ms")
      (name, r)
    }

    val intSort = results.find(_._1 == "IntColumn").get._2.sortMs
    info("-" * 70)
    info("Relative to IntColumn (sortIndices):")
    results.foreach { case (name, r) =>
      val ratio = r.sortMs / intSort
      info(f"  $name%-18s ${ratio}%.2fx")
    }

    succeed
  }

  "Sort dispatch diagnostic" should "isolate sort from Column dispatch" taggedAs Benchmark in {
    val rawLongArray = Array.tabulate(N)(i => i.toLong)

    def directSort(data: IArray[Long])(lt: (Long, Long) => Boolean): Array[Int] =
      (0 until N).sortWith((a, b) => lt(data(a), data(b))).toArray

    def timeSort(label: String, f: => Array[Int]): Double = {
      val times = (0 until Warmup + Measured).map { _ =>
        val t0 = System.nanoTime()
        f
        System.nanoTime() - t0
      }.drop(Warmup)
      medianMs(times)
    }

    val iarray = IArray.unsafeFromArray(rawLongArray)

    val directLt = timeSort("direct _ < _", directSort(iarray)(_ < _))
    val directCompare = timeSort("direct Long.compare", directSort(iarray)((a, b) => java.lang.Long.compare(a, b) < 0))
    val viaLongCol = timeSort("LongColumn dispatch", Column.sortIndicesByColumn(longCol, N))
    val viaDecimalCol = timeSort("DecimalColumn dispatch", Column.sortIndicesByColumn(decimalCol, N))
    val viaTimestampCol = timeSort("TimestampColumn dispatch", Column.sortIndicesByColumn(timestampCol, N))

    info(f"\n${"=" * 50}")
    info("Sort dispatch diagnostic — 1M Long values")
    info(f"${"=" * 50}")
    info(f"  direct _ < _           ${directLt}%9.1f ms")
    info(f"  direct Long.compare    ${directCompare}%9.1f ms")
    info(f"  LongColumn dispatch    ${viaLongCol}%9.1f ms")
    info(f"  DecimalColumn dispatch ${viaDecimalCol}%9.1f ms")
    info(f"  TimestampCol dispatch  ${viaTimestampCol}%9.1f ms")

    succeed
  }
}
