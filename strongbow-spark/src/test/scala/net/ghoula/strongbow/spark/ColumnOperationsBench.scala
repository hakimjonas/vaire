package net.ghoula.strongbow.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.BitSet

import net.ghoula.strongbow.column.Column
import net.ghoula.strongbow.prelude.*

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

  private lazy val randomIndices: Array[Int] = {
    val rng = new java.util.Random(42)
    (0 until N).filter(_ => rng.nextBoolean()).toArray
  }

  private lazy val intCol = Column.int(Array.tabulate(N)(i => i))
  private lazy val floatCol = Column.float(Array.tabulate(N)(i => i.toFloat))
  private lazy val longCol = Column.long(Array.tabulate(N)(i => i.toLong))
  private lazy val decimalCol = Column.decimal(Array.tabulate(N)(i => i.toLong), 10, 2)
  private lazy val timestampCol = Column.timestamp(Array.tabulate(N)(i => i.toLong * 1000))
  private lazy val stringCol = Column.string(Array.tabulate(N)(i => f"val$i%07d"))
  private lazy val binaryCol = Column.binaryFromArrays(
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

  "Distinct + sortByExprs" should "benchmark review fix paths at 100K rows" taggedAs Benchmark in {
    val rows = 100_000
    val groups = 1_000

    case class Row2(key: String, value: Int)
    given row2Schema: Schema[Row2] = Schema.derived

    val rng = new java.util.Random(42)
    val keyCol = Column.string(Array.tabulate(rows)(i => s"group${rng.nextInt(groups)}"))
    val valCol = Column.int(Array.tabulate(rows)(i => rng.nextInt(10000)))
    val ds = Dataset.fromColumns(Vector(keyCol, valCol), row2Schema).toOption.get

    val keyCell: Expr[Row2, String] = Expr.Cell("key_value", ColumnIndex(0))
    val valCell: Expr[Row2, Int] = Expr.Cell("value_value", ColumnIndex(1))

    def timeOp(label: String, f: => Either[?, ?]): Double = {
      val times = (0 until Warmup + Measured).map { _ =>
        val t0 = System.nanoTime()
        f
        System.nanoTime() - t0
      }.drop(Warmup)
      medianMs(times)
    }

    val distinctMs = timeOp("distinct", DatasetInterpreter.execute(ds.distinct))

    val sortKeys = Vector(
      SortSpec(valCell, summon[Ordering[Int]], ColumnType.IntType, true),
      SortSpec(keyCell, summon[Ordering[String]], ColumnType.StringType, false)
    )
    val sortByExprsMs = timeOp("sortByExprs", DatasetInterpreter.execute(ds.sortByExprs(sortKeys)))

    info(f"\n${"=" * 50}")
    info(f"Distinct + SortByExprs — ${rows / 1000}K rows, $groups groups")
    info(f"${"=" * 50}")
    info(f"  distinct (sort-based)  ${distinctMs}%9.1f ms")
    info(f"  sortByExprs (@tailrec) ${sortByExprsMs}%9.1f ms")

    succeed
  }

  "Distinct old vs new" should "compare HashSet vs sort-based at multiple scales" taggedAs Benchmark in {

    def oldDistinct(columns: Vector[Column[?]], rowCount: Int): Array[Int] = {
      val seen = scala.collection.mutable.HashSet.empty[Vector[Any]]
      (0 until rowCount).filter { rowIdx =>
        val row = columns.map(_.getValue(rowIdx))
        if (seen.contains(row)) false
        else { seen.add(row); true }
      }.toArray
    }

    def newDistinct(columns: Vector[Column[?]], rowCount: Int): Array[Int] = {
      @scala.annotation.tailrec
      def compareRows(a: Int, b: Int, ci: Int): Int =
        if (ci >= columns.length) 0
        else {
          val cmp = Column.compareAt(columns(ci), a, b)
          if (cmp != 0) cmp
          else compareRows(a, b, ci + 1)
        }

      val sorted = (0 until rowCount).sortWith((a, b) => compareRows(a, b, 0) < 0).toArray
      sorted.tail
        .foldLeft(Vector(sorted.head)) { (acc, idx) =>
          if (compareRows(acc.last, idx, 0) != 0) acc :+ idx
          else acc
        }
        .toArray
    }

    def timeDistinct(f: => Array[Int]): Double = {
      val times = (0 until Warmup + Measured).map { _ =>
        val t0 = System.nanoTime()
        val result = f
        val elapsed = System.nanoTime() - t0
        assert(result.length > 0)
        elapsed
      }.drop(Warmup)
      medianMs(times)
    }

    info(f"\n${"=" * 65}")
    info(f"Distinct comparison — HashSet vs sort+compareAt, 3 columns")
    info(f"${"=" * 65}")
    info(f"${"Scale"}%-10s ${"HashSet"}%12s ${"Sort+dedup"}%12s ${"ratio"}%8s")
    info("-" * 65)

    Vector(100_000, 500_000, 1_000_000).foreach { rows =>
      val rng = new java.util.Random(42)
      val cols = Vector(
        Column.int(Array.tabulate(rows)(i => rng.nextInt(1000))),
        Column.string(Array.tabulate(rows)(i => s"val${rng.nextInt(500)}")),
        Column.double(Array.tabulate(rows)(i => rng.nextDouble() * 100))
      )
      val label = if (rows >= 1_000_000) s"${rows / 1_000_000}M" else s"${rows / 1_000}K"
      val oldMs = timeDistinct(oldDistinct(cols, rows))
      val newMs = timeDistinct(newDistinct(cols, rows))
      val ratio = newMs / oldMs
      info(f"$label%-10s ${oldMs}%9.1f ms ${newMs}%9.1f ms ${ratio}%7.2fx")
    }

    succeed
  }
}
