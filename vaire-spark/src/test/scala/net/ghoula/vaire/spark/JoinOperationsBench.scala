package net.ghoula.vaire.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.prelude.*

/** Join-operator benchmark for the in-memory interpreter. No Spark dependency — pure core
  * `DatasetInterpreter` execution. Phase 0 of `docs/join-optimization-plan.md`: establishes the
  * current performance curve with skew represented, so the contended cost in the keyed path is
  * visible before any fix is made. Nothing is fixed here; this file only reports.
  *
  * The keyed-family sweeps put the skew on the side each join *indexes*, which differs per
  * operator: `InnerJoinOn` indexes the left, `LeftJoinOn`/`FullJoinOn` index the right
  * (`DatasetInterpreter.scala` `KeyIndex.build` / `innerJoinOnExpr` / `probeJoinOnKeys` /
  * `fullJoinOnExpr`). The probe side is kept disjoint from the index keys so the output stays small
  * and the measured time isolates the index build.
  *
  * Run via the assembled jar (`sbt spark/assembly; java -jar vaire-spark-bench.jar -s
  * net.ghoula.vaire.spark.JoinOperationsBench`) or by clearing the suite's Benchmark exclusion:
  * `sbt 'set spark/Test/testOptions := Seq()' 'spark/Test/testOnly
  * net.ghoula.vaire.spark.JoinOperationsBench'`.
  */
class JoinOperationsBench extends AnyFlatSpec with Matchers {

  private val Warmup = 1
  private val Measured = 3

  private val keyExpr: Expr[Int, Int] = Expr.Cell("value", ColumnIndex(0))
  private val intType = ColumnType.IntType

  // ---------------------------------------------------------------------------
  // data
  // ---------------------------------------------------------------------------

  private val skewSizes = Vector(100_000, 200_000, 400_000)
  private val KeyBase = 1_000_000
  private val ProbeBase = 900_000_000
  private val HotKey = 424_242

  private def indexSide(regime: String, n: Int): Array[Int] = regime match {
    case "hot" => Array.fill(n)(HotKey) // one distinct key, n rows — the contended build
    case "few" => Array.tabulate(n)(i => i % (math.sqrt(n.toDouble).toInt.max(1))) // k ≈ √n
    case "uniform" => Array.tabulate(n)(i => KeyBase + i) // k ≈ n, no duplicates
  }

  // Disjoint from every index side above, so matched output stays ~0.
  private def probeSide(n: Int): Array[Int] = Array.tabulate(n)(i => ProbeBase + i)

  private def intDs(values: Array[Int]): Dataset[Int] =
    Dataset.fromColumns(Vector(Column.int(values)), Schema.intSchema).toOption.get

  // ---------------------------------------------------------------------------
  // benchmarking
  // ---------------------------------------------------------------------------

  private case class MeasuredRows(ms: Double, rowsOut: Int)

  private def medianMs(values: Seq[Long]): Double = {
    val sorted = values.sorted
    sorted(sorted.length / 2) / 1e6
  }

  private def bench(f: => Int): MeasuredRows = {
    var rowsOut = -1
    val timings = (0 until Warmup + Measured).map { _ =>
      val t0 = System.nanoTime()
      rowsOut = f
      System.nanoTime() - t0
    }.drop(Warmup)
    MeasuredRows(medianMs(timings), rowsOut)
  }

  private def runKeyed(plan: Dataset[?]): Int = DatasetInterpreter.execute(plan).toOption.get.rowCount

  // ---------------------------------------------------------------------------
  // 1. Keyed joins — index-build scaling under skew
  // ---------------------------------------------------------------------------

  "Keyed joins" should "show index-build scaling under skew on the indexed side" taggedAs Benchmark in {
    info(f"\n${"=" * 78}")
    info("InnerJoinOn — index built on LEFT; RIGHT probe disjoint (output ~0)")
    info(f"${"=" * 78}")
    info(
      f"${"n"}%-8s ${"regime"}%-8s ${"probe"}%-8s ${"rows-in"}%-12s ${"rows-out"}%-10s ${"median ms"}%10s ${"vs prev"}%8s"
    )
    info("-" * 78)

    Vector("hot", "few", "uniform").foreach { regime =>
      var prev: Option[Double] = None
      skewSizes.foreach { n =>
        val leftN = n
        val rightN = n
        val left = intDs(indexSide(regime, leftN))
        val right = intDs(probeSide(rightN))
        val m = bench(runKeyed(left.joinOn(right, keyExpr, keyExpr, intType, intType)))
        val ratio = prev.fold("—")(p => f"${m.ms / p}%6.2fx")
        info(f"$n%-8d $regime%-8s $rightN%-8d ${leftN + rightN}%-12d ${m.rowsOut}%-10d ${m.ms}%9.1f  $ratio")
        prev = Some(m.ms)
      }
    }

    succeed
  }

  "Keyed joins" should "show which side absorbs the hot key per operator" taggedAs Benchmark in {
    info(f"\n${"=" * 78}")
    info("LeftJoinOn / FullJoinOn — index built on RIGHT; hot key on right, LEFT probe disjoint")
    info(f"${"=" * 78}")
    info(f"${"op"}%-14s ${"n"}%-8s ${"regime"}%-8s ${"rows-in"}%-12s ${"rows-out"}%-10s ${"median ms"}%10s")
    info("-" * 78)

    val n = 200_000
    Vector("hot", "few", "uniform").foreach { regime =>
      val left = intDs(probeSide(n)) // probe, disjoint
      val right = intDs(indexSide(regime, n)) // indexed side carries the skew
      val rowsIn = n + n
      val leftJoin = bench(runKeyed(left.leftJoinOn(right, keyExpr, keyExpr, intType, intType)))
      val fullJoin = bench(runKeyed(left.fullJoinOn(right, keyExpr, keyExpr, intType, intType)))
      info(f"${"LeftJoinOn"}%-14s $n%-8d $regime%-8s $rowsIn%-12d ${leftJoin.rowsOut}%-10d ${leftJoin.ms}%9.1f")
      info(f"${"FullJoinOn"}%-14s $n%-8d $regime%-8s $rowsIn%-12d ${fullJoin.rowsOut}%-10d ${fullJoin.ms}%9.1f")
    }

    succeed
  }

  // ---------------------------------------------------------------------------
  // 2. Keyed joins — realistic fit / probe shapes
  // ---------------------------------------------------------------------------

  "Keyed joins" should "show fit-to-probe shapes with real output" taggedAs Benchmark in {
    info(f"\n${"=" * 78}")
    info("Uniform keyed joins with a ~25% overlap — output size realistic")
    info(f"${"=" * 78}")
    info(f"${"op"}%-14s ${"left"}%-8s ${"right"}%-8s ${"rows-in"}%-12s ${"rows-out"}%-10s ${"median ms"}%10s")
    info("-" * 78)

    def report(label: String, leftN: Int, rightN: Int, plan: => Dataset[?]): Unit = {
      val m = bench(runKeyed(plan))
      info(f"$label%-14s $leftN%-8d $rightN%-8d ${leftN + rightN}%-12d ${m.rowsOut}%-10d ${m.ms}%9.1f")
    }

    // small lookup × large fact: left 1k distinct keys, right 500k rows keyed mod 4k → 25% match
    val smallN = 1_000
    val largeN = 500_000
    val small = intDs(Array.tabulate(smallN)(i => i))
    val large = intDs(Array.tabulate(largeN)(i => i % (smallN * 4)))

    report("InnerJoinOn", smallN, largeN, small.joinOn(large, keyExpr, keyExpr, intType, intType))
    report("LeftJoinOn", smallN, largeN, small.leftJoinOn(large, keyExpr, keyExpr, intType, intType))
    report("FullJoinOn", smallN, largeN, small.fullJoinOn(large, keyExpr, keyExpr, intType, intType))

    // both sides large, same distinct-key space, ~25% int-key overlap
    val bothN = 200_000
    val leftBig = intDs(Array.tabulate(bothN)(i => KeyBase + i))
    val rightBig = intDs(Array.tabulate(bothN)(i => KeyBase + (i % (bothN * 4))))
    report("InnerJoinOn", bothN, bothN, leftBig.joinOn(rightBig, keyExpr, keyExpr, intType, intType))

    succeed
  }

  // ---------------------------------------------------------------------------
  // 3. Predicate joins — O(n·m) family
  // ---------------------------------------------------------------------------

  "Predicate joins" should "benchmark small frames at small and medium sizes" taggedAs Benchmark in {
    info(f"\n${"=" * 78}")
    info("Predicate joins — O(n·m) by design; small frames only")
    info(f"${"=" * 78}")
    info(f"${"op"}%-14s ${"left"}%-8s ${"right"}%-8s ${"rows-in"}%-12s ${"rows-out"}%-10s ${"median ms"}%10s")
    info("-" * 78)

    def report(label: String, leftN: Int, rightN: Int, plan: => Dataset[?]): Unit = {
      val m = bench(runKeyed(plan))
      info(f"$label%-14s $leftN%-8d $rightN%-8d ${leftN + rightN}%-12d ${m.rowsOut}%-10d ${m.ms}%9.1f")
    }

    val smallN = 2_000
    val medN = 20_000
    val keysS = intDs(Array.tabulate(smallN)(i => i))
    val keysM = intDs(Array.tabulate(medN)(i => i % smallN))

    report("inner", smallN, smallN, keysS.join(keysS, (a: Int, b: Int) => a == b))
    report("inner", smallN, medN, keysS.join(keysM, (a: Int, b: Int) => a == b))
    report("left", smallN, medN, keysS.leftJoin(keysM, (a: Int, b: Int) => a == b))
    report("full", smallN, medN, keysS.fullJoin(keysM, (a: Int, b: Int) => a == b))
    report("anti", smallN, medN, keysS.antiJoin(keysM, (a: Int, b: Int) => a == b))

    succeed
  }
}
