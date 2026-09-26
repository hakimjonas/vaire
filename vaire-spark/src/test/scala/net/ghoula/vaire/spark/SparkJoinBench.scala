package net.ghoula.vaire.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.prelude.*

/** Spark-backend keyed-join benchmark.
  *
  * The Spark path is native Catalyst (`col(...) === col(...)`), so this measures Spark's join, not
  * the in-memory `KeyIndex`; it exists to show the two backends' curves side by side. Sizes and
  * iterations are overridable with `-Dvaire.bench.sizes`, `-Dvaire.bench.warmup`,
  * `-Dvaire.bench.measured`. Large sizes are heavy on a `local[2]` session.
  */
class SparkJoinBench extends AnyFlatSpec with Matchers with SparkTestBase {

  private val Warmup: Int = sys.props.get("vaire.bench.warmup").flatMap(_.toIntOption).getOrElse(1)
  private val Measured: Int = sys.props.get("vaire.bench.measured").flatMap(_.toIntOption).getOrElse(3)
  private val skewSizes: Vector[Int] =
    sys.props
      .get("vaire.bench.sizes")
      .map(_.split(',').toVector.flatMap(_.trim.toIntOption))
      .filter(_.nonEmpty)
      .getOrElse(Vector(100_000, 200_000, 400_000))

  private val keyExpr: Expr[Int, Int] = Expr.Cell("value", ColumnIndex(0))
  private val intType = ColumnType.IntType
  private val KeyBase = 1_000_000
  private val ProbeBase = 900_000_000
  private val HotKey = 424_242

  private def indexSide(regime: String, n: Int): Array[Int] = regime match {
    case "hot" => Array.fill(n)(HotKey)
    case "few" => Array.tabulate(n)(i => i % (math.sqrt(n.toDouble).toInt.max(1)))
    case "uniform" => Array.tabulate(n)(i => KeyBase + i)
    case "sparse" => Array.tabulate(n)(i => KeyBase + i * 37)
  }

  private def probeSide(n: Int): Array[Int] = Array.tabulate(n)(i => ProbeBase + i)

  private def intDs(values: Array[Int]): Dataset[Int] =
    Dataset.fromColumns(Vector(Column.int(values)), Schema.intSchema).toOption.get

  private def medianMs(timings: Vector[Long]): Double = {
    val sorted = timings.sorted
    sorted(sorted.length / 2) / 1e6
  }

  private def bench(f: => Long): Double = {
    val timings = (0 until Warmup + Measured).map { _ =>
      val t0 = System.nanoTime()
      f
      System.nanoTime() - t0
    }.drop(Warmup).toVector
    medianMs(timings)
  }

  "Spark keyed joins" should "show index-build scaling under skew" taggedAs Benchmark in {
    info(f"\n${"=" * 72}")
    info("InnerJoinOn on the Spark backend (native Catalyst join), disjoint probe (output ~0)")
    info(f"${"=" * 72}")
    info(f"${"n"}%-9s ${"regime"}%-9s ${"median ms"}%10s")
    info("-" * 72)

    Vector("hot", "few", "uniform", "sparse").foreach { regime =>
      skewSizes.foreach { n =>
        val left = intDs(indexSide(regime, n))
        val right = intDs(probeSide(n))
        val plan = left.joinOn(right, keyExpr, keyExpr, intType, intType)
        val ms = bench {
          sparkInterpreter.toDataFrame(plan).toOption.get.count()
        }
        info(f"$n%-9d $regime%-9s $ms%9.1f")
      }
    }

    succeed
  }
}
