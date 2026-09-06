package net.ghoula.vaire.spark

import org.apache.spark.sql.functions.{aggregate, col, filter, lit, transform}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.column.Column
import net.ghoula.vaire.prelude.*
import net.ghoula.vaire.types.ColumnIndex

/** Higher-order lambda expression benchmark — the measurement behind the in-memory design (body
  * evaluated once over the flat element dimension) vs the per-row `Dataset.map`-style
  * Scala-function fallback, plus the Spark native pushdown timings.
  *
  * Shapes: 1M in-memory rows and 200K Spark rows of int arrays with k ∈ {1, 10, 100} elements;
  * bodies are trivial arithmetic (`x + 1`, `x > 1`, sum fold) so the numbers isolate the machinery
  * overhead, not the body cost.
  *
  * The element budget is held at 10M per shape (rows shrink as k grows) so the boxed data itself
  * fits the heap; eval time is element-count-dominated and roughly flat across k.
  *
  * Diagnostic results (2026-08-30, JDK 25, ZGC, 4G heap, local[2]):
  * {{{
  *   In-memory, 10M elements (median ms):
  *     k=1   rows=10M  transform  462  filter 1231  aggregate 15796  exists  798  fallback  15
  *     k=10  rows=1M   transform  475  filter  582  aggregate 14957  exists  522  fallback  36
  *     k=100 rows=100K transform  412  filter  549  aggregate 14702  exists  372  fallback  16
  *   Spark, 200K rows x k=10 (noop sink): transform 171  filter 171  aggregate 162 ms
  * }}}
  * Findings: the transform/filter/exists family costs ~35-60 ms per 1M elements (interpreter
  * dispatch dominates; ~30x over a raw per-row Scala loop, consistent with the interpreter's
  * general overhead profile). aggregate is the outlier at ~1.5 us per sequential element eval (~15
  * s per 10M) — the predicted cost center; for large data prefer the Spark pushdown (flat ~165 ms
  * at 200K rows x 10 elements).
  */
class HigherOrderBench extends AnyFlatSpec with Matchers with SparkTestBase {

  private val ElementBudget = 10_000_000
  private val SparkRows = 200_000
  private val Warmup = 1
  private val Measured = 3

  private def rowsFor(k: Int): Int = ElementBudget / k

  private def arrayData(k: Int): Array[Any] = {
    val rows = rowsFor(k)
    Array.tabulate[Any](rows)(i => Seq.tabulate(k)(j => (i + j) % 1000))
  }

  private def medianMs(times: Seq[Long]): Double = {
    val sorted = times.sorted
    sorted(sorted.length / 2) / 1e6
  }

  private def bench(body: => Long): Double = {
    val times = (0 until Warmup + Measured).map { _ =>
      val t0 = System.nanoTime()
      val probe = body
      require(probe >= 0)
      System.nanoTime() - t0
    }.drop(Warmup)
    medianMs(times)
  }

  private def evalExpr[A](expr: Expr[A, ?], columns: Vector[Column[?]]): Long = {
    val result = net.ghoula.vaire.interpreter.ExprInterpreter.evalColumn(
      expr,
      columns,
      net.ghoula.vaire.column.ColumnType.AnyType
    )
    result.fold(_ => -1L, _.length.toLong)
  }

  "higher-order in-memory" should "measure expr eval vs per-row Scala fallback" taggedAs Benchmark in {
    Vector(1, 10, 100).foreach { k =>
      val data = arrayData(k)
      val col = Column.any(data)
      val cell = Expr.Cell[Any, Seq[Int]]("xs", ColumnIndex(0))
      val columns = Vector(col)

      val transformExpr = cell.transform(x => x + Expr.const(1))
      val filterExpr = cell.filter(x => x > Expr.const(1))
      val aggExpr = cell.aggregate(Expr.const(0))((acc, x) => acc + x)
      val existsExpr = cell.exists(x => x > Expr.const(1))

      // Dataset.map-equivalent fallback: a plain per-row Scala function over the raw Seqs.
      val rows = rowsFor(k)
      def fallback: Long =
        data
          .take(rows)
          .iterator
          .map {
            case s: Seq[?] => s.size
            case _ => 0
          }
          .sum

      val tTransform = bench(evalExpr(transformExpr, columns))
      val tFilter = bench(evalExpr(filterExpr, columns))
      val tAggregate = bench(evalExpr(aggExpr, columns))
      val tExists = bench(evalExpr(existsExpr, columns))
      val tFallback = bench(fallback)
      println(
        f"k=$k%3d rows=${rowsFor(k)}%7d  transform=$tTransform%9.1f  filter=$tFilter%9.1f  " +
          f"aggregate=$tAggregate%9.1f  exists=$tExists%9.1f  fallback=$tFallback%9.1f ms"
      )
    }
  }

  "higher-order spark" should "measure native lambda pushdown via noop sink" taggedAs Benchmark in {
    val k = 10
    val schema = org.apache.spark.sql.types.StructType(
      Seq(
        org.apache.spark.sql.types.StructField(
          "xs",
          org.apache.spark.sql.types.ArrayType(org.apache.spark.sql.types.IntegerType),
          nullable = true
        )
      )
    )
    val rows = java.util.Arrays.asList(
      Array.tabulate(SparkRows) { i =>
        org.apache.spark.sql.Row(Seq.tabulate(k)(j => (i + j) % 1000))
      }*
    )
    val df = spark.createDataFrame(rows, schema).cache()
    df.count()

    def planMs(build: org.apache.spark.sql.Column): Double = {
      val times = (0 until Warmup + Measured).map { _ =>
        val t0 = System.nanoTime()
        df.select(build).write.format("noop").mode("overwrite").save()
        System.nanoTime() - t0
      }.drop(Warmup)
      medianMs(times)
    }

    val xs = col("xs")
    val tTransform = planMs(transform(xs, (x: org.apache.spark.sql.Column) => x + lit(1)))
    val tFilter = planMs(filter(xs, (x: org.apache.spark.sql.Column) => x > lit(1)))
    val tAggregate =
      planMs(aggregate(xs, lit(0), (acc: org.apache.spark.sql.Column, x: org.apache.spark.sql.Column) => acc + x))
    println(f"spark k=$k%3d  transform=$tTransform%9.1f  filter=$tFilter%9.1f  aggregate=$tAggregate%9.1f ms")
    df.unpersist()
  }
}
