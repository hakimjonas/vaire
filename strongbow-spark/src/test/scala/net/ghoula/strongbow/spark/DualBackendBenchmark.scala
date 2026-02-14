package net.ghoula.strongbow.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.{Column, ColumnType, Dataset, DatasetInterpreter, Expr, Interpreter, Schema}
import net.ghoula.strongbow.errors.DecodeError
import net.ghoula.strongbow.types.ColumnIndex

/** Benchmark comparing Columnar (in-memory) vs Spark interpreter on identical plans.
  *
  * Runs each operation through both interpreters and reports timing. Validates that results match
  * (parity) while measuring the overhead of Spark's distributed machinery on local data.
  *
  * Tagged with Ignore by default — run explicitly via:
  * {{{
  * sbt "spark/testOnly *DualBackendBenchmark"
  * }}}
  */
class DualBackendBenchmark extends AnyFlatSpec with Matchers with SparkTestBase {

  private val scales = Vector(1000, 10000, 50000)
  private val warmups = 3
  private val runs = 10

  private def generatePairDataset(n: Int): Dataset[(String, Int)] = {
    val rng = new scala.util.Random(42)
    val keys = (0 until n).map(_ => s"group${rng.nextInt(100)}").toArray
    val values = (0 until n).map(_ => rng.nextInt(1000)).toArray
    val keyCol = Column.string(keys)
    val valCol = Column.int(values)

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

    Dataset.fromColumns(Vector(keyCol, valCol), summon[Schema[(String, Int)]]).toOption.get
  }

  private def benchmark[T](label: String, interpreter: Interpreter, plan: Dataset[T]): Double = {
    // Warmup
    (0 until warmups).foreach(_ => interpreter.execute(plan))

    // Timed runs
    val times = (0 until runs).map { _ =>
      val start = System.nanoTime()
      interpreter.execute(plan)
      val end = System.nanoTime()
      (end - start) / 1_000_000.0
    }
    val sorted = times.sorted
    sorted(runs / 2) // median
  }

  private def runComparison(operation: String, plan: Dataset[?]): Unit = {
    val columnarMs = benchmark("Columnar", DatasetInterpreter, plan)
    val sparkMs = benchmark("Spark", sparkInterpreter, plan)
    val ratio = sparkMs / columnarMs
    println(f"    $operation%-20s  Columnar: $columnarMs%8.2f ms  Spark: $sparkMs%8.2f ms  ratio: ${ratio}%5.1fx")
  }

  "Dual backend benchmark" should "compare Filter" in {
    for (n <- scales) {
      println(s"\n  --- Scale: $n rows ---")
      val ds = generatePairDataset(n)
      val valCell = Expr.Cell[(String, Int), Int]("value", ColumnIndex(1))
      val plan = ds.filter(valCell > Expr.const(500))
      runComparison(s"Filter($n)", plan)
    }
  }

  it should "compare GroupBy + ReduceByKey" in {
    given Schema[(String, Int)] = Schema.tuple2Schema[String, Int]
    for (n <- scales) {
      println(s"\n  --- Scale: $n rows ---")
      val ds = generatePairDataset(n)
      val plan = ds.groupBy(_._1).reduceByKey((a, b) => (a._1, a._2 + b._2)).toPairs
      runComparison(s"GroupByReduce($n)", plan)
    }
  }

  it should "compare Sort (lambda)" in {
    for (n <- scales) {
      println(s"\n  --- Scale: $n rows ---")
      val ds = generatePairDataset(n)
      val plan = ds.sortBy(_._2)(using Ordering[Int])
      runComparison(s"SortLambda($n)", plan)
    }
  }

  it should "compare Sort (expr-based, native Spark pushdown)" in {
    for (n <- scales) {
      println(s"\n  --- Scale: $n rows ---")
      val ds = generatePairDataset(n)
      val valCell = Expr.Cell[(String, Int), Int]("value", ColumnIndex(1))
      val plan = ds.sortByExpr(valCell, ColumnType.IntType)(using Ordering[Int])
      runComparison(s"SortExpr($n)", plan)
    }
  }

  it should "compare Map" in {
    given Schema[(String, Int)] = Schema.tuple2Schema[String, Int]
    for (n <- scales) {
      println(s"\n  --- Scale: $n rows ---")
      val ds = generatePairDataset(n)
      val plan = ds.map(p => (p._1, p._2 * 2))
      runComparison(s"Map($n)", plan)
    }
  }

  it should "compare Distinct" in {
    for (n <- scales) {
      println(s"\n  --- Scale: $n rows ---")
      val ds = generatePairDataset(n)
      val plan = ds.distinct
      runComparison(s"Distinct($n)", plan)
    }
  }

  it should "compare chained pipeline" in {
    given Schema[(String, Int)] = Schema.tuple2Schema[String, Int]
    for (n <- scales) {
      println(s"\n  --- Scale: $n rows ---")
      val ds = generatePairDataset(n)
      val valCell = Expr.Cell[(String, Int), Int]("value", ColumnIndex(1))
      val plan = ds
        .filter(valCell > Expr.const(200))
        .map(p => (p._1, p._2 * 2))
        .groupBy(_._1)
        .reduceByKey((a, b) => (a._1, a._2 + b._2))
        .toPairs
      runComparison(s"Pipeline($n)", plan)
    }
  }
}
