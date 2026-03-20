package net.ghoula.strongbow.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.{Column, ColumnType, Dataset, Expr, Schema}
import net.ghoula.strongbow.specs.{AggSpec, KeySpec, SortSpec, WindowExprSpec}
import net.ghoula.strongbow.WindowSpec
import net.ghoula.strongbow.types.ColumnIndex

/** Overhead benchmark: strongbow Dataset plan vs native Spark for identical operations.
  *
  * Measures three phases separately: (a) DataFrame creation, (b) operation execution, (c) result
  * collection/decode. Reports strongbow time, native time, and overhead ratio.
  *
  * Runs 5 warmup + 10 measured iterations per operation.
  */
class SparkOverheadBench extends AnyFlatSpec with Matchers with SparkTestBase {

  import org.apache.spark.sql.{functions => F, DataFrame, Row}
  import org.apache.spark.sql.types.{
    DoubleType => SparkDoubleType,
    IntegerType,
    StringType => SparkStringType,
    StructField,
    StructType
  }
  import org.apache.spark.sql.expressions.Window

  private val N = 5_000_000
  private val JoinN = 1_000_000
  private val MultiN = 1_000_000
  private val WindowN = 500_000
  private val NumGroups = 1_000
  private val Warmup = 5
  private val Measured = 10

  private val valueExpr: Expr[Int, Int] = Expr.Cell("value", ColumnIndex(0))

  // --- helpers ---

  private case class TimingResult(
    createMs: Double,
    executeMs: Double,
    collectMs: Double,
    totalMs: Double,
    rowCount: Long
  )

  private def median(values: Seq[Double]): Double = {
    val sorted = values.sorted
    val n = sorted.length
    if (n % 2 == 0) (sorted(n / 2 - 1) + sorted(n / 2)) / 2.0
    else sorted(n / 2)
  }

  private def formatResult(label: String, sb: TimingResult, native: TimingResult): Unit = {
    val ratio = if (native.totalMs > 0) sb.totalMs / native.totalMs else Double.PositiveInfinity
    info(f"--- $label ---")
    info(
      f"  Strongbow  : create=${sb.createMs}%8.1f ms  execute=${sb.executeMs}%8.1f ms  collect=${sb.collectMs}%8.1f ms  total=${sb.totalMs}%8.1f ms  rows=${sb.rowCount}"
    )
    info(
      f"  Native     : create=${native.createMs}%8.1f ms  execute=${native.executeMs}%8.1f ms  collect=${native.collectMs}%8.1f ms  total=${native.totalMs}%8.1f ms  rows=${native.rowCount}"
    )
    info(f"  Overhead   : ${ratio}%.2fx")
  }

  private def intDataset(data: Array[Int]): Dataset[Int] = {
    Dataset.fromColumns(Vector(Column.int(data)), Schema.intSchema).toOption.get
  }

  private def nativeIntDf(data: Array[Int]): DataFrame = {
    val structType = StructType(Array(StructField("value", IntegerType, nullable = false)))
    val rows = java.util.Arrays.asList(data.map(Row(_))*)
    spark.createDataFrame(rows, structType)
  }

  private def timeNanos(block: => Any): Long = {
    val start = System.nanoTime()
    block
    System.nanoTime() - start
  }

  // --- multi-column case classes ---

  case class BenchRecord(dept: String, amount: Double, quantity: Int, id: Int)
  given benchRecordSchema: Schema[BenchRecord] = Schema.derived

  case class BenchAggResult(dept: String, totalAmount: Double, cnt: Long)
  given benchAggResultSchema: Schema[BenchAggResult] = Schema.derived

  case class BenchWindowResult(dept: String, amount: Double, quantity: Int, id: Int, rowNum: Int)
  given benchWindowResultSchema: Schema[BenchWindowResult] = Schema.derived

  // Schema.derived column names:
  //   BenchRecord:       dept_value(0), amount_value(1), quantity_value(2), id_value(3)
  //   BenchAggResult:    dept_value(0), totalAmount_value(1), cnt_value(2)
  //   BenchWindowResult: dept_value(0), amount_value(1), quantity_value(2), id_value(3), rowNum_value(4)

  private def benchRecordDataset(n: Int, groups: Int): Dataset[BenchRecord] = {
    val depts = Array.tabulate[String | Null](n)(i => s"dept${i % groups}")
    val amounts = Array.tabulate(n)(i => (i % 1000) + 0.5)
    val quantities = Array.tabulate(n)(i => i % 100)
    val ids = Array.tabulate(n)(identity)
    Dataset
      .fromColumns(
        Vector(Column.string(depts), Column.double(amounts), Column.int(quantities), Column.int(ids)),
        benchRecordSchema
      )
      .toOption
      .get
  }

  private def nativeBenchDf(n: Int, groups: Int): DataFrame = {
    val structType = StructType(
      Array(
        StructField("dept_value", SparkStringType, nullable = false),
        StructField("amount_value", SparkDoubleType, nullable = false),
        StructField("quantity_value", IntegerType, nullable = false),
        StructField("id_value", IntegerType, nullable = false)
      )
    )
    val rows = java.util.Arrays.asList(
      Array.tabulate(n) { i =>
        Row(s"dept${i % groups}", (i % 1000) + 0.5, i % 100, i)
      }*
    )
    spark.createDataFrame(rows, structType)
  }

  // ---------------------------------------------------------------------------
  // Benchmark 1: Filter 5M rows
  // ---------------------------------------------------------------------------

  "Filter overhead" should "be measured for 5M rows" in {
    val threshold = (N * 0.9).toInt
    val data = Array.tabulate(N)(identity)

    val sbTimings = (0 until Warmup + Measured).map { _ =>
      val t0 = System.nanoTime()
      val ds = intDataset(data).filter(
        Expr.Gt(valueExpr, Expr.Const(threshold), summon[Ordering[Int]])
      )
      val t1 = System.nanoTime()
      val result = sparkInterpreter.execute(ds)
      val t2 = System.nanoTime()
      val vec = result.toOption.get.toVectorUnsafe
      val t3 = System.nanoTime()
      TimingResult(
        (t1 - t0) / 1e6,
        (t2 - t1) / 1e6,
        (t3 - t2) / 1e6,
        (t3 - t0) / 1e6,
        vec.size.toLong
      )
    }.drop(Warmup)

    val nativeTimings = (0 until Warmup + Measured).map { _ =>
      val t0 = System.nanoTime()
      val df = nativeIntDf(data)
      val t1 = System.nanoTime()
      val filtered = df.filter(F.col("value") > F.lit(threshold))
      val t2 = System.nanoTime()
      val rows = filtered.collect()
      val t3 = System.nanoTime()
      TimingResult(
        (t1 - t0) / 1e6,
        (t2 - t1) / 1e6,
        (t3 - t2) / 1e6,
        (t3 - t0) / 1e6,
        rows.length.toLong
      )
    }.drop(Warmup)

    val sbMedian = TimingResult(
      median(sbTimings.map(_.createMs)),
      median(sbTimings.map(_.executeMs)),
      median(sbTimings.map(_.collectMs)),
      median(sbTimings.map(_.totalMs)),
      sbTimings.head.rowCount
    )
    val nativeMedian = TimingResult(
      median(nativeTimings.map(_.createMs)),
      median(nativeTimings.map(_.executeMs)),
      median(nativeTimings.map(_.collectMs)),
      median(nativeTimings.map(_.totalMs)),
      nativeTimings.head.rowCount
    )

    formatResult("Filter 5M rows (top 10%)", sbMedian, nativeMedian)
    sbMedian.rowCount shouldBe nativeMedian.rowCount
  }

  // ---------------------------------------------------------------------------
  // Benchmark 2: Distinct on 5M rows
  // ---------------------------------------------------------------------------

  "Distinct overhead" should "be measured for 5M rows" in {
    val data = Array.tabulate(N)(i => i % (N / 2)) // 50% duplicates

    val sbTimings = (0 until Warmup + Measured).map { _ =>
      val t0 = System.nanoTime()
      val ds = intDataset(data).distinct
      val t1 = System.nanoTime()
      val result = sparkInterpreter.execute(ds)
      val t2 = System.nanoTime()
      val vec = result.toOption.get.toVectorUnsafe
      val t3 = System.nanoTime()
      TimingResult(
        (t1 - t0) / 1e6,
        (t2 - t1) / 1e6,
        (t3 - t2) / 1e6,
        (t3 - t0) / 1e6,
        vec.size.toLong
      )
    }.drop(Warmup)

    val nativeTimings = (0 until Warmup + Measured).map { _ =>
      val t0 = System.nanoTime()
      val df = nativeIntDf(data)
      val t1 = System.nanoTime()
      val distinct = df.distinct()
      val t2 = System.nanoTime()
      val rows = distinct.collect()
      val t3 = System.nanoTime()
      TimingResult(
        (t1 - t0) / 1e6,
        (t2 - t1) / 1e6,
        (t3 - t2) / 1e6,
        (t3 - t0) / 1e6,
        rows.length.toLong
      )
    }.drop(Warmup)

    val sbMedian = TimingResult(
      median(sbTimings.map(_.createMs)),
      median(sbTimings.map(_.executeMs)),
      median(sbTimings.map(_.collectMs)),
      median(sbTimings.map(_.totalMs)),
      sbTimings.head.rowCount
    )
    val nativeMedian = TimingResult(
      median(nativeTimings.map(_.createMs)),
      median(nativeTimings.map(_.executeMs)),
      median(nativeTimings.map(_.collectMs)),
      median(nativeTimings.map(_.totalMs)),
      nativeTimings.head.rowCount
    )

    formatResult("Distinct 5M rows (50% dupes)", sbMedian, nativeMedian)
    sbMedian.rowCount shouldBe nativeMedian.rowCount
  }

  // ---------------------------------------------------------------------------
  // Benchmark 3: JoinOn 1M x 1M
  // ---------------------------------------------------------------------------

  "JoinOn overhead" should "be measured for 1M x 1M" in {
    given Schema[(Int, Int)] = Schema.tuple2Schema[Int, Int]

    val half = JoinN / 2
    val data1 = Array.tabulate(JoinN)(identity)
    val data2 = Array.tabulate(JoinN)(i => i + half)

    val leftKey = Expr.Cell[Int, Int]("value", ColumnIndex(0))
    val rightKey = Expr.Cell[Int, Int]("value", ColumnIndex(0))

    val sbTimings = (0 until Warmup + Measured).map { _ =>
      val t0 = System.nanoTime()
      val ds = intDataset(data1).joinOn(
        intDataset(data2),
        leftKey,
        rightKey,
        ColumnType.IntType,
        ColumnType.IntType
      )
      val t1 = System.nanoTime()
      val result = sparkInterpreter.execute(ds)
      val t2 = System.nanoTime()
      val vec = result.toOption.get.toVectorUnsafe
      val t3 = System.nanoTime()
      TimingResult(
        (t1 - t0) / 1e6,
        (t2 - t1) / 1e6,
        (t3 - t2) / 1e6,
        (t3 - t0) / 1e6,
        vec.size.toLong
      )
    }.drop(Warmup)

    val nativeTimings = (0 until Warmup + Measured).map { _ =>
      val t0 = System.nanoTime()
      val df1 = nativeIntDf(data1).alias("_l")
      val df2 = nativeIntDf(data2).alias("_r")
      val t1 = System.nanoTime()
      val joined = df1.join(df2, F.col("_l.value") === F.col("_r.value"))
      val t2 = System.nanoTime()
      val rows = joined.collect()
      val t3 = System.nanoTime()
      TimingResult(
        (t1 - t0) / 1e6,
        (t2 - t1) / 1e6,
        (t3 - t2) / 1e6,
        (t3 - t0) / 1e6,
        rows.length.toLong
      )
    }.drop(Warmup)

    val sbMedian = TimingResult(
      median(sbTimings.map(_.createMs)),
      median(sbTimings.map(_.executeMs)),
      median(sbTimings.map(_.collectMs)),
      median(sbTimings.map(_.totalMs)),
      sbTimings.head.rowCount
    )
    val nativeMedian = TimingResult(
      median(nativeTimings.map(_.createMs)),
      median(nativeTimings.map(_.executeMs)),
      median(nativeTimings.map(_.collectMs)),
      median(nativeTimings.map(_.totalMs)),
      nativeTimings.head.rowCount
    )

    formatResult("JoinOn 1M x 1M (50% overlap)", sbMedian, nativeMedian)
    sbMedian.rowCount shouldBe nativeMedian.rowCount
  }

  // ---------------------------------------------------------------------------
  // Benchmark 4: SelectExprs arithmetic on 5M
  // ---------------------------------------------------------------------------

  "SelectExprs overhead" should "be measured for 5M rows" in {
    val data = Array.tabulate(N)(identity)

    val doubled = Expr
      .Add(
        Expr.Mul(valueExpr, Expr.Const(2)),
        Expr.Const(1)
      )
      .asInstanceOf[Expr[Int, Any]]

    val sbTimings = (0 until Warmup + Measured).map { _ =>
      val t0 = System.nanoTime()
      val ds = intDataset(data).selectAs[Int](("result", doubled, ColumnType.IntType))
      val t1 = System.nanoTime()
      val result = sparkInterpreter.execute(ds)
      val t2 = System.nanoTime()
      val vec = result.toOption.get.toVectorUnsafe
      val t3 = System.nanoTime()
      TimingResult(
        (t1 - t0) / 1e6,
        (t2 - t1) / 1e6,
        (t3 - t2) / 1e6,
        (t3 - t0) / 1e6,
        vec.size.toLong
      )
    }.drop(Warmup)

    val nativeTimings = (0 until Warmup + Measured).map { _ =>
      val t0 = System.nanoTime()
      val df = nativeIntDf(data)
      val t1 = System.nanoTime()
      val selected = df.select((F.col("value") * F.lit(2) + F.lit(1)).as("result"))
      val t2 = System.nanoTime()
      val rows = selected.collect()
      val t3 = System.nanoTime()
      TimingResult(
        (t1 - t0) / 1e6,
        (t2 - t1) / 1e6,
        (t3 - t2) / 1e6,
        (t3 - t0) / 1e6,
        rows.length.toLong
      )
    }.drop(Warmup)

    val sbMedian = TimingResult(
      median(sbTimings.map(_.createMs)),
      median(sbTimings.map(_.executeMs)),
      median(sbTimings.map(_.collectMs)),
      median(sbTimings.map(_.totalMs)),
      sbTimings.head.rowCount
    )
    val nativeMedian = TimingResult(
      median(nativeTimings.map(_.createMs)),
      median(nativeTimings.map(_.executeMs)),
      median(nativeTimings.map(_.collectMs)),
      median(nativeTimings.map(_.totalMs)),
      nativeTimings.head.rowCount
    )

    formatResult("SelectExprs value*2+1 on 5M rows", sbMedian, nativeMedian)
    sbMedian.rowCount shouldBe nativeMedian.rowCount
  }

  // ---------------------------------------------------------------------------
  // Benchmark 5: Intersect 2.5M x 2.5M
  // ---------------------------------------------------------------------------

  "Intersect overhead" should "be measured for 2.5M x 2.5M" in {
    val half = N / 2
    val third = N / 3
    val data1 = Array.tabulate(half)(identity)
    val data2 = Array.tabulate(half)(i => i + third)

    val sbTimings = (0 until Warmup + Measured).map { _ =>
      val t0 = System.nanoTime()
      val ds = intDataset(data1).intersect(intDataset(data2))
      val t1 = System.nanoTime()
      val result = sparkInterpreter.execute(ds)
      val t2 = System.nanoTime()
      val vec = result.toOption.get.toVectorUnsafe
      val t3 = System.nanoTime()
      TimingResult(
        (t1 - t0) / 1e6,
        (t2 - t1) / 1e6,
        (t3 - t2) / 1e6,
        (t3 - t0) / 1e6,
        vec.size.toLong
      )
    }.drop(Warmup)

    val nativeTimings = (0 until Warmup + Measured).map { _ =>
      val t0 = System.nanoTime()
      val df1 = nativeIntDf(data1)
      val df2 = nativeIntDf(data2)
      val t1 = System.nanoTime()
      val intersected = df1.intersect(df2)
      val t2 = System.nanoTime()
      val rows = intersected.collect()
      val t3 = System.nanoTime()
      TimingResult(
        (t1 - t0) / 1e6,
        (t2 - t1) / 1e6,
        (t3 - t2) / 1e6,
        (t3 - t0) / 1e6,
        rows.length.toLong
      )
    }.drop(Warmup)

    val sbMedian = TimingResult(
      median(sbTimings.map(_.createMs)),
      median(sbTimings.map(_.executeMs)),
      median(sbTimings.map(_.collectMs)),
      median(sbTimings.map(_.totalMs)),
      sbTimings.head.rowCount
    )
    val nativeMedian = TimingResult(
      median(nativeTimings.map(_.createMs)),
      median(nativeTimings.map(_.executeMs)),
      median(nativeTimings.map(_.collectMs)),
      median(nativeTimings.map(_.totalMs)),
      nativeTimings.head.rowCount
    )

    formatResult("Intersect 2.5M x 2.5M", sbMedian, nativeMedian)
    sbMedian.rowCount shouldBe nativeMedian.rowCount
  }

  // ---------------------------------------------------------------------------
  // Benchmark 6: End-to-end: filter -> join -> distinct
  // ---------------------------------------------------------------------------

  "End-to-end pipeline overhead" should "be measured for filter -> join -> distinct" in {
    given Schema[(Int, Int)] = Schema.tuple2Schema[Int, Int]

    val pipeN = JoinN
    val half = pipeN / 2
    val threshold = (pipeN * 0.5).toInt
    val data1 = Array.tabulate(pipeN)(identity)
    val data2 = Array.tabulate(pipeN)(i => i + half)

    val leftKey = Expr.Cell[Int, Int]("value", ColumnIndex(0))
    val rightKey = Expr.Cell[Int, Int]("value", ColumnIndex(0))

    val sbTimings = (0 until Warmup + Measured).map { _ =>
      val t0 = System.nanoTime()
      val ds = intDataset(data1)
        .filter(Expr.Gt(valueExpr, Expr.Const(threshold), summon[Ordering[Int]]))
        .joinOn(
          intDataset(data2),
          leftKey,
          rightKey,
          ColumnType.IntType,
          ColumnType.IntType
        )
      val t1 = System.nanoTime()
      val result = sparkInterpreter.execute(ds)
      val t2 = System.nanoTime()
      val vec = result.toOption.get.toVectorUnsafe
      val t3 = System.nanoTime()
      TimingResult(
        (t1 - t0) / 1e6,
        (t2 - t1) / 1e6,
        (t3 - t2) / 1e6,
        (t3 - t0) / 1e6,
        vec.size.toLong
      )
    }.drop(Warmup)

    val nativeTimings = (0 until Warmup + Measured).map { _ =>
      val t0 = System.nanoTime()
      val df1 = nativeIntDf(data1)
        .alias("_l")
        .filter(F.col("value") > F.lit(threshold))
      val df2 = nativeIntDf(data2).alias("_r")
      val t1 = System.nanoTime()
      val joined = df1.join(df2, F.col("_l.value") === F.col("_r.value"))
      val t2 = System.nanoTime()
      val rows = joined.collect()
      val t3 = System.nanoTime()
      TimingResult(
        (t1 - t0) / 1e6,
        (t2 - t1) / 1e6,
        (t3 - t2) / 1e6,
        (t3 - t0) / 1e6,
        rows.length.toLong
      )
    }.drop(Warmup)

    val sbMedian = TimingResult(
      median(sbTimings.map(_.createMs)),
      median(sbTimings.map(_.executeMs)),
      median(sbTimings.map(_.collectMs)),
      median(sbTimings.map(_.totalMs)),
      sbTimings.head.rowCount
    )
    val nativeMedian = TimingResult(
      median(nativeTimings.map(_.createMs)),
      median(nativeTimings.map(_.executeMs)),
      median(nativeTimings.map(_.collectMs)),
      median(nativeTimings.map(_.totalMs)),
      nativeTimings.head.rowCount
    )

    formatResult("End-to-end: filter -> join (1M x 1M)", sbMedian, nativeMedian)
    sbMedian.rowCount shouldBe nativeMedian.rowCount
  }

  // ---------------------------------------------------------------------------
  // Benchmark 7: GroupByAgg (1M rows, 1K groups)
  // ---------------------------------------------------------------------------

  "GroupByAgg overhead" should "be measured for 1M rows with 1K groups" in {
    val deptCell: Expr[BenchRecord, Any] = Expr.Cell("dept_value", ColumnIndex(0))
    val amountCell: Expr[BenchRecord, Double] = Expr.Cell("amount_value", ColumnIndex(1))

    val keys = Vector(
      KeySpec[BenchRecord, Any]("dept", deptCell, ColumnType.StringType)
    )
    val aggs = Vector(
      AggSpec("totalAmount", Expr.SumDouble(amountCell), ColumnType.DoubleType),
      AggSpec("cnt", Expr.Count[BenchRecord](), ColumnType.LongType)
    )

    val sbTimings = (0 until Warmup + Measured).map { _ =>
      val t0 = System.nanoTime()
      val ds = benchRecordDataset(MultiN, NumGroups).groupByAgg[BenchAggResult](keys, aggs)
      val t1 = System.nanoTime()
      val result = sparkInterpreter.execute(ds)
      val t2 = System.nanoTime()
      val vec = result.toOption.get.toVectorUnsafe
      val t3 = System.nanoTime()
      TimingResult(
        (t1 - t0) / 1e6,
        (t2 - t1) / 1e6,
        (t3 - t2) / 1e6,
        (t3 - t0) / 1e6,
        vec.size.toLong
      )
    }.drop(Warmup)

    val nativeTimings = (0 until Warmup + Measured).map { _ =>
      val t0 = System.nanoTime()
      val df = nativeBenchDf(MultiN, NumGroups)
      val t1 = System.nanoTime()
      val grouped = df
        .groupBy(F.col("dept_value"))
        .agg(
          F.sum("amount_value").as("totalAmount"),
          F.count("*").as("cnt")
        )
      val t2 = System.nanoTime()
      val rows = grouped.collect()
      val t3 = System.nanoTime()
      TimingResult(
        (t1 - t0) / 1e6,
        (t2 - t1) / 1e6,
        (t3 - t2) / 1e6,
        (t3 - t0) / 1e6,
        rows.length.toLong
      )
    }.drop(Warmup)

    val sbMedian = TimingResult(
      median(sbTimings.map(_.createMs)),
      median(sbTimings.map(_.executeMs)),
      median(sbTimings.map(_.collectMs)),
      median(sbTimings.map(_.totalMs)),
      sbTimings.head.rowCount
    )
    val nativeMedian = TimingResult(
      median(nativeTimings.map(_.createMs)),
      median(nativeTimings.map(_.executeMs)),
      median(nativeTimings.map(_.collectMs)),
      median(nativeTimings.map(_.totalMs)),
      nativeTimings.head.rowCount
    )

    formatResult("GroupByAgg 1M rows (1K groups)", sbMedian, nativeMedian)
    sbMedian.rowCount shouldBe nativeMedian.rowCount
  }

  // ---------------------------------------------------------------------------
  // Benchmark 8: SortByExprs (1M rows, 2-column sort)
  // ---------------------------------------------------------------------------

  "SortByExprs overhead" should "be measured for 1M rows with 2-column sort" in {
    val quantityCell: Expr[BenchRecord, Int] = Expr.Cell("quantity_value", ColumnIndex(2))
    val idCell: Expr[BenchRecord, Int] = Expr.Cell("id_value", ColumnIndex(3))

    val sortKeys = Vector(
      SortSpec(quantityCell, summon[Ordering[Int]], ColumnType.IntType, true),
      SortSpec(idCell, summon[Ordering[Int]], ColumnType.IntType, false)
    )

    val sbTimings = (0 until Warmup + Measured).map { _ =>
      val t0 = System.nanoTime()
      val ds = benchRecordDataset(MultiN, NumGroups).sortByExprs(sortKeys)
      val t1 = System.nanoTime()
      val result = sparkInterpreter.execute(ds)
      val t2 = System.nanoTime()
      val vec = result.toOption.get.toVectorUnsafe
      val t3 = System.nanoTime()
      TimingResult(
        (t1 - t0) / 1e6,
        (t2 - t1) / 1e6,
        (t3 - t2) / 1e6,
        (t3 - t0) / 1e6,
        vec.size.toLong
      )
    }.drop(Warmup)

    val nativeTimings = (0 until Warmup + Measured).map { _ =>
      val t0 = System.nanoTime()
      val df = nativeBenchDf(MultiN, NumGroups)
      val t1 = System.nanoTime()
      val sorted = df.sort(F.col("quantity_value").asc, F.col("id_value").desc)
      val t2 = System.nanoTime()
      val rows = sorted.collect()
      val t3 = System.nanoTime()
      TimingResult(
        (t1 - t0) / 1e6,
        (t2 - t1) / 1e6,
        (t3 - t2) / 1e6,
        (t3 - t0) / 1e6,
        rows.length.toLong
      )
    }.drop(Warmup)

    val sbMedian = TimingResult(
      median(sbTimings.map(_.createMs)),
      median(sbTimings.map(_.executeMs)),
      median(sbTimings.map(_.collectMs)),
      median(sbTimings.map(_.totalMs)),
      sbTimings.head.rowCount
    )
    val nativeMedian = TimingResult(
      median(nativeTimings.map(_.createMs)),
      median(nativeTimings.map(_.executeMs)),
      median(nativeTimings.map(_.collectMs)),
      median(nativeTimings.map(_.totalMs)),
      nativeTimings.head.rowCount
    )

    formatResult("SortByExprs 1M rows (2-column sort)", sbMedian, nativeMedian)
    sbMedian.rowCount shouldBe nativeMedian.rowCount
  }

  // ---------------------------------------------------------------------------
  // Benchmark 9: WithWindow (500K rows, ROW_NUMBER)
  // ---------------------------------------------------------------------------

  "WithWindow overhead" should "be measured for 500K rows with ROW_NUMBER" in {
    val deptCell: Expr[BenchRecord, Any] = Expr.Cell("dept_value", ColumnIndex(0))
    val amountCell: Expr[BenchRecord, Double] = Expr.Cell("amount_value", ColumnIndex(1))

    val windowSpec = WindowSpec[BenchRecord](
      partitionBy = Vector(
        KeySpec[BenchRecord, Any]("dept", deptCell, ColumnType.StringType)
      ),
      orderBy = Vector(
        SortSpec[BenchRecord, Double](amountCell, summon[Ordering[Double]], ColumnType.DoubleType, false)
      )
    )

    val windowExprs = Vector(
      WindowExprSpec("rowNum", Expr.RowNumber[BenchRecord](), ColumnType.IntType)
    )

    val sbTimings = (0 until Warmup + Measured).map { _ =>
      val t0 = System.nanoTime()
      val ds = benchRecordDataset(WindowN, NumGroups).withWindow[BenchWindowResult](windowExprs, windowSpec)
      val t1 = System.nanoTime()
      val result = sparkInterpreter.execute(ds)
      val t2 = System.nanoTime()
      val vec = result.toOption.get.toVectorUnsafe
      val t3 = System.nanoTime()
      TimingResult(
        (t1 - t0) / 1e6,
        (t2 - t1) / 1e6,
        (t3 - t2) / 1e6,
        (t3 - t0) / 1e6,
        vec.size.toLong
      )
    }.drop(Warmup)

    val nativeTimings = (0 until Warmup + Measured).map { _ =>
      val t0 = System.nanoTime()
      val df = nativeBenchDf(WindowN, NumGroups)
      val t1 = System.nanoTime()
      val windowed = df.withColumn(
        "rowNum",
        F.row_number().over(Window.partitionBy("dept_value").orderBy(F.col("amount_value").desc))
      )
      val t2 = System.nanoTime()
      val rows = windowed.collect()
      val t3 = System.nanoTime()
      TimingResult(
        (t1 - t0) / 1e6,
        (t2 - t1) / 1e6,
        (t3 - t2) / 1e6,
        (t3 - t0) / 1e6,
        rows.length.toLong
      )
    }.drop(Warmup)

    val sbMedian = TimingResult(
      median(sbTimings.map(_.createMs)),
      median(sbTimings.map(_.executeMs)),
      median(sbTimings.map(_.collectMs)),
      median(sbTimings.map(_.totalMs)),
      sbTimings.head.rowCount
    )
    val nativeMedian = TimingResult(
      median(nativeTimings.map(_.createMs)),
      median(nativeTimings.map(_.executeMs)),
      median(nativeTimings.map(_.collectMs)),
      median(nativeTimings.map(_.totalMs)),
      nativeTimings.head.rowCount
    )

    formatResult("WithWindow 500K rows (ROW_NUMBER, 1K groups)", sbMedian, nativeMedian)
    sbMedian.rowCount shouldBe nativeMedian.rowCount
  }
}
