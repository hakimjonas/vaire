package net.ghoula.vaire.spark

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.execution.ExplainMode

import net.ghoula.vaire.prelude.*

/** Comet acceleration benchmark for Vairë's Spark backend.
  *
  * Runs a fixed set of Vairë plans on one Spark distribution and reports, per plan and per input
  * size, the median wall-clock of a `collect()`, the row count, a SHA-256 digest of the collected
  * rows (canonically encoded, sorted), and the `Comet*` operator names found in the physical plan.
  * The same object is submitted twice by `scripts/comet-bench.sh`: once on plain Spark and once
  * with the Apache DataFusion Comet plugin on the classpath. The object contains no Comet
  * reference, so the control run is genuinely Comet-free; Comet is switched on entirely from the
  * submit command.
  *
  * The source data is generated once, written to Parquet under `-Dvaire.bench.dir` (default
  * `/tmp/vaire-comet`), and read back with `spark.read.parquet`, so every measured plan starts at a
  * real file scan. Seeding through `Dataset.fromColumns`/`DataFrameBuilder.fromMaterialized` would
  * produce a `LocalRelation` (a `LocalTableScanExec`), which Comet disables by default and which
  * would take the scan out of the comparison before anything the harness means to measure. The
  * Parquet reader reports every column nullable, so the scan is read into a nullable mirror schema
  * and narrowed to the non-optional one the plans use.
  *
  * The two driver-collecting plans (Map + groupByAgg, and the predicate join) run on a reduced fact
  * table: both rebuild the frame on the driver, and the predicate join is O(rows x dimension) by
  * construction (`SparkInterpreter.applyInnerJoin`), so a full 5M-row fact would not finish within
  * the fork's 4G heap and budget. Their role here is to show which paths bypass Comet, not to time
  * them at scale.
  */
object CometBench {

  private val Warmup = 3
  private val Measured = 7
  private val NumGroups = 1000
  private val ShufflePartitions = 8
  private val LimitRows = 1_000
  private val FactScales: Vector[Long] =
    csvProp("vaire.bench.fact.sizes", Vector(1_000_000L, 5_000_000L))
  private val DriverCollectScale: Long =
    csvProp("vaire.bench.driver.scale", Vector(200_000L)).head

  /** Parse a comma-separated list of sizes from a system property, for smoke runs. The defaults are
    * the sizes the harness reports.
    */
  private def csvProp(key: String, default: Vector[Long]): Vector[Long] =
    sys.props
      .get(key)
      .map(_.split(",").iterator.flatMap(_.trim.toLongOption).toVector)
      .filter(_.nonEmpty)
      .getOrElse(default)

  case class Fact(key: Long, value: Int, id: Long)
  given factSchema: Schema[Fact] = Schema.derived

  case class Dim(key: Long, weight: Int)
  given dimSchema: Schema[Dim] = Schema.derived

  /** Nullable mirror of [[Fact]] for the Parquet scan; `narrow` reinterprets it as [[Fact]]. */
  case class FactSource(key: Option[Long], value: Option[Int], id: Option[Long])
  given factSourceSchema: Schema[FactSource] = Schema.derived

  /** Nullable mirror of [[Dim]] for the Parquet scan; `narrow` reinterprets it as [[Dim]]. */
  case class DimSource(key: Option[Long], weight: Option[Int])
  given dimSourceSchema: Schema[DimSource] = Schema.derived

  case class Grouped(key: Long, total: Long, cnt: Long)
  given groupedSchema: Schema[Grouped] = Schema.derived

  case class GlobalAgg(total: Long, cnt: Long)
  given globalAggSchema: Schema[GlobalAgg] = Schema.derived

  case class Windowed(key: Long, value: Int, id: Long, rowNum: Int, rnk: Int)
  given windowedSchema: Schema[Windowed] = Schema.derived

  case class Mapped(key: Long, doubled: Int, id: Long)
  given mappedSchema: Schema[Mapped] = Schema.derived

  def main(args: Array[String]): Unit = {
    val root = sys.props.get("vaire.bench.dir").getOrElse("/tmp/vaire-comet")
    val spark = newSession()
    try {
      val comet = spark.conf.get("spark.comet.version", "<not enabled>")
      val heapMb = Runtime.getRuntime.maxMemory / (1024L * 1024L)
      println(
        s"##VERSION spark=${spark.version} comet=$comet shuffle_partitions=$ShufflePartitions max_heap_mb=$heapMb"
      )
      println(
        s"##DATA dir=$root groups=$NumGroups fact_scales=${FactScales.mkString(",")} driver_scale=$DriverCollectScale"
      )

      FactScales.foreach { scale =>
        val bench = new Bench(spark, root, scale)
        runPlan("filter-groupby", scale, () => bench.filterGroupBy)
        runPlan("join-on", scale, () => bench.joinOn)
        runPlan("window", scale, () => bench.window)
        runPlan("sort-limit", scale, () => bench.sortLimit)
        runPlan("distinct", scale, () => bench.distinct)
        runPlan("aggregate", scale, () => bench.aggregate)
      }

      val bench = new Bench(spark, root, DriverCollectScale)
      runPlan("map-groupby", DriverCollectScale, () => bench.mapGroupBy)
      runPlan("predicate-join", DriverCollectScale, () => bench.predicateJoin)
    } finally {
      spark.stop()
    }
  }

  private def newSession(): SparkSession = {
    val session = SparkSession
      .builder()
      .appName("vaire-comet-bench")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", ShufflePartitions.toString)
      // Spark 4.x artifact session isolation breaks Janino during whole-stage codegen in local
      // mode; see SparkTestSession for the same setting and rationale.
      .config("spark.sql.artifact.isolation.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.shuffle.compress", "false")
      .config("spark.shuffle.spill.compress", "false")
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    session
  }

  /** Generate the fact and dimension Parquet datasets, then expose the plans over them. */
  private final class Bench(spark: SparkSession, root: String, scale: Long) {

    private val factPath = ensureFact(spark, root, scale, NumGroups)
    private val dimPath = ensureDim(spark, root, NumGroups)
    private val interp = SparkInterpreter(spark)
    // The Parquet reader reports every column nullable. Read into the nullable mirror schema and
    // `narrow` to the declared one; the generated data holds no nulls, so the narrow check passes.
    private val fact: Dataset[Fact] =
      SparkDatasets
        .fromDataFrame(spark.read.parquet(factPath), factSourceSchema)
        .narrow[Fact]
    private val dim: Dataset[Dim] =
      SparkDatasets
        .fromDataFrame(spark.read.parquet(dimPath), dimSourceSchema)
        .narrow[Dim]

    private val keyCell: Expr[Fact, Long] = Expr.Cell("key", ColumnIndex(0))
    private val valueCell: Expr[Fact, Int] = Expr.Cell("value", ColumnIndex(1))
    private val idCell: Expr[Fact, Long] = Expr.Cell("id", ColumnIndex(2))
    private val dimKeyCell: Expr[Dim, Long] = Expr.Cell("key", ColumnIndex(0))

    private def df[T](ds: Dataset[T]): DataFrame =
      interp.toDataFrame(ds) match {
        case Right(d) => d
        case Left(e) => sys.error(s"plan build failed: $e")
      }

    private def sumAndCount: Vector[AggSpec[Fact]] = Vector(
      AggSpec("total", Expr.Sum(valueCell), ColumnType.LongType),
      AggSpec("cnt", Expr.Count[Fact](), ColumnType.LongType)
    )

    def filterGroupBy: DataFrame =
      df(
        fact
          .where(_.value > 500)
          .groupByAgg[Grouped](
            Vector(KeySpec("key", keyCell, ColumnType.LongType)),
            sumAndCount
          )
      )

    def joinOn: DataFrame =
      df(fact.joinOn(dim, keyCell, dimKeyCell, ColumnType.LongType, ColumnType.LongType))

    def window: DataFrame = {
      val spec = WindowSpec[Fact](
        partitionBy = Vector(KeySpec("key", keyCell, ColumnType.LongType)),
        // `row_number()` is only deterministic under a total order. `value` alone leaves ties
        // within a partition, and their positions differ between physical plans; the unique `id`
        // breaks ties so the result — and its digest — is stable across the control and Comet runs.
        orderBy = Vector(
          SortSpec[Fact, Int](valueCell, summon[Ordering[Int]], ColumnType.IntType, true),
          SortSpec[Fact, Long](idCell, summon[Ordering[Long]], ColumnType.LongType, true)
        )
      )
      val exprs = Vector(
        WindowExprSpec("rowNum", Expr.RowNumber[Fact](), ColumnType.IntType),
        WindowExprSpec("rnk", Expr.Rank[Fact](), ColumnType.IntType)
      )
      df(fact.withWindow[Windowed](exprs, spec))
    }

    def sortLimit: DataFrame = {
      val keys = Vector(
        SortSpec[Fact, Int](valueCell, summon[Ordering[Int]], ColumnType.IntType, true),
        SortSpec[Fact, Long](idCell, summon[Ordering[Long]], ColumnType.LongType, false)
      )
      df(fact.sortByExprs(keys).limit(LimitRows))
    }

    def distinct: DataFrame = df(fact.distinct)

    def aggregate: DataFrame =
      df(fact.aggregate[GlobalAgg](sumAndCount))

    def mapGroupBy: DataFrame = {
      val mapped = fact.map(f => Mapped(f.key, f.value * 2, f.id))
      val mappedKey: Expr[Mapped, Long] = Expr.Cell("key", ColumnIndex(0))
      val doubledCell: Expr[Mapped, Int] = Expr.Cell("doubled", ColumnIndex(1))
      val aggs = Vector(
        AggSpec("total", Expr.Sum(doubledCell), ColumnType.LongType),
        AggSpec("cnt", Expr.Count[Mapped](), ColumnType.LongType)
      )
      df(mapped.groupByAgg[Grouped](Vector(KeySpec("key", mappedKey, ColumnType.LongType)), aggs))
    }

    def predicateJoin: DataFrame =
      df(fact.join(dim, (f, d) => f.key == d.key && f.value > d.weight))
  }

  private def ensureFact(spark: SparkSession, root: String, rows: Long, groups: Int): String = {
    val path = s"$root/fact-$rows-$groups"
    if (!new java.io.File(path).exists()) {
      val base = spark.range(0L, rows).toDF("id")
      base
        .select(
          (F.col("id") % F.lit(groups.toLong)).as("key"),
          ((F.col("id") * F.lit(2654435761L)) % F.lit(1000L)).cast("int").as("value"),
          F.col("id").as("id")
        )
        .write
        .mode("errorifexists")
        .parquet(path)
    }
    path
  }

  private def ensureDim(spark: SparkSession, root: String, groups: Int): String = {
    val path = s"$root/dim-$groups"
    if (!new java.io.File(path).exists()) {
      val base = spark.range(0L, groups.toLong).toDF("id")
      base
        .select(
          F.col("id").as("key"),
          (F.col("id") % F.lit(7L)).cast("int").as("weight")
        )
        .write
        .mode("errorifexists")
        .parquet(path)
    }
    path
  }

  private def runPlan(id: String, scale: Long, plan: () => DataFrame): Unit = {
    val df = plan()
    var lastRows: Array[Row] | Null = null
    val samples = new Array[Double](Warmup + Measured)
    var i = 0
    while (i < Warmup + Measured) {
      lastRows = null // release the previous run's result before collecting the next
      val t0 = System.nanoTime()
      val collected = df.collect()
      val t1 = System.nanoTime()
      samples(i) = (t1 - t0) / 1e6
      lastRows = collected
      i += 1
    }
    val median = samples.drop(Warmup).sorted.apply(Measured / 2)
    val (count, digest) = rowsDigest(lastRows.nn)
    val ops = cometOperators(df)
    val opText = if (ops.isEmpty) "-" else ops.mkString(",")
    val medianText = "%.1f".formatLocal(java.util.Locale.ROOT, median)
    println(
      s"##RESULT plan=$id scale=$scale median_ms=$medianText rows=$count digest=$digest comet_ops=$opText"
    )
    if (ops.isEmpty) {
      println(s"##EXPLAIN plan=$id scale=$scale")
      println(physicalPlan(df).replace("\n", "\n##  "))
    }
  }

  private def rowsDigest(rows: Array[Row]): (Long, String) = {
    val canonical = rows.iterator.map(canonicalRow).toVector.sorted
    val md = MessageDigest.getInstance("SHA-256")
    var i = 0
    while (i < canonical.length) {
      md.update(canonical(i).getBytes(StandardCharsets.UTF_8))
      md.update(0x0a.toByte)
      i += 1
    }
    val hex = md.digest().map(b => f"${b & 0xff}%02x").mkString
    (rows.length.toLong, hex)
  }

  private def canonicalRow(row: Row): String = {
    val sb = new StringBuilder
    var i = 0
    while (i < row.length) {
      if (i > 0) sb.append('\u0001')
      sb.append(canonicalValue(row.get(i)))
      i += 1
    }
    sb.toString
  }

  private def canonicalValue(value: Any): String = value match {
    case null => "\u0000N"
    case r: Row => "\u0002" + canonicalRow(r)
    case other => other.toString
  }

  private def physicalPlan(df: DataFrame): String =
    df.queryExecution.explainString(ExplainMode.fromString("extended"))

  private def cometOperators(df: DataFrame): Vector[String] =
    "Comet\\w+".r.findAllIn(physicalPlan(df)).toVector.distinct.sorted
}
