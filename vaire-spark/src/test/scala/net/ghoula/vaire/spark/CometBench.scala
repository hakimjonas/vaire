package net.ghoula.vaire.spark

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.execution.ExplainMode

import net.ghoula.vaire.prelude.*
import net.ghoula.vaire.types.{DayTimeInterval, Timestamp}

import scala.collection.mutable

/** Comet acceleration benchmark for Vairë's Spark backend.
  *
  * Runs a fixed set of Vairë plans on one Spark distribution and reports, per plan and per input
  * size, the measured `collect()` samples with their min/median/max, the row count, a SHA-256
  * digest of the collected rows (canonically encoded, sorted), and the `Comet*` operator names
  * found in the physical plan. The same object is submitted twice by `scripts/comet-bench.sh`: once
  * on plain Spark and once with the Apache DataFusion Comet plugin on the classpath. The object
  * contains no Comet reference, so the control run is genuinely Comet-free; Comet is switched on
  * entirely from the submit command.
  *
  * The source data is generated once, written to Parquet under `-Dvaire.bench.dir` (default
  * `/tmp/vaire-comet`), and read back with `spark.read.parquet`, so every measured plan starts at a
  * real file scan. Seeding through `Dataset.fromColumns`/`DataFrameBuilder.fromMaterialized` would
  * produce a `LocalRelation` (a `LocalTableScanExec`), which Comet disables by default and which
  * would take the scan out of the comparison before anything the harness means to measure. The
  * Parquet reader reports every column nullable, so each scan is read into a nullable mirror schema
  * and narrowed to the non-optional one the plans use.
  *
  * Before the first reported scale block, [[runBlockWarmup]] runs the same distributed plan set
  * once at a small scale and prints it under `##WARMUP`; the results are discarded. The per-plan
  * warmups inside [[runPlan]] absorb per-iteration cost, but they do not absorb a fixed penalty
  * that attaches to the first scale block in the JVM, which is why the first block used to read
  * high. The warmup block is timing-only: it changes no digest, row count, or operator inventory,
  * and its lines carry a prefix the extraction does not match.
  *
  * Two plans probe the semantics rather than the throughput. `time-bucket` uses `time_bucket` over
  * a timestamp column, one of the Spark 4.2-only functions Vairë calls, so it fails on 4.1.3 and is
  * the check that the container really runs 4.2.0. `window` orders by a non-unique key on purpose:
  * `row_number` is then plan-dependent among rows tied on that key, so its digest covers only the
  * tie-invariant `(key, value, rnk)` multiset and a separate check asserts that `rowNum` is a
  * permutation of `1..n` within each partition.
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
  private val BlockWarmupScale = 200_000L
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

  /** Nullable mirror of [[Fact]] for the Parquet scan; `narrow` reinterprets it as [[Fact]]. */
  case class FactSource(key: Option[Long], value: Option[Int], id: Option[Long])
  given factSourceSchema: Schema[FactSource] = Schema.derived

  case class Dim(key: Long, weight: Int)
  given dimSchema: Schema[Dim] = Schema.derived

  /** Nullable mirror of [[Dim]] for the Parquet scan; `narrow` reinterprets it as [[Dim]]. */
  case class DimSource(key: Option[Long], weight: Option[Int])
  given dimSourceSchema: Schema[DimSource] = Schema.derived

  /** Timestamp source for the 4.2-only `time_bucket` plan. */
  case class Clockin(key: Long, ts: Timestamp)
  given clockinSchema: Schema[Clockin] = Schema.derived

  /** Nullable mirror of [[Clockin]] for the Parquet scan; `narrow` reinterprets it as [[Clockin]].
    */
  case class ClockinSource(key: Option[Long], ts: Option[Timestamp])
  given clockinSourceSchema: Schema[ClockinSource] = Schema.derived

  case class Bucketed(key: Long, bucket: Timestamp)
  given bucketedSchema: Schema[Bucketed] = Schema.derived

  case class Grouped(key: Long, total: Long, cnt: Long)
  given groupedSchema: Schema[Grouped] = Schema.derived

  case class GlobalAgg(total: Long, cnt: Long)
  given globalAggSchema: Schema[GlobalAgg] = Schema.derived

  case class Windowed(key: Long, value: Int, id: Long, rowNum: Int, rnk: Int)
  given windowedSchema: Schema[Windowed] = Schema.derived

  case class Mapped(key: Long, doubled: Int, id: Long)
  given mappedSchema: Schema[Mapped] = Schema.derived

  /** A distributed plan and the extra arguments its measurement needs. */
  private final case class DistributedPlan(
    id: String,
    build: () => DataFrame,
    project: Array[Row] => Array[Row] = rows => rows,
    check: Array[Row] => Option[String] = _ => None,
    asserted: Boolean = false
  )

  /** The distributed plan set, shared so the warmup block and the reported blocks run the same
    * code.
    */
  private def distributedPlans(bench: Bench): Vector[DistributedPlan] = Vector(
    DistributedPlan("filter-groupby", () => bench.filterGroupBy),
    DistributedPlan("join-on", () => bench.joinOn),
    DistributedPlan("window", () => bench.window, windowDigestRows, rowNumberPermutation, asserted = true),
    DistributedPlan("sort-limit", () => bench.sortLimit),
    DistributedPlan("distinct", () => bench.distinct),
    DistributedPlan("aggregate", () => bench.aggregate),
    DistributedPlan("time-bucket", () => bench.timeBucket)
  )

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
        s"##DATA dir=$root groups=$NumGroups fact_scales=${FactScales.mkString(",")} driver_scale=$DriverCollectScale block_warmup_scale=$BlockWarmupScale"
      )

      runBlockWarmup(spark, root)

      FactScales.foreach { scale =>
        val bench = new Bench(spark, root, scale)
        distributedPlans(bench).foreach(p => runPlan(p.id, scale, p.build, p.project, p.check, p.asserted))
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
      // Route join-on through a shuffle instead of a broadcast: the 1000-row dimension sits well
      // under the 10 MB default threshold, so the default plan would exercise
      // CometBroadcastExchange rather than CometNativeShuffle. A negative threshold disables
      // broadcast joins for every plan.
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
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

  /** Generate the Parquet datasets, then expose the plans over them. */
  private final class Bench(spark: SparkSession, root: String, scale: Long) {

    private val factPath = ensureFact(spark, root, scale, NumGroups)
    private val dimPath = ensureDim(spark, root, NumGroups)
    private val clockPath = ensureClock(spark, root, scale, NumGroups)
    private val interp = SparkInterpreter(spark)
    // The Parquet reader reports every column nullable. Read into the nullable mirror schema and
    // `narrow` to the declared one; the generated data holds no nulls, so the narrow check passes.
    private val fact: Dataset[Fact] =
      SparkDatasets.fromDataFrame(spark.read.parquet(factPath), factSourceSchema).narrow[Fact]
    private val dim: Dataset[Dim] =
      SparkDatasets.fromDataFrame(spark.read.parquet(dimPath), dimSourceSchema).narrow[Dim]
    private val clock: Dataset[Clockin] =
      SparkDatasets.fromDataFrame(spark.read.parquet(clockPath), clockinSourceSchema).narrow[Clockin]

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
          .groupByAgg[Grouped](Vector(KeySpec("key", keyCell, ColumnType.LongType)), sumAndCount)
      )

    def joinOn: DataFrame =
      df(fact.joinOn(dim, keyCell, dimKeyCell, ColumnType.LongType, ColumnType.LongType))

    def window: DataFrame = {
      // `value` alone leaves ties within a partition, so `row_number` is not stable across
      // physical plans. The digest drops `rowNum` (see `windowDigestRows`) and `rowNumberPermutation`
      // checks the invariant it still has to satisfy.
      val spec = WindowSpec[Fact](
        partitionBy = Vector(KeySpec("key", keyCell, ColumnType.LongType)),
        orderBy = Vector(SortSpec[Fact, Int](valueCell, summon[Ordering[Int]], ColumnType.IntType, true))
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

    def timeBucket: DataFrame = {
      val clockKey: Expr[Clockin, Long] = Expr.Cell("key", ColumnIndex(0))
      val tsCell: Expr[Clockin, Timestamp] = Expr.Cell("ts", ColumnIndex(1))
      val bucket = Expr.timeBucket[Clockin](
        Expr.const(DayTimeInterval.ofMicros(15L * 60L * 1000000L)),
        tsCell,
        Expr.const(Timestamp.ofEpochMicro(0L))
      )
      df(
        clock.selectAs[Bucketed](
          ("key", clockKey, ColumnType.LongType),
          ("bucket", bucket.asInstanceOf[Expr[Clockin, Any]], ColumnType.TimestampType)
        )
      )
    }

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
        .select(F.col("id").as("key"), (F.col("id") % F.lit(7L)).cast("int").as("weight"))
        .write
        .mode("errorifexists")
        .parquet(path)
    }
    path
  }

  private def ensureClock(spark: SparkSession, root: String, rows: Long, groups: Int): String = {
    val path = s"$root/clock-$rows-$groups"
    if (!new java.io.File(path).exists()) {
      val base = spark.range(0L, rows).toDF("id")
      base
        .select(
          (F.col("id") % F.lit(groups.toLong)).as("key"),
          // Milliseconds within one day, so `time_bucket` has a bounded domain to floor.
          F.timestamp_micros(F.col("id") % F.lit(86_400_000_000L)).as("ts")
        )
        .write
        .mode("errorifexists")
        .parquet(path)
    }
    path
  }

  /** Pay the JVM's first-block cost at a small scale, before any reported block. Timing only: the
    * results are discarded and the lines carry a prefix the extraction ignores.
    */
  private def runBlockWarmup(spark: SparkSession, root: String): Unit = {
    val bench = new Bench(spark, root, BlockWarmupScale)
    distributedPlans(bench).foreach { p =>
      val df = p.build()
      val t0 = System.nanoTime()
      val rows = df.collect()
      val t1 = System.nanoTime()
      cometOperators(df) // warm plan inspection too; not reported
      println(
        s"##WARMUP plan=${p.id} scale=$BlockWarmupScale ms=${fmtMs((t1 - t0) / 1e6)} rows=${rows.length}"
      )
    }
  }

  /** `(key, value, rnk)` — invariant under the tie order `row_number` assigns. */
  private def windowDigestRows(rows: Array[Row]): Array[Row] =
    rows.map(r => Row(r.getLong(0), r.getInt(1), r.getInt(4)))

  /** `rowNum` must be a permutation of `1..n` within each `key` partition. */
  private def rowNumberPermutation(rows: Array[Row]): Option[String] = {
    val byKey = mutable.HashMap.empty[Long, mutable.ArrayBuffer[Int]]
    var i = 0
    while (i < rows.length) {
      byKey.getOrElseUpdate(rows(i).getLong(0), mutable.ArrayBuffer.empty) += rows(i).getInt(3)
      i += 1
    }
    byKey.iterator.collectFirst {
      case (key, nums) if nums.sorted != (1 to nums.length).toVector =>
        s"key=$key rowNum is not a permutation of 1..${nums.length}"
    }
  }

  private def fmtMs(value: Double): String =
    "%.1f".formatLocal(java.util.Locale.ROOT, value)

  private def runPlan(
    id: String,
    scale: Long,
    plan: () => DataFrame,
    project: Array[Row] => Array[Row] = rows => rows,
    check: Array[Row] => Option[String] = _ => None,
    asserted: Boolean = false
  ): Unit = {
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
    val rows = lastRows.nn
    val measured = samples.drop(Warmup)
    val sorted = measured.sorted
    val digest = digestOf(project(rows))
    val ops = cometOperators(df)
    val opText = if (ops.isEmpty) "-" else ops.mkString(",")
    println(
      s"##RESULT plan=$id scale=$scale n=${measured.length} min_ms=${fmtMs(sorted.head)} median_ms=${fmtMs(sorted(sorted.length / 2))} max_ms=${fmtMs(sorted.last)} samples=${measured.map(fmtMs).mkString(",")} rows=${rows.length} digest=$digest comet_ops=$opText"
    )
    if (asserted) {
      check(rows) match {
        case Some(message) => println(s"##CHECK plan=$id scale=$scale asserted=true ok=false message=$message")
        case None => println(s"##CHECK plan=$id scale=$scale asserted=true ok=true")
      }
    } else {
      println(s"##CHECK plan=$id scale=$scale asserted=false ok=true")
    }
    if (ops.isEmpty) {
      println(s"##EXPLAIN plan=$id scale=$scale")
      println(physicalPlan(df).replace("\n", "\n##  "))
    }
  }

  private def digestOf(rows: Array[Row]): String = {
    val canonical = rows.iterator.map(canonicalRow).toVector.sorted
    val md = MessageDigest.getInstance("SHA-256")
    var i = 0
    while (i < canonical.length) {
      md.update(canonical(i).getBytes(StandardCharsets.UTF_8))
      md.update(0x0a.toByte)
      i += 1
    }
    md.digest().map(b => f"${b & 0xff}%02x").mkString
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
