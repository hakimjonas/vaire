package net.ghoula.vaire.spark

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.execution.ExplainMode

import net.ghoula.vaire.prelude.*
import net.ghoula.vaire.types.{DayTimeInterval, Decimal, Timestamp}

import scala.collection.mutable

/** Comet acceleration benchmark for Vairë's Spark backend.
  *
  * Runs a fixed set of Vairë plans on one Spark distribution and reports, per plan and per input
  * size, two measured actions with their min/median/max and samples: `collect_ms`, the wall-clock
  * of `DataFrame.collect()`, and `exec_ms`, the wall-clock of a `noop` write that forces the whole
  * plan without materialising the result on the driver. The off/on ratio uses `exec_ms`;
  * `collect_ms` is kept because the digest, row count and window check come from the collected
  * rows. [[cometOperators]] is read from `df.queryExecution`, the read plan, while `exec_ms` times
  * the separate `noop` write whose child is that read plan, so the operator inventory describes the
  * read plan and the timing the write plan.
  *
  * The same object is submitted eight times by `scripts/comet-bench.sh`: twice on plain Spark and
  * twice with the Apache DataFusion Comet plugin on the classpath, each pair at the property's
  * fact-scale order and at its reverse (the order-effect test pools the two repeats). The object
  * contains no Comet reference, so the control runs are genuinely Comet-free; Comet is switched on
  * entirely from the submit command.
  *
  * The source data is generated once, written to Parquet under `-Dvaire.bench.dir` (default
  * `/tmp/vaire-comet`), and read back with `spark.read.parquet`, so every measured plan starts at a
  * real file scan. The Parquet reader reports every column nullable, so each scan is read into a
  * nullable mirror schema and narrowed to the non-optional one the plans use.
  *
  * Before the first reported block, and before each reported scale block, [[runWarmup]] runs that
  * block's plan set once and prints it under `##WARMUP`; the results are discarded. The per-plan
  * warmups inside [[runPlan]] absorb per-iteration cost, but they do not absorb the first-block
  * cost, which is why the warmup blocks exist.
  *
  * Each reported plan is bracketed by `##PLAN-BEGIN`/`##PLAN-END` so `scripts/comet-bench.sh` can
  * window the driver log and harvest Comet's `[COMET: ...]` fallback warnings into `##FALLBACK`.
  *
  * Two plans probe the semantics rather than the throughput. `time-bucket` uses `time_bucket` over
  * a timestamp column, one of the Spark 4.2-only functions Vairë calls, so it fails on 4.1.3 and is
  * the check that the container really runs 4.2.0; it also leaves work on Spark, which validates
  * the fallback harvesting. `window` orders by a non-unique key on purpose: `row_number` is then
  * plan-dependent among rows tied on that key, so its digest covers only the tie-invariant
  * `(key, value, rnk)` multiset and a separate check asserts that `rowNum` is a permutation of
  * `1..n` within each partition.
  *
  * Every distributed shape runs twice: once over the narrow `Fact` table and once over the wide
  * table, which adds a low-cardinality string, a unique string, a nullable decimal, a nested
  * `Seq[String]`, a boolean, and a timestamp. Three wide-only plans exercise the new types: a
  * string filter-and-group, a nullable-decimal aggregate, and a nested-list transform. Wide rows
  * carry the `wide-` prefix, so the effect of data shape is read off the same tables.
  *
  * The two driver-collecting plans (Map + groupByAgg, and the predicate join) run on a reduced fact
  * table: both rebuild the frame on the driver, and the predicate join is O(rows x dimension) by
  * construction (`SparkInterpreter.applyInnerJoin`). Their role here is to show which paths bypass
  * Comet, not to time them at scale.
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

  /** Precision and scale for the wide table's `amount` column. Precision ≤ 18 keeps the column an
    * unscaled `Array[Long]` on both Vairë and Spark. It is scoped to this object so the derived
    * schemas below resolve it ahead of the companion's default `Schema[Decimal]`.
    */
  given wideDecimalSchema: Schema[Decimal] = Schema.decimalWith(12, 2)

  /** Wide fact table: the narrow [[Fact]] columns plus the new data shapes. `ts` is carried so the
    * `time-bucket` shape runs over the same table; `name` is low-cardinality, `label`
    * high-cardinality, `tags` is Vairë's `Seq[A]` array (there is no `Array[A]` schema), `amount`
    * is nullable so its null path is exercised, and `flag` is a boolean.
    */
  case class Wide(
    key: Long,
    value: Int,
    id: Long,
    ts: Timestamp,
    name: String,
    label: String,
    amount: Option[Decimal],
    tags: Seq[String],
    flag: Boolean
  )
  given wideSchema: Schema[Wide] = Schema.derived

  /** Nullable mirror of [[Wide]] for the Parquet scan; `narrow` reinterprets it as [[Wide]]. */
  case class WideSource(
    key: Option[Long],
    value: Option[Int],
    id: Option[Long],
    ts: Option[Timestamp],
    name: Option[String],
    label: Option[String],
    amount: Option[Decimal],
    tags: Option[Seq[String]],
    flag: Option[Boolean]
  )
  given wideSourceSchema: Schema[WideSource] = Schema.derived

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

  /** Output of the wide string filter-and-group plan. */
  case class NameGrouped(name: String, cnt: Long)
  given nameGroupedSchema: Schema[NameGrouped] = Schema.derived

  /** Output of the wide nullable-decimal aggregate; `max`/`min` are `None` for an all-null group.
    */
  case class DecimalAgg(maxAmount: Option[Decimal], minAmount: Option[Decimal], cnt: Long)
  given decimalAggSchema: Schema[DecimalAgg] = Schema.derived

  /** Output of the wide nested-list plan: the transformed `tags` array plus its size. */
  case class Tagged(key: Long, tags2: Seq[String], n: Int)
  given taggedSchema: Schema[Tagged] = Schema.derived

  /** `withWindow` appends the window columns to the parent's, so the wide window output carries all
    * nine wide columns before `rowNum` and `rnk`.
    */
  case class WideWindowed(
    key: Long,
    value: Int,
    id: Long,
    ts: Timestamp,
    name: String,
    label: String,
    amount: Option[Decimal],
    tags: Seq[String],
    flag: Boolean,
    rowNum: Int,
    rnk: Int
  )
  given wideWindowedSchema: Schema[WideWindowed] = Schema.derived

  /** A distributed plan and the extra arguments its measurement needs. */
  private final case class DistributedPlan(
    id: String,
    build: () => DataFrame,
    project: Array[Row] => Array[Row] = rows => rows,
    check: Array[Row] => Option[String] = _ => None,
    asserted: Boolean = false
  )

  /** The narrow distributed plan set, shared so the warmup blocks and the reported blocks run the
    * same code. Labels are unchanged from the previous increment so the recorded narrow baseline
    * stays comparable.
    */
  private def narrowPlans(bench: Bench): Vector[DistributedPlan] = Vector(
    DistributedPlan("filter-groupby", () => bench.filterGroupBy),
    DistributedPlan("join-on", () => bench.joinOn),
    DistributedPlan("window", () => bench.window, windowDigestRows, rowNumberPermutation, asserted = true),
    DistributedPlan("sort-limit", () => bench.sortLimit),
    DistributedPlan("distinct", () => bench.distinct),
    DistributedPlan("aggregate", () => bench.aggregate),
    DistributedPlan("time-bucket", () => bench.timeBucket)
  )

  /** The same seven shapes over the wide table, plus three plans that exercise the new types. Each
    * wide label carries the `wide-` prefix so narrow and wide rows are distinguishable everywhere.
    */
  private def widePlans(bench: Bench): Vector[DistributedPlan] = Vector(
    DistributedPlan("wide-filter-groupby", () => bench.wideFilterGroupBy),
    DistributedPlan("wide-join-on", () => bench.wideJoinOn),
    DistributedPlan(
      "wide-window",
      () => bench.wideWindow,
      wideWindowDigestRows,
      wideRowNumberPermutation,
      asserted = true
    ),
    DistributedPlan("wide-sort-limit", () => bench.wideSortLimit),
    DistributedPlan("wide-distinct", () => bench.wideDistinct),
    DistributedPlan("wide-aggregate", () => bench.wideAggregate),
    DistributedPlan("wide-time-bucket", () => bench.wideTimeBucket),
    DistributedPlan("wide-string-groupby", () => bench.wideStringGroupBy),
    DistributedPlan("wide-decimal-agg", () => bench.wideDecimalAgg),
    DistributedPlan("wide-tags-transform", () => bench.wideTagsTransform)
  )

  /** Narrow plus wide distributed plan sets. */
  private def distributedPlans(bench: Bench): Vector[DistributedPlan] =
    narrowPlans(bench) ++ widePlans(bench)

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
        s"##DATA dir=$root groups=$NumGroups fact_scales=${FactScales.mkString(",")} driver_scale=$DriverCollectScale block_warmup_scale=$BlockWarmupScale tables=narrow,wide amount=decimal(12,2)"
      )

      runWarmup(spark, root, BlockWarmupScale)

      FactScales.foreach { scale =>
        runWarmup(spark, root, scale)
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
    // WARN, not ERROR: Comet emits its `[COMET: ...]` fallback reasons as warnings when
    // spark.comet.explain.fallback.log.enabled is set, and the harness harvests them.
    session.sparkContext.setLogLevel("WARN")
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
    private val widePath = ensureWide(spark, root, scale, NumGroups)
    private val wide: Dataset[Wide] =
      SparkDatasets.fromDataFrame(spark.read.parquet(widePath), wideSourceSchema).narrow[Wide]

    private val keyCell: Expr[Fact, Long] = Expr.Cell("key", ColumnIndex(0))
    private val valueCell: Expr[Fact, Int] = Expr.Cell("value", ColumnIndex(1))
    private val idCell: Expr[Fact, Long] = Expr.Cell("id", ColumnIndex(2))
    private val dimKeyCell: Expr[Dim, Long] = Expr.Cell("key", ColumnIndex(0))

    private val wideKeyCell: Expr[Wide, Long] = Expr.Cell("key", ColumnIndex(0))
    private val wideValueCell: Expr[Wide, Int] = Expr.Cell("value", ColumnIndex(1))
    private val wideIdCell: Expr[Wide, Long] = Expr.Cell("id", ColumnIndex(2))
    private val wideNameCell: Expr[Wide, String] = Expr.Cell("name", ColumnIndex(4))
    private val wideAmountCell: Expr[Wide, Decimal] = Expr.Cell("amount", ColumnIndex(6))
    private val wideTagsCell: Expr[Wide, Seq[String]] = Expr.Cell("tags", ColumnIndex(7))

    private def df[T](ds: Dataset[T]): DataFrame =
      interp.toDataFrame(ds) match {
        case Right(d) => d
        case Left(e) => sys.error(s"plan build failed: $e")
      }

    private def sumAndCount: Vector[AggSpec[Fact]] = Vector(
      AggSpec("total", Expr.Sum(valueCell), ColumnType.LongType),
      AggSpec("cnt", Expr.Count[Fact](), ColumnType.LongType)
    )

    private def wideSumAndCount: Vector[AggSpec[Wide]] = Vector(
      AggSpec("total", Expr.Sum(wideValueCell), ColumnType.LongType),
      AggSpec("cnt", Expr.Count[Wide](), ColumnType.LongType)
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

    // --- The seven narrow shapes over the wide table -------------------------------------------

    def wideFilterGroupBy: DataFrame =
      df(
        wide
          .where(_.value > 500)
          .groupByAgg[Grouped](Vector(KeySpec("key", wideKeyCell, ColumnType.LongType)), wideSumAndCount)
      )

    def wideJoinOn: DataFrame =
      df(wide.joinOn(dim, wideKeyCell, dimKeyCell, ColumnType.LongType, ColumnType.LongType))

    def wideWindow: DataFrame = {
      val spec = WindowSpec[Wide](
        partitionBy = Vector(KeySpec("key", wideKeyCell, ColumnType.LongType)),
        orderBy = Vector(SortSpec[Wide, Int](wideValueCell, summon[Ordering[Int]], ColumnType.IntType, true))
      )
      val exprs = Vector(
        WindowExprSpec("rowNum", Expr.RowNumber[Wide](), ColumnType.IntType),
        WindowExprSpec("rnk", Expr.Rank[Wide](), ColumnType.IntType)
      )
      df(wide.withWindow[WideWindowed](exprs, spec))
    }

    def wideSortLimit: DataFrame = {
      val keys = Vector(
        SortSpec[Wide, Int](wideValueCell, summon[Ordering[Int]], ColumnType.IntType, true),
        SortSpec[Wide, Long](wideIdCell, summon[Ordering[Long]], ColumnType.LongType, false)
      )
      df(wide.sortByExprs(keys).limit(LimitRows))
    }

    def wideDistinct: DataFrame = df(wide.distinct)

    def wideAggregate: DataFrame =
      df(wide.aggregate[GlobalAgg](wideSumAndCount))

    def wideTimeBucket: DataFrame = {
      val tsCell: Expr[Wide, Timestamp] = Expr.Cell("ts", ColumnIndex(3))
      val bucket = Expr.timeBucket[Wide](
        Expr.const(DayTimeInterval.ofMicros(15L * 60L * 1000000L)),
        tsCell,
        Expr.const(Timestamp.ofEpochMicro(0L))
      )
      df(
        wide.selectAs[Bucketed](
          ("key", wideKeyCell, ColumnType.LongType),
          ("bucket", bucket.asInstanceOf[Expr[Wide, Any]], ColumnType.TimestampType)
        )
      )
    }

    // --- Plans that exercise the new types ------------------------------------------------------

    /** Filter on the low-cardinality string and group by it. */
    def wideStringGroupBy: DataFrame =
      df(
        wide
          .filter(wideNameCell === Expr.const[Wide, String]("n3"))
          .groupByAgg[NameGrouped](
            Vector(KeySpec("name", wideNameCell, ColumnType.StringType)),
            Vector(AggSpec("cnt", Expr.Count[Wide](), ColumnType.LongType))
          )
      )

    /** Global max/min over the nullable decimal, so the null path runs. */
    def wideDecimalAgg: DataFrame = {
      val aggs = Vector(
        AggSpec(
          "maxAmount",
          Expr.Max(wideAmountCell, summon[Ordering[Decimal]]),
          ColumnType.OptionType(ColumnType.DecimalType(12, 2))
        ),
        AggSpec(
          "minAmount",
          Expr.Min(wideAmountCell, summon[Ordering[Decimal]]),
          ColumnType.OptionType(ColumnType.DecimalType(12, 2))
        ),
        AggSpec("cnt", Expr.Count[Wide](), ColumnType.LongType)
      )
      df(wide.aggregate[DecimalAgg](aggs))
    }

    /** Transform the nested string list element-wise and expose its size. */
    def wideTagsTransform: DataFrame = {
      val tags2 = wideTagsCell.transform(t => t ++ Expr.const("!"))
      df(
        wide.selectAs[Tagged](
          ("key", wideKeyCell, ColumnType.LongType),
          ("tags2", tags2.asInstanceOf[Expr[Wide, Any]], ColumnType.ArrayType(ColumnType.StringType)),
          ("n", wideTagsCell.arraySize, ColumnType.IntType)
        )
      )
    }
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

  /** The wide fact table: the narrow [[Fact]] columns plus a timestamp and the new data shapes,
    * generated with the same key and value distributions so joins and groupings behave identically.
    * `amount` is null on every seventh row so the decimal null path is exercised; `name` has 10
    * distinct values, `label` is unique per row, and `tags` is a two-element string array. The
    * `amount` column is written as `decimal(12, 2)`, matching [[wideDecimalSchema]].
    */
  private def ensureWide(spark: SparkSession, root: String, rows: Long, groups: Int): String = {
    val path = s"$root/fact-wide-$rows-$groups"
    if (!new java.io.File(path).exists()) {
      val decimal = org.apache.spark.sql.types.DecimalType(12, 2)
      val base = spark.range(0L, rows).toDF("id")
      base
        .select(
          (F.col("id") % F.lit(groups.toLong)).as("key"),
          ((F.col("id") * F.lit(2654435761L)) % F.lit(1000L)).cast("int").as("value"),
          F.col("id").as("id"),
          F.timestamp_micros(F.col("id") % F.lit(86_400_000_000L)).as("ts"),
          F.concat(F.lit("n"), (F.col("id") % F.lit(10L)).cast("string")).as("name"),
          F.concat(F.lit("l-"), F.col("id").cast("string")).as("label"),
          F.when(F.col("id") % F.lit(7L) === F.lit(0L), F.lit(null).cast(decimal))
            .otherwise((F.col("id") % F.lit(100000L)).cast(decimal))
            .as("amount"),
          F.array(
            F.concat(F.lit("t"), (F.col("id") % F.lit(3L)).cast("string")),
            F.concat(F.lit("u"), (F.col("id") % F.lit(5L)).cast("string"))
          ).as("tags"),
          (F.col("id") % F.lit(2L) === F.lit(0L)).as("flag")
        )
        .write
        .mode("errorifexists")
        .parquet(path)
    }
    path
  }

  /** Pay the JVM's first-block cost at a scale before its reported block. Timing only: both actions
    * run once, the results are discarded, and the lines carry a prefix the extraction ignores.
    */
  private def runWarmup(spark: SparkSession, root: String, scale: Long): Unit = {
    val bench = new Bench(spark, root, scale)
    distributedPlans(bench).foreach { p =>
      val df = p.build()
      val c0 = System.nanoTime()
      val rows = df.collect()
      val c1 = System.nanoTime()
      val e0 = System.nanoTime()
      df.write.format("noop").mode("overwrite").save()
      val e1 = System.nanoTime()
      cometOperators(df) // warm plan inspection too; not reported
      println(
        s"##WARMUP plan=${p.id} scale=$scale collect_ms=${fmtMs((c1 - c0) / 1e6)} exec_ms=${fmtMs((e1 - e0) / 1e6)} rows=${rows.length}"
      )
    }
  }

  /** `(key, value, rnk)` — invariant under the tie order `row_number` assigns. `rnkIndex` locates
    * `rnk` in the output row (4 for the narrow [[Windowed]], 10 for the wide [[WideWindowed]]).
    */
  private def windowDigestAt(rnkIndex: Int)(rows: Array[Row]): Array[Row] =
    rows.map(r => Row(r.getLong(0), r.getInt(1), r.getInt(rnkIndex)))

  private def windowDigestRows(rows: Array[Row]): Array[Row] = windowDigestAt(4)(rows)

  private def wideWindowDigestRows(rows: Array[Row]): Array[Row] = windowDigestAt(10)(rows)

  /** `rowNum` must be a permutation of `1..n` within each `key` partition. */
  private def rowNumberPermutationAt(rowNumIndex: Int)(rows: Array[Row]): Option[String] = {
    val byKey = mutable.HashMap.empty[Long, mutable.ArrayBuffer[Int]]
    var i = 0
    while (i < rows.length) {
      byKey.getOrElseUpdate(rows(i).getLong(0), mutable.ArrayBuffer.empty) += rows(i).getInt(rowNumIndex)
      i += 1
    }
    byKey.iterator.collectFirst {
      case (key, nums) if nums.sorted != (1 to nums.length).toVector =>
        s"key=$key rowNum is not a permutation of 1..${nums.length}"
    }
  }

  private def rowNumberPermutation(rows: Array[Row]): Option[String] = rowNumberPermutationAt(3)(rows)

  private def wideRowNumberPermutation(rows: Array[Row]): Option[String] = rowNumberPermutationAt(9)(rows)

  private def fmtMs(value: Double): String =
    "%.1f".formatLocal(java.util.Locale.ROOT, value)

  /** `min_ms=… median_ms=… max_ms=… samples=…` for one action. */
  private def summarize(name: String, measured: Array[Double]): String = {
    val sorted = measured.sorted
    s"${name}_min_ms=${fmtMs(sorted.head)} ${name}_median_ms=${fmtMs(sorted(sorted.length / 2))} ${name}_max_ms=${fmtMs(sorted.last)} ${name}_samples=${measured.map(fmtMs).mkString(",")}"
  }

  private def runPlan(
    id: String,
    scale: Long,
    plan: () => DataFrame,
    project: Array[Row] => Array[Row] = rows => rows,
    check: Array[Row] => Option[String] = _ => None,
    asserted: Boolean = false
  ): Unit = {
    println(s"##PLAN-BEGIN plan=$id scale=$scale")
    val df = plan()
    var lastRows: Array[Row] | Null = null
    val collectSamples = new Array[Double](Warmup + Measured)
    val execSamples = new Array[Double](Warmup + Measured)
    var i = 0
    while (i < Warmup + Measured) {
      lastRows = null // release the previous run's result before collecting the next
      val c0 = System.nanoTime()
      val collected = df.collect()
      val c1 = System.nanoTime()
      collectSamples(i) = (c1 - c0) / 1e6
      // Force the same plan without materialising it on the driver. The noop data source runs the
      // whole query and returns nothing.
      val e0 = System.nanoTime()
      df.write.format("noop").mode("overwrite").save()
      val e1 = System.nanoTime()
      execSamples(i) = (e1 - e0) / 1e6
      lastRows = collected
      i += 1
    }
    val rows = lastRows.nn
    val collectMeasured = collectSamples.drop(Warmup)
    val execMeasured = execSamples.drop(Warmup)
    val digest = digestOf(project(rows))
    val ops = cometOperators(df)
    val opText = if (ops.isEmpty) "-" else ops.mkString(",")
    println(
      s"##RESULT plan=$id scale=$scale n=${collectMeasured.length} ${summarize("collect", collectMeasured)} ${summarize("exec", execMeasured)} rows=${rows.length} digest=$digest comet_ops=$opText"
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
    println(s"##PLAN-END plan=$id scale=$scale")
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
