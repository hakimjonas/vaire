package net.ghoula.strongbow.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.prelude.*

/** Parity tests: every Dataset operation should produce the same result via SparkInterpreter as via
  * DatasetInterpreter (in-memory columnar).
  *
  * Results are compared as sorted sets since Spark doesn't guarantee order.
  */
class SparkInterpreterSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  private def assertParity[T: Ordering](dataset: Dataset[T]): Unit = {
    val inMemory = DatasetInterpreter.execute(dataset).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(dataset).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  // --- Root ---

  "Root" should "produce same results" in {
    val col = Column.int(Array(1, 2, 3))
    val ds = Dataset.fromColumns(Vector(col), Schema.intSchema).toOption.get
    assertParity(ds)
  }

  // --- Filter ---

  "Filter" should "produce same results via Expr" in {
    val col = Column.int(Array(10, 20, 30, 40, 50))
    val ds = Dataset
      .fromColumns(Vector(col), Schema.intSchema)
      .toOption
      .get
      .filter(
        Expr.Gt(
          Expr.Cell("value", ColumnIndex(0)),
          Expr.Const(25),
          summon[Ordering[Int]]
        )
      )
    assertParity(ds)
  }

  // --- Map ---

  "Map" should "produce same results" in {
    val col = Column.int(Array(1, 2, 3))
    val ds = Dataset
      .fromColumns(Vector(col), Schema.intSchema)
      .toOption
      .get
      .map(_ * 10)
    assertParity(ds)
  }

  // --- FlatMap ---

  "FlatMap" should "produce same results" in {
    val col = Column.int(Array(1, 2, 3))
    val ds = Dataset
      .fromColumns(Vector(col), Schema.intSchema)
      .toOption
      .get
      .flatMap(x => List(x, x * 10))
    assertParity(ds)
  }

  // --- Distinct ---

  "Distinct" should "produce same results" in {
    val col = Column.int(Array(1, 2, 2, 3, 1))
    val ds = Dataset.fromColumns(Vector(col), Schema.intSchema).toOption.get.distinct
    assertParity(ds)
  }

  // --- Limit ---

  "Limit" should "produce same results" in {
    val col = Column.int(Array(10, 20, 30, 40, 50))
    val ds = Dataset.fromColumns(Vector(col), Schema.intSchema).toOption.get.limit(3)
    // Limit preserves order, but Spark may not — compare as sets
    val inMemory = DatasetInterpreter.execute(ds).map(_.toVectorUnsafe)
    val sparkResult = sparkInterpreter.execute(ds).map(_.toVectorUnsafe)
    sparkResult.map(_.toSet) shouldBe inMemory.map(_.toSet)
    sparkResult.map(_.size) shouldBe inMemory.map(_.size)
  }

  // --- Union ---

  "Union" should "produce same results" in {
    val col1 = Column.int(Array(1, 2, 3))
    val col2 = Column.int(Array(4, 5, 6))
    val ds1 = Dataset.fromColumns(Vector(col1), Schema.intSchema).toOption.get
    val ds2 = Dataset.fromColumns(Vector(col2), Schema.intSchema).toOption.get
    assertParity(ds1.union(ds2))
  }

  // --- Intersect ---

  "Intersect" should "produce same results" in {
    val col1 = Column.int(Array(1, 2, 3, 4))
    val col2 = Column.int(Array(3, 4, 5, 6))
    val ds1 = Dataset.fromColumns(Vector(col1), Schema.intSchema).toOption.get
    val ds2 = Dataset.fromColumns(Vector(col2), Schema.intSchema).toOption.get
    assertParity(ds1.intersect(ds2))
  }

  // --- Except ---

  "Except" should "produce same results" in {
    val col1 = Column.int(Array(1, 2, 3, 4))
    val col2 = Column.int(Array(3, 4, 5, 6))
    val ds1 = Dataset.fromColumns(Vector(col1), Schema.intSchema).toOption.get
    val ds2 = Dataset.fromColumns(Vector(col2), Schema.intSchema).toOption.get
    assertParity(ds1.except(ds2))
  }

  // --- Sort ---

  "Sort" should "produce same results" in {
    val col = Column.int(Array(5, 3, 1, 4, 2))
    val ds = Dataset.fromColumns(Vector(col), Schema.intSchema).toOption.get.sort
    val inMemory = DatasetInterpreter.execute(ds).map(_.toVectorUnsafe)
    val sparkResult = sparkInterpreter.execute(ds).map(_.toVectorUnsafe)
    sparkResult shouldBe inMemory
  }

  // --- SortBy ---

  "SortBy" should "produce same results" in {
    val col = Column.int(Array(5, 3, 1, 4, 2))
    val ds = Dataset
      .fromColumns(Vector(col), Schema.intSchema)
      .toOption
      .get
      .sortBy(identity)
    val inMemory = DatasetInterpreter.execute(ds).map(_.toVectorUnsafe)
    val sparkResult = sparkInterpreter.execute(ds).map(_.toVectorUnsafe)
    sparkResult shouldBe inMemory
  }

  // --- ZipWithIndex ---

  "ZipWithIndex" should "produce tuples with sequential indices" in {
    given Schema[(Int, Long)] = Schema.tuple2Schema[Int, Long]
    val col = Column.int(Array(10, 20, 30))
    val ds = Dataset.fromColumns(Vector(col), Schema.intSchema).toOption.get.zipWithIndex
    val inMemory = DatasetInterpreter.execute(ds).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(ds).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  // --- ZipWithUniqueId ---

  "ZipWithUniqueId" should "produce unique IDs with same count as parent" in {
    given Schema[(Int, Long)] = Schema.tuple2Schema[Int, Long]
    val col = Column.int(Array(10, 20, 30))
    val ds = Dataset.fromColumns(Vector(col), Schema.intSchema).toOption.get.zipWithUniqueId
    val sparkResult = sparkInterpreter.execute(ds).map(_.toVectorUnsafe)
    sparkResult match {
      case Right(values) =>
        values.length shouldBe 3
        values.map(_._1).sorted shouldBe Vector(10, 20, 30)
        // IDs should all be unique (may differ from sequential)
        values.map(_._2).distinct.length shouldBe 3
      case Left(err) => fail(s"Spark ZipWithUniqueId failed: $err")
    }
  }

  // --- Persist ---

  "Persist" should "produce same results as parent" in {
    val col = Column.int(Array(1, 2, 3))
    val ds = Dataset.fromColumns(Vector(col), Schema.intSchema).toOption.get.persist
    assertParity(ds)
  }

  // --- Rebalance ---

  "Rebalance" should "produce same results as parent" in {
    val col = Column.int(Array(1, 2, 3, 4, 5))
    val ds = Dataset.fromColumns(Vector(col), Schema.intSchema).toOption.get.rebalance(2)
    assertParity(ds)
  }

  // --- InnerJoin ---

  "InnerJoin" should "produce same results" in {
    given Schema[(Int, Int)] = Schema.tuple2Schema[Int, Int]
    val col1 = Column.int(Array(1, 2, 3))
    val col2 = Column.int(Array(2, 3, 4))
    val ds1 = Dataset.fromColumns(Vector(col1), Schema.intSchema).toOption.get
    val ds2 = Dataset.fromColumns(Vector(col2), Schema.intSchema).toOption.get
    val joined = ds1.join(ds2, (a: Int, b: Int) => a == b)
    assertParity(joined)
  }

  // --- LeftJoin ---

  "LeftJoin" should "produce same results" in {
    given optSchema: Schema[Option[Int]] = Schema.optionSchema[Int]
    given tupleSchema: Schema[(Int, Option[Int])] = Schema.tuple2Schema[Int, Option[Int]]
    val col1 = Column.int(Array(1, 2, 3))
    val col2 = Column.int(Array(2, 3, 4))
    val ds1 = Dataset.fromColumns(Vector(col1), Schema.intSchema).toOption.get
    val ds2 = Dataset.fromColumns(Vector(col2), Schema.intSchema).toOption.get
    val joined = ds1.leftJoin(ds2, (a: Int, b: Int) => a == b)
    assertParity(joined)
  }

  // --- RightJoin ---

  "RightJoin" should "produce same results" in {
    given optSchema: Schema[Option[Int]] = Schema.optionSchema[Int]
    given tupleSchema: Schema[(Option[Int], Int)] = Schema.tuple2Schema[Option[Int], Int]
    val col1 = Column.int(Array(1, 2, 3))
    val col2 = Column.int(Array(2, 3, 4))
    val ds1 = Dataset.fromColumns(Vector(col1), Schema.intSchema).toOption.get
    val ds2 = Dataset.fromColumns(Vector(col2), Schema.intSchema).toOption.get
    val joined = ds1.rightJoin(ds2, (a: Int, b: Int) => a == b)
    assertParity(joined)
  }

  // --- FullJoin ---

  "FullJoin" should "produce same results" in {
    given optIntSchema: Schema[Option[Int]] = Schema.optionSchema[Int]
    given tupleSchema: Schema[(Option[Int], Option[Int])] =
      Schema.tuple2Schema[Option[Int], Option[Int]]
    val col1 = Column.int(Array(1, 2, 3))
    val col2 = Column.int(Array(2, 3, 4))
    val ds1 = Dataset.fromColumns(Vector(col1), Schema.intSchema).toOption.get
    val ds2 = Dataset.fromColumns(Vector(col2), Schema.intSchema).toOption.get
    val joined = ds1.fullJoin(ds2, (a: Int, b: Int) => a == b)
    assertParity(joined)
  }

  // --- LeftAntiJoin ---

  "LeftAntiJoin" should "produce same results" in {
    val col1 = Column.int(Array(1, 2, 3))
    val col2 = Column.int(Array(2, 3, 4))
    val ds1 = Dataset.fromColumns(Vector(col1), Schema.intSchema).toOption.get
    val ds2 = Dataset.fromColumns(Vector(col2), Schema.intSchema).toOption.get
    val joined = ds1.antiJoin(ds2, (a: Int, b: Int) => a == b)
    assertParity(joined)
  }

  // --- Expression-based joins ---

  "InnerJoinOn" should "produce same results as lambda join" in {
    given Schema[(Int, Int)] = Schema.tuple2Schema[Int, Int]
    val col1 = Column.int(Array(1, 2, 3))
    val col2 = Column.int(Array(2, 3, 4))
    val ds1 = Dataset.fromColumns(Vector(col1), Schema.intSchema).toOption.get
    val ds2 = Dataset.fromColumns(Vector(col2), Schema.intSchema).toOption.get

    val leftKey = Expr.Cell[Int, Int]("value", ColumnIndex(0))
    val rightKey = Expr.Cell[Int, Int]("value", ColumnIndex(0))
    val joined = ds1.joinOn(ds2, leftKey, rightKey, ColumnType.IntType, ColumnType.IntType)
    assertParity(joined)
  }

  "LeftAntiJoinOn" should "produce same results as lambda anti join" in {
    val col1 = Column.int(Array(1, 2, 3))
    val col2 = Column.int(Array(2, 3, 4))
    val ds1 = Dataset.fromColumns(Vector(col1), Schema.intSchema).toOption.get
    val ds2 = Dataset.fromColumns(Vector(col2), Schema.intSchema).toOption.get

    val leftKey = Expr.Cell[Int, Int]("value", ColumnIndex(0))
    val rightKey = Expr.Cell[Int, Int]("value", ColumnIndex(0))
    val joined = ds1.antiJoinOn(ds2, leftKey, rightKey, ColumnType.IntType, ColumnType.IntType)
    assertParity(joined)
  }

  // --- SelectExprs ---

  "SelectExprs" should "produce same results" in {
    val col = Column.int(Array(1, 2, 3))
    val ds = Dataset.fromColumns(Vector(col), Schema.intSchema).toOption.get
    val selected = ds.select(
      (
        "doubled",
        Expr
          .Mul(
            Expr.Cell("value", ColumnIndex(0)),
            Expr.Const(2)
          )
          .asInstanceOf[Expr[Int, Any]],
        ColumnType.IntType
      )
    )
    val inMemory = DatasetInterpreter.execute(selected).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(selected).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  // --- SelectAs (type-changing select) ---

  "selectAs" should "change output type from String to Int via length (Spark parity)" in {
    val col = Column.string(Array("hi", "hello", "greetings"))
    val ds = Dataset.fromColumns(Vector(col), Schema.stringSchema).toOption.get

    val strCell: Expr[String, String] = Expr.Cell("value", ColumnIndex(0))
    val selected = ds.selectAs[Int](
      ("result", strCell.length.asInstanceOf[Expr[String, Any]], ColumnType.IntType)
    )

    val inMemory = DatasetInterpreter.execute(selected).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(selected).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
    sparkResult shouldBe Right(Vector(2, 5, 9))
  }

  // --- Chained operations ---

  "Chained operations" should "filter then map" in {
    val col = Column.int(Array(1, 2, 3, 4, 5))
    val ds = Dataset
      .fromColumns(Vector(col), Schema.intSchema)
      .toOption
      .get
      .filter(
        Expr.Gt(
          Expr.Cell("value", ColumnIndex(0)),
          Expr.Const(2),
          summon[Ordering[Int]]
        )
      )
      .map(_ * 10)
    assertParity(ds)
  }

  it should "union then distinct" in {
    val col1 = Column.int(Array(1, 2, 3))
    val col2 = Column.int(Array(2, 3, 4))
    val ds1 = Dataset.fromColumns(Vector(col1), Schema.intSchema).toOption.get
    val ds2 = Dataset.fromColumns(Vector(col2), Schema.intSchema).toOption.get
    assertParity(ds1.union(ds2).distinct)
  }

  // --- String type ---

  "String datasets" should "produce same results" in {
    val col = Column.string(Array("hello", "world", "test"))
    val ds = Dataset.fromColumns(Vector(col), Schema.stringSchema).toOption.get
    assertParity(ds)
  }

  // --- New numeric types ---

  "Float datasets" should "produce same results" in {
    val col = Column.float(Array(1.0f, 2.5f, 3.7f))
    val ds = Dataset.fromColumns(Vector(col), Schema.floatSchema).toOption.get
    assertParity(ds)
  }

  "Short datasets" should "produce same results" in {
    val col = Column.short(Array[Short](10, 20, 30))
    val ds = Dataset.fromColumns(Vector(col), Schema.shortSchema).toOption.get
    assertParity(ds)
  }

  "Byte datasets" should "produce same results" in {
    val col = Column.byte(Array[Byte](1, 2, 3))
    val ds = Dataset.fromColumns(Vector(col), Schema.byteSchema).toOption.get
    assertParity(ds)
  }

  // --- toDataFrame ---

  "toDataFrame" should "return a valid DataFrame for Root" in {
    val col = Column.int(Array(1, 2, 3))
    val ds = Dataset.fromColumns(Vector(col), Schema.intSchema).toOption.get
    val df = sparkInterpreter.toDataFrame(ds)
    df.isRight shouldBe true
    df.toOption.get.count() shouldBe 3L
  }

  it should "return count matching execute().rowCount for filter" in {
    val col = Column.int(Array(10, 20, 30, 40, 50))
    val ds = Dataset
      .fromColumns(Vector(col), Schema.intSchema)
      .toOption
      .get
      .filter(
        Expr.Gt(
          Expr.Cell("value", ColumnIndex(0)),
          Expr.Const(25),
          summon[Ordering[Int]]
        )
      )
    val dfCount = sparkInterpreter.toDataFrame(ds).toOption.get.count()
    val executeCount = sparkInterpreter.execute(ds).toOption.get.rowCount.toLong
    dfCount shouldBe executeCount
  }

  it should "return count matching execute().rowCount for join" in {
    given Schema[(Int, Int)] = Schema.tuple2Schema[Int, Int]
    val col1 = Column.int(Array(1, 2, 3))
    val col2 = Column.int(Array(2, 3, 4))
    val ds1 = Dataset.fromColumns(Vector(col1), Schema.intSchema).toOption.get
    val ds2 = Dataset.fromColumns(Vector(col2), Schema.intSchema).toOption.get
    val leftKey = Expr.Cell[Int, Int]("value", ColumnIndex(0))
    val rightKey = Expr.Cell[Int, Int]("value", ColumnIndex(0))
    val joined = ds1.joinOn(ds2, leftKey, rightKey, ColumnType.IntType, ColumnType.IntType)
    val dfCount = sparkInterpreter.toDataFrame(joined).toOption.get.count()
    val executeCount = sparkInterpreter.execute(joined).toOption.get.rowCount.toLong
    dfCount shouldBe executeCount
  }

  it should "return count matching execute().rowCount for groupByAgg" in {
    case class Sale(region: String, amount: Double)
    given saleSchema: Schema[Sale] = Schema.derived
    case class Result(region: String, total: Double)
    given resultSchema: Schema[Result] = Schema.derived

    val regionCol = Column.string(Array("East", "West", "East"))
    val amountCol = Column.double(Array(10.0, 20.0, 30.0))
    val ds = Dataset.fromColumns(Vector(regionCol, amountCol), saleSchema).toOption.get

    val regionCell: Expr[Sale, Any] = Expr.Cell("region_value", ColumnIndex(0))
    val amountCell: Expr[Sale, Double] = Expr.Cell("amount_value", ColumnIndex(1))
    val keys = Vector(KeySpec[Sale, Any]("region", regionCell, ColumnType.StringType))
    val aggs = Vector(AggSpec("total", Expr.SumDouble(amountCell), ColumnType.DoubleType))
    val grouped = ds.groupByAgg[Result](keys, aggs)

    val dfCount = sparkInterpreter.toDataFrame(grouped).toOption.get.count()
    val executeCount = sparkInterpreter.execute(grouped).toOption.get.rowCount.toLong
    dfCount shouldBe executeCount
  }

  it should "produce a DataFrame that can be registered as a temp view" in {
    val col = Column.int(Array(1, 2, 3, 4, 5))
    val ds = Dataset.fromColumns(Vector(col), Schema.intSchema).toOption.get
    val df = sparkInterpreter.toDataFrame(ds).toOption.get
    df.createOrReplaceTempView("test_view")
    val sqlResult = spark.sql("SELECT count(*) FROM test_view").collect()
    sqlResult.head.getLong(0) shouldBe 5L
  }

  // --- SparkSource (zero-overhead path) ---

  "SparkSource" should "skip array conversion for root" in {
    import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
    import org.apache.spark.sql.Row

    val rows = java.util.Arrays.asList(Row(1), Row(2), Row(3))
    val structType = StructType(Array(StructField("value", IntegerType, nullable = false)))
    val nativeDf = spark.createDataFrame(rows, structType)

    val ds = SparkDatasets.fromDataFrame(nativeDf, Schema.intSchema)
    val df = sparkInterpreter.toDataFrame(ds)
    df.isRight shouldBe true
    df.toOption.get.count() shouldBe 3L
  }

  it should "produce same results as InMemorySource for filter" in {
    import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
    import org.apache.spark.sql.Row

    val data = Array(10, 20, 30, 40, 50)
    val rows = java.util.Arrays.asList(data.map(Row(_))*)
    val structType = StructType(Array(StructField("value", IntegerType, nullable = false)))
    val nativeDf = spark.createDataFrame(rows, structType)

    val sparkDs = SparkDatasets
      .fromDataFrame(nativeDf, Schema.intSchema)
      .filter(Expr.Gt(Expr.Cell("value", ColumnIndex(0)), Expr.Const(25), summon[Ordering[Int]]))

    val inMemDs = Dataset
      .fromColumns(Vector(Column.int(data)), Schema.intSchema)
      .toOption
      .get
      .filter(Expr.Gt(Expr.Cell("value", ColumnIndex(0)), Expr.Const(25), summon[Ordering[Int]]))

    val sparkCount = sparkInterpreter.toDataFrame(sparkDs).toOption.get.count()
    val inMemCount = sparkInterpreter.toDataFrame(inMemDs).toOption.get.count()
    sparkCount shouldBe inMemCount
  }

  it should "work with groupByAgg" in {
    import org.apache.spark.sql.types.{
      DoubleType => SparkDoubleType,
      StringType => SparkStringType,
      StructField,
      StructType
    }
    import org.apache.spark.sql.Row

    case class Sale(region: String, amount: Double)
    given saleSchema: Schema[Sale] = Schema.derived
    case class Result(region: String, total: Double)
    given resultSchema: Schema[Result] = Schema.derived

    val rows = java.util.Arrays.asList(
      Row("East", 10.0),
      Row("West", 20.0),
      Row("East", 30.0)
    )
    val structType = StructType(
      Array(
        StructField("region_value", SparkStringType, nullable = false),
        StructField("amount_value", SparkDoubleType, nullable = false)
      )
    )
    val nativeDf = spark.createDataFrame(rows, structType)

    val regionCell: Expr[Sale, Any] = Expr.Cell("region_value", ColumnIndex(0))
    val amountCell: Expr[Sale, Double] = Expr.Cell("amount_value", ColumnIndex(1))
    val keys = Vector(KeySpec[Sale, Any]("region", regionCell, ColumnType.StringType))
    val aggs = Vector(AggSpec("total", Expr.SumDouble(amountCell), ColumnType.DoubleType))

    val ds = SparkDatasets
      .fromDataFrame[Sale](nativeDf, saleSchema)
      .groupByAgg[Result](keys, aggs)

    val df = sparkInterpreter.toDataFrame(ds).toOption.get
    df.count() shouldBe 2L
  }
}
