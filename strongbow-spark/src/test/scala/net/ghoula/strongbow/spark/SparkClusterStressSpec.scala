package net.ghoula.strongbow.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.prelude.*

/** Stress tests targeting distributed Spark execution.
  *
  * These tests use operations that execute natively on Spark (Expr-based filter, distinct,
  * intersect, except, joinOn, sortByExpr, selectExprs).
  *
  * Data arrays are allocated once and reused. Verification uses count() or sampling rather than
  * collecting entire datasets to the driver.
  *
  * For real cluster testing (`-Dspark.test.master=spark://...`), increase N via system property
  * `-Dstress.rows=5000000`.
  */
class SparkClusterStressSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.sql.shuffle.partitions", "8")
  }

  private val N = 5_000_000
  private val Half = N / 2
  private val Third = N / 3
  private val JoinSize = N / 5

  private val valueExpr: Expr[Int, Int] = Expr.Cell("value", ColumnIndex(0))

  // Allocate data arrays once, reuse across tests
  private lazy val fullData = Array.tabulate(N)(identity)
  private lazy val halfData1 = Array.tabulate(Half)(identity)
  private lazy val halfData2 = Array.tabulate(Half)(i => i + Third)
  private lazy val joinData1 = Array.tabulate(JoinSize)(identity)
  private lazy val joinData2 = Array.tabulate(JoinSize)(i => i + JoinSize / 2)

  private def intDataset(data: Array[Int]): Dataset[Int] =
    Dataset.fromColumns(Vector(Column.int(data)), Schema.intSchema).toOption.get

  // ---------------------------------------------------------------------------
  // Test 1: Large expression-based filter
  // ---------------------------------------------------------------------------

  "Distributed filter" should s"handle $N rows via Expr predicate" in {
    val threshold = (N * 0.9).toInt
    val ds = intDataset(fullData).filter(
      Expr.Gt(valueExpr, Expr.Const(threshold), summon[Ordering[Int]])
    )

    val df = sparkInterpreter.toDataFrame(ds).toOption.get
    val count = df.count()
    val expected = N - threshold - 1

    count shouldBe expected
  }

  // ---------------------------------------------------------------------------
  // Test 2: Union + Distinct
  // ---------------------------------------------------------------------------

  "Union then distinct" should "deduplicate with 50% overlap" in {
    val ds = intDataset(halfData1).union(intDataset(halfData2)).distinct

    val df = sparkInterpreter.toDataFrame(ds).toOption.get
    val count = df.count()
    val expectedDistinct = Half + Third
    count shouldBe expectedDistinct
  }

  // ---------------------------------------------------------------------------
  // Test 3: Intersect
  // ---------------------------------------------------------------------------

  "Intersect" should "find common elements between two datasets" in {
    val ds = intDataset(halfData1).intersect(intDataset(halfData2))

    val df = sparkInterpreter.toDataFrame(ds).toOption.get
    val count = df.count()
    val expectedOverlap = Half - Third
    count shouldBe expectedOverlap
  }

  // ---------------------------------------------------------------------------
  // Test 4: Except
  // ---------------------------------------------------------------------------

  "Except" should "compute set difference" in {
    val ds = intDataset(halfData1).except(intDataset(halfData2))

    val df = sparkInterpreter.toDataFrame(ds).toOption.get
    val count = df.count()
    count shouldBe Third
  }

  // ---------------------------------------------------------------------------
  // Test 5: Expression-based join
  // ---------------------------------------------------------------------------

  "JoinOn" should "perform distributed hash join" in {
    given Schema[(Int, Int)] = Schema.tuple2Schema[Int, Int]

    val leftKey = Expr.Cell[Int, Int]("value", ColumnIndex(0))
    val rightKey = Expr.Cell[Int, Int]("value", ColumnIndex(0))

    val ds = intDataset(joinData1).joinOn(
      intDataset(joinData2),
      leftKey,
      rightKey,
      ColumnType.IntType,
      ColumnType.IntType
    )

    val df = sparkInterpreter.toDataFrame(ds).toOption.get
    val count = df.count()
    val expectedMatches = JoinSize / 2
    count shouldBe expectedMatches
  }

  // ---------------------------------------------------------------------------
  // Test 6: Multi-stage pipeline — filter → union → distinct
  // ---------------------------------------------------------------------------

  "Chained distributed operations" should "execute a 3-stage pipeline" in {
    val quarter = N / 4
    val threshold = Half - quarter - 1

    val filtered1 = intDataset(halfData1).filter(
      Expr.Gt(valueExpr, Expr.Const(threshold), summon[Ordering[Int]])
    )
    val filtered2 = intDataset(halfData2).filter(
      Expr.Gt(valueExpr, Expr.Const(threshold + quarter), summon[Ordering[Int]])
    )

    val ds = filtered1.union(filtered2).distinct

    val df = sparkInterpreter.toDataFrame(ds).toOption.get
    val count = df.count()
    // filtered1: (threshold .. Half-1] → quarter values
    // filtered2: (threshold+quarter .. Third+Half-1] → Third+Half - (threshold+quarter) - 1 values
    // These ranges are disjoint so distinct = sum of both
    val filtered1Count = Half - threshold - 1
    val filtered2Count = (Third + Half) - (threshold + quarter) - 1
    val expectedDistinct = filtered1Count + filtered2Count
    count shouldBe expectedDistinct
  }

  // ---------------------------------------------------------------------------
  // Test 7: SelectExprs — Catalyst code generation
  // ---------------------------------------------------------------------------

  "SelectExprs" should "evaluate expressions via Catalyst" in {
    val doubled = Expr
      .Add(
        Expr.Mul(valueExpr, Expr.Const(2)),
        Expr.Const(1)
      )
      .asInstanceOf[Expr[Int, Any]]

    val selected = intDataset(fullData).selectAs[Int](("result", doubled, ColumnType.IntType))
    val filtered = selected.filter(
      Expr.Gt(
        Expr.Cell[Int, Int]("result", ColumnIndex(0)),
        Expr.Const(N),
        summon[Ordering[Int]]
      )
    )

    val df = sparkInterpreter.toDataFrame(filtered).toOption.get
    val count = df.count()
    val minInput = (N + 1) / 2
    val expectedCount = N - minInput
    count shouldBe expectedCount
  }

  // ---------------------------------------------------------------------------
  // Test 8: Large string dataset
  // ---------------------------------------------------------------------------

  "String filter" should "handle string rows via Expr predicate" in {
    val strSize = N / 3
    val data = Array.tabulate[String | Null](strSize)(i => f"record-$i%08d")
    val ds = Dataset
      .fromColumns(Vector(Column.string(data)), Schema.stringSchema)
      .toOption
      .get

    val threshold = f"record-${strSize / 2}%08d"
    val strExpr: Expr[String, String] = Expr.Cell("value", ColumnIndex(0))

    val filtered = ds.filter(
      Expr.Gt(strExpr, Expr.Const(threshold), summon[Ordering[String]])
    )

    val df = sparkInterpreter.toDataFrame(filtered).toOption.get
    val count = df.count()
    val expectedCount = strSize - strSize / 2 - 1
    count shouldBe expectedCount
  }
}
