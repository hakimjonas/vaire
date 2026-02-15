package net.ghoula.strongbow.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.{Column, ColumnType, Dataset, Expr, Schema}
import net.ghoula.strongbow.types.ColumnIndex

/** Stress tests targeting distributed Spark execution.
  *
  * These tests create datasets with millions of rows and use only operations that execute natively on
  * Spark (Expr-based filter, distinct, intersect, except, joinOn, sortByExpr, selectExprs) rather
  * than operations that collect to the driver (Map, FlatMap, lambda-based joins, Sort, SortBy).
  *
  * In local[2] mode these still pass but exercise nothing interesting. Against a real cluster
  * (`-Dspark.test.master=spark://...`) they produce real shuffles, network I/O, and memory pressure.
  */
class SparkClusterStressSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  // Bump shuffle partitions before any test runs.
  // The default of 2 (from SparkTestBase) creates only 2 shuffle tasks — useless for stress.
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.sql.shuffle.partitions", "200")
  }

  private val N = 5_000_000

  private val valueExpr: Expr[Int, Int] = Expr.Cell("value", ColumnIndex(0))

  private def intDataset(data: Array[Int]): Dataset[Int] = {
    Dataset.fromColumns(Vector(Column.int(data)), Schema.intSchema).toOption.get
  }

  // ---------------------------------------------------------------------------
  // Test 1: Large expression-based filter (distributed, no driver collection)
  // ---------------------------------------------------------------------------

  "Distributed filter" should "handle 5M rows via Expr predicate" in {
    val threshold = (N * 0.9).toInt // keep top 10%
    val ds = intDataset(Array.tabulate(N)(identity))
      .filter(
        Expr.Gt(
          valueExpr,
          Expr.Const(threshold),
          summon[Ordering[Int]]
        )
      )

    val result = sparkInterpreter.execute(ds)
    result.isRight shouldBe true

    val vec = result.toOption.get.toVectorUnsafe
    val expected = N - threshold - 1

    vec.size shouldBe expected
    vec.foreach { v =>
      v should be > threshold
    }
  }

  // ---------------------------------------------------------------------------
  // Test 2: Union + Distinct — two shuffles on millions of rows
  // ---------------------------------------------------------------------------

  "Union then distinct" should "deduplicate across 6M rows with 50% overlap" in {
    // ds1: 0 .. 3M-1,  ds2: 1.5M .. 4.5M-1  → overlap of 1.5M values
    val half = N / 2 // 2.5M
    val third = N / 3

    val ds1 = intDataset(Array.tabulate(half)(identity)) // 0 .. 2_499_999
    val ds2 = intDataset(Array.tabulate(half)(i => i + third)) // 1_666_666 .. 4_166_665

    val ds = ds1.union(ds2).distinct

    val result = sparkInterpreter.execute(ds)
    result.isRight shouldBe true

    val vec = result.toOption.get.toVectorUnsafe
    // Union has half + half = 5M rows, distinct removes the overlap
    val expectedDistinct = half + third // unique values span 0 .. 4_166_665
    vec.size shouldBe expectedDistinct
  }

  // ---------------------------------------------------------------------------
  // Test 3: Intersect — shuffle-based set intersection at scale
  // ---------------------------------------------------------------------------

  "Intersect" should "find common elements between two 2.5M-row datasets" in {
    val half = N / 2
    val third = N / 3

    val ds1 = intDataset(Array.tabulate(half)(identity)) // 0 .. 2_499_999
    val ds2 = intDataset(Array.tabulate(half)(i => i + third)) // 1_666_666 .. 4_166_665

    val ds = ds1.intersect(ds2)

    val result = sparkInterpreter.execute(ds)
    result.isRight shouldBe true

    val vec = result.toOption.get.toVectorUnsafe
    // Overlap: third .. half-1 → half - third values
    val expectedOverlap = half - third
    vec.size shouldBe expectedOverlap
    vec.foreach { v =>
      v should be >= third
      v should be < half
    }
  }

  // ---------------------------------------------------------------------------
  // Test 4: Except — shuffle-based set difference at scale
  // ---------------------------------------------------------------------------

  "Except" should "compute set difference on 2.5M-row datasets" in {
    val half = N / 2
    val third = N / 3

    val ds1 = intDataset(Array.tabulate(half)(identity)) // 0 .. 2_499_999
    val ds2 = intDataset(Array.tabulate(half)(i => i + third)) // 1_666_666 .. 4_166_665

    val ds = ds1.except(ds2)

    val result = sparkInterpreter.execute(ds)
    result.isRight shouldBe true

    val vec = result.toOption.get.toVectorUnsafe
    // ds1 minus overlap: 0 .. third-1 → third values
    vec.size shouldBe third
    vec.foreach { v =>
      v should be >= 0
      v should be < third
    }
  }

  // ---------------------------------------------------------------------------
  // Test 5: Expression-based join — distributed hash join
  // ---------------------------------------------------------------------------

  "JoinOn" should "perform distributed hash join on 1M-row datasets" in {
    given Schema[(Int, Int)] = Schema.tuple2Schema[Int, Int]

    val joinSize = N / 5 // 1M each
    // ds1: 0 .. 999_999   ds2: 500_000 .. 1_499_999  → 500K overlap
    val ds1 = intDataset(Array.tabulate(joinSize)(identity))
    val ds2 = intDataset(Array.tabulate(joinSize)(i => i + joinSize / 2))

    val leftKey = Expr.Cell[Int, Int]("value", ColumnIndex(0))
    val rightKey = Expr.Cell[Int, Int]("value", ColumnIndex(0))

    val ds = ds1.joinOn(ds2, leftKey, rightKey, ColumnType.IntType, ColumnType.IntType)

    val result = sparkInterpreter.execute(ds)
    result.isRight shouldBe true

    val vec = result.toOption.get.toVectorUnsafe
    val expectedMatches = joinSize / 2 // 500K matching pairs
    vec.size shouldBe expectedMatches

    // Every matched pair should have equal left and right keys
    vec.foreach { case (l, r) =>
      l shouldBe r
    }
  }

  // ---------------------------------------------------------------------------
  // Test 6: Multi-stage pipeline — filter → union → distinct (3 distributed stages)
  // ---------------------------------------------------------------------------

  "Chained distributed operations" should "execute a 3-stage pipeline on millions of rows" in {
    val quarter = N / 4

    // Two datasets of 2.5M rows each, filter to top 25% (~625K each), union, distinct
    val ds1 = intDataset(Array.tabulate(N / 2)(identity))
    val ds2 = intDataset(Array.tabulate(N / 2)(i => i + quarter))

    val threshold = (N / 2) - quarter - 1 // keep top 25% of each

    val filtered1 = ds1.filter(
      Expr.Gt(valueExpr, Expr.Const(threshold), summon[Ordering[Int]])
    )
    val filtered2 = ds2.filter(
      Expr.Gt(valueExpr, Expr.Const(threshold + quarter), summon[Ordering[Int]])
    )

    val ds = filtered1.union(filtered2).distinct

    val result = sparkInterpreter.execute(ds)
    result.isRight shouldBe true

    val vec = result.toOption.get.toVectorUnsafe
    // filtered1: threshold+1 .. N/2-1 → quarter values
    // filtered2: threshold+quarter+1 .. N/2+quarter-1 → quarter values
    // These ranges partially overlap (threshold+quarter+1 .. N/2-1)
    // Total distinct = values from threshold+1 .. N/2+quarter-1
    val expectedDistinct = N / 2 + quarter - threshold - 1
    vec.size shouldBe expectedDistinct

    vec.foreach { v =>
      v should be > threshold
    }
  }

  // ---------------------------------------------------------------------------
  // Test 7: SelectExprs — Catalyst code generation at scale
  // ---------------------------------------------------------------------------

  "SelectExprs" should "evaluate expressions on 5M rows via Catalyst" in {
    val ds = intDataset(Array.tabulate(N)(identity))

    // Compute value * 2 + 1 via Expr, then filter results > N
    val doubled = Expr.Add(
      Expr.Mul(valueExpr, Expr.Const(2)),
      Expr.Const(1)
    ).asInstanceOf[Expr[Int, Any]]

    val selected = ds.selectAs[Int](("result", doubled, ColumnType.IntType))
    val filtered = selected.filter(
      Expr.Gt(
        Expr.Cell[Int, Int]("result", ColumnIndex(0)),
        Expr.Const(N),
        summon[Ordering[Int]]
      )
    )

    val result = sparkInterpreter.execute(filtered)
    result.isRight shouldBe true

    val vec = result.toOption.get.toVectorUnsafe
    // value * 2 + 1 > N  ⟹  value > (N-1)/2  ⟹  values from ceil((N+1)/2) .. N-1
    // That's roughly N/2 values
    val minInput = (N + 1) / 2 // = 2_500_001
    val expectedCount = N - minInput
    vec.size shouldBe expectedCount

    vec.foreach { v =>
      v should be > N
    }
  }

  // ---------------------------------------------------------------------------
  // Test 8: Large string dataset — higher per-row memory, tests serialization
  // ---------------------------------------------------------------------------

  "String filter" should "handle 2M string rows via Expr predicate" in {
    val strSize = N / 3
    val data = Array.tabulate(strSize)(i => f"record-$i%08d")
    val ds = Dataset
      .fromColumns(Vector(Column.string(data)), Schema.stringSchema)
      .toOption
      .get

    val threshold = f"record-${strSize / 2}%08d"
    val strExpr: Expr[String, String] = Expr.Cell("value", ColumnIndex(0))

    // Filter strings > threshold (lexicographic) — keeps roughly half
    val filtered = ds.filter(
      Expr.Gt(
        strExpr,
        Expr.Const(threshold),
        summon[Ordering[String]]
      )
    )

    val result = sparkInterpreter.execute(filtered)
    result.isRight shouldBe true

    val vec = result.toOption.get.toVectorUnsafe
    // Lexicographic comparison on zero-padded integers is equivalent to numeric comparison
    // Values > "record-00833333" are "record-00833334" .. "record-01666665"
    val expectedCount = strSize - strSize / 2 - 1
    vec.size shouldBe expectedCount

    vec.foreach { v =>
      v should be > threshold
    }
  }
}
