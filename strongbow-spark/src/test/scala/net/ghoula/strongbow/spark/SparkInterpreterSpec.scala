package net.ghoula.strongbow.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.{Column, ColumnType, Dataset, DatasetInterpreter, Expr, Schema}
import net.ghoula.strongbow.types.ColumnIndex

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
}
