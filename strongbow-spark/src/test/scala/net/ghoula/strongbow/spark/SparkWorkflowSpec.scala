package net.ghoula.strongbow.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.{Column, Dataset, DatasetInterpreter, Expr, Schema}
import net.ghoula.strongbow.types.ColumnIndex

/** End-to-end workflow tests: chained operations with parity checks. */
class SparkWorkflowSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  "filter -> map -> sort -> limit" should "produce same results" in {
    val col = Column.int(Array(5, 3, 8, 1, 9, 2, 7, 4, 6, 10))
    val ds = Dataset.fromColumns(Vector(col), Schema.intSchema).toOption.get
      .filter(Expr.Gt(
        Expr.Cell("value", ColumnIndex(0)),
        Expr.Const(3),
        summon[Ordering[Int]]
      ))
      .map(_ * 2)
      .sort
      .limit(3)

    val inMemory = DatasetInterpreter.execute(ds).map(_.toVectorUnsafe)
    val sparkResult = sparkInterpreter.execute(ds).map(_.toVectorUnsafe)
    sparkResult shouldBe inMemory
  }

  "groupBy -> reduceByKey -> values -> sort" should "produce same results" in {
    val col = Column.int(Array(1, 2, 3, 1, 2, 3, 1))
    val ds = Dataset.fromColumns(Vector(col), Schema.intSchema).toOption.get
      .groupBy(identity)
      .reduceByKey(_ + _)
      .values
      .sort

    val inMemory = DatasetInterpreter.execute(ds).map(_.toVectorUnsafe)
    val sparkResult = sparkInterpreter.execute(ds).map(_.toVectorUnsafe)
    sparkResult shouldBe inMemory
  }

  "union -> distinct -> sort" should "produce same results" in {
    val col1 = Column.int(Array(1, 2, 3, 4))
    val col2 = Column.int(Array(3, 4, 5, 6))
    val ds1 = Dataset.fromColumns(Vector(col1), Schema.intSchema).toOption.get
    val ds2 = Dataset.fromColumns(Vector(col2), Schema.intSchema).toOption.get
    val ds = ds1.union(ds2).distinct.sort

    val inMemory = DatasetInterpreter.execute(ds).map(_.toVectorUnsafe)
    val sparkResult = sparkInterpreter.execute(ds).map(_.toVectorUnsafe)
    sparkResult shouldBe inMemory
  }

  "except -> map" should "produce same results" in {
    val col1 = Column.int(Array(1, 2, 3, 4, 5))
    val col2 = Column.int(Array(2, 4))
    val ds1 = Dataset.fromColumns(Vector(col1), Schema.intSchema).toOption.get
    val ds2 = Dataset.fromColumns(Vector(col2), Schema.intSchema).toOption.get
    val ds = ds1.except(ds2).map(_ * 100)

    val inMemory = DatasetInterpreter.execute(ds).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(ds).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  "using SparkInterpreter via DatasetActions" should "work with given" in {
    import net.ghoula.strongbow.DatasetActions.*

    given net.ghoula.strongbow.Interpreter = sparkInterpreter

    val col = Column.int(Array(1, 2, 3))
    val ds = Dataset.fromColumns(Vector(col), Schema.intSchema).toOption.get

    ds.collect shouldBe Right(Vector(1, 2, 3))
    ds.count shouldBe Right(3L)
    ds.isEmpty shouldBe Right(false)
  }
}
