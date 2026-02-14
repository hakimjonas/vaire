package net.ghoula.strongbow

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.prelude.*
import net.ghoula.strongbow.types.ColumnIndex

class DatasetActionsSpec extends AnyFlatSpec with Matchers {

  "collect" should "materialize all elements" in {
    val dataset = createIntDataset(Vector(1, 2, 3, 4, 5))

    val result = dataset.collect match {
      case Right(values) => values
      case Left(err) => fail(s"Collect failed: $err")
    }

    result shouldBe Vector(1, 2, 3, 4, 5)
  }

  "count" should "return row count without materializing" in {
    val dataset = createIntDataset(Vector(1, 2, 3))

    val result = dataset.count match {
      case Right(cnt) => cnt
      case Left(err) => fail(s"Count failed: $err")
    }

    result shouldBe 3
  }

  "isEmpty" should "detect empty datasets" in {
    // Note: MaterializedDataset doesn't support truly empty datasets (requires columns.nonEmpty)
    // So we test with a dataset that has columns but filters out all rows
    val dataset = createIntDataset(Vector(1, 2, 3))
    val emptyFiltered = dataset.filter(Expr.Cell("value", ColumnIndex(0)) > Expr.Const(100))
    val nonEmpty = createIntDataset(Vector(1))

    emptyFiltered.isEmpty shouldBe Right(true)
    nonEmpty.isEmpty shouldBe Right(false)
  }

  "take" should "limit and collect in one call" in {
    val dataset = createIntDataset(Vector(1, 2, 3, 4, 5))

    val result = dataset.take(3) match {
      case Right(values) => values
      case Left(err) => fail(s"Take failed: $err")
    }

    result shouldBe Vector(1, 2, 3)
  }

  "explain" should "show AST structure" in {
    val dataset = createIntDataset(Vector(1, 2, 3))
      .filter(Expr.Cell("value", ColumnIndex(0)) > Expr.Const(1))
      .distinct
      .limit(10)

    val plan = dataset.explain

    plan should include("Limit(10)")
    plan should include("Distinct")
    plan should include("Filter")
    plan should include("Root")
  }

  "show" should "format rows as table" in {
    val dataset = createIntDataset(Vector(10, 20, 30))

    val table = dataset.show(5) match {
      case Right(str) => str
      case Left(err) => fail(s"Show failed: $err")
    }

    table should include("value")
    table should include("10")
    table should include("20")
    table should include("30")
  }

  private def createIntDataset(values: Vector[Int]): Dataset[Int] = {
    val column = Column.int(values.toArray)
    val schema = Schema.intSchema

    Dataset.fromColumns(Vector(column), schema) match {
      case Right(ds) => ds
      case Left(errors) => fail(s"Dataset creation failed: ${errors.toList}")
    }
  }
}
