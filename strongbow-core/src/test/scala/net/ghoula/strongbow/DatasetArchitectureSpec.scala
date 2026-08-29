package net.ghoula.strongbow

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.prelude.*
import net.ghoula.strongbow.types.ColumnIndex

/** Basic smoke test for columnar interpreter.
  *
  * Verifies the one-cast-at-boundary architecture works end-to-end.
  */
class DatasetArchitectureSpec extends AnyFlatSpec with Matchers {

  "DatasetInterpreter" should "filter with typed columns using one-cast-at-boundary" in {
    // Create typed columns
    val ageColumn = Column.int(Array(25, 30, 18, 42, 15))

    val columns = Vector(ageColumn)

    // Create schema for Int
    val schema = Schema.intSchema

    // Create dataset - should succeed
    val dataset = Dataset.fromColumns(columns, schema) match {
      case Right(ds) => ds
      case Left(errors) => fail(s"Dataset creation failed: ${errors.toList}")
    }

    // Filter: age > 18
    val filtered = dataset.filter(
      Expr.Gt(
        Expr.Cell("age", ColumnIndex(0)),
        Expr.Const(18),
        summon[Ordering[Int]]
      )
    )

    // Execute
    val result = DatasetInterpreter.execute(filtered) match {
      case Right(ds) => ds
      case Left(err) => fail(s"Execution failed: $err")
    }

    // Verify results
    val values = result.toVectorUnsafe
    values shouldBe Vector(25, 30, 42)
  }

  it should "remove duplicates with distinct" in {
    val column = Column.int(Array(1, 2, 2, 3, 1, 4))
    val schema = Schema.intSchema

    val dataset = Dataset.fromColumns(Vector(column), schema) match {
      case Right(ds) => ds
      case Left(errors) => fail(s"Dataset creation failed: ${errors.toList}")
    }

    val distinctDataset = dataset.distinct
    val result = DatasetInterpreter.execute(distinctDataset) match {
      case Right(ds) => ds
      case Left(err) => fail(s"Execution failed: $err")
    }

    val values = result.toVectorUnsafe.sorted
    values shouldBe Vector(1, 2, 3, 4)
  }

  it should "restrict rows with limit" in {
    val column = Column.int(Array(10, 20, 30, 40, 50))
    val schema = Schema.intSchema

    val dataset = Dataset.fromColumns(Vector(column), schema) match {
      case Right(ds) => ds
      case Left(errors) => fail(s"Dataset creation failed: ${errors.toList}")
    }

    val limited = dataset.limit(3)
    val result = DatasetInterpreter.execute(limited) match {
      case Right(ds) => ds
      case Left(err) => fail(s"Execution failed: $err")
    }

    val values = result.toVectorUnsafe
    values shouldBe Vector(10, 20, 30)
  }

  it should "sort rows in order" in {
    val column = Column.int(Array(5, 2, 8, 1, 9))
    val schema = Schema.intSchema

    val dataset = Dataset.fromColumns(Vector(column), schema) match {
      case Right(ds) => ds
      case Left(errors) => fail(s"Dataset creation failed: ${errors.toList}")
    }

    val sorted = dataset.sort
    val result = DatasetInterpreter.execute(sorted) match {
      case Right(ds) => ds
      case Left(err) => fail(s"Execution failed: $err")
    }

    val values = result.toVectorUnsafe
    values shouldBe Vector(1, 2, 5, 8, 9)
  }

  it should "combine datasets with union" in {
    val col1 = Column.int(Array(1, 2, 3))
    val col2 = Column.int(Array(4, 5, 6))
    val schema = Schema.intSchema

    val ds1 = Dataset.fromColumns(Vector(col1), schema) match {
      case Right(ds) => ds
      case Left(errors) => fail(s"Dataset 1 creation failed: ${errors.toList}")
    }

    val ds2 = Dataset.fromColumns(Vector(col2), schema) match {
      case Right(ds) => ds
      case Left(errors) => fail(s"Dataset 2 creation failed: ${errors.toList}")
    }

    val unioned = ds1.union(ds2)
    val result = DatasetInterpreter.execute(unioned) match {
      case Right(ds) => ds
      case Left(err) => fail(s"Execution failed: $err")
    }

    val values = result.toVectorUnsafe
    values shouldBe Vector(1, 2, 3, 4, 5, 6)
  }

  "GADT evidence" should "be accessible via pattern matching" in {
    // This test verifies the key architectural win:
    // Gt expression carries Ordering evidence that's accessible in interpreter
    val gtExpr: Expr[Int, Boolean] = Expr.Gt(
      Expr.Const[Int, Int](5),
      Expr.Const[Int, Int](3),
      summon[Ordering[Int]]
    )

    // Pattern match gives us the ordering (as done in ExprInterpreter)
    (gtExpr: @unchecked) match {
      case gt: Expr.Gt[Int, ?] =>
        // Verify the ordering is accessible - this is what ExprInterpreter does
        // The actual comparison uses gt.ordering.gt(l, r) with NO CAST
        gt.left shouldBe a[Expr.Const[?, ?]]
        gt.right shouldBe a[Expr.Const[?, ?]]
        gt.ordering shouldBe an[Ordering[?]]
      case _ =>
        fail("Expected Gt expression")
    }
  }

  "Typed column accessors" should "return properly typed values without casts" in {
    val intCol = Column.int(Array(1, 2, 3))
    val stringCol = Column.string(Array("a", "b", "c"))

    intCol match {
      case Column.IntColumn(data, _) => data(0) shouldBe 1
      case _ => fail("Expected IntColumn")
    }
    stringCol match {
      case Column.StringColumn(data, _) => data(1) shouldBe "b"
      case _ => fail("Expected StringColumn")
    }
  }

  "ExprInterpreter" should "evaluate expressions with one cast at Cell boundary" in {
    // Create a simple dataset
    val column = Column.int(Array(10, 20, 30))
    val schema = Schema.intSchema

    val dataset = Dataset.fromColumns(Vector(column), schema) match {
      case Right(ds) => ds
      case Left(errors) => fail(s"Dataset creation failed: ${errors.toList}")
    }

    // Expression: cell(0) > 15
    val expr = Expr.Gt(
      Expr.Cell[Int, Int]("value", ColumnIndex(0)),
      Expr.Const[Int, Int](15),
      summon[Ordering[Int]]
    )

    // Filter using the expression
    val filtered = dataset.filter(expr)
    val result = DatasetInterpreter.execute(filtered) match {
      case Right(ds) => ds
      case Left(err) => fail(s"Execution failed: $err")
    }

    // Should keep 20 and 30
    val values = result.toVectorUnsafe
    values shouldBe Vector(20, 30)
  }

  "selectAs" should "change output type from String to Int via length expression" in {
    val col = Column.string(Array("hi", "hello", "greetings"))
    val ds = Dataset.fromColumns(Vector(col), Schema.stringSchema).toOption.get

    val strCell: Expr[String, String] = Expr.Cell("value", ColumnIndex(0))
    val selected = ds.selectAs[Int](
      ("result", strCell.length: Expr[String, Any], ColumnType.IntType)
    )

    val result = DatasetInterpreter.execute(selected) match {
      case Right(ds) => ds.toVectorUnsafe
      case Left(err) => fail(s"Execution failed: $err")
    }

    result shouldBe Vector(2, 5, 9)
  }

  "selectAs" should "change output type with arithmetic expression" in {
    val col = Column.int(Array(10, 21, 35))
    val ds = Dataset.fromColumns(Vector(col), Schema.intSchema).toOption.get

    val intCell: Expr[Int, Int] = Expr.Cell("value", ColumnIndex(0))
    val selected = ds.selectAs[Int](
      ("result", intCell / Expr.const(10): Expr[Int, Any], ColumnType.IntType)
    )

    val result = DatasetInterpreter.execute(selected) match {
      case Right(ds) => ds.toVectorUnsafe
      case Left(err) => fail(s"Execution failed: $err")
    }

    result shouldBe Vector(1, 2, 3)
  }

  "Schema validation" should "catch column count mismatch" in {
    val column1 = Column.int(Array(1, 2, 3))
    val column2 = Column.string(Array("a", "b", "c"))
    val schema = Schema.intSchema // Expects 1 column

    val result = Dataset.fromColumns(Vector(column1, column2), schema)

    result match {
      case Left(errors) =>
        errors.head shouldBe a[SchemaError.ColumnCountMismatch]
      case Right(_) =>
        fail("Should have failed with column count mismatch")
    }
  }

  "Schema validation" should "catch column type mismatch" in {
    val stringColumn = Column.string(Array("a", "b", "c"))
    val schema = Schema.intSchema // Expects IntType

    val result = Dataset.fromColumns(Vector(stringColumn), schema)

    result match {
      case Left(errors) =>
        errors.head shouldBe a[SchemaError.ColumnTypeMismatch]
      case Right(_) =>
        fail("Should have failed with column type mismatch")
    }
  }
}
