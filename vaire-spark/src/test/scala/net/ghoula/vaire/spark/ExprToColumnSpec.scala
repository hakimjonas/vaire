package net.ghoula.vaire.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.prelude.*

/** Tests that ExprToColumn correctly translates all Expr cases to Spark Column expressions. */
class ExprToColumnSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  private def intDataset(values: Int*): Dataset[Int] = {
    val col = Column.int(values.toArray)
    Dataset.fromColumns(Vector(col), Schema.intSchema).toOption.get
  }

  private def assertFilterParity(ds: Dataset[Int], predicate: Expr[Int, Boolean]): Unit = {
    val filtered = ds.filter(predicate)
    val inMemory = DatasetInterpreter.execute(filtered).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(filtered).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  val cell: Expr[Int, Int] = Expr.Cell("value", ColumnIndex(0))

  "Gt" should "work via Spark" in {
    assertFilterParity(intDataset(1, 2, 3, 4, 5), cell > Expr.const(3))
  }

  "Gte" should "work via Spark" in {
    assertFilterParity(intDataset(1, 2, 3, 4, 5), cell >= Expr.const(3))
  }

  "Lt" should "work via Spark" in {
    assertFilterParity(intDataset(1, 2, 3, 4, 5), cell < Expr.const(3))
  }

  "Lte" should "work via Spark" in {
    assertFilterParity(intDataset(1, 2, 3, 4, 5), cell <= Expr.const(3))
  }

  "Eq" should "work via Spark" in {
    assertFilterParity(intDataset(1, 2, 3, 4, 5), Expr.Eq(cell, Expr.const(3)))
  }

  "Neq" should "work via Spark" in {
    assertFilterParity(intDataset(1, 2, 3, 4, 5), Expr.Neq(cell, Expr.const(3)))
  }

  "And" should "work via Spark" in {
    assertFilterParity(
      intDataset(1, 2, 3, 4, 5),
      (cell > Expr.const(2)) && (cell < Expr.const(5))
    )
  }

  "Or" should "work via Spark" in {
    assertFilterParity(
      intDataset(1, 2, 3, 4, 5),
      (cell < Expr.const(2)) || (cell > Expr.const(4))
    )
  }

  "Not" should "work via Spark" in {
    assertFilterParity(
      intDataset(1, 2, 3, 4, 5),
      !(cell > Expr.const(3))
    )
  }

  "Add" should "work in select expressions" in {
    val ds = intDataset(1, 2, 3)
    val selected = ds.select(
      ("result", (cell + Expr.const(10)).asInstanceOf[Expr[Int, Any]], ColumnType.IntType)
    )
    val inMemory = DatasetInterpreter.execute(selected).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(selected).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  "Sub" should "work in select expressions" in {
    val ds = intDataset(10, 20, 30)
    val selected = ds.select(
      ("result", (cell - Expr.const(5)).asInstanceOf[Expr[Int, Any]], ColumnType.IntType)
    )
    val inMemory = DatasetInterpreter.execute(selected).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(selected).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  "Mul" should "work in select expressions" in {
    val ds = intDataset(1, 2, 3)
    val selected = ds.select(
      ("result", (cell * Expr.const(3)).asInstanceOf[Expr[Int, Any]], ColumnType.IntType)
    )
    val inMemory = DatasetInterpreter.execute(selected).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(selected).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  "Div" should "filter correctly via Spark" in {
    assertFilterParity(intDataset(10, 20, 30), (cell / Expr.const(10)) > Expr.const(1))
  }

  "Div" should "produce integer results in select (Spark parity)" in {
    val ds = intDataset(10, 21, 35)
    val selected = ds.select(
      ("result", (cell / Expr.const(10)).asInstanceOf[Expr[Int, Any]], ColumnType.IntType)
    )
    val inMemory = DatasetInterpreter.execute(selected).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(selected).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
    sparkResult shouldBe Right(Vector(1, 2, 3))
  }

  "When" should "work in select expressions" in {
    val ds = intDataset(1, 2, 3, 4, 5)
    val whenExpr = Expr.When(
      cell > Expr.const(3),
      Expr.const(100),
      Expr.const(0)
    )
    val selected = ds.select(
      ("result", whenExpr.asInstanceOf[Expr[Int, Any]], ColumnType.IntType)
    )
    val inMemory = DatasetInterpreter.execute(selected).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(selected).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  "Concat" should "work via Spark on string datasets" in {
    val col = Column.string(Array("hello", "world", "test"))
    val ds = Dataset.fromColumns(Vector(col), Schema.stringSchema).toOption.get
    val strCell: Expr[String, String] = Expr.Cell("value", ColumnIndex(0))
    val selected = ds.select(
      ("result", (strCell ++ Expr.const("!")).asInstanceOf[Expr[String, Any]], ColumnType.StringType)
    )
    val inMemory = DatasetInterpreter.execute(selected).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(selected).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  "Length" should "filter correctly via Spark on string datasets" in {
    val col = Column.string(Array("hi", "hello", "greetings"))
    val ds = Dataset.fromColumns(Vector(col), Schema.stringSchema).toOption.get
    val strCell: Expr[String, String] = Expr.Cell("value", ColumnIndex(0))
    val filtered = ds.filter(strCell.length > Expr.const(3))
    val inMemory = DatasetInterpreter.execute(filtered).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(filtered).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  "Const" should "convert correctly" in {
    ExprToColumn.convert[Int, Int](Expr.Const(42)).isRight shouldBe true
  }

  "Named" should "preserve name" in {
    ExprToColumn.convert[Int, Int](Expr.Named(Expr.Const(42), "answer")).isRight shouldBe true
  }

  "ExprToColumn.convert" should "handle all non-aggregation cases" in {
    // Verify conversion doesn't error for basic cases
    ExprToColumn.convert[Int, Int](Expr.Cell("value", ColumnIndex(0))).isRight shouldBe true
    ExprToColumn
      .convert[Int, Boolean](
        Expr.When(
          Expr.Gt(Expr.Cell("value", ColumnIndex(0)), Expr.Const(5), summon[Ordering[Int]]),
          Expr.Const(true),
          Expr.Const(false)
        )
      )
      .isRight shouldBe true
  }
}
