package net.ghoula.strongbow

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.prelude.*

class DatasetTransformationsSpec extends AnyFlatSpec with Matchers {

  "sample" should "return fraction of rows" in {
    val dataset = createIntDataset((1 to 100).toVector)

    val sampled = dataset.sample(0.5, seed = 42).collect match {
      case Right(values) => values
      case Left(err) => fail(s"Sample failed: $err")
    }

    // Should be approximately 50 rows (±10 for randomness)
    sampled.length should be >= 40
    sampled.length should be <= 60
  }

  "sample with seed" should "be deterministic" in {
    val dataset = createIntDataset((1 to 100).toVector)

    val sample1 = dataset.sample(0.3, seed = 123).collect.toOption.get
    val sample2 = dataset.sample(0.3, seed = 123).collect.toOption.get

    sample1 shouldBe sample2
  }

  "zipWithIndex" should "add sequential indices" in {
    val dataset = createIntDataset(Vector(10, 20, 30))

    val indexed = dataset.zipWithIndex.collect match {
      case Right(values) => values
      case Left(err) => fail(s"ZipWithIndex failed: $err")
    }

    indexed shouldBe Vector((10, 0L), (20, 1L), (30, 2L))
  }

  "++" should "alias union" in {
    val ds1 = createIntDataset(Vector(1, 2))
    val ds2 = createIntDataset(Vector(3, 4))

    val unioned = (ds1 ++ ds2).collect.toOption.get

    unioned shouldBe Vector(1, 2, 3, 4)
  }

  "partition" should "split dataset into matching and non-matching" in {
    val dataset = createIntDataset(Vector(1, 2, 3, 4, 5, 6))

    val predicate = Expr.Gt(
      Expr.Cell[Int, Int]("value", ColumnIndex(0)),
      Expr.Const[Int, Int](3),
      summon[Ordering[Int]]
    )

    val (matching, nonMatching) = dataset.partition(predicate)

    matching.collect.toOption.get shouldBe Vector(4, 5, 6)
    nonMatching.collect.toOption.get shouldBe Vector(1, 2, 3)
  }

  it should "handle empty partitions" in {
    val dataset = createIntDataset(Vector(1, 2, 3))

    val predicate = Expr.Gt(
      Expr.Cell[Int, Int]("value", ColumnIndex(0)),
      Expr.Const[Int, Int](10),
      summon[Ordering[Int]]
    )

    val (matching, nonMatching) = dataset.partition(predicate)

    matching.collect.toOption.get shouldBe empty
    nonMatching.collect.toOption.get shouldBe Vector(1, 2, 3)
  }

  "zipWithUniqueId" should "add unique IDs" in {
    val dataset = createIntDataset(Vector(10, 20, 30))

    val indexed = dataset.zipWithUniqueId.collect match {
      case Right(values) => values
      case Left(err) => fail(s"ZipWithUniqueId failed: $err")
    }

    // In-memory: unique IDs are sequential (same as zipWithIndex)
    indexed shouldBe Vector((10, 0L), (20, 1L), (30, 2L))
  }

  it should "produce unique IDs" in {
    val dataset = createIntDataset((1 to 100).toVector)

    val ids = dataset.zipWithUniqueId.collect.toOption.get.map(_._2)

    ids.distinct.length shouldBe 100
  }

  "persist" should "return same results as parent" in {
    val dataset = createIntDataset(Vector(1, 2, 3, 4, 5))

    val result = dataset.persist.collect.toOption.get

    result shouldBe Vector(1, 2, 3, 4, 5)
  }

  "checkpoint" should "return same results as parent" in {
    val dataset = createIntDataset(Vector(1, 2, 3, 4, 5))

    val result = dataset.checkpoint.collect.toOption.get

    result shouldBe Vector(1, 2, 3, 4, 5)
  }

  "rebalance" should "return same results as parent (no-op in-memory)" in {
    val dataset = createIntDataset(Vector(1, 2, 3, 4, 5))

    val result = dataset.rebalance.collect.toOption.get
    result shouldBe Vector(1, 2, 3, 4, 5)

    val resultN = dataset.rebalance(4).collect.toOption.get
    resultN shouldBe Vector(1, 2, 3, 4, 5)
  }

  "Option2Iterable" should "convert Some to single-element iterable in row-level eval" in {
    val anyColumn = Column.AnyColumn(Array(Some(1), None, Some(3)), nulls = scala.collection.immutable.BitSet.empty)
    val columns = Vector(anyColumn)

    import net.ghoula.strongbow.types.RowIndex

    val expr = Expr.Option2Iterable(Expr.Cell[Any, Option[Int]]("value", ColumnIndex(0)))

    val result0 = ExprInterpreter.evalAt(expr, columns, RowIndex(0))
    result0 shouldBe Right(List(1))

    val result1 = ExprInterpreter.evalAt(expr, columns, RowIndex(1))
    result1 shouldBe Right(List())

    val result2 = ExprInterpreter.evalAt(expr, columns, RowIndex(2))
    result2 shouldBe Right(List(3))
  }

  private def createIntDataset(values: Vector[Int]): Dataset[Int] = {
    val column = Column.int(values.toArray)
    Dataset.fromColumns(Vector(column), Schema.intSchema).toOption.get
  }
}
