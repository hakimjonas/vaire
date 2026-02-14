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

  private def createIntDataset(values: Vector[Int]): Dataset[Int] = {
    val column = Column.int(values.toArray)
    Dataset.fromColumns(Vector(column), Schema.intSchema).toOption.get
  }
}
