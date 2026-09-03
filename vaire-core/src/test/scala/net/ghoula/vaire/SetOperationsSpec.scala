package net.ghoula.vaire

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.prelude.*

class SetOperationsSpec extends AnyFlatSpec with Matchers {

  private def createIntDataset(values: Vector[Int]): Dataset[Int] = {
    val mat = MaterializedDataset.fromVector(values).toOption.get
    Dataset.Root(InMemorySource(mat.columns), summon[Schema[Int]])
  }

  "intersect" should "return common rows, deduplicated" in {
    val ds1 = createIntDataset(Vector(1, 2, 3, 4, 5))
    val ds2 = createIntDataset(Vector(3, 4, 5, 6, 7))

    val result = ds1.intersect(ds2).collect.toOption.get

    result should contain theSameElementsAs Vector(3, 4, 5)
  }

  "intersect" should "deduplicate results" in {
    val ds1 = createIntDataset(Vector(1, 1, 2, 2, 3))
    val ds2 = createIntDataset(Vector(1, 2, 2, 4))

    val result = ds1.intersect(ds2).collect.toOption.get

    // Spark semantics: intersect deduplicates
    result should contain theSameElementsAs Vector(1, 2)
  }

  "intersect" should "return empty for disjoint datasets" in {
    val ds1 = createIntDataset(Vector(1, 2, 3))
    val ds2 = createIntDataset(Vector(4, 5, 6))

    val result = ds1.intersect(ds2).collect.toOption.get

    result shouldBe empty
  }

  "intersect" should "return empty when one side is empty" in {
    val ds1 = createIntDataset(Vector(1, 2, 3))
    val ds2 = createIntDataset(Vector.empty)

    val result = ds1.intersect(ds2).collect.toOption.get

    result shouldBe empty
  }

  "except" should "return rows in left but not right, deduplicated" in {
    val ds1 = createIntDataset(Vector(1, 2, 3, 4, 5))
    val ds2 = createIntDataset(Vector(3, 4, 5, 6, 7))

    val result = ds1.except(ds2).collect.toOption.get

    result should contain theSameElementsAs Vector(1, 2)
  }

  "except" should "deduplicate results" in {
    val ds1 = createIntDataset(Vector(1, 1, 2, 2, 3))
    val ds2 = createIntDataset(Vector(3))

    val result = ds1.except(ds2).collect.toOption.get

    // Spark semantics: except deduplicates
    result should contain theSameElementsAs Vector(1, 2)
  }

  "except" should "return empty for identical datasets" in {
    val ds1 = createIntDataset(Vector(1, 2, 3))
    val ds2 = createIntDataset(Vector(1, 2, 3))

    val result = ds1.except(ds2).collect.toOption.get

    result shouldBe empty
  }

  "except" should "return all rows when right is empty" in {
    val ds1 = createIntDataset(Vector(1, 2, 3))
    val ds2 = createIntDataset(Vector.empty)

    val result = ds1.except(ds2).collect.toOption.get

    // Dedup still applies
    result should contain theSameElementsAs Vector(1, 2, 3)
  }

  "except from empty" should "return empty" in {
    val ds1 = createIntDataset(Vector.empty)
    val ds2 = createIntDataset(Vector(1, 2, 3))

    val result = ds1.except(ds2).collect.toOption.get

    result shouldBe empty
  }

  "intersect and except" should "be complementary" in {
    val ds1 = createIntDataset(Vector(1, 2, 3, 4, 5))
    val ds2 = createIntDataset(Vector(3, 4, 5, 6, 7))

    val common = ds1.intersect(ds2).collect.toOption.get
    val onlyLeft = ds1.except(ds2).collect.toOption.get

    // common + onlyLeft should cover all distinct elements of ds1
    (common ++ onlyLeft).sorted shouldBe Vector(1, 2, 3, 4, 5)
  }

  "intersect" should "work with derived schema types" in {
    case class Point(x: Int, y: Int)
    given Schema[Point] = Schema.derived

    val points1 = Vector(Point(1, 2), Point(3, 4), Point(5, 6))
    val points2 = Vector(Point(3, 4), Point(5, 6), Point(7, 8))

    val mat1 = MaterializedDataset.fromVector(points1).toOption.get
    val mat2 = MaterializedDataset.fromVector(points2).toOption.get
    val ds1 = Dataset.Root(InMemorySource(mat1.columns), summon[Schema[Point]])
    val ds2 = Dataset.Root(InMemorySource(mat2.columns), summon[Schema[Point]])

    val result = ds1.intersect(ds2).collect.toOption.get

    result should contain theSameElementsAs Vector(Point(3, 4), Point(5, 6))
  }
}
