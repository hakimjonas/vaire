package net.ghoula.vaire

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.BitSet

import net.ghoula.vaire.prelude.*

/** Locks the keyed-join semantics against the equivalent predicate join and against the index
  * direction.
  *
  * The keyed joins choose which side to index from the row counts (Phase 3a), so the same logical
  * join can run with the left or the right side indexed. These checks assert that:
  *
  *   - every keyed join agrees with the equivalent `==` predicate join as a multiset, and
  *   - an inner join is symmetric: joining `left` to `right` and `right` to `left` yields the same
  *     pairs, even though the two calls index opposite sides.
  *
  * Datasets are generated from a seeded RNG so the checks are deterministic.
  */
class KeyedJoinEquivalenceSpec extends AnyFlatSpec with Matchers {

  private val rng = new scala.util.Random(20260926L)

  private val key: Expr.Cell[Int, Int] = Expr.Cell[Int, Int]("value", ColumnIndex(0))

  private def intDataset(values: Vector[Int]): Dataset[Int] =
    Dataset.fromColumns(Vector(Column.int(values.toArray)), Schema.intSchema).toOption.get

  private def rows[T](dataset: Dataset[T]): Vector[T] =
    dataset.collect.getOrElse(fail("join execution failed"))

  private def uniformValues(n: Int): Vector[Int] = Vector.fill(n)(rng.nextInt(6))

  private def skewedValues(n: Int): Vector[Int] =
    if (n == 0) Vector.empty
    else Vector.fill(n)(if (rng.nextInt(10) < 8) 0 else rng.nextInt(4))

  private def equal(a: Int, b: Int): Boolean = a == b

  private def checkAllJoins(leftValues: Vector[Int], rightValues: Vector[Int]): Unit = {
    val left = intDataset(leftValues)
    val right = intDataset(rightValues)

    rows(left.joinOn(right, key, key, ColumnType.IntType, ColumnType.IntType)).sorted shouldBe
      rows(left.join(right, equal)).sorted

    rows(left.leftJoinOn(right, key, key, ColumnType.IntType, ColumnType.IntType)).sorted shouldBe
      rows(left.leftJoin(right, equal)).sorted

    rows(left.rightJoinOn(right, key, key, ColumnType.IntType, ColumnType.IntType)).sorted shouldBe
      rows(left.rightJoin(right, equal)).sorted

    rows(left.fullJoinOn(right, key, key, ColumnType.IntType, ColumnType.IntType)).sorted shouldBe
      rows(left.fullJoin(right, equal)).sorted

    rows(left.antiJoinOn(right, key, key, ColumnType.IntType, ColumnType.IntType)).sorted shouldBe
      rows(left.antiJoin(right, equal)).sorted
  }

  private val optKey: Expr.Cell[Option[Int], Int] = Expr.Cell[Option[Int], Int]("value", ColumnIndex(0))

  private def optIntDataset(values: Vector[Int], nulls: BitSet): Dataset[Option[Int]] =
    Dataset.fromColumns(Vector(Column.int(values.toArray, nulls)), Schema.optionSchema[Int]).toOption.get

  private def nullableValues(n: Int): (Vector[Int], BitSet) = {
    val values = uniformValues(n)
    val nulls = BitSet(values.indices.filter(_ => rng.nextInt(4) == 0)*)
    (values, nulls)
  }

  private def nullIntolerant(a: Option[Int], b: Option[Int]): Boolean =
    a.isDefined && b.isDefined && a == b

  private def checkNullableJoins(
    leftValues: Vector[Int],
    leftNulls: BitSet,
    rightValues: Vector[Int],
    rightNulls: BitSet
  ): Unit = {
    val left = optIntDataset(leftValues, leftNulls)
    val right = optIntDataset(rightValues, rightNulls)

    rows(left.joinOn(right, optKey, optKey, ColumnType.IntType, ColumnType.IntType)).sorted shouldBe
      rows(left.join(right, nullIntolerant)).sorted

    rows(left.leftJoinOn(right, optKey, optKey, ColumnType.IntType, ColumnType.IntType)).sorted shouldBe
      rows(left.leftJoin(right, nullIntolerant)).sorted

    rows(left.rightJoinOn(right, optKey, optKey, ColumnType.IntType, ColumnType.IntType)).sorted shouldBe
      rows(left.rightJoin(right, nullIntolerant)).sorted

    rows(left.fullJoinOn(right, optKey, optKey, ColumnType.IntType, ColumnType.IntType)).sorted shouldBe
      rows(left.fullJoin(right, nullIntolerant)).sorted

    rows(left.antiJoinOn(right, optKey, optKey, ColumnType.IntType, ColumnType.IntType)).sorted shouldBe
      rows(left.antiJoin(right, nullIntolerant)).sorted
  }

  private def checkInnerSymmetry(leftValues: Vector[Int], rightValues: Vector[Int]): Unit = {
    val left = intDataset(leftValues)
    val right = intDataset(rightValues)

    val forward = rows(left.joinOn(right, key, key, ColumnType.IntType, ColumnType.IntType)).sorted
    val backward = rows(right.joinOn(left, key, key, ColumnType.IntType, ColumnType.IntType)).map(_.swap).sorted

    backward shouldBe forward
  }

  "keyed joins" should "agree with the equivalent predicate join on uniform keys" in {
    (0 until 60).foreach { _ =>
      checkAllJoins(uniformValues(rng.nextInt(13)), uniformValues(rng.nextInt(13)))
    }
  }

  "keyed joins" should "agree with the equivalent predicate join on skewed keys" in {
    (0 until 60).foreach { _ =>
      checkAllJoins(skewedValues(rng.nextInt(13)), skewedValues(rng.nextInt(13)))
    }
  }

  "inner joinOn" should "be symmetric when the index direction flips" in {
    (0 until 60).foreach { _ =>
      checkInnerSymmetry(uniformValues(rng.nextInt(13)), uniformValues(rng.nextInt(13)))
    }
  }

  "inner joinOn" should "be symmetric across extreme size ratios" in {
    checkInnerSymmetry(Vector(1), uniformValues(40))
    checkInnerSymmetry(uniformValues(40), Vector(1))
    checkInnerSymmetry(Vector.empty, uniformValues(10))
    checkInnerSymmetry(uniformValues(10), Vector.empty)
  }

  "inner joinOn" should "emit the same row order whichever side is indexed" in {
    // left (2) <= right (2): forward branch, index the left
    val forward =
      rows(intDataset(Vector(1, 2)).joinOn(intDataset(Vector(2, 1)), key, key, ColumnType.IntType, ColumnType.IntType))
    // left (3) > right (2): reversed branch, index the right
    val reversed = rows(
      intDataset(Vector(1, 2, 3)).joinOn(intDataset(Vector(2, 1)), key, key, ColumnType.IntType, ColumnType.IntType)
    )

    forward shouldBe Vector((2, 2), (1, 1))
    reversed shouldBe Vector((2, 2), (1, 1))
  }

  "antiJoinOn" should "keep the left order whichever side is indexed" in {
    val left = Vector(3, 1, 2)
    val forward =
      rows(intDataset(left).antiJoinOn(intDataset(Vector(9, 8, 7)), key, key, ColumnType.IntType, ColumnType.IntType))
    val reversed = rows(
      intDataset(left).antiJoinOn(intDataset(Vector(9, 8, 7, 6)), key, key, ColumnType.IntType, ColumnType.IntType)
    )

    forward shouldBe left
    reversed shouldBe left
  }

  "keyed joins with null keys" should "agree with a null-intolerant predicate join" in {
    (0 until 60).foreach { _ =>
      val (leftValues, leftNulls) = nullableValues(rng.nextInt(13))
      val (rightValues, rightNulls) = nullableValues(rng.nextInt(13))
      checkNullableJoins(leftValues, leftNulls, rightValues, rightNulls)
    }
  }

  "keyed joins with null keys" should "agree across extreme size ratios" in {
    val (rightValues, rightNulls) = nullableValues(40)
    checkNullableJoins(Vector(0), BitSet(0), rightValues, rightNulls)
    checkNullableJoins(rightValues, rightNulls, Vector(0), BitSet(0))
  }
}
