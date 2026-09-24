package net.ghoula.vaire

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.prelude.*
import net.ghoula.vaire.types.{ColumnIndex, RowIndex}

/** Locks the SQL-NULL-key semantics of the keyed-join family.
  *
  * The keyed index treats every null key as one shared key, so null rows on both sides join to each
  * other. These specs pin that behavior for the primitive fast path (Int) and the object path
  * (String), where the interpreter keeps dedicated null buckets.
  */
class JoinOnNullKeysSpec extends AnyFlatSpec with Matchers {

  private def intDataset(values: Array[Int], nulls: scala.collection.immutable.BitSet): Dataset[Int] =
    Dataset
      .fromColumns(Vector(Column.int(values, nulls)), Schema.intSchema)
      .toOption
      .get

  private def intKey: Expr.Cell[Int, Int] =
    Expr.Cell[Int, Int]("value", ColumnIndex(0))

  "innerJoinOn" should "match null keys to null keys on the primitive fast path" in {
    val left = intDataset(Array(7, 9), scala.collection.immutable.BitSet(0))
    val right = intDataset(Array(9, 11), scala.collection.immutable.BitSet(1))

    val joined = left.joinOn(
      right,
      intKey,
      intKey,
      ColumnType.IntType,
      ColumnType.IntType
    )

    val result = DatasetInterpreter.execute(joined).toOption.get
    // Right-major probe order: 9 matches 9 first, then the null keys match each other.
    result.rowCount shouldBe 2
    val outLeft = result.columns(0)
    val outRight = result.columns(1)
    outLeft.getValue(0) shouldBe 9
    outRight.getValue(0) shouldBe 9
    outLeft.isNull(RowIndex(1)) shouldBe true
    outRight.isNull(RowIndex(1)) shouldBe true
  }

  it should "match null keys to null keys on the object path" in {
    case class Row(value: String)
    given Schema[Row] = Schema.derived

    val leftCol = Column.string(Array[String | Null]("a", null, "b"), scala.collection.immutable.BitSet(1))
    val rightCol = Column.string(Array[String | Null]("b", null), scala.collection.immutable.BitSet(1))
    val left = Dataset.fromColumns(Vector(leftCol), summon[Schema[Row]]).toOption.get
    val right = Dataset.fromColumns(Vector(rightCol), summon[Schema[Row]]).toOption.get

    val joined = left.joinOn(
      right,
      Expr.Cell[Row, String]("value", ColumnIndex(0)),
      Expr.Cell[Row, String]("value", ColumnIndex(0)),
      ColumnType.StringType,
      ColumnType.StringType
    )

    val result = DatasetInterpreter.execute(joined).toOption.get
    result.rowCount shouldBe 2
    result.columns(0).getValue(0) shouldBe "b"
    result.columns(1).getValue(0) shouldBe "b"
    result.columns(0).isNull(RowIndex(1)) shouldBe true
    result.columns(1).isNull(RowIndex(1)) shouldBe true
  }

  "semiJoinOn and antiJoinOn" should "treat null keys as a shared key" in {
    val left = intDataset(Array(1, 2, 3), scala.collection.immutable.BitSet(0)) // [null, 2, 3]
    val right = intDataset(Array(2, 4), scala.collection.immutable.BitSet(1)) // [2, null]

    val semi = DatasetInterpreter
      .execute(left.semiJoinOn(right, intKey, intKey, ColumnType.IntType, ColumnType.IntType))
      .toOption
      .get
    val semiCol = semi.columns(0)
    // right keys are {2, null}, so left rows {null, 2} match.
    semi.rowCount shouldBe 2
    semiCol.isNull(RowIndex(0)) shouldBe true
    semiCol.getValue(1) shouldBe 2

    val anti = DatasetInterpreter
      .execute(left.antiJoinOn(right, intKey, intKey, ColumnType.IntType, ColumnType.IntType))
      .toOption
      .get
    val antiCol = anti.columns(0)
    anti.rowCount shouldBe 1
    antiCol.getValue(0) shouldBe 3
  }
}
