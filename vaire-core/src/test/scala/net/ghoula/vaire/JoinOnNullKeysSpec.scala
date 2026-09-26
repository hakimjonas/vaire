package net.ghoula.vaire

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.BitSet

import net.ghoula.vaire.prelude.*
import net.ghoula.vaire.types.{ColumnIndex, RowIndex}

/** Locks the SQL-NULL-key semantics of the keyed-join family to Spark 4.2.
  *
  * Spark 4.2 runs with `spark.sql.ansi.enabled = true` by default, and `EqualTo` is
  * null-intolerant: a null on either side of the comparison yields no match. The in-memory keyed
  * joins follow the same rule, so a null key on one side never joins to a null key on the other.
  * Nullable keys are `Option[T]` fields stored as a single nullable column; the value type is `T`.
  */
class JoinOnNullKeysSpec extends AnyFlatSpec with Matchers {

  private def intDataset(values: Array[Int], nulls: BitSet): Dataset[Option[Int]] =
    Dataset.fromColumns(Vector(Column.int(values, nulls)), Schema.optionSchema[Int]).toOption.get

  private def intKey: Expr.Cell[Option[Int], Int] =
    Expr.Cell[Option[Int], Int]("value", ColumnIndex(0))

  "innerJoinOn" should "not match null keys to null keys on the primitive fast path" in {
    val left = intDataset(Array(7, 9), BitSet(0)) // [null, 9]
    val right = intDataset(Array(9, 11), BitSet(1)) // [9, null]

    val joined = left.joinOn(right, intKey, intKey, ColumnType.IntType, ColumnType.IntType)

    val result = DatasetInterpreter.execute(joined).toOption.get
    // Only 9 = 9 matches; neither null key matches.
    result.rowCount shouldBe 1
    result.columns(0).getValue(0) shouldBe 9
    result.columns(1).getValue(0) shouldBe 9
  }

  it should "not match null keys to null keys on the object path" in {
    val left = Dataset
      .fromColumns(
        Vector(Column.string(Array[String | Null]("a", null, "b"), BitSet(1))), // scalafix:ok DisableSyntax.null
        Schema.optionSchema[String]
      )
      .toOption
      .get
    val right = Dataset
      .fromColumns(
        Vector(Column.string(Array[String | Null]("b", null), BitSet(1))), // scalafix:ok DisableSyntax.null
        Schema.optionSchema[String]
      )
      .toOption
      .get

    val key = Expr.Cell[Option[String], String]("value", ColumnIndex(0))
    val joined = left.joinOn(right, key, key, ColumnType.StringType, ColumnType.StringType)

    val result = DatasetInterpreter.execute(joined).toOption.get
    result.rowCount shouldBe 1
    result.columns(0).getValue(0) shouldBe "b"
    result.columns(1).getValue(0) shouldBe "b"
  }

  "semiJoinOn and antiJoinOn" should "treat null keys as matching nothing" in {
    val left = intDataset(Array(1, 2, 3), BitSet(0)) // [null, 2, 3]
    val right = intDataset(Array(2, 4), BitSet(1)) // [2, null]

    val semi = DatasetInterpreter
      .execute(left.semiJoinOn(right, intKey, intKey, ColumnType.IntType, ColumnType.IntType))
      .toOption
      .get
    val semiCol = semi.columns(0)
    // right keys are {2, null}; only left 2 matches.
    semi.rowCount shouldBe 1
    semiCol.getValue(0) shouldBe 2

    val anti = DatasetInterpreter
      .execute(left.antiJoinOn(right, intKey, intKey, ColumnType.IntType, ColumnType.IntType))
      .toOption
      .get
    val antiCol = anti.columns(0)
    // left null and 3 have no match; 2 is excluded.
    anti.rowCount shouldBe 2
    antiCol.isNull(RowIndex(0)) shouldBe true
    antiCol.getValue(1) shouldBe 3
  }
}
