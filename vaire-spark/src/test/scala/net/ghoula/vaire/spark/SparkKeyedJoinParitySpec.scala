package net.ghoula.vaire.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.BitSet

import net.ghoula.vaire.prelude.*

/** Backend-parity checks for the keyed-join key contract.
  *
  * The in-memory interpreter and the Spark backend must agree on which keyed joins are valid: the
  * two key columns have to share a `ColumnType`. Both call
  * [[net.ghoula.vaire.internal.KeyedJoin.validateKeyTypes]], and these specs assert the Spark
  * wiring passes the types through. They also pin null-key parity: Spark 4.2's `EqualTo` is
  * null-intolerant, and the in-memory index now matches that.
  */
class SparkKeyedJoinParitySpec extends AnyFlatSpec with Matchers with SparkTestBase {

  private def intDs(values: Array[Int], nulls: BitSet = BitSet.empty): Dataset[Int] =
    Dataset.fromColumns(Vector(Column.int(values, nulls)), Schema.intSchema).toOption.get

  private def intKey: Expr.Cell[Int, Int] =
    Expr.Cell[Int, Int]("value", ColumnIndex(0))

  "keyed joins" should "agree between in-memory and Spark" in {
    val left = intDs(Array(1, 2, 2, 3))
    val right = intDs(Array(2, 3, 4))

    val plan = left.joinOn(right, intKey, intKey, ColumnType.IntType, ColumnType.IntType)

    val inMemory = DatasetInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)

    sparkResult.shouldBe(inMemory)
  }

  "keyed joins with null keys" should "agree between in-memory and Spark" in {
    val left = intDs(Array(7, 9), BitSet(0)) // [null, 9]
    val right = intDs(Array(9, 11), BitSet(1)) // [9, null]

    val plan = left.joinOn(right, intKey, intKey, ColumnType.IntType, ColumnType.IntType)

    val inMemory = DatasetInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)

    inMemory.shouldBe(Right(Vector((9, 9))))
    sparkResult.shouldBe(inMemory)
  }

  "keyed joins with mismatched key types" should "fail in both backends" in {
    val left = Dataset.fromColumns(Vector(Column.long(Array(1L))), Schema.longSchema).toOption.get
    val right = intDs(Array(1))

    val plan = left.joinOn(
      right,
      Expr.Cell[Long, Long]("value", ColumnIndex(0)),
      Expr.Cell[Int, Long]("value", ColumnIndex(0)),
      ColumnType.LongType,
      ColumnType.IntType
    )

    DatasetInterpreter.execute(plan).isLeft shouldBe true
    sparkInterpreter.execute(plan).isLeft shouldBe true
  }

  "keyed joins with a declared type that disagrees with the column" should "fail in both backends" in {
    val side = Dataset.fromColumns(Vector(Column.long(Array(1L))), Schema.longSchema).toOption.get

    val plan = side.joinOn(
      side,
      Expr.Cell[Long, Long]("value", ColumnIndex(0)),
      Expr.Cell[Long, Long]("value", ColumnIndex(0)),
      ColumnType.IntType,
      ColumnType.IntType
    )

    DatasetInterpreter.execute(plan).isLeft shouldBe true
    sparkInterpreter.execute(plan).isLeft shouldBe true
  }
}
