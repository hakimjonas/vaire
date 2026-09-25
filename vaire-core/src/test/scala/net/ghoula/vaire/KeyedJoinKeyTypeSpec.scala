package net.ghoula.vaire

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.BitSet
import scala.collection.mutable.ArrayBuffer

import net.ghoula.vaire.column.{Column, ColumnType}
import net.ghoula.vaire.dataset.MaterializedDataset
import net.ghoula.vaire.errors.ExecutionError
import net.ghoula.vaire.internal.KeyIndex
import net.ghoula.vaire.prelude.*
import net.ghoula.vaire.types.ColumnIndex

/** Locks the key-type contract of the keyed-join family.
  *
  * The typed API's shared `K` says both keys have the same Scala type, but `ColumnType` travels as
  * a runtime value, so both backends validate that the two key columns have the same `ColumnType`
  * and fail with `ExecutionError.TypeMismatch` otherwise (see
  * [[net.ghoula.vaire.internal.KeyedJoin]]). This mirrors Spark 4.2 rejecting incompatible key
  * types instead of silently returning no rows.
  *
  * The specs also pin the fast-path raw-key coverage (`FastTypes` must stay in sync with
  * `KeyIndex.rawKey`) and inner-join symmetry.
  */
class KeyedJoinKeyTypeSpec extends AnyFlatSpec with Matchers {

  private def dsInt(values: Int*): Dataset[Int] =
    Dataset.fromColumns(Vector(Column.int(values.toArray)), Schema.intSchema).toOption.get

  private def dsLong(values: Long*): Dataset[Long] =
    Dataset.fromColumns(Vector(Column.long(values.toArray)), Schema.longSchema).toOption.get

  private def intKey[T]: Expr.Cell[T, Int] =
    Expr.Cell[T, Int]("value", ColumnIndex(0))

  private def longKey[T]: Expr.Cell[T, Long] =
    Expr.Cell[T, Long]("value", ColumnIndex(0))

  private def intsOf(ds: MaterializedDataset[(Int, Int)], column: Int): Vector[Int] =
    ds.columns(column) match {
      case Column.IntColumn(data, _) => (0 until ds.rowCount).map(data).toVector
      case other => fail(s"expected IntColumn, got $other")
    }

  "keyed joins" should "reject mismatched key ColumnTypes with TypeMismatch" in {
    val longSide = dsLong(1L)
    val intSide = dsInt(1)

    val plan = longSide.joinOn(
      intSide,
      longKey[Long],
      intKey[Int],
      ColumnType.LongType,
      ColumnType.IntType
    )

    DatasetInterpreter.execute(plan) match {
      case Left(ExecutionError.TypeMismatch(expected, actual, _)) =>
        expected shouldBe "LongType"
        actual shouldBe "IntType"
      case other => fail(s"expected TypeMismatch, got $other")
    }
  }

  it should "reject mismatched key types for semi and anti too" in {
    val longSide = dsLong(1L)
    val intSide = dsInt(1)

    DatasetInterpreter
      .execute(longSide.semiJoinOn(intSide, longKey[Long], intKey[Int], ColumnType.LongType, ColumnType.IntType))
      .isLeft shouldBe true

    DatasetInterpreter
      .execute(longSide.antiJoinOn(intSide, longKey[Long], intKey[Int], ColumnType.LongType, ColumnType.IntType))
      .isLeft shouldBe true
  }

  "the key index" should "not match a null probe key on the primitive path" in {
    val index = KeyIndex.build(Column.int(Array(1, 2), BitSet(0)), 2) // [null, 2]
    val received = ArrayBuffer.empty[Int]

    index.probe(Column.int(Array(0, 1), BitSet(0)), 0)(received += _) shouldBe false
    received shouldBe empty

    index.probe(Column.int(Array(2)), 0)(received += _) shouldBe true
    received.toList shouldBe List(1)
  }

  it should "not match a null probe key on the object path" in {
    val nullProbe = Column.string(Array[String | Null](null), BitSet(0)) // scalafix:ok DisableSyntax.null
    val index = KeyIndex.build(
      Column.string(Array[String | Null](null, "x"), BitSet(0)), // scalafix:ok DisableSyntax.null
      2
    )

    index.probe(nullProbe, 0)(_ => ()) shouldBe false
  }

  it should "build and probe every fast type without a missing rawKey case" in {
    val samples: Vector[(ColumnType, Column[?])] = Vector(
      ColumnType.IntType -> Column.int(Array(1)),
      ColumnType.LongType -> Column.long(Array(1L)),
      ColumnType.ShortType -> Column.short(Array(1.toShort)),
      ColumnType.ByteType -> Column.byte(Array(1.toByte)),
      ColumnType.FloatType -> Column.float(Array(1.0f)),
      ColumnType.DoubleType -> Column.double(Array(1.0)),
      ColumnType.BooleanType -> Column.boolean(Array(true)),
      ColumnType.DateType -> Column.date(Array(1)),
      ColumnType.TimestampType -> Column.timestamp(Array(1L)),
      ColumnType.TimestampNTZType -> Column.timestampNTZ(Array(1L))
    )

    samples.foreach { case (columnType, column) =>
      withClue(s"fast type $columnType: ") {
        KeyIndex.FastTypes should contain(columnType)
        val received = ArrayBuffer.empty[Int]
        KeyIndex.build(column, 1).probe(column, 0)(received += _) shouldBe true
        received.toList shouldBe List(0)
      }
    }
  }

  "innerJoinOn" should "be symmetric" in {
    val left = dsInt(1, 2, 2, 3)
    val right = dsInt(2, 3, 4)

    val ab = DatasetInterpreter
      .execute(left.joinOn(right, intKey[Int], intKey[Int], ColumnType.IntType, ColumnType.IntType))
      .toOption
      .get
    val ba = DatasetInterpreter
      .execute(right.joinOn(left, intKey[Int], intKey[Int], ColumnType.IntType, ColumnType.IntType))
      .toOption
      .get

    val abPairs = intsOf(ab, 0).zip(intsOf(ab, 1))
    val baPairs = intsOf(ba, 0).zip(intsOf(ba, 1)).map { case (l, r) => (r, l) }

    abPairs.sorted.shouldBe(baPairs.sorted)
  }
}
