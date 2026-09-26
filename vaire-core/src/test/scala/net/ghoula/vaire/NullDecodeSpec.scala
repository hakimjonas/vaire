package net.ghoula.vaire

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.BitSet

import net.ghoula.vaire.column.Column
import net.ghoula.vaire.dataset.MaterializedDataset
import net.ghoula.vaire.errors.{DecodeError, ExecutionError}

/** Locks that decoding a null into a non-optional target returns `DecodeError.NullValue` rather
  * than dereferencing null. The columnar layer tracks nulls in a `BitSet` independently of the
  * schema, so a decoder can meet a null value even for a non-optional type; that is a data error,
  * not a crash.
  */
class NullDecodeSpec extends AnyFlatSpec with Matchers {

  private def withNull: MaterializedDataset[Int] =
    MaterializedDataset(Vector(Column.int(Array(1, 2), BitSet(0))), Schema.intSchema)

  "toVector" should "return NullValue instead of throwing for a null in a non-optional column" in {
    withNull.toVector.shouldBe(Vector(Left(DecodeError.NullValue(0)), Right(2)))
  }

  "toVectorOrError" should "surface the first decode failure as DecodeFailed" in {
    withNull.toVectorOrError.shouldBe(Left(ExecutionError.DecodeFailed(DecodeError.NullValue(0))))
  }
}
