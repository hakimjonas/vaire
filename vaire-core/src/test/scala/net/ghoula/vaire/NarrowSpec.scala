package net.ghoula.vaire

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.BitSet

import net.ghoula.vaire.errors.ExecutionError
import net.ghoula.vaire.prelude.*

/** Locks `narrow`: reinterpreting an optional schema as non-optional, asserting no nulls. */
class NarrowSpec extends AnyFlatSpec with Matchers {

  "narrow" should "reinterpret an optional schema as non-optional when no nulls are present" in {
    val ds = Dataset.fromColumns(Vector(Column.int(Array(1, 2))), Schema.optionSchema[Int]).toOption.get
    DatasetInterpreter.execute(ds.narrow[Int]).toOption.get.toVectorUnsafe shouldBe Vector(1, 2)
  }

  it should "fail when a field declared non-optional holds a null" in {
    val ds = Dataset
      .fromColumns(Vector(Column.int(Array(1, 0), BitSet(1))), Schema.optionSchema[Int])
      .toOption
      .get
    DatasetInterpreter.execute(ds.narrow[Int]) match {
      case Left(_: ExecutionError.InvalidPlan) => ()
      case other => fail(s"expected InvalidPlan, got $other")
    }
  }
}
