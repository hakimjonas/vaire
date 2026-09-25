package net.ghoula.vaire

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.BitSet
import scala.collection.mutable.ArrayBuffer

import net.ghoula.vaire.column.Column
import net.ghoula.vaire.internal.KeyIndex

/** Cross-type probe semantics of the keyed-join index.
  *
  * The fast path matches a probe key only when the probed column has the index's own `ColumnType`,
  * reproducing boxed-`Any` equality where an `Integer` and a `Long` never equal each other. The one
  * exception is null keys, which are checked before the type gate, so nulls still match across a
  * type mismatch — the old boxed index treated `null` as a single shared `Any` key.
  */
class KeyIndexCrossTypeSpec extends AnyFlatSpec with Matchers {

  "A primitive index" should "match nothing for a non-null probe of a different key type" in {
    val idx = KeyIndex.build(Column.int(Array(1, 2, 3)), 3)
    val received = ArrayBuffer.empty[Int]

    idx.probe(Column.long(Array(2L)), 0)(received += _) shouldBe false
    received shouldBe empty

    idx.probe(Column.int(Array(0, 2)), 1)(received += _) shouldBe true
    received.toList shouldBe List(1)
  }

  it should "match null keys across a type mismatch, before the type gate" in {
    val idx = KeyIndex.build(Column.int(Array(1, 2), BitSet(0)), 2)
    val received = ArrayBuffer.empty[Int]

    idx.probe(Column.long(Array(7L), BitSet(0)), 0)(received += _) shouldBe true
    received.toList shouldBe List(0)

    received.clear()
    idx.probe(Column.long(Array(0L, 2L)), 1)(received += _) shouldBe false
    received shouldBe empty
  }
}
