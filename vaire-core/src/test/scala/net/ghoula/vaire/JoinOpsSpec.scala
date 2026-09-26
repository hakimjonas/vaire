package net.ghoula.vaire

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.internal.JoinOps

/** Covers `JoinOps.fullJoin` directly.
  *
  * The predicate full join tracks unmatched rows per row, not by value equality, so a row that is
  * equal to a matched row but does not match the condition is still emitted as unmatched. `Tagged`
  * has an `equals` on `id` only while the condition compares `tag`, which separates the two rules.
  */
class JoinOpsSpec extends AnyFlatSpec with Matchers {

  private final case class Tagged(id: Int, tag: String) {
    override def equals(other: Any): Boolean = other match {
      case that: Tagged => that.id == id
      case _ => false
    }
    override def hashCode(): Int = id
  }

  private def tags(result: Vector[(Option[Tagged], Option[Tagged])]): Vector[(Option[String], Option[String])] =
    result.map { case (left, right) => (left.map(_.tag), right.map(_.tag)) }

  "fullJoin" should "emit an unmatched row that is value-equal to a matched row" in {
    val lefts = Vector(Tagged(1, "x"), Tagged(1, "y"))
    val rights = Vector(Tagged(1, "x"))

    val result = JoinOps.fullJoin(lefts, rights, (left: Tagged, right: Tagged) => left.tag == right.tag)

    tags(result) shouldBe Vector((Some("x"), Some("x")), (Some("y"), None))
  }

  "fullJoin" should "preserve the input order and duplicate matches" in {
    val lefts = Vector(1, 2, 2)
    val rights = Vector(2, 3)

    val result = JoinOps.fullJoin(lefts, rights, (left: Int, right: Int) => left == right)

    result shouldBe Vector(
      (Some(2), Some(2)),
      (Some(2), Some(2)),
      (Some(1), None),
      (None, Some(3))
    )
  }
}
