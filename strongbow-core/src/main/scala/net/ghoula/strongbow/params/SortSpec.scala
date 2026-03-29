package net.ghoula.strongbow.params

import net.ghoula.strongbow.column.ColumnType
import net.ghoula.strongbow.expr.Expr

/** Typed sort specification linking Expr[T, K] with Ordering[K] evidence.
  *
  * The abstract type member `K` enables `Vector[SortSpec[T]]` where each element has a different
  * key type, while the compiler still enforces that each element's `ordering` is consistent with
  * its `expr`.
  */
sealed trait SortSpec[T] {
  type K
  val expr: Expr[T, K]
  val ordering: Ordering[K]
  val columnType: ColumnType
  val ascending: Boolean
}

object SortSpec {
  def apply[T, K0](
    e: Expr[T, K0],
    ord: Ordering[K0],
    ct: ColumnType,
    asc: Boolean
  ): SortSpec[T] = {
    new SortSpec[T] {
      type K = K0
      val expr: Expr[T, K0] = e
      val ordering: Ordering[K0] = ord
      val columnType: ColumnType = ct
      val ascending: Boolean = asc
    }
  }
}
