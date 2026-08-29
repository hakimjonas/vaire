package net.ghoula.strongbow.params

import net.ghoula.strongbow.column.ColumnType
import net.ghoula.strongbow.expr.Expr

/** Typed aggregation specification for GROUP BY aggregations. */
sealed trait AggSpec[T] {
  type K
  val name: String
  val expr: Expr[T, K]
  val columnType: ColumnType
}

object AggSpec {
  def apply[T, K0](n: String, e: Expr[T, K0], ct: ColumnType): AggSpec[T] = {
    new AggSpec[T] {
      type K = K0
      val name: String = n
      val expr: Expr[T, K0] = e
      val columnType: ColumnType = ct
    }
  }
}
