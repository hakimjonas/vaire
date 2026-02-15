package net.ghoula.strongbow.specs

import net.ghoula.strongbow.{ColumnType, Expr}

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
      val name = n
      val expr = e
      val columnType = ct
    }
  }
}
