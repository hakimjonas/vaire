package net.ghoula.strongbow.params

import net.ghoula.strongbow.column.ColumnType
import net.ghoula.strongbow.expr.Expr

/** Typed window expression specification. */
sealed trait WindowExprSpec[T] {
  type K
  val name: String
  val expr: Expr[T, K]
  val columnType: ColumnType
}

object WindowExprSpec {
  def apply[T, K0](n: String, e: Expr[T, K0], ct: ColumnType): WindowExprSpec[T] = {
    new WindowExprSpec[T] {
      type K = K0
      val name = n
      val expr = e
      val columnType = ct
    }
  }
}
