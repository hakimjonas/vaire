package net.ghoula.strongbow.params

import net.ghoula.strongbow.column.ColumnType
import net.ghoula.strongbow.expr.Expr

/** Typed key specification for GROUP BY / PARTITION BY expressions. */
sealed trait KeySpec[T] {
  type K
  val name: String
  val expr: Expr[T, K]
  val columnType: ColumnType
}

object KeySpec {
  def apply[T, K0](n: String, e: Expr[T, K0], ct: ColumnType): KeySpec[T] = {
    new KeySpec[T] {
      type K = K0
      val name = n
      val expr = e
      val columnType = ct
    }
  }
}
