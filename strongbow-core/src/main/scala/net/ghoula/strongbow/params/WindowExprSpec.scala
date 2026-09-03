package net.ghoula.strongbow.params

import net.ghoula.strongbow.column.ColumnType
import net.ghoula.strongbow.expr.Expr

/** Typed window expression specification. */
sealed trait WindowExprSpec[T] {

  /** The key expression's type. */
  type K

  /** The output column name. */
  val name: String

  /** The window function expression. */
  val expr: Expr[T, K]

  /** The output column's logical type. */
  val columnType: ColumnType
}

/** Construction of typed specs with an abstract key type. */
object WindowExprSpec {

  /** A spec from a name, expression and output type. */
  def apply[T, K0](n: String, e: Expr[T, K0], ct: ColumnType): WindowExprSpec[T] = {
    new WindowExprSpec[T] {
      type K = K0
      val name: String = n
      val expr: Expr[T, K0] = e
      val columnType: ColumnType = ct
    }
  }
}
