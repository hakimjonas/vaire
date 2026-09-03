package net.ghoula.strongbow.params

import net.ghoula.strongbow.column.ColumnType
import net.ghoula.strongbow.expr.Expr

/** Typed key specification for GROUP BY / PARTITION BY expressions. */
sealed trait KeySpec[T] {

  /** The key expression's type. */
  type K

  /** The output column name. */
  val name: String

  /** The grouping key expression. */
  val expr: Expr[T, K]

  /** The output column's logical type. */
  val columnType: ColumnType
}

/** Construction of typed specs with an abstract key type. */
object KeySpec {

  /** A spec from a name, expression and output type. */
  def apply[T, K0](n: String, e: Expr[T, K0], ct: ColumnType): KeySpec[T] = {
    new KeySpec[T] {
      type K = K0
      val name: String = n
      val expr: Expr[T, K0] = e
      val columnType: ColumnType = ct
    }
  }
}
