package net.ghoula.vaire.params

import net.ghoula.vaire.column.ColumnType
import net.ghoula.vaire.expr.Expr

/** Typed aggregation specification for GROUP BY aggregations. */
sealed trait AggSpec[T] {

  /** The aggregation expression's result type. */
  type K

  /** The output column name. */
  val name: String

  /** The aggregation expression. */
  val expr: Expr[T, K]

  /** The output column's logical type. */
  val columnType: ColumnType
}

/** Construction of typed specs with an abstract key type. */
object AggSpec {

  /** A spec from a name, expression and output type. */
  def apply[T, K0](n: String, e: Expr[T, K0], ct: ColumnType): AggSpec[T] = {
    new AggSpec[T] {
      type K = K0
      val name: String = n
      val expr: Expr[T, K0] = e
      val columnType: ColumnType = ct
    }
  }
}
