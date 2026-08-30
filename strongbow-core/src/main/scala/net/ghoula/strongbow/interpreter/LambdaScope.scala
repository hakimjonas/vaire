package net.ghoula.strongbow.interpreter

import net.ghoula.strongbow.column.Column
import net.ghoula.strongbow.expr.Binder

/** Lambda-variable bindings for the currently evaluated higher-order expression body.
  *
  * The interpreter and the Spark converter both thread this implicitly, so ordinary expression arms
  * need no changes; only higher-order arms add entries, and `LambdaVar` looks its binder up here. A
  * binder that is not present means the variable was used outside the higher-order expression that
  * binds it.
  */
final case class LambdaScope private[interpreter] (bindings: Map[Binder[?], Column[?]]) {

  def updated(binder: Binder[?], column: Column[?]): LambdaScope =
    LambdaScope(bindings.updated(binder, column))

  def get(binder: Binder[?]): Option[Column[?]] = bindings.get(binder)
}

object LambdaScope {
  val empty: LambdaScope = LambdaScope(Map.empty)
}
