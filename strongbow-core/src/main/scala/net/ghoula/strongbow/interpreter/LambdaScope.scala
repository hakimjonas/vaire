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

  /** A scope extended with one lambda-variable binding. */
  def updated(binder: Binder[?], column: Column[?]): LambdaScope =
    LambdaScope(bindings.updated(binder, column))

  /** The binding for the given lambda variable, if it is in scope. */
  def get(binder: Binder[?]): Option[Column[?]] = bindings.get(binder)
}

/** The scope of lambda-variable bindings for one evaluation. */
object LambdaScope {

  /** The empty scope at evaluation entry. */
  val empty: LambdaScope = LambdaScope(Map.empty)
}
