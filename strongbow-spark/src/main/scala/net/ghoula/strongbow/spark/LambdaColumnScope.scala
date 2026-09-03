package net.ghoula.strongbow.spark

import org.apache.spark.sql.{Column => SparkColumn}

import net.ghoula.strongbow.expr.Binder

/** Lambda-variable bindings for the Spark conversion of a higher-order expression body, mirroring
  * [[net.ghoula.strongbow.interpreter.LambdaScope]]: higher-order arms bind each
  * [[net.ghoula.strongbow.expr.Binder]] to the fresh lambda-variable column handed back by Spark's
  * `functions.*` callback, and `LambdaVar` converts to that column.
  */
final case class LambdaColumnScope private[spark] (bindings: Map[Binder[?], SparkColumn]) {

  /** A scope extended with one lambda-variable binding to a Spark column. */
  def updated(binder: Binder[?], column: SparkColumn): LambdaColumnScope =
    LambdaColumnScope(bindings.updated(binder, column))

  /** The binding for the given lambda variable, if it is in scope. */
  def get(binder: Binder[?]): Option[SparkColumn] = bindings.get(binder)
}

/** The Spark-side scope of lambda-variable bindings for one compilation. */
object LambdaColumnScope {

  /** The empty scope at compilation entry. */
  val empty: LambdaColumnScope = LambdaColumnScope(Map.empty)
}
