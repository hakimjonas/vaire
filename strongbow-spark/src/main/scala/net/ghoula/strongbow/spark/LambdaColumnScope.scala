package net.ghoula.strongbow.spark

import org.apache.spark.sql.{Column => SparkColumn}

import net.ghoula.strongbow.expr.Binder

/** Lambda-variable bindings for the Spark conversion of a higher-order expression body, mirroring
  * [[net.ghoula.strongbow.interpreter.LambdaScope]]: higher-order arms bind each
  * [[net.ghoula.strongbow.expr.Binder]] to the fresh lambda-variable column handed back by Spark's
  * `functions.*` callback, and `LambdaVar` converts to that column.
  */
final case class LambdaColumnScope private[spark] (bindings: Map[Binder[?], SparkColumn]) {

  def updated(binder: Binder[?], column: SparkColumn): LambdaColumnScope =
    LambdaColumnScope(bindings.updated(binder, column))

  def get(binder: Binder[?]): Option[SparkColumn] = bindings.get(binder)
}

object LambdaColumnScope {
  given empty: LambdaColumnScope = LambdaColumnScope(Map.empty)
}
