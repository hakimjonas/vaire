package net.ghoula.strongbow.spark

/** Spark module prelude — import this alongside the core prelude.
  *
  * Usage: {{{
  * import net.ghoula.strongbow.prelude.*
  * import net.ghoula.strongbow.spark.prelude.*
  * }}}
  */
object prelude {
  export net.ghoula.strongbow.prelude.*
  export net.ghoula.strongbow.spark.{SparkInterpreter, SchemaConverter, RowConverter, ExprToColumn}
}
