package net.ghoula.vaire.spark

/** Spark module prelude — import this alongside the core prelude.
  *
  * Usage: {{{import net.ghoula.vaire.prelude.* import net.ghoula.vaire.spark.prelude.*}}}
  */
object prelude {
  export net.ghoula.vaire.prelude.*
  export net.ghoula.vaire.spark.{SparkInterpreter, SchemaConverter, RowConverter, ExprToColumn}
}
