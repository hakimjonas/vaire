package net.ghoula.vaire.spark

/** Test-only SQL-NULL placeholder.
  *
  * The single null literal of the test suite: represents a SQL NULL cell whose absence is
  * authoritatively tracked by the column's null BitSet, mirroring the production idiom.
  */
object SqlNull {
  def value: Any | Null = null // scalafix:ok DisableSyntax.null
}
