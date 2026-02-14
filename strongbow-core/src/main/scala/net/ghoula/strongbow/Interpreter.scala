package net.ghoula.strongbow

import net.ghoula.strongbow.errors.ExecutionError

/** Shared contract for dataset execution backends.
  *
  * Both the in-memory columnar interpreter and the Spark interpreter implement this trait, enabling
  * pluggable execution via `given Interpreter`.
  */
trait Interpreter {
  def execute[T](dataset: Dataset[T]): Either[ExecutionError, MaterializedDataset[T]]
}
