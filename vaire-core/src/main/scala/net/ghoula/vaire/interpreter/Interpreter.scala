package net.ghoula.vaire.interpreter

import net.ghoula.vaire.dataset.{Dataset, MaterializedDataset}
import net.ghoula.vaire.errors.ExecutionError

/** Shared contract for dataset execution backends.
  *
  * Both the in-memory columnar interpreter and the Spark interpreter implement this trait, enabling
  * pluggable execution via `given Interpreter`.
  */
trait Interpreter {

  /** Execute the plan to a materialized result (fail-fast semantics). */
  def execute[T](dataset: Dataset[T]): Either[ExecutionError, MaterializedDataset[T]]

  /** Execute a policy-scoped plan (one carrying a `withErrorPolicy` node), returning the
    * materialized values alongside the per-row errors recorded inside the scope. Backends that
    * cannot surface per-row errors (Spark) reject the plan instead.
    */
  def executeCollect[T](dataset: Dataset[T]): Either[ExecutionError, CollectedDataset[T]]
}
