package net.ghoula.strongbow.interpreter

import net.ghoula.strongbow.column.Column
import net.ghoula.strongbow.errors.ExecutionError

/** Per-row error handling policy for the in-memory interpreter.
  *
  * `FailFast` is the default (and the Spark contract): the first per-row failure aborts the
  * evaluation with its error. `Collect(maxErrors)` instead null-marks failed rows and records the
  * failures — bounded by `maxErrors`, with a `truncated` flag past the bound — so one run surfaces
  * every offending row. Structural errors (wrong column type, unbound lambda variables,
  * untyped-column misuse) fail under every policy: they are usage errors, not data errors. On the
  * Spark backend the policy does not apply — Spark fails its own queries.
  */
enum ErrorPolicy derives CanEqual {
  case FailFast
  case Collect(maxErrors: Int)
}

/** The result of a Collect-policy evaluation: the value column with failed rows null-marked, the
  * bounded per-row error list, whether the list was truncated at `maxErrors`, and a by-kind summary
  * of every recorded error (complete even when the raw list is truncated).
  */
final case class Collected(
  values: Column[?],
  errors: Vector[(Int, ExecutionError)],
  truncated: Boolean,
  byKind: Map[String, Int]
)

/** Records per-row errors during one evaluation. FailFast discards them (the arm aborts with the
  * first); Collect records them bounded by `maxErrors`.
  */
sealed trait RowErrors {
  def add(row: Int, err: ExecutionError): Unit
  def isCollect: Boolean
  def result: (Vector[(Int, ExecutionError)], Boolean, Map[String, Int])
}

object RowErrors {

  def apply(policy: ErrorPolicy): RowErrors = policy match {
    case ErrorPolicy.FailFast => failFast
    case ErrorPolicy.Collect(maxErrors) => collecting(maxErrors)
  }

  val failFast: RowErrors = new RowErrors {
    def add(row: Int, err: ExecutionError): Unit = ()
    def isCollect: Boolean = false
    def result: (Vector[(Int, ExecutionError)], Boolean, Map[String, Int]) = (Vector.empty, false, Map.empty)
  }

  def collecting(maxErrors: Int): RowErrors = new RowErrors {
    private val entries =
      new java.util.concurrent.atomic.AtomicReference(Vector.empty[(Int, ExecutionError)])
    private val truncatedFlag = new java.util.concurrent.atomic.AtomicBoolean(false)
    private val kindCounts =
      new java.util.concurrent.atomic.AtomicReference(Map.empty[String, Int])

    def add(row: Int, err: ExecutionError): Unit = {
      val current = entries.get
      if current.size < maxErrors then entries.compareAndSet(current, current :+ ((row, err)))
      else truncatedFlag.set(true)
      val kind = err.productPrefix
      val kc = kindCounts.get
      kindCounts.compareAndSet(kc, kc.updated(kind, kc.getOrElse(kind, 0) + 1))
    }

    def isCollect: Boolean = true

    def result: (Vector[(Int, ExecutionError)], Boolean, Map[String, Int]) =
      (entries.get, truncatedFlag.get, kindCounts.get)
  }
}
