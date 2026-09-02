package net.ghoula.strongbow.interpreter

import net.ghoula.strongbow.column.Column
import net.ghoula.strongbow.dataset.MaterializedDataset
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

/** How much of the offending input value a quarantine entry carries alongside the reason.
  *
  * This is the payload channel from `docs/error-policy-design.md` §4: input data is leak-prone
  * (CWE-209), so previews are opt-in and never part of the log-safe `ExecutionError` message
  * channel. `Off` discards the input, `Truncated(maxChars)` keeps its prefix (the reason usually
  * lives there — an unclosed tag, a bad number), `Full` keeps it verbatim; a user-supplied redactor
  * is applied to whatever is kept.
  */
enum InputPreview derives CanEqual {
  case Off
  case Truncated(maxChars: Int)
  case Full
}

object InputPreview {

  /** Apply the preview knob to a raw input string, returning the payload-channel value. */
  def render(preview: InputPreview, raw: String, redact: String => String): Option[String] =
    preview match {
      case InputPreview.Off => None
      case InputPreview.Truncated(maxChars) => Some(redact(raw.take(maxChars)))
      case InputPreview.Full => Some(redact(raw))
    }
}

/** One quarantined row: the log-safe reason plus the opt-in input preview (payload channel). The
  * reason is safe to log or aggregate; the input preview is only present when the caller's preview
  * knob asked for it.
  */
final case class QuarantinedRow(error: ExecutionError, input: Option[String]) derives CanEqual

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

/** The dataset-level analogue of [[Collected]]: the materialized result of a policy-scoped plan
  * plus the errors collected across every expression evaluation inside the `withErrorPolicy` scope.
  * Row indices are positions within the operation that recorded the error (the column being
  * evaluated at that point), not global row numbers.
  */
final case class CollectedDataset[T](
  values: MaterializedDataset[T],
  errors: Vector[(Int, ExecutionError)],
  truncated: Boolean,
  byKind: Map[String, Int]
)

/** The result of a quarantine evaluation: the value column with failed rows null-marked, and the
  * dense per-row error column — an [[net.ghoula.strongbow.column.Column.AnyColumn AnyColumn]]
  * boxing [[QuarantinedRow]] at every failed row, null where the row succeeded. Unlike the Collect
  * list, the error column is complete: exactly one entry per failed row, with no bound, so
  * dead-letter routing never silently drops reasons.
  */
final case class Quarantined(values: Column[?], errors: Column[?])

/** Records per-row errors during one evaluation. FailFast discards them (the arm aborts with the
  * first); Collect records them bounded by `maxErrors`; the quarantine recorder keeps every entry.
  * `input` is the offending row's input on the payload channel — never forced by FailFast or
  * Collect, rendered by the quarantine recorder per its preview knob.
  */
sealed trait RowErrors {
  def add(row: Int, err: ExecutionError, input: => String): Unit
  def isCollect: Boolean
  def result: (Vector[(Int, ExecutionError)], Boolean, Map[String, Int])

  /** The complete per-row quarantine records; empty outside quarantine mode. */
  def quarantineResult: Vector[(Int, QuarantinedRow)]
}

object RowErrors {

  def apply(policy: ErrorPolicy): RowErrors = policy match {
    case ErrorPolicy.FailFast => failFast
    case ErrorPolicy.Collect(maxErrors) => collecting(maxErrors)
  }

  val failFast: RowErrors = new RowErrors {
    def add(row: Int, err: ExecutionError, input: => String): Unit = ()
    def isCollect: Boolean = false
    def result: (Vector[(Int, ExecutionError)], Boolean, Map[String, Int]) = (Vector.empty, false, Map.empty)
    def quarantineResult: Vector[(Int, QuarantinedRow)] = Vector.empty
  }

  def collecting(maxErrors: Int): RowErrors = new RowErrors {
    private val entries =
      new java.util.concurrent.atomic.AtomicReference(Vector.empty[(Int, ExecutionError)])
    private val truncatedFlag = new java.util.concurrent.atomic.AtomicBoolean(false)
    private val kindCounts =
      new java.util.concurrent.atomic.AtomicReference(Map.empty[String, Int])

    def add(row: Int, err: ExecutionError, input: => String): Unit = {
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

    def quarantineResult: Vector[(Int, QuarantinedRow)] = Vector.empty
  }

  /** The quarantine recorder: every per-row failure is kept — no bound, exactly one entry per
    * failed row — with its input rendered through the preview knob and redactor.
    */
  def quarantining(preview: InputPreview, redact: String => String): RowErrors = new RowErrors {
    private val entries =
      new java.util.concurrent.atomic.AtomicReference(Vector.empty[(Int, QuarantinedRow)])

    def add(row: Int, err: ExecutionError, input: => String): Unit = {
      val entry = QuarantinedRow(err, InputPreview.render(preview, input, redact))
      val current = entries.get
      entries.compareAndSet(current, current :+ ((row, entry)))
      ()
    }

    def isCollect: Boolean = true

    def result: (Vector[(Int, ExecutionError)], Boolean, Map[String, Int]) = (Vector.empty, false, Map.empty)

    def quarantineResult: Vector[(Int, QuarantinedRow)] = entries.get
  }
}
