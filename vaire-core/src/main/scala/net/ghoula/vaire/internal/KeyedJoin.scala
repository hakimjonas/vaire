package net.ghoula.vaire.internal

import net.ghoula.vaire.column.ColumnType
import net.ghoula.vaire.errors.ExecutionError

/** Key-type contract for the keyed-join family.
  *
  * A keyed join indexes one side's key column and probes it with the other side's, so the two key
  * columns must have the same logical `ColumnType`. The typed API's shared `K` expresses this at
  * the Scala level, but `ColumnType` is carried as a runtime value, so both backends validate it
  * here rather than trusting the caller. A mismatch is a caller error and is surfaced as an
  * `ExecutionError.TypeMismatch` instead of silently producing an empty result.
  *
  * Spark 4.2 (`spark.sql.ansi` is on by default) implicitly coerces some pairs (e.g. Int vs Long)
  * and rejects others (e.g. Long vs Timestamp). Requiring an explicit cast to a common type keeps
  * the typed API honest and the in-memory and Spark backends in agreement; see
  * `docs/join-optimization-plan.md`.
  */
private[vaire] object KeyedJoin {

  /** `None` when the two key types match, else the error to surface. */
  def validateKeyTypes(left: ColumnType, right: ColumnType): Option[ExecutionError] =
    if (left == right) None
    else
      Some(
        ExecutionError.TypeMismatch(
          expected = left.toString,
          actual = right.toString,
          context =
            "keyed join: left and right key columns must have the same ColumnType (cast one side explicitly to widen)"
        )
      )
}
