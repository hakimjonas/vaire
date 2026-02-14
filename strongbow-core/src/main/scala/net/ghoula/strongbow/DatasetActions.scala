package net.ghoula.strongbow

import net.ghoula.strongbow.errors.ExecutionError

/** Action methods that materialize Dataset results.
  *
  * Actions execute the Dataset plan and return values directly,
  * bypassing the lazy AST construction.
  */
object DatasetActions {
  extension [T](dataset: Dataset[T]) {

    /** Collect all elements into a Vector.
      *
      * Materializes the entire dataset. Use with caution on large datasets.
      */
    def collect: Either[ExecutionError, Vector[T]] = {
      DatasetInterpreter.execute(dataset).map(_.toVectorUnsafe)
    }

    /** Count the number of elements.
      *
      * More efficient than collect.map(_.length) as it avoids
      * materializing individual rows.
      */
    def count: Either[ExecutionError, Long] = {
      DatasetInterpreter.execute(dataset).map(_.rowCount.toLong)
    }

    /** Check if dataset is empty.
      *
      * Short-circuits on first row.
      */
    def isEmpty: Either[ExecutionError, Boolean] = {
      count.map(_ == 0)
    }

    /** Take first n elements.
      *
      * Convenience for limit(n).collect.
      */
    def take(n: Int): Either[ExecutionError, Vector[T]] = {
      dataset.limit(n).collect
    }

    /** Return pretty-printed AST for debugging.
      *
      * Shows the logical plan structure before execution.
      */
    def explain: String = {
      DatasetExplainer.explain(dataset)
    }

    /** Materialize and format first n rows as table.
      *
      * Useful for interactive development and debugging.
      */
    def show(n: Int = 20): Either[ExecutionError, String] = {
      DatasetInterpreter.execute(dataset.limit(n)).map { materialized =>
        val header = materialized.schema.columnNames.mkString(" | ")
        val separator = "-" * (materialized.schema.columnNames.map(_.length).sum + (materialized.schema.columnCount - 1) * 3)
        val rows = (0 until materialized.rowCount).map { rowIdx =>
          materialized.columns.map(_.getValue(rowIdx)).mkString(" | ")
        }.mkString("\n")

        s"$header\n$separator\n$rows"
      }
    }
  }
}
