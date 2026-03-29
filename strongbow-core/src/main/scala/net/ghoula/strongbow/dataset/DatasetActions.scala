package net.ghoula.strongbow.dataset

import net.ghoula.strongbow.errors.ExecutionError
import net.ghoula.strongbow.interpreter.{DatasetInterpreter, Interpreter}

/** Action methods that materialize Dataset results.
  *
  * Actions execute the Dataset plan and return values directly, bypassing the lazy AST
  * construction.
  *
  * The interpreter defaults to the in-memory columnar DatasetInterpreter. Users opt into Spark via
  * `given Interpreter = SparkInterpreter(spark)`.
  */
object DatasetActions {
  extension [T](dataset: Dataset[T]) {

    /** Collect all elements into a Vector.
      *
      * Materializes the entire dataset. Use with caution on large datasets.
      */
    def collect(using interpreter: Interpreter = DatasetInterpreter): Either[ExecutionError, Vector[T]] = {
      interpreter.execute(dataset).map(_.toVectorUnsafe)
    }

    /** Count the number of elements.
      *
      * More efficient than collect.map(_.length) as it avoids materializing individual rows.
      */
    def count(using interpreter: Interpreter = DatasetInterpreter): Either[ExecutionError, Long] = {
      interpreter.execute(dataset).map(_.rowCount.toLong)
    }

    /** Check if the dataset is empty.
      *
      * Short-circuits on the first row.
      */
    def isEmpty(using interpreter: Interpreter = DatasetInterpreter): Either[ExecutionError, Boolean] = {
      count.map(_ == 0)
    }

    /** Take first n elements.
      *
      * Convenience for limit(n).collect.
      */
    def take(n: Int)(using interpreter: Interpreter = DatasetInterpreter): Either[ExecutionError, Vector[T]] = {
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
    def show(n: Int = 20)(using interpreter: Interpreter = DatasetInterpreter): Either[ExecutionError, String] = {
      interpreter.execute(dataset.limit(n)).map { materialized =>
        val header = materialized.schema.columnNames.mkString(" | ")
        val separator =
          "-" * (materialized.schema.columnNames.map(_.length).sum + (materialized.schema.columnCount - 1) * 3)
        val rows = (0 until materialized.rowCount).map { rowIdx =>
          materialized.columns.map(_.getValue(rowIdx)).mkString(" | ")
        }.mkString("\n")

        s"$header\n$separator\n$rows"
      }
    }
  }
}
