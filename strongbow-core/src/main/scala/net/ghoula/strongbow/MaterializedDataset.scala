package net.ghoula.strongbow

import net.ghoula.strongbow.errors.ExecutionError

/** Result of executing a Dataset plan.
  *
  * MaterializedDataset represents the concrete output after interpreting a Dataset. It wraps the
  * columnar data with schema information and provides methods to access rows and cells.
  *
  * @tparam T
  *   The row type decoded from columns
  */
final case class MaterializedDataset[T](
  columns: Vector[Column[?]],
  schema: Schema[T]
) {
  require(columns.nonEmpty, "MaterializedDataset cannot be empty")
  require(
    columns.forall(_.length == columns.head.length),
    "All columns must have the same length"
  )

  /** Number of rows in the dataset. */
  def rowCount: Int = columns.head.length

  /** Number of columns in the dataset. */
  def columnCount: Int = columns.length

  /** Get a column by index. */
  def column(idx: Int): Column[?] = columns(idx)

  /** Get all rows as a Vector. Decodes each row using the schema. */
  def toVector: Vector[Either[errors.DecodeError, T]] = {
    (0 until rowCount).map { rowIdx =>
      val values = columns.map(_.getValue(rowIdx))
      schema.decode(values)
    }.toVector
  }

  /** Get all rows, throwing on decode errors.
    *
    * This is the unsafe version. Prefer `toVector` which returns Either. This method exists for
    * convenience when you know decoding cannot fail.
    */
  def toVectorUnsafe: Vector[T] = {
    toVector.map {
      case Right(value) => value
      case Left(err) => throw new RuntimeException(s"Decode error: $err") // scalafix:ok DisableSyntax.throw
    }
  }

  /** Iterate over rows. */
  def foreach(f: Either[errors.DecodeError, T] => Unit): Unit = {
    (0 until rowCount).foreach { rowIdx =>
      val values = columns.map(_.getValue(rowIdx))
      f(schema.decode(values))
    }
  }

  /** Map over decoded rows. */
  def map[U](f: T => U)(using schemaU: Schema[U]): Either[ExecutionError, MaterializedDataset[U]] = {
    val newRows = toVectorUnsafe.map(f)
    val encodedRows = newRows.map(schemaU.encode)

    // Transpose to get columns
    val newColumnsOrError = if (encodedRows.isEmpty) {
      Right(Vector.empty)
    } else {
      (0 until schemaU.columnCount).foldLeft[Either[ExecutionError, Vector[Column[?]]]](Right(Vector.empty)) {
        (acc, colIdx) =>
          acc.flatMap { cols =>
            val values = encodedRows.map(_(colIdx))
            Column.fromValues(values, schemaU.columnTypes(colIdx)).map(cols :+ _)
          }
      }
    }

    newColumnsOrError.map(cols => MaterializedDataset(cols, schemaU))
  }

  /** Show first n rows for debugging. */
  def show(n: Int = 20): String = {
    val sb = new StringBuilder

    // Header
    sb.append(schema.columnNames.mkString(" | "))
    sb.append("\n")
    sb.append("-" * (schema.columnNames.map(_.length).sum + (columnCount - 1) * 3))
    sb.append("\n")

    // Rows
    val limit = math.min(n, rowCount)
    (0 until limit).foreach { rowIdx =>
      val values = columns.map(_.getValue(rowIdx))
      sb.append(values.mkString(" | "))
      sb.append("\n")
    }

    if (rowCount > n) {
      sb.append(s"... (${rowCount - n} more rows)\n")
    }

    sb.toString
  }
}

object MaterializedDataset {

  /** Create from a Vector of values and schema. */
  def fromVector[T](values: Vector[T])(using schema: Schema[T]): Either[ExecutionError, MaterializedDataset[T]] = {
    val encodedRows = values.map(schema.encode)

    val columnsOrError = if (encodedRows.isEmpty) {
      Right(Vector.fill(schema.columnCount)(Column.empty(schema.columnTypes.head)))
    } else {
      (0 until schema.columnCount).foldLeft[Either[ExecutionError, Vector[Column[?]]]](Right(Vector.empty)) {
        (acc, colIdx) =>
          acc.flatMap { cols =>
            val colValues = encodedRows.map(_(colIdx))
            Column.fromValues(colValues, schema.columnTypes(colIdx)).map(cols :+ _)
          }
      }
    }

    columnsOrError.map(cols => MaterializedDataset(cols, schema))
  }
}
