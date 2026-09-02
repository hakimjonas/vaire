package net.ghoula.strongbow.dataset

import net.ghoula.strongbow.Schema
import net.ghoula.strongbow.column.{Column, ColumnType}
import net.ghoula.strongbow.errors.{DecodeError, ExecutionError}

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
  def toVector: Vector[Either[DecodeError, T]] = {
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
      case Left(err) => sys.error(s"Decode error: $err")
    }
  }

  /** Iterate over rows. */
  def foreach(f: Either[DecodeError, T] => Unit): Unit = {
    (0 until rowCount).foreach { rowIdx =>
      val values = columns.map(_.getValue(rowIdx))
      f(schema.decode(values))
    }
  }

  /** Map over decoded rows. */
  def map[U](f: T => U)(using schemaU: Schema[U]): Either[ExecutionError, MaterializedDataset[U]] =
    MaterializedDataset.fromVector(toVectorUnsafe.map(f))

  /** Show first n rows for debugging. */
  def show(n: Int = 20): String = {
    val sb = new StringBuilder

    sb.append(schema.columnNames.mkString(" | "))
    sb.append("\n")
    sb.append("-" * (schema.columnNames.map(_.length).sum + (columnCount - 1) * 3))
    sb.append("\n")

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

/** Construction of materialized datasets from rows or columns. */
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
            schema.columnTypes(colIdx) match {
              case ColumnType.StructType(fields) =>
                schema.nestedSchemas.lift(colIdx).flatten match {
                  case Some(nested) =>
                    Column.structFromValues(colValues, nested, fields).map(cols :+ _)
                  case None =>
                    Left(
                      ExecutionError.UnsupportedOperation(
                        "StructType column requires a nested schema (Schema.nestedSchemas)"
                      )
                    )
                }
              case ct =>
                Column.fromValues(colValues, ct).map(cols :+ _)
            }
          }
      }
    }

    columnsOrError.map(cols => MaterializedDataset(cols, schema))
  }
}
