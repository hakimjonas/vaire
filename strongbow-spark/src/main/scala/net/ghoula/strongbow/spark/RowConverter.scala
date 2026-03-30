package net.ghoula.strongbow.spark

import scala.collection.immutable.BitSet

import org.apache.spark.sql.Row

import net.ghoula.strongbow.Schema
import net.ghoula.strongbow.column.{Column, ColumnType}
import net.ghoula.strongbow.dataset.MaterializedDataset
import net.ghoula.strongbow.errors.{DecodeError, ExecutionError}

/** Type bridge between Strongbow values and Spark Rows using Schema[T]. */
object RowConverter {

  /** Encode a Strongbow value to a Spark Row via Schema. */
  def toRow[T](value: T, schema: Schema[T]): Row = {
    val encoded = schema.encode(value)
    val converted = encoded.zip(schema.columnTypes).map { case (v, ct) =>
      ct match {
        case ColumnType.DateType =>
          v match {
            case d: java.time.LocalDate => java.sql.Date.valueOf(d)
            case other => other
          }
        case _ => v
      }
    }
    Row.fromSeq(converted)
  }

  /** Decode a Spark Row to a Strongbow value via Schema. */
  def fromRow[T](row: Row, schema: Schema[T]): Either[DecodeError, T] = {
    val values = (0 until row.size).map { i =>
      if (row.isNullAt(i)) null // scalafix:ok DisableSyntax.null
      else {
        row.get(i) match {
          case d: java.sql.Date => d.toLocalDate
          case other => other
        }
      }
    }.toVector
    schema.decode(values)
  }

  /** Decode a Spark Row, throwing on failure. For use in mapPartitions. */
  def fromRowUnsafe[T](row: Row, schema: Schema[T]): T = {
    fromRow(row, schema) match {
      case Right(value) => value
      case Left(err) => sys.error(s"Row decode failed: $err")
    }
  }

  /** Convert Spark Rows directly to a MaterializedDataset via typed column extraction.
    *
    * Bypasses the box→case class→encode→Column.fromValues-round-trip by reading typed Spark Row
    * accessors (getInt, getDouble, etc.) directly into primitive arrays. One typed read per cell,
    * zero intermediate boxing.
    */
  def toMaterialized[T](rows: Array[Row], schema: Schema[T]): Either[ExecutionError, MaterializedDataset[T]] = {
    if (rows.isEmpty) {
      Right(MaterializedDataset(schema.columnTypes.map(Column.empty), schema))
    } else {
      val rowCount = rows.length
      val columns = schema.columnTypes.zipWithIndex.map { case (ct, colIdx) =>
        extractColumn(rows, colIdx, ct, rowCount)
      }
      Right(MaterializedDataset(columns, schema))
    }
  }

  /** Extract a single typed Column from an Array of Spark Rows.
    *
    * Uses typed Row accessors (getInt, getLong, getDouble, etc.) to read directly into primitive
    * arrays. Null values are tracked via BitSet, matching Column's SQL NULL contract.
    */
  private def extractColumn(rows: Array[Row], colIdx: Int, ct: ColumnType, rowCount: Int): Column[?] = {
    ct match {
      case ColumnType.IntType =>
        val nulls = BitSet.fromSpecific((0 until rowCount).filter(rows(_).isNullAt(colIdx)))
        Column.int(Array.tabulate(rowCount)(i => if (nulls.contains(i)) 0 else rows(i).getInt(colIdx)), nulls)

      case ColumnType.LongType =>
        val nulls = BitSet.fromSpecific((0 until rowCount).filter(rows(_).isNullAt(colIdx)))
        Column.long(Array.tabulate(rowCount)(i => if (nulls.contains(i)) 0L else rows(i).getLong(colIdx)), nulls)

      case ColumnType.DoubleType =>
        val nulls = BitSet.fromSpecific((0 until rowCount).filter(rows(_).isNullAt(colIdx)))
        Column.double(Array.tabulate(rowCount)(i => if (nulls.contains(i)) 0.0 else rows(i).getDouble(colIdx)), nulls)

      case ColumnType.FloatType =>
        val nulls = BitSet.fromSpecific((0 until rowCount).filter(rows(_).isNullAt(colIdx)))
        Column.float(Array.tabulate(rowCount)(i => if (nulls.contains(i)) 0.0f else rows(i).getFloat(colIdx)), nulls)

      case ColumnType.StringType =>
        val nulls = BitSet.fromSpecific((0 until rowCount).filter(rows(_).isNullAt(colIdx)))
        Column.string(
          Array.tabulate(rowCount)(i =>
            if (nulls.contains(i)) null else rows(i).getString(colIdx) // scalafix:ok DisableSyntax.null
          ),
          nulls
        )

      case ColumnType.BooleanType =>
        val nulls = BitSet.fromSpecific((0 until rowCount).filter(rows(_).isNullAt(colIdx)))
        Column.boolean(
          Array.tabulate(rowCount)(i => if (nulls.contains(i)) false else rows(i).getBoolean(colIdx)),
          nulls
        )

      case ColumnType.DateType =>
        val nulls = BitSet.fromSpecific((0 until rowCount).filter(rows(_).isNullAt(colIdx)))
        Column.date(
          Array.tabulate(rowCount)(i =>
            if (nulls.contains(i)) 0 else rows(i).getDate(colIdx).toLocalDate.toEpochDay.toInt
          ),
          nulls
        )

      case ColumnType.AnyType | ColumnType.OptionType(_) | ColumnType.ArrayType(_) | ColumnType.MapType(_, _) =>
        val nulls = BitSet.fromSpecific((0 until rowCount).filter(rows(_).isNullAt(colIdx)))
        Column.any(
          Array.tabulate(rowCount)(i =>
            if (nulls.contains(i)) null else rows(i).get(colIdx) // scalafix:ok DisableSyntax.null
          ),
          nulls
        )
    }
  }
}
