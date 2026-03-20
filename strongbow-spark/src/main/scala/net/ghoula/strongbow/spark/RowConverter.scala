package net.ghoula.strongbow.spark

import scala.collection.immutable.BitSet

import org.apache.spark.sql.Row

import net.ghoula.strongbow.{Column, ColumnType, MaterializedDataset, Schema}
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
      case Left(err) => throw new RuntimeException(s"Row decode failed: $err") // scalafix:ok DisableSyntax.throw
    }
  }

  /** Convert Spark Rows directly to a MaterializedDataset via typed column extraction.
    *
    * Bypasses the box→case class→encode→Column.fromValues round-trip by reading typed Spark Row
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
        val data = new Array[Int](rowCount)
        val nulls = scala.collection.mutable.BitSet.empty
        var i = 0 // scalafix:ok DisableSyntax.var
        while (i < rowCount) {
          if (rows(i).isNullAt(colIdx)) nulls += i
          else data(i) = rows(i).getInt(colIdx)
          i += 1
        }
        Column.int(data, if (nulls.isEmpty) BitSet.empty else BitSet.empty ++ nulls)

      case ColumnType.LongType =>
        val data = new Array[Long](rowCount)
        val nulls = scala.collection.mutable.BitSet.empty
        var i = 0 // scalafix:ok DisableSyntax.var
        while (i < rowCount) {
          if (rows(i).isNullAt(colIdx)) nulls += i
          else data(i) = rows(i).getLong(colIdx)
          i += 1
        }
        Column.long(data, if (nulls.isEmpty) BitSet.empty else BitSet.empty ++ nulls)

      case ColumnType.DoubleType =>
        val data = new Array[Double](rowCount)
        val nulls = scala.collection.mutable.BitSet.empty
        var i = 0 // scalafix:ok DisableSyntax.var
        while (i < rowCount) {
          if (rows(i).isNullAt(colIdx)) nulls += i
          else data(i) = rows(i).getDouble(colIdx)
          i += 1
        }
        Column.double(data, if (nulls.isEmpty) BitSet.empty else BitSet.empty ++ nulls)

      case ColumnType.StringType =>
        val data = new Array[String](rowCount)
        val nulls = scala.collection.mutable.BitSet.empty
        var i = 0 // scalafix:ok DisableSyntax.var
        while (i < rowCount) {
          if (rows(i).isNullAt(colIdx)) nulls += i
          else data(i) = rows(i).getString(colIdx)
          i += 1
        }
        Column.string(data, if (nulls.isEmpty) BitSet.empty else BitSet.empty ++ nulls)

      case ColumnType.BooleanType =>
        val data = new Array[Boolean](rowCount)
        val nulls = scala.collection.mutable.BitSet.empty
        var i = 0 // scalafix:ok DisableSyntax.var
        while (i < rowCount) {
          if (rows(i).isNullAt(colIdx)) nulls += i
          else data(i) = rows(i).getBoolean(colIdx)
          i += 1
        }
        Column.boolean(data, if (nulls.isEmpty) BitSet.empty else BitSet.empty ++ nulls)

      case ColumnType.DateType =>
        val data = new Array[Int](rowCount)
        val nulls = scala.collection.mutable.BitSet.empty
        var i = 0 // scalafix:ok DisableSyntax.var
        while (i < rowCount) {
          if (rows(i).isNullAt(colIdx)) nulls += i
          else data(i) = rows(i).getDate(colIdx).toLocalDate.toEpochDay.toInt
          i += 1
        }
        Column.date(data, if (nulls.isEmpty) BitSet.empty else BitSet.empty ++ nulls)

      case ColumnType.AnyType | ColumnType.OptionType(_) | ColumnType.ArrayType(_) | ColumnType.MapType(_, _) =>
        val data = new Array[Any](rowCount)
        val nulls = scala.collection.mutable.BitSet.empty
        var i = 0 // scalafix:ok DisableSyntax.var
        while (i < rowCount) {
          if (rows(i).isNullAt(colIdx)) nulls += i
          else data(i) = rows(i).get(colIdx)
          i += 1
        }
        Column.any(data, if (nulls.isEmpty) BitSet.empty else BitSet.empty ++ nulls)
    }
  }
}
