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
    * Bypasses the box->case class->encode->Column.fromValues-round-trip by reading typed Spark Row
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

  private def extractColumn(rows: Array[Row], colIdx: Int, ct: ColumnType, rowCount: Int): Column[?] = {
    val nulls = BitSet.fromSpecific((0 until rowCount).filter(rows(_).isNullAt(colIdx)))

    def extract[T: scala.reflect.ClassTag](
      defaultVal: T,
      get: (Row, Int) => T,
      wrap: (Array[T], BitSet) => Column[?]
    ): Column[?] =
      wrap(Array.tabulate(rowCount)(i => if (nulls.contains(i)) defaultVal else get(rows(i), colIdx)), nulls)

    ct match {
      case ColumnType.IntType               => extract(0, _.getInt(_), Column.int(_, _))
      case ColumnType.LongType              => extract(0L, _.getLong(_), Column.long(_, _))
      case ColumnType.DoubleType            => extract(0.0, _.getDouble(_), Column.double(_, _))
      case ColumnType.FloatType             => extract(0.0f, _.getFloat(_), Column.float(_, _))
      case ColumnType.ShortType             => extract((0: Short), _.getShort(_), Column.short(_, _))
      case ColumnType.ByteType              => extract((0: Byte), _.getByte(_), Column.byte(_, _))
      case ColumnType.TimestampType         => extract(0L, _.getLong(_), Column.timestamp(_, _))
      case ColumnType.TimestampNTZType      => extract(0L, _.getLong(_), Column.timestampNTZ(_, _))
      case ColumnType.YearMonthIntervalType => extract(0, _.getInt(_), Column.yearMonthInterval(_, _))
      case ColumnType.DayTimeIntervalType   => extract(0L, _.getLong(_), Column.dayTimeInterval(_, _))
      case ColumnType.BooleanType           => extract(false, _.getBoolean(_), Column.boolean(_, _))
      case ColumnType.DateType              => extract(0, (r, c) => r.getDate(c).toLocalDate.toEpochDay.toInt, Column.date(_, _))

      case ColumnType.StringType | ColumnType.CharType(_) | ColumnType.VarcharType(_) =>
        Column.string(
          Array.tabulate(rowCount)(i =>
            if (nulls.contains(i)) null else rows(i).getString(colIdx) // scalafix:ok DisableSyntax.null
          ),
          nulls
        )

      case ColumnType.BinaryType =>
        val byteArrays = Array.tabulate(rowCount)(i =>
          if (nulls.contains(i)) Array.empty[Byte] else rows(i).getAs[Array[Byte]](colIdx)
        )
        val (flatData, offsets) = byteArrays.foldLeft((Array.empty[Byte], Vector(0))) { case ((bytes, offs), ba) =>
          val newBytes = new Array[Byte](bytes.length + ba.length)
          System.arraycopy(bytes, 0, newBytes, 0, bytes.length)
          System.arraycopy(ba, 0, newBytes, bytes.length, ba.length)
          (newBytes, offs :+ newBytes.length)
        }
        Column.binary(flatData, offsets.toArray, nulls)

      case ColumnType.AnyType | ColumnType.OptionType(_) | ColumnType.ArrayType(_) | ColumnType.MapType(_, _) =>
        Column.any(
          Array.tabulate(rowCount)(i =>
            if (nulls.contains(i)) null else rows(i).get(colIdx) // scalafix:ok DisableSyntax.null
          ),
          nulls
        )
    }
  }
}
