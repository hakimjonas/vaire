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
        case ColumnType.DecimalType(_, s) =>
          v match {
            case l: Long => java.math.BigDecimal.valueOf(l, s)
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
      case ColumnType.IntType => extract(0, _.getInt(_), Column.int)
      case ColumnType.LongType => extract(0L, _.getLong(_), Column.long)
      case ColumnType.DoubleType => extract(0.0, _.getDouble(_), Column.double)
      case ColumnType.FloatType => extract(0.0f, _.getFloat(_), Column.float)
      case ColumnType.ShortType => extract(0: Short, _.getShort(_), Column.short)
      case ColumnType.ByteType => extract(0: Byte, _.getByte(_), Column.byte)
      case ColumnType.TimestampType => extract(0L, _.getLong(_), Column.timestamp)
      case ColumnType.TimestampNTZType => extract(0L, _.getLong(_), Column.timestampNTZ)
      case ColumnType.YearMonthIntervalType => extract(0, _.getInt(_), Column.yearMonthInterval)
      case ColumnType.DayTimeIntervalType => extract(0L, _.getLong(_), Column.dayTimeInterval)
      case ColumnType.BooleanType => extract(false, _.getBoolean(_), Column.boolean)
      case ColumnType.DateType => extract(0, (r, c) => r.getDate(c).toLocalDate.toEpochDay.toInt, Column.date)

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
        Column.binaryFromArrays(byteArrays, nulls)

      case ColumnType.DecimalType(p, s) if p <= 18 =>
        import net.ghoula.strongbow.types.Decimal
        val converted = Array.tabulate(rowCount) { i =>
          if (nulls.contains(i)) None
          else Decimal.fromBigDecimal(rows(i).getDecimal(colIdx))
        }
        val overflowNulls =
          BitSet.fromSpecific((0 until rowCount).filter(i => !nulls.contains(i) && converted(i).isEmpty))
        val data = converted.map(_.fold(0L)(_.toUnscaled))
        Column.decimal(data, p, s, nulls | overflowNulls)

      case ColumnType.DecimalType(_, _) =>
        Column.any(
          Array.tabulate(rowCount)(i =>
            if (nulls.contains(i)) null else rows(i).getDecimal(colIdx) // scalafix:ok DisableSyntax.null
          ),
          nulls
        )

      case ColumnType.VariantType | ColumnType.AnyType | ColumnType.OptionType(_) | ColumnType.ArrayType(_) |
          ColumnType.MapType(_, _) =>
        Column.any(
          Array.tabulate(rowCount)(i =>
            if (nulls.contains(i)) null else rows(i).get(colIdx) // scalafix:ok DisableSyntax.null
          ),
          nulls
        )
    }
  }
}
