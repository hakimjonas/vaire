package net.ghoula.vaire.spark

import scala.collection.immutable.BitSet

import org.apache.spark.sql.Row

import net.ghoula.vaire.Schema
import net.ghoula.vaire.column.{Column, ColumnType}
import net.ghoula.vaire.dataset.MaterializedDataset
import net.ghoula.vaire.errors.{DecodeError, ExecutionError}
import net.ghoula.vaire.types.Time

/** Type bridge between Vairë values and Spark Rows using Schema[T]. */
object RowConverter {

  /** Encode a Vairë value to a Spark Row via Schema. */
  def toRow[T](value: T, schema: Schema[T]): Row = {
    val encoded = schema.encode(value)
    val converted = encoded.zip(schema.columnTypes).map { case (v, ct) =>
      toSparkValue(v, ct)
    }
    Row.fromSeq(converted)
  }

  /** Decode a Spark Row to a Vairë value via Schema. */
  def fromRow[T](row: Row, schema: Schema[T]): Either[DecodeError, T] = {
    val values = (0 until row.size).map { i =>
      if (row.isNullAt(i)) null // scalafix:ok DisableSyntax.null
      else fromSparkValue(row.get(i), schema.columnTypes(i))
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

  /** Convert Vairë-encoded values to the representation Spark expects at Row boundaries, recursing
    * into struct/array/map layouts.
    */
  private def toSparkValue(v: Any | Null, ct: ColumnType): Any | Null =
    if (Option(v).isEmpty) null // scalafix:ok DisableSyntax.null
    else
      ct match {
        case ColumnType.TimeType =>
          v match {
            case lt: java.time.LocalTime => lt
            case l: Long => Time.ofMicros(l).toLocalTime
            case other => other
          }
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
        case ColumnType.StructType(fields) =>
          v match {
            case p: Product =>
              Row.fromSeq(p.productIterator.toVector.zip(fields).map { case (fv, (_, fct)) =>
                toSparkValue(fv, fct)
              })
            case other => other
          }
        case ColumnType.ArrayType(elem) =>
          v match {
            case s: scala.collection.Seq[?] => s.map(e => toSparkValue(e, elem)).toSeq
            case other => other
          }
        case ColumnType.MapType(kt, vt) =>
          v match {
            case m: Map[?, ?] => m.map { case (mk, mv) => (toSparkValue(mk, kt), toSparkValue(mv, vt)) }
            case other => other
          }
        case _ => v
      }

  /** Convert Spark Row values to Vairë-encoded values, recursing into struct/array/map layouts.
    */
  private def fromSparkValue(v: Any | Null, ct: ColumnType): Any | Null =
    if (Option(v).isEmpty) null // scalafix:ok DisableSyntax.null
    else
      ct match {
        case ColumnType.TimeType =>
          v match {
            case lt: java.time.LocalTime => Time.fromLocalTime(lt)
            case other => other
          }
        case ColumnType.DateType =>
          v match {
            case d: java.sql.Date => d.toLocalDate
            case other => other
          }
        case ColumnType.StructType(fields) =>
          v match {
            case r: Row =>
              Row.fromSeq(
                r.toSeq.zip(fields).map { case (fv, (_, fct)) => fromSparkValue(fv, fct) }
              )
            case other => other
          }
        case ColumnType.ArrayType(elem) =>
          v match {
            case s: scala.collection.Seq[?] => s.map(e => fromSparkValue(e, elem)).toSeq
            case other => other
          }
        case ColumnType.MapType(kt, vt) =>
          v match {
            case m: Map[?, ?] =>
              m.map { case (mk, mv) => (fromSparkValue(mk, kt), fromSparkValue(mv, vt)) }
            case other => other
          }
        case _ => v
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
      val columnsOrError = schema.columnTypes.zipWithIndex.foldLeft[Either[ExecutionError, Vector[Column[?]]]](
        Right(Vector.empty)
      ) { (acc, pair) =>
        acc.flatMap { cols =>
          val colIdx = pair._2
          extractColumn(rows, colIdx, pair._1, rowCount, schema.nestedSchemas.lift(colIdx).flatten).map(cols :+ _)
        }
      }
      columnsOrError.map(columns => MaterializedDataset(columns, schema))
    }
  }

  private def timestampFromRows(
    rows: Array[Row],
    colIdx: Int,
    rowCount: Int,
    nulls: BitSet,
    wrap: (Array[Long], BitSet) => Column[?]
  ): Either[ExecutionError, Column[?]] = {
    val converted = (0 until rowCount).map { i =>
      if (nulls.contains(i)) Right(0L)
      else
        rows(i).get(colIdx) match {
          case t: java.sql.Timestamp =>
            Right(t.getTime * 1000L + (t.getNanos.toLong % 1000000L) / 1000L)
          case ldt: java.time.LocalDateTime =>
            Right(ldt.toEpochSecond(java.time.ZoneOffset.UTC) * 1000000L + ldt.getNano.toLong / 1000L)
          case l: Long => Right(l)
          case other =>
            Left(ExecutionError.TypeMismatch("Timestamp", other.getClass.getSimpleName, "RowConverter"))
        }
    }
    converted
      .foldLeft[Either[ExecutionError, Vector[Long]]](Right(Vector.empty)) { (acc, item) =>
        acc.flatMap(vs => item.map(vs :+ _))
      }
      .map(vs => wrap(vs.toArray, nulls))
  }

  private def extractColumn(
    rows: Array[Row],
    colIdx: Int,
    ct: ColumnType,
    rowCount: Int,
    nestedSchema: Option[Schema[?]]
  ): Either[ExecutionError, Column[?]] = {
    val nulls = BitSet.fromSpecific((0 until rowCount).filter(rows(_).isNullAt(colIdx)))

    def extract[T: scala.reflect.ClassTag](
      defaultVal: T,
      get: (Row, Int) => T,
      wrap: (Array[T], BitSet) => Column[?]
    ): Column[?] =
      wrap(Array.tabulate(rowCount)(i => if (nulls.contains(i)) defaultVal else get(rows(i), colIdx)), nulls)

    ct match {
      case ColumnType.IntType => Right(extract(0, _.getInt(_), Column.int))
      case ColumnType.LongType => Right(extract(0L, _.getLong(_), Column.long))
      case ColumnType.DoubleType => Right(extract(0.0, _.getDouble(_), Column.double))
      case ColumnType.FloatType => Right(extract(0.0f, _.getFloat(_), Column.float))
      case ColumnType.ShortType => Right(extract(0: Short, _.getShort(_), Column.short))
      case ColumnType.ByteType => Right(extract(0: Byte, _.getByte(_), Column.byte))
      case ColumnType.TimestampType =>
        timestampFromRows(rows, colIdx, rowCount, nulls, Column.timestamp)
      case ColumnType.TimestampNTZType =>
        timestampFromRows(rows, colIdx, rowCount, nulls, Column.timestampNTZ)
      case ColumnType.TimeType =>
        val converted = (0 until rowCount).map { i =>
          if (nulls.contains(i)) Right(0L)
          else
            rows(i).get(colIdx) match {
              case lt: java.time.LocalTime => Right(Time.fromLocalTime(lt).toMicros)
              case l: Long => Right(l)
              case other =>
                Left(ExecutionError.TypeMismatch("Time", other.getClass.getSimpleName, "RowConverter"))
            }
        }
        converted
          .foldLeft[Either[ExecutionError, Vector[Long]]](Right(Vector.empty)) { (acc, item) =>
            acc.flatMap(vs => item.map(vs :+ _))
          }
          .map(vs => Column.time(vs.toArray, nulls))
      case ColumnType.YearMonthIntervalType => Right(extract(0, _.getInt(_), Column.yearMonthInterval))
      case ColumnType.DayTimeIntervalType => Right(extract(0L, _.getLong(_), Column.dayTimeInterval))
      case ColumnType.BooleanType => Right(extract(false, _.getBoolean(_), Column.boolean))
      case ColumnType.DateType =>
        Right(extract(0, (r, c) => r.getDate(c).toLocalDate.toEpochDay.toInt, Column.date))

      case ColumnType.StringType | ColumnType.CharType(_) | ColumnType.VarcharType(_) =>
        Right(
          Column.string(
            Array.tabulate(rowCount)(i =>
              if (nulls.contains(i)) null else rows(i).getString(colIdx) // scalafix:ok DisableSyntax.null
            ),
            nulls
          )
        )

      case ColumnType.BinaryType =>
        val byteArrays = Array.tabulate(rowCount)(i =>
          if (nulls.contains(i)) Array.empty[Byte] else rows(i).getAs[Array[Byte]](colIdx)
        )
        Right(Column.binaryFromArrays(byteArrays, nulls))

      case ColumnType.DecimalType(p, s) if p <= 18 =>
        import net.ghoula.vaire.types.Decimal
        val converted = Array.tabulate(rowCount) { i =>
          if (nulls.contains(i)) None
          else Decimal.fromBigDecimal(rows(i).getDecimal(colIdx))
        }
        val overflowNulls =
          BitSet.fromSpecific((0 until rowCount).filter(i => !nulls.contains(i) && converted(i).isEmpty))
        val data = converted.map(_.fold(0L)(_.toUnscaled))
        Right(Column.decimal(data, p, s, nulls | overflowNulls))

      case ColumnType.DecimalType(_, _) =>
        Right(
          Column.any(
            Array.tabulate(rowCount)(i =>
              if (nulls.contains(i)) null else rows(i).getDecimal(colIdx) // scalafix:ok DisableSyntax.null
            ),
            nulls
          )
        )

      case ColumnType.ArrayType(elementType) =>
        val perRow = (0 until rowCount).foldLeft[Either[ExecutionError, Vector[Vector[Any]]]](Right(Vector.empty)) {
          (acc, i) =>
            acc.flatMap { rowsAcc =>
              if (nulls.contains(i)) Right(rowsAcc :+ Vector.empty)
              else
                rows(i).get(colIdx) match {
                  case s: scala.collection.Seq[?] => Right(rowsAcc :+ s.toVector)
                  case other =>
                    Left(ExecutionError.TypeMismatch("Seq", other.getClass.getSimpleName, "RowConverter"))
                }
            }
        }
        perRow.flatMap(Column.arrayFromVectors(_, elementType, nulls))

      case ColumnType.MapType(keyType, valueType) =>
        val perRow = (0 until rowCount).foldLeft[
          Either[ExecutionError, Vector[(Vector[Any], Vector[Any])]]
        ](Right(Vector.empty)) { (acc, i) =>
          acc.flatMap { rowsAcc =>
            if (nulls.contains(i)) Right(rowsAcc :+ ((Vector.empty, Vector.empty)))
            else
              rows(i).get(colIdx) match {
                case m: scala.collection.Map[?, ?] =>
                  Right(rowsAcc :+ ((m.keys.toVector, m.values.toVector)))
                case other =>
                  Left(ExecutionError.TypeMismatch("Map", other.getClass.getSimpleName, "RowConverter"))
              }
          }
        }
        perRow.flatMap(Column.mapFromVectors(_, keyType, valueType, nulls))

      case ColumnType.StructType(fields) =>
        nestedSchema match {
          case Some(nested) =>
            val subRows = Array.tabulate(rowCount) { i =>
              if (nulls.contains(i))
                Row.fromSeq(Vector.fill(fields.size)(null)) // scalafix:ok DisableSyntax.null
              else rows(i).getStruct(colIdx)
            }
            structColumnFromRows(subRows, nested, rowCount, nulls)
          case None =>
            Left(
              ExecutionError.UnsupportedOperation(
                "StructType column requires a nested schema (Schema.nestedSchemas)"
              )
            )
        }

      case ColumnType.VariantType | ColumnType.AnyType | ColumnType.OptionType(_) =>
        Right(
          Column.any(
            Array.tabulate(rowCount)(i =>
              if (nulls.contains(i)) null else rows(i).get(colIdx) // scalafix:ok DisableSyntax.null
            ),
            nulls
          )
        )
    }
  }

  private def structColumnFromRows(
    rows: Array[Row],
    schema: Schema[?],
    rowCount: Int,
    nulls: BitSet
  ): Either[ExecutionError, Column[?]] =
    schema.columnTypes.zipWithIndex
      .foldLeft[Either[ExecutionError, Vector[Column[?]]]](Right(Vector.empty)) { (acc, pair) =>
        acc.flatMap { cols =>
          val subNested = schema.nestedSchemas.lift(pair._2).flatten
          extractColumn(rows, pair._2, pair._1, rowCount, subNested).map(cols :+ _)
        }
      }
      .map(columns => structColumnOf(columns, schema, nulls))

  private def structColumnOf[T](columns: Vector[Column[?]], schema: Schema[T], nulls: BitSet): Column[?] =
    Column.struct(columns, schema, nulls)
}
