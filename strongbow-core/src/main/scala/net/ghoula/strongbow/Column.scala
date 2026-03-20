package net.ghoula.strongbow

import scala.collection.immutable.BitSet

import net.ghoula.strongbow.errors.ExecutionError
import net.ghoula.strongbow.types.RowIndex

/** Specialized columnar storage avoiding boxing.
  *
  * Each column type uses primitive arrays where possible. Nullability tracked via BitSet for memory
  * efficiency. The type parameter `A` tracks the logical element type via GADT refinement.
  */
enum Column[+A] {
  case IntColumn(data: Array[Int], nulls: BitSet) extends Column[Int]
  case LongColumn(data: Array[Long], nulls: BitSet) extends Column[Long]
  case DoubleColumn(data: Array[Double], nulls: BitSet) extends Column[Double]
  case StringColumn(data: Array[String], nulls: BitSet) extends Column[String]
  case BooleanColumn(data: Array[Boolean], nulls: BitSet) extends Column[Boolean]
  case DateColumn(data: Array[Int], nulls: BitSet) extends Column[types.Date]
  case AnyColumn(data: Array[Any], nulls: BitSet) extends Column[Any]

  inline def length: Int = this match {
    case IntColumn(data, _) => data.length
    case LongColumn(data, _) => data.length
    case DoubleColumn(data, _) => data.length
    case StringColumn(data, _) => data.length
    case BooleanColumn(data, _) => data.length
    case DateColumn(data, _) => data.length
    case AnyColumn(data, _) => data.length
  }

  inline def columnType: ColumnType = this match {
    case IntColumn(_, _) => ColumnType.IntType
    case LongColumn(_, _) => ColumnType.LongType
    case DoubleColumn(_, _) => ColumnType.DoubleType
    case StringColumn(_, _) => ColumnType.StringType
    case BooleanColumn(_, _) => ColumnType.BooleanType
    case DateColumn(_, _) => ColumnType.DateType
    case AnyColumn(_, _) => ColumnType.AnyType
  }

  inline def isNull(index: RowIndex): Boolean = this match {
    case IntColumn(_, nulls) => nulls.contains(index.toInt)
    case LongColumn(_, nulls) => nulls.contains(index.toInt)
    case DoubleColumn(_, nulls) => nulls.contains(index.toInt)
    case StringColumn(_, nulls) => nulls.contains(index.toInt)
    case BooleanColumn(_, nulls) => nulls.contains(index.toInt)
    case DateColumn(_, nulls) => nulls.contains(index.toInt)
    case AnyColumn(_, nulls) => nulls.contains(index.toInt)
  }

  /** Get value at index. Returns null if the value is null. */
  inline def getValue(index: Int): Any = this match {
    case IntColumn(data, nulls) => if (nulls.contains(index)) null else data(index) // scalafix:ok DisableSyntax.null
    case LongColumn(data, nulls) => if (nulls.contains(index)) null else data(index) // scalafix:ok DisableSyntax.null
    case DoubleColumn(data, nulls) => if (nulls.contains(index)) null else data(index) // scalafix:ok DisableSyntax.null
    case StringColumn(data, nulls) => if (nulls.contains(index)) null else data(index) // scalafix:ok DisableSyntax.null
    case BooleanColumn(data, nulls) =>
      if (nulls.contains(index)) null else data(index) // scalafix:ok DisableSyntax.null
    case DateColumn(data, nulls) =>
      if (nulls.contains(index)) null // scalafix:ok DisableSyntax.null
      else types.Date.ofEpochDay(data(index).toLong)
    case AnyColumn(data, nulls) => if (nulls.contains(index)) null else data(index) // scalafix:ok DisableSyntax.null
  }

  /** Typed prefix slicing — takes the first n elements without boxing.
    *
    * Uses `Array.copyOfRange` on typed arrays for zero-boxing, cache-friendly copies. This replaces
    * the `getValue` + `fromValues` round-trip in `limit`.
    *
    * @param n
    *   Number of elements to take from the front. Clamped to column length.
    */
  def take(n: Int): Column[A] = {
    val len = Math.min(n, length)
    this match {
      case IntColumn(data, nulls) =>
        IntColumn(java.util.Arrays.copyOfRange(data, 0, len), nulls.filter(_ < len))
      case LongColumn(data, nulls) =>
        LongColumn(java.util.Arrays.copyOfRange(data, 0, len), nulls.filter(_ < len))
      case DoubleColumn(data, nulls) =>
        DoubleColumn(java.util.Arrays.copyOfRange(data, 0, len), nulls.filter(_ < len))
      case StringColumn(data, nulls) =>
        StringColumn(java.util.Arrays.copyOfRange(data, 0, len), nulls.filter(_ < len))
      case BooleanColumn(data, nulls) =>
        BooleanColumn(java.util.Arrays.copyOfRange(data, 0, len), nulls.filter(_ < len))
      case DateColumn(data, nulls) =>
        DateColumn(java.util.Arrays.copyOfRange(data, 0, len), nulls.filter(_ < len))
      case AnyColumn(data, nulls) =>
        val arr = new Array[Any](len)
        System.arraycopy(data, 0, arr, 0, len)
        AnyColumn(arr, nulls.filter(_ < len))
    }
  }

  /** Typed array concatenation — appends another column without boxing.
    *
    * Uses `System.arraycopy` on typed arrays. Returns error if column types don't match.
    *
    * @param other
    *   Column to append. Must be the same column type.
    */
  def concat(other: Column[?]): Either[ExecutionError, Column[A]] = {
    if (this.columnType != other.columnType) {
      Left(ExecutionError.TypeMismatch(this.columnType.toString, other.columnType.toString, "Column.concat"))
    } else {
      val leftLen = this.length
      val rightLen = other.length
      val rightNulls = other match {
        case IntColumn(_, n) => n
        case LongColumn(_, n) => n
        case DoubleColumn(_, n) => n
        case StringColumn(_, n) => n
        case BooleanColumn(_, n) => n
        case DateColumn(_, n) => n
        case AnyColumn(_, n) => n
      }
      val leftNulls = this match {
        case IntColumn(_, n) => n
        case LongColumn(_, n) => n
        case DoubleColumn(_, n) => n
        case StringColumn(_, n) => n
        case BooleanColumn(_, n) => n
        case DateColumn(_, n) => n
        case AnyColumn(_, n) => n
      }
      val combinedNulls = leftNulls | rightNulls.map(_ + leftLen)

      (this, other) match {
        case (IntColumn(l, _), IntColumn(r, _)) =>
          val arr = new Array[Int](leftLen + rightLen)
          System.arraycopy(l, 0, arr, 0, leftLen)
          System.arraycopy(r, 0, arr, leftLen, rightLen)
          Right(IntColumn(arr, combinedNulls))
        case (LongColumn(l, _), LongColumn(r, _)) =>
          val arr = new Array[Long](leftLen + rightLen)
          System.arraycopy(l, 0, arr, 0, leftLen)
          System.arraycopy(r, 0, arr, leftLen, rightLen)
          Right(LongColumn(arr, combinedNulls))
        case (DoubleColumn(l, _), DoubleColumn(r, _)) =>
          val arr = new Array[Double](leftLen + rightLen)
          System.arraycopy(l, 0, arr, 0, leftLen)
          System.arraycopy(r, 0, arr, leftLen, rightLen)
          Right(DoubleColumn(arr, combinedNulls))
        case (StringColumn(l, _), StringColumn(r, _)) =>
          val arr = new Array[String](leftLen + rightLen)
          System.arraycopy(l, 0, arr, 0, leftLen)
          System.arraycopy(r, 0, arr, leftLen, rightLen)
          Right(StringColumn(arr, combinedNulls))
        case (BooleanColumn(l, _), BooleanColumn(r, _)) =>
          val arr = new Array[Boolean](leftLen + rightLen)
          System.arraycopy(l, 0, arr, 0, leftLen)
          System.arraycopy(r, 0, arr, leftLen, rightLen)
          Right(BooleanColumn(arr, combinedNulls))
        case (DateColumn(l, _), DateColumn(r, _)) =>
          val arr = new Array[Int](leftLen + rightLen)
          System.arraycopy(l, 0, arr, 0, leftLen)
          System.arraycopy(r, 0, arr, leftLen, rightLen)
          Right(DateColumn(arr, combinedNulls))
        case (AnyColumn(l, _), AnyColumn(r, _)) =>
          val arr = new Array[Any](leftLen + rightLen)
          System.arraycopy(l, 0, arr, 0, leftLen)
          System.arraycopy(r, 0, arr, leftLen, rightLen)
          Right(AnyColumn(arr, combinedNulls))
        case _ =>
          Left(ExecutionError.TypeMismatch(this.columnType.toString, other.columnType.toString, "Column.concat"))
      }
    }
  }

  /** Type-specialized slicing for efficient filter operations.
    *
    * Creates a new column with only the specified indices, avoiding boxing and intermediate
    * allocations. This is the key optimization for filter performance.
    *
    * SQL NULL HANDLING: For SQL NULL values, we use default values (0 for primitives, null for
    * objects) as placeholders in the array. These placeholder values are never read because the
    * nulls BitSet is the authoritative source of which indices contain NULL. This approach avoids
    * Option boxing while maintaining SQL NULL semantics.
    *
    * The null values in StringColumn and AnyColumn are SAFE because:
    *   1. They are only used as array placeholders for SQL NULL
    *   2. The nulls BitSet tracks which indices are NULL
    *   3. All accessors check nulls BitSet before reading array values
    *   4. These nulls never escape - external code must use Option-based accessors
    *
    * @param indices
    *   Array of source indices to include in the result. Using Array instead of IndexedSeq provides
    *   2x better performance (benchmarked at 500K elements).
    */
  def slice(indices: Array[Int]): Column[A] = this match {
    case IntColumn(data, nulls) =>
      IntColumn(sliceArray(data, indices, nulls, 0), buildNullSet(nulls, indices))
    case LongColumn(data, nulls) =>
      LongColumn(sliceArray(data, indices, nulls, 0L), buildNullSet(nulls, indices))
    case DoubleColumn(data, nulls) =>
      DoubleColumn(sliceArray(data, indices, nulls, 0.0), buildNullSet(nulls, indices))
    case StringColumn(data, nulls) =>
      StringColumn(
        sliceArray(data, indices, nulls, null), // scalafix:ok DisableSyntax.null
        buildNullSet(nulls, indices)
      )
    case BooleanColumn(data, nulls) =>
      BooleanColumn(sliceArray(data, indices, nulls, false), buildNullSet(nulls, indices))
    case DateColumn(data, nulls) =>
      DateColumn(sliceArray(data, indices, nulls, 0), buildNullSet(nulls, indices))
    case AnyColumn(data, nulls) =>
      AnyColumn(sliceArray(data, indices, nulls, null), buildNullSet(nulls, indices)) // scalafix:ok DisableSyntax.null
  }

  private def sliceArray[T: scala.reflect.ClassTag](
    data: Array[T],
    indices: Array[Int],
    nulls: BitSet,
    defaultValue: T
  ): Array[T] = {
    val newData = new Array[T](indices.length)
    var i = 0 // scalafix:ok DisableSyntax.var
    while (i < indices.length) {
      val srcIdx = indices(i)
      newData(i) = if (nulls.contains(srcIdx)) defaultValue else data(srcIdx)
      i += 1
    }
    newData
  }

  private def buildNullSet(nulls: BitSet, indices: Array[Int]): BitSet = {
    val mutableSet = scala.collection.mutable.BitSet.empty
    var i = 0 // scalafix:ok DisableSyntax.var
    while (i < indices.length) {
      if (nulls.contains(indices(i))) mutableSet += i
      i += 1
    }
    BitSet.empty ++ mutableSet
  }
}

object Column {
  inline def int(data: Array[Int], nulls: BitSet = BitSet.empty): Column[Int] = {
    IntColumn(data, nulls)
  }

  inline def long(data: Array[Long], nulls: BitSet = BitSet.empty): Column[Long] = {
    LongColumn(data, nulls)
  }

  inline def double(data: Array[Double], nulls: BitSet = BitSet.empty): Column[Double] = {
    DoubleColumn(data, nulls)
  }

  inline def string(data: Array[String], nulls: BitSet = BitSet.empty): Column[String] = {
    StringColumn(data, nulls)
  }

  inline def boolean(data: Array[Boolean], nulls: BitSet = BitSet.empty): Column[Boolean] = {
    BooleanColumn(data, nulls)
  }

  /** Create a DateColumn from epoch day values. */
  inline def date(data: Array[Int], nulls: BitSet = BitSet.empty): Column[types.Date] = {
    DateColumn(data, nulls)
  }

  inline def any(data: Array[Any], nulls: BitSet = BitSet.empty): Column[Any] = {
    AnyColumn(data, nulls)
  }

  /** Create an empty column of the given type. */
  def empty(columnType: ColumnType): Column[?] = columnType match {
    case ColumnType.IntType => IntColumn(Array.empty[Int], BitSet.empty)
    case ColumnType.LongType => LongColumn(Array.empty[Long], BitSet.empty)
    case ColumnType.DoubleType => DoubleColumn(Array.empty[Double], BitSet.empty)
    case ColumnType.StringType => StringColumn(Array.empty[String], BitSet.empty)
    case ColumnType.BooleanType => BooleanColumn(Array.empty[Boolean], BitSet.empty)
    case ColumnType.DateType => DateColumn(Array.empty[Int], BitSet.empty)
    case ColumnType.AnyType => AnyColumn(Array.empty[Any], BitSet.empty)
    case ColumnType.OptionType(_) => AnyColumn(Array.empty[Any], BitSet.empty)
    case ColumnType.ArrayType(_) => AnyColumn(Array.empty[Any], BitSet.empty)
    case ColumnType.MapType(_, _) => AnyColumn(Array.empty[Any], BitSet.empty)
  }

  /** Create a column from a vector of values.
    *
    * SQL NULL HANDLING: Input values may contain Scala null to represent SQL NULL. We detect these
    * using Option(v).isEmpty and track them in the nulls BitSet. For primitive types, we use
    * default values (0, 0L, 0.0, false) as array placeholders. For String, we use null as the
    * placeholder.
    *
    * These null placeholders are SAFE because:
    *   1. The nulls BitSet is the authoritative source of which indices are NULL
    *   2. All accessors check the BitSet before reading array values
    *   3. External code must use Option-based accessors or check isNull first
    *   4. The null values never escape to safe code
    *
    * Uses single-pass foldLeft with VectorBuilder to avoid double traversal and short-circuits on
    * first error.
    */
  def fromValues(values: Vector[Any], columnType: ColumnType): Either[ExecutionError, Column[?]] = {
    val nullIndices = values.zipWithIndex.collect {
      case (v, idx) if Option(v).isEmpty => idx
    }.to(BitSet)

    columnType match {
      case ColumnType.IntType =>
        val result = values.foldLeft[Either[ExecutionError, scala.collection.immutable.VectorBuilder[Int]]](
          Right(new scala.collection.immutable.VectorBuilder[Int]())
        ) {
          case (Left(err), _) => Left(err)
          case (Right(builder), v) =>
            Option(v) match {
              case None => Right(builder += 0)
              case Some(i: Int) => Right(builder += i)
              case Some(other) =>
                Left(ExecutionError.TypeMismatch("Int", other.getClass.getSimpleName, "Column.fromValues"))
            }
        }

        result.map(builder => IntColumn(builder.result().toArray, nullIndices))

      case ColumnType.LongType =>
        val result = values.foldLeft[Either[ExecutionError, scala.collection.immutable.VectorBuilder[Long]]](
          Right(new scala.collection.immutable.VectorBuilder[Long]())
        ) {
          case (Left(err), _) => Left(err)
          case (Right(builder), v) =>
            Option(v) match {
              case None => Right(builder += 0L)
              case Some(l: Long) => Right(builder += l)
              case Some(other) =>
                Left(ExecutionError.TypeMismatch("Long", other.getClass.getSimpleName, "Column.fromValues"))
            }
        }

        result.map(builder => LongColumn(builder.result().toArray, nullIndices))

      case ColumnType.DoubleType =>
        val result = values.foldLeft[Either[ExecutionError, scala.collection.immutable.VectorBuilder[Double]]](
          Right(new scala.collection.immutable.VectorBuilder[Double]())
        ) {
          case (Left(err), _) => Left(err)
          case (Right(builder), v) =>
            Option(v) match {
              case None => Right(builder += 0.0)
              case Some(d: Double) => Right(builder += d)
              case Some(other) =>
                Left(ExecutionError.TypeMismatch("Double", other.getClass.getSimpleName, "Column.fromValues"))
            }
        }

        result.map(builder => DoubleColumn(builder.result().toArray, nullIndices))

      case ColumnType.StringType =>
        val result = values.foldLeft[Either[ExecutionError, scala.collection.immutable.VectorBuilder[String]]](
          Right(new scala.collection.immutable.VectorBuilder[String]())
        ) {
          case (Left(err), _) => Left(err)
          case (Right(builder), v) =>
            Option(v) match {
              case None =>
                Right(builder += null) // scalafix:ok DisableSyntax.null
              case Some(s: String) => Right(builder += s)
              case Some(other) =>
                Left(ExecutionError.TypeMismatch("String", other.getClass.getSimpleName, "Column.fromValues"))
            }
        }

        result.map(builder => StringColumn(builder.result().toArray, nullIndices))

      case ColumnType.BooleanType =>
        val result = values.foldLeft[Either[ExecutionError, scala.collection.immutable.VectorBuilder[Boolean]]](
          Right(new scala.collection.immutable.VectorBuilder[Boolean]())
        ) {
          case (Left(err), _) => Left(err)
          case (Right(builder), v) =>
            Option(v) match {
              case None => Right(builder += false)
              case Some(b: Boolean) => Right(builder += b)
              case Some(other) =>
                Left(ExecutionError.TypeMismatch("Boolean", other.getClass.getSimpleName, "Column.fromValues"))
            }
        }

        result.map(builder => BooleanColumn(builder.result().toArray, nullIndices))

      case ColumnType.DateType =>
        val result = values.foldLeft[Either[ExecutionError, scala.collection.immutable.VectorBuilder[Int]]](
          Right(new scala.collection.immutable.VectorBuilder[Int]())
        ) {
          case (Left(err), _) => Left(err)
          case (Right(builder), v) =>
            Option(v) match {
              case None => Right(builder += 0)
              case Some(d: java.time.LocalDate) => Right(builder += d.toEpochDay.toInt)
              case Some(i: Int) => Right(builder += i)
              case Some(other) =>
                Left(ExecutionError.TypeMismatch("LocalDate", other.getClass.getSimpleName, "Column.fromValues"))
            }
        }

        result.map(builder => DateColumn(builder.result().toArray, nullIndices))

      case ColumnType.AnyType | ColumnType.OptionType(_) | ColumnType.ArrayType(_) | ColumnType.MapType(_, _) =>
        Right(AnyColumn(values.toArray, nullIndices))
    }
  }
}
