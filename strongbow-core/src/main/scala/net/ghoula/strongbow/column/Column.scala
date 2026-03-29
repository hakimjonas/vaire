package net.ghoula.strongbow.column

import scala.collection.immutable.BitSet

import net.ghoula.strongbow.errors.ExecutionError
import net.ghoula.strongbow.types
import net.ghoula.strongbow.types.RowIndex

/** Specialized columnar storage avoiding boxing.
  *
  * Each column type uses primitive arrays where possible. Nullability is tracked via BitSet for
  * memory efficiency. The type parameter `A` tracks the logical element type via GADT refinement.
  */
enum Column[+A] {
  case IntColumn(data: Array[Int], nulls: BitSet) extends Column[Int]
  case LongColumn(data: Array[Long], nulls: BitSet) extends Column[Long]
  case DoubleColumn(data: Array[Double], nulls: BitSet) extends Column[Double]
  case StringColumn(data: Array[String | Null], nulls: BitSet) extends Column[String]
  case BooleanColumn(data: Array[Boolean], nulls: BitSet) extends Column[Boolean]
  case DateColumn(data: Array[Int], nulls: BitSet) extends Column[types.Date]
  case AnyColumn(data: Array[Any | Null], nulls: BitSet) extends Column[Any]

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

  private inline def nullSet: BitSet = this match {
    case IntColumn(_, nulls) => nulls
    case LongColumn(_, nulls) => nulls
    case DoubleColumn(_, nulls) => nulls
    case StringColumn(_, nulls) => nulls
    case BooleanColumn(_, nulls) => nulls
    case DateColumn(_, nulls) => nulls
    case AnyColumn(_, nulls) => nulls
  }

  inline def isNull(index: RowIndex): Boolean = nullSet.contains(index.toInt)

  /** Untyped single-value extraction for interop boundaries.
    *
    * Returns Any because Column[+A] in a heterogeneous Vector[Column[?]] erases the type parameter.
    * For typed access, we pattern match the Column variant directly to get the typed Array. Returns
    * null for SQL NULL rows (BitSet is authoritative).
    */
  inline def getValue(index: Int): Any | Null = { // scalafix:ok DisableSyntax.null
    if (nullSet.contains(index)) null // scalafix:ok DisableSyntax.null
    else
      this match {
        case IntColumn(data, _) => data(index)
        case LongColumn(data, _) => data(index)
        case DoubleColumn(data, _) => data(index)
        case StringColumn(data, _) => data(index)
        case BooleanColumn(data, _) => data(index)
        case DateColumn(data, _) => types.Date.ofEpochDay(data(index).toLong)
        case AnyColumn(data, _) => data(index)
      }
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
    val trimmedNulls = nullSet.filter(_ < len)
    this match {
      case IntColumn(data, _) => IntColumn(java.util.Arrays.copyOfRange(data, 0, len), trimmedNulls)
      case LongColumn(data, _) => LongColumn(java.util.Arrays.copyOfRange(data, 0, len), trimmedNulls)
      case DoubleColumn(data, _) => DoubleColumn(java.util.Arrays.copyOfRange(data, 0, len), trimmedNulls)
      case StringColumn(data, _) => StringColumn(java.util.Arrays.copyOfRange(data, 0, len), trimmedNulls)
      case BooleanColumn(data, _) => BooleanColumn(java.util.Arrays.copyOfRange(data, 0, len), trimmedNulls)
      case DateColumn(data, _) => DateColumn(java.util.Arrays.copyOfRange(data, 0, len), trimmedNulls)
      case AnyColumn(data, _) =>
        val arr = new Array[Any | Null](len)
        System.arraycopy(data, 0, arr, 0, len)
        AnyColumn(arr, trimmedNulls)
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
      val rightNulls = other.nullSet
      val leftNulls = this.nullSet
      val combinedNulls = leftNulls | rightNulls.map(_ + leftLen)

      def concatArrays[T: scala.reflect.ClassTag](
        l: Array[T],
        r: Array[T],
        wrap: (Array[T], BitSet) => Column[A]
      ): Either[ExecutionError, Column[A]] = {
        val arr = new Array[T](leftLen + rightLen)
        System.arraycopy(l, 0, arr, 0, leftLen)
        System.arraycopy(r, 0, arr, leftLen, rightLen)
        Right(wrap(arr, combinedNulls))
      }

      (this, other) match {
        case (IntColumn(l, _), IntColumn(r, _)) => concatArrays(l, r, IntColumn(_, _))
        case (LongColumn(l, _), LongColumn(r, _)) => concatArrays(l, r, LongColumn(_, _))
        case (DoubleColumn(l, _), DoubleColumn(r, _)) => concatArrays(l, r, DoubleColumn(_, _))
        case (StringColumn(l, _), StringColumn(r, _)) => concatArrays(l, r, StringColumn(_, _))
        case (BooleanColumn(l, _), BooleanColumn(r, _)) => concatArrays(l, r, BooleanColumn(_, _))
        case (DateColumn(l, _), DateColumn(r, _)) => concatArrays(l, r, DateColumn(_, _))
        case (AnyColumn(l, _), AnyColumn(r, _)) => concatArrays(l, r, AnyColumn(_, _))
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
    Array.tabulate(indices.length)(i => if (nulls.contains(indices(i))) defaultValue else data(indices(i)))
  }

  private def buildNullSet(nulls: BitSet, indices: Array[Int]): BitSet = {
    BitSet.fromSpecific(indices.indices.filter(i => nulls.contains(indices(i))))
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

  inline def string(data: Array[String | Null], nulls: BitSet = BitSet.empty): Column[String] = {
    StringColumn(data, nulls)
  }

  inline def boolean(data: Array[Boolean], nulls: BitSet = BitSet.empty): Column[Boolean] = {
    BooleanColumn(data, nulls)
  }

  /** Create a DateColumn from epoch day values. */
  inline def date(data: Array[Int], nulls: BitSet = BitSet.empty): Column[types.Date] = {
    DateColumn(data, nulls)
  }

  inline def any(data: Array[Any | Null], nulls: BitSet = BitSet.empty): Column[Any] = {
    AnyColumn(data, nulls)
  }

  /** Create an empty column of the given type. */
  def empty(columnType: ColumnType): Column[?] = columnType match {
    case ColumnType.IntType => IntColumn(Array.empty[Int], BitSet.empty)
    case ColumnType.LongType => LongColumn(Array.empty[Long], BitSet.empty)
    case ColumnType.DoubleType => DoubleColumn(Array.empty[Double], BitSet.empty)
    case ColumnType.StringType => StringColumn(Array.empty[String | Null], BitSet.empty)
    case ColumnType.BooleanType => BooleanColumn(Array.empty[Boolean], BitSet.empty)
    case ColumnType.DateType => DateColumn(Array.empty[Int], BitSet.empty)
    case ColumnType.AnyType => AnyColumn(Array.empty[Any | Null], BitSet.empty)
    case ColumnType.OptionType(_) => AnyColumn(Array.empty[Any | Null], BitSet.empty)
    case ColumnType.ArrayType(_) => AnyColumn(Array.empty[Any | Null], BitSet.empty)
    case ColumnType.MapType(_, _) => AnyColumn(Array.empty[Any | Null], BitSet.empty)
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
    * the first error.
    */
  def fromValues(values: Vector[Any], columnType: ColumnType): Either[ExecutionError, Column[?]] = {
    val nullIndices = values.zipWithIndex.collect {
      case (v, idx) if Option(v).isEmpty => idx
    }.to(BitSet)

    def buildColumn[T: scala.reflect.ClassTag](
      typeName: String,
      defaultVal: T,
      cast: PartialFunction[Any, T],
      wrap: (Array[T], BitSet) => Column[?]
    ): Either[ExecutionError, Column[?]] = {
      val arr = new Array[T](values.length)
      val error = values.zipWithIndex.foldLeft(Option.empty[ExecutionError]) {
        case (err @ Some(_), _) => err
        case (None, (v, idx)) =>
          Option(v) match {
            case None => arr(idx) = defaultVal; None
            case Some(x) if cast.isDefinedAt(x) => arr(idx) = cast(x); None
            case Some(x) => Some(ExecutionError.TypeMismatch(typeName, x.getClass.getSimpleName, "Column.fromValues"))
          }
      }
      error.toLeft(wrap(arr, nullIndices))
    }

    columnType match {
      case ColumnType.IntType =>
        buildColumn[Int]("Int", 0, { case i: Int => i }, IntColumn(_, _))
      case ColumnType.LongType =>
        buildColumn[Long]("Long", 0L, { case l: Long => l }, LongColumn(_, _))
      case ColumnType.DoubleType =>
        buildColumn[Double]("Double", 0.0, { case d: Double => d }, DoubleColumn(_, _))
      case ColumnType.StringType =>
        buildColumn[String | Null](
          "String",
          null,
          { case s: String => s },
          StringColumn(_, _)
        ) // scalafix:ok DisableSyntax.null
      case ColumnType.BooleanType =>
        buildColumn[Boolean]("Boolean", false, { case b: Boolean => b }, BooleanColumn(_, _))
      case ColumnType.DateType =>
        buildColumn[Int](
          "LocalDate",
          0,
          { case d: java.time.LocalDate => d.toEpochDay.toInt; case i: Int => i },
          DateColumn(_, _)
        )
      case ColumnType.AnyType | ColumnType.OptionType(_) | ColumnType.ArrayType(_) | ColumnType.MapType(_, _) =>
        Right(AnyColumn(values.toArray, nullIndices))
    }
  }

  def sortIndicesByColumn(col: Column[?], rowCount: Int): Array[Int] = {
    def sort[T](data: IArray[T])(lt: (T, T) => Boolean): Array[Int] =
      (0 until rowCount).sortWith((a, b) => lt(data(a), data(b))).toArray

    col match {
      case IntColumn(data, _) => sort(IArray.unsafeFromArray(data))(_ < _)
      case LongColumn(data, _) => sort(IArray.unsafeFromArray(data))(_ < _)
      case DoubleColumn(data, _) => sort(IArray.unsafeFromArray(data))((a, b) => java.lang.Double.compare(a, b) < 0)
      case StringColumn(data, _) => sort(IArray.unsafeFromArray(data))((a, b) => a.nn.compareTo(b) < 0)
      case DateColumn(data, _) => sort(IArray.unsafeFromArray(data))(_ < _)
      case BooleanColumn(data, _) => sort(IArray.unsafeFromArray(data))((a, b) => !a && b)
      case AnyColumn(data, _) =>
        sort(IArray.unsafeFromArray(data))((a, b) => String.valueOf(a).compareTo(String.valueOf(b)) < 0)
    }
  }
}
