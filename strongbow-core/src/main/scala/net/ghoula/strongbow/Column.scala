package net.ghoula.strongbow

import net.ghoula.strongbow.types.RowIndex

import scala.collection.immutable.BitSet

/** Specialized columnar storage avoiding boxing.
  *
  * Each column type uses primitive arrays where possible. Nullability tracked via BitSet for
  * memory efficiency.
  */
enum Column {
  case IntColumn(data: Array[Int], nulls: BitSet)
  case LongColumn(data: Array[Long], nulls: BitSet)
  case DoubleColumn(data: Array[Double], nulls: BitSet)
  case StringColumn(data: Array[String], nulls: BitSet)
  case BooleanColumn(data: Array[Boolean], nulls: BitSet)
  case AnyColumn(data: Array[Any], nulls: BitSet)

  inline def length: Int = this match {
    case IntColumn(data, _) => data.length
    case LongColumn(data, _) => data.length
    case DoubleColumn(data, _) => data.length
    case StringColumn(data, _) => data.length
    case BooleanColumn(data, _) => data.length
    case AnyColumn(data, _) => data.length
  }

  inline def columnType: ColumnType = this match {
    case IntColumn(_, _) => ColumnType.IntType
    case LongColumn(_, _) => ColumnType.LongType
    case DoubleColumn(_, _) => ColumnType.DoubleType
    case StringColumn(_, _) => ColumnType.StringType
    case BooleanColumn(_, _) => ColumnType.BooleanType
    case AnyColumn(_, _) => ColumnType.AnyType
  }

  inline def isNull(index: RowIndex): Boolean = this match {
    case IntColumn(_, nulls) => nulls.contains(index.toInt)
    case LongColumn(_, nulls) => nulls.contains(index.toInt)
    case DoubleColumn(_, nulls) => nulls.contains(index.toInt)
    case StringColumn(_, nulls) => nulls.contains(index.toInt)
    case BooleanColumn(_, nulls) => nulls.contains(index.toInt)
    case AnyColumn(_, nulls) => nulls.contains(index.toInt)
  }

  /** Get value at index. Returns null if the value is null. */
  inline def getValue(index: Int): Any = this match {
    case IntColumn(data, nulls) => if (nulls.contains(index)) null else data(index)
    case LongColumn(data, nulls) => if (nulls.contains(index)) null else data(index)
    case DoubleColumn(data, nulls) => if (nulls.contains(index)) null else data(index)
    case StringColumn(data, nulls) => if (nulls.contains(index)) null else data(index)
    case BooleanColumn(data, nulls) => if (nulls.contains(index)) null else data(index)
    case AnyColumn(data, nulls) => if (nulls.contains(index)) null else data(index)
  }

  /** Typed accessors for zero-cast evaluation.
    *
    * These provide typed access to column data, allowing one-cast-at-boundary pattern. Call the
    * appropriate accessor based on column type to get a properly typed value.
    */
  inline def getInt(index: Int): Int = this match {
    case IntColumn(data, nulls) =>
      if (nulls.contains(index)) 0 else data(index)
    case _ =>
      throw new IllegalStateException(s"Cannot get Int from ${this.columnType}")
  }

  inline def getLong(index: Int): Long = this match {
    case LongColumn(data, nulls) =>
      if (nulls.contains(index)) 0L else data(index)
    case _ =>
      throw new IllegalStateException(s"Cannot get Long from ${this.columnType}")
  }

  inline def getDouble(index: Int): Double = this match {
    case DoubleColumn(data, nulls) =>
      if (nulls.contains(index)) 0.0 else data(index)
    case _ =>
      throw new IllegalStateException(s"Cannot get Double from ${this.columnType}")
  }

  inline def getString(index: Int): String = this match {
    case StringColumn(data, nulls) =>
      if (nulls.contains(index)) null else data(index)
    case _ =>
      throw new IllegalStateException(s"Cannot get String from ${this.columnType}")
  }

  inline def getBoolean(index: Int): Boolean = this match {
    case BooleanColumn(data, nulls) =>
      if (nulls.contains(index)) false else data(index)
    case _ =>
      throw new IllegalStateException(s"Cannot get Boolean from ${this.columnType}")
  }

  /** Type-specialized slicing for efficient filter operations.
    *
    * Creates a new column with only the specified indices, avoiding boxing and intermediate
    * allocations. This is the key optimization for filter performance.
    */
  def slice(indices: IndexedSeq[Int]): Column = this match {
    case IntColumn(data, nulls) =>
      IntColumn(sliceArray(data, indices, nulls, 0), buildNullSet(nulls, indices))
    case LongColumn(data, nulls) =>
      LongColumn(sliceArray(data, indices, nulls, 0L), buildNullSet(nulls, indices))
    case DoubleColumn(data, nulls) =>
      DoubleColumn(sliceArray(data, indices, nulls, 0.0), buildNullSet(nulls, indices))
    case StringColumn(data, nulls) =>
      StringColumn(sliceArray(data, indices, nulls, null), buildNullSet(nulls, indices))
    case BooleanColumn(data, nulls) =>
      BooleanColumn(sliceArray(data, indices, nulls, false), buildNullSet(nulls, indices))
    case AnyColumn(data, nulls) =>
      AnyColumn(sliceArray(data, indices, nulls, null), buildNullSet(nulls, indices))
  }

  /** Generic array slicing with null handling. */
  private def sliceArray[T: scala.reflect.ClassTag](
      data: Array[T],
      indices: IndexedSeq[Int],
      nulls: BitSet,
      defaultValue: T
  ): Array[T] = {
    val newData = new Array[T](indices.size)
    var i = 0
    while (i < indices.size) {
      val srcIdx = indices(i)
      newData(i) = if (nulls.contains(srcIdx)) defaultValue else data(srcIdx)
      i += 1
    }
    newData
  }

  /** Build new null BitSet for sliced indices. */
  private def buildNullSet(nulls: BitSet, indices: IndexedSeq[Int]): BitSet = {
    val builder = BitSet.newBuilder
    var i = 0
    while (i < indices.size) {
      if (nulls.contains(indices(i))) builder += i
      i += 1
    }
    builder.result()
  }
}

object Column {
  // Smart constructors
  inline def int(data: Array[Int], nulls: BitSet = BitSet.empty): Column = {
    IntColumn(data, nulls)
  }

  inline def long(data: Array[Long], nulls: BitSet = BitSet.empty): Column = {
    LongColumn(data, nulls)
  }

  inline def double(data: Array[Double], nulls: BitSet = BitSet.empty): Column = {
    DoubleColumn(data, nulls)
  }

  inline def string(data: Array[String], nulls: BitSet = BitSet.empty): Column = {
    StringColumn(data, nulls)
  }

  inline def boolean(data: Array[Boolean], nulls: BitSet = BitSet.empty): Column = {
    BooleanColumn(data, nulls)
  }

  inline def any(data: Array[Any], nulls: BitSet = BitSet.empty): Column = {
    AnyColumn(data, nulls)
  }

  /** Create an empty column of the given type. */
  def empty(columnType: ColumnType): Column = columnType match {
    case ColumnType.IntType => IntColumn(Array.empty[Int], BitSet.empty)
    case ColumnType.LongType => LongColumn(Array.empty[Long], BitSet.empty)
    case ColumnType.DoubleType => DoubleColumn(Array.empty[Double], BitSet.empty)
    case ColumnType.StringType => StringColumn(Array.empty[String], BitSet.empty)
    case ColumnType.BooleanType => BooleanColumn(Array.empty[Boolean], BitSet.empty)
    case ColumnType.AnyType => AnyColumn(Array.empty[Any], BitSet.empty)
    case ColumnType.OptionType(_) => AnyColumn(Array.empty[Any], BitSet.empty)
  }

  /** Create a column from a vector of values. */
  def fromValues(values: Vector[Any], columnType: ColumnType): Column = {
    val nullIndices = values.zipWithIndex.collect {
      case (v, idx) if Option(v).isEmpty => idx
    }.to(BitSet)

    columnType match {
      case ColumnType.IntType =>
        val data = values.map { v =>
          Option(v) match {
            case None => 0
            case Some(i: Int) => i
            case Some(other) => throw new IllegalArgumentException(s"Expected Int, got $other")
          }
        }.toArray
        IntColumn(data, nullIndices)

      case ColumnType.LongType =>
        val data = values.map { v =>
          Option(v) match {
            case None => 0L
            case Some(l: Long) => l
            case Some(other) => throw new IllegalArgumentException(s"Expected Long, got $other")
          }
        }.toArray
        LongColumn(data, nullIndices)

      case ColumnType.DoubleType =>
        val data = values.map { v =>
          Option(v) match {
            case None => 0.0
            case Some(d: Double) => d
            case Some(other) => throw new IllegalArgumentException(s"Expected Double, got $other")
          }
        }.toArray
        DoubleColumn(data, nullIndices)

      case ColumnType.StringType =>
        val data = values.map { v =>
          Option(v) match {
            case None => null
            case Some(s: String) => s
            case Some(other) => throw new IllegalArgumentException(s"Expected String, got $other")
          }
        }.toArray
        StringColumn(data, nullIndices)

      case ColumnType.BooleanType =>
        val data = values.map { v =>
          Option(v) match {
            case None => false
            case Some(b: Boolean) => b
            case Some(other) => throw new IllegalArgumentException(s"Expected Boolean, got $other")
          }
        }.toArray
        BooleanColumn(data, nullIndices)

      case ColumnType.AnyType | ColumnType.OptionType(_) =>
        AnyColumn(values.toArray, nullIndices)
    }
  }
}
