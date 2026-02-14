package net.ghoula.strongbow

import net.ghoula.strongbow.errors.{NonEmptyList, SchemaError}

/** Immutable description of dataset transformations.
  *
  * Dataset[T] is pure data—just an enum describing computation. No execution happens until
  * interpreted.
  *
  * @tparam T
  *   The row type of this dataset
  */
enum Dataset[+T] {
  case Root[T](columns: Vector[Column], schema: Schema[T]) extends Dataset[T]
  case Filter[T](parent: Dataset[T], predicate: Expr[T, Boolean]) extends Dataset[T]
  case Map[A, B](parent: Dataset[A], func: A => B, schema: Schema[B]) extends Dataset[B]
  case FlatMap[A, B](parent: Dataset[A], func: A => Iterable[B], schema: Schema[B]) extends Dataset[B]
  case Select[T, U](parent: Dataset[T], projection: T => U, schema: Schema[U]) extends Dataset[U]
  case SelectExprs[T](parent: Dataset[T], exprs: Vector[(String, Expr[T, Any], ColumnType)]) extends Dataset[T]
  case Distinct[T](parent: Dataset[T]) extends Dataset[T]
  case Limit[T](parent: Dataset[T], n: Int) extends Dataset[T]
  case Union[T](left: Dataset[T], right: Dataset[T]) extends Dataset[T]
  case InnerJoin[A, B](left: Dataset[A], right: Dataset[B], condition: (A, B) => Boolean) extends Dataset[(A, B)]
  case LeftJoin[A, B](left: Dataset[A], right: Dataset[B], condition: (A, B) => Boolean) extends Dataset[(A, Option[B])]
  case RightJoin[A, B](left: Dataset[A], right: Dataset[B], condition: (A, B) => Boolean) extends Dataset[(Option[A], B)]
  case FullJoin[A, B](left: Dataset[A], right: Dataset[B], condition: (A, B) => Boolean) extends Dataset[(Option[A], Option[B])]
  case LeftAntiJoin[A, B](left: Dataset[A], right: Dataset[B], condition: (A, B) => Boolean) extends Dataset[A]
  case Intersect[T](left: Dataset[T], right: Dataset[T]) extends Dataset[T]
  case Except[T](left: Dataset[T], right: Dataset[T]) extends Dataset[T]
  case Sort[T](parent: Dataset[T], ordering: Ordering[T]) extends Dataset[T]
  case SortBy[T, K](parent: Dataset[T], key: T => K, ordering: Ordering[K]) extends Dataset[T]
  case Sample[T](
    parent: Dataset[T],
    fraction: Double,
    seed: Long,
    withReplacement: Boolean
  ) extends Dataset[T]
  case ZipWithIndex[T](parent: Dataset[T]) extends Dataset[(T, Long)]
  case GroupedToPairs[K, V](grouped: Grouped[K, V], schemaK: Schema[K], schemaV: Schema[V]) extends Dataset[(K, V)]
  case GroupedKeys[K, V](grouped: Grouped[K, V], schemaK: Schema[K]) extends Dataset[K]
  case GroupedValues[K, V](grouped: Grouped[K, V], schemaV: Schema[V]) extends Dataset[V]
}

object Dataset {

  /** Smart constructor with validation using Either (no deps).
    *
    * Returns Either[NonEmptyList[SchemaError], Dataset[T]] NonEmptyList ensures at least one error
    * on Left.
    */
  def fromColumns[T](
    cols: Vector[Column],
    schema: Schema[T]
  ): Either[NonEmptyList[SchemaError], Dataset[T]] = {
    val validations = List(
      validateColumnCount(cols, schema),
      validateColumnTypes(cols, schema),
      validateColumnLengths(cols)
    )

    val errors = validations.collect { case Left(e) => e }.flatten
    if (errors.isEmpty) {
      Right(Root(cols, schema))
    } else {
      Left(NonEmptyList.fromListUnsafe(errors))
    }
  }

  /** Unsafe constructor for internal use when validation already done. */
  private[strongbow] inline def unsafeRoot[T](cols: Vector[Column], schema: Schema[T]): Dataset[T] = {
    Root(cols, schema)
  }

  // Extension methods on Dataset
  extension [T](ds: Dataset[T]) {
    inline def filter(predicate: Expr[T, Boolean]): Dataset[T] = {
      Filter(ds, predicate)
    }

    inline def map[U](f: T => U)(using schema: Schema[U]): Dataset[U] = {
      Map(ds, f, schema)
    }

    inline def flatMap[U](f: T => Iterable[U])(using schema: Schema[U]): Dataset[U] = {
      FlatMap(ds, f, schema)
    }

    inline def distinct: Dataset[T] = {
      Distinct(ds)
    }

    inline def limit(n: Int): Dataset[T] = {
      Limit(ds, n)
    }

    inline def union(other: Dataset[T]): Dataset[T] = {
      Union(ds, other)
    }

    /** Set intersection - rows present in both datasets (deduplicated).
      *
      * Matches Spark's `Dataset.intersect()` semantics.
      */
    inline def intersect(other: Dataset[T]): Dataset[T] = {
      Intersect(ds, other)
    }

    /** Set difference - rows in this dataset but not in other (deduplicated).
      *
      * Matches Spark's `Dataset.except()` semantics.
      */
    inline def except(other: Dataset[T]): Dataset[T] = {
      Except(ds, other)
    }

    inline def sort(using ord: Ordering[T]): Dataset[T] = {
      Sort(ds, ord)
    }

    inline def sortBy[K](key: T => K)(using ord: Ordering[K]): Dataset[T] = {
      SortBy(ds, key, ord)
    }

    inline def groupBy[K](key: T => K): Grouped[K, T] = {
      Grouped.GroupBy(ds, key)
    }

    inline def keyBy[K](key: T => K): Grouped[K, T] = {
      groupBy(key)
    }

    /** Select columns by evaluating expressions. */
    inline def select(exprs: (String, Expr[T, Any], ColumnType)*): Dataset[T] = {
      SelectExprs(ds, exprs.toVector)
    }

    /** Sample fraction of rows.
      *
      * @param fraction Sampling fraction 0.0 to 1.0
      * @param seed Random seed for reproducibility
      * @param withReplacement Allow duplicate samples
      */
    inline def sample(
      fraction: Double,
      seed: Long = scala.util.Random.nextLong(),
      withReplacement: Boolean = false
    ): Dataset[T] = {
      require(fraction >= 0.0 && fraction <= 1.0, "fraction must be between 0 and 1")
      Sample(ds, fraction, seed, withReplacement)
    }

    /** Zip dataset with sequential indices.
      *
      * For deterministic results, sort before zipping.
      */
    inline def zipWithIndex: Dataset[(T, Long)] = {
      ZipWithIndex(ds)
    }

    /** Alias for union. */
    inline def ++(other: Dataset[T]): Dataset[T] = {
      union(other)
    }

    /** Inner join with another dataset on a condition.
      *
      * @param other The right dataset to join with
      * @param condition Join predicate evaluated on pairs of rows
      * @return Dataset of tuples (T, U) for matching rows
      */
    inline def join[U](other: Dataset[U], condition: (T, U) => Boolean): Dataset[(T, U)] = {
      InnerJoin(ds, other, condition)
    }

    /** Left outer join with another dataset.
      *
      * @param other The right dataset to join with
      * @param condition Join predicate evaluated on pairs of rows
      * @return Dataset of tuples (T, Option[U]) where U is None for unmatched left rows
      */
    inline def leftJoin[U](other: Dataset[U], condition: (T, U) => Boolean): Dataset[(T, Option[U])] = {
      LeftJoin(ds, other, condition)
    }

    /** Right outer join with another dataset.
      *
      * @param other The right dataset to join with
      * @param condition Join predicate evaluated on pairs of rows
      * @return Dataset of tuples (Option[T], U) where T is None for unmatched right rows
      */
    inline def rightJoin[U](other: Dataset[U], condition: (T, U) => Boolean): Dataset[(Option[T], U)] = {
      RightJoin(ds, other, condition)
    }

    /** Full outer join with another dataset.
      *
      * @param other The right dataset to join with
      * @param condition Join predicate evaluated on pairs of rows
      * @return Dataset of tuples (Option[T], Option[U]) where either side may be None for unmatched rows
      */
    inline def fullJoin[U](other: Dataset[U], condition: (T, U) => Boolean): Dataset[(Option[T], Option[U])] = {
      FullJoin(ds, other, condition)
    }

    /** Left anti join - returns rows from left with no match in right.
      *
      * @param other The right dataset to join with
      * @param condition Join predicate evaluated on pairs of rows
      * @return Dataset of T rows from left that have no matching right rows
      */
    inline def antiJoin[U](other: Dataset[U], condition: (T, U) => Boolean): Dataset[T] = {
      LeftAntiJoin(ds, other, condition)
    }
  }

  // Validation helpers (return Option[List[Error]])
  private def validateColumnCount[T](
    cols: Vector[Column],
    schema: Schema[T]
  ): Either[List[SchemaError], Unit] = {
    if (cols.length == schema.columnCount) {
      Right(())
    } else {
      Left(List(SchemaError.ColumnCountMismatch(schema.columnCount, cols.length)))
    }
  }

  private def validateColumnTypes[T](
    cols: Vector[Column],
    schema: Schema[T]
  ): Either[List[SchemaError], Unit] = {
    val mismatches = cols.zip(schema.columnTypes).zipWithIndex.collect {
      case ((col, expectedType), idx) if col.columnType != expectedType =>
        SchemaError.ColumnTypeMismatch(idx, expectedType, col.columnType)
    }

    if (mismatches.isEmpty) {
      Right(())
    } else {
      Left(mismatches.toList)
    }
  }

  private def validateColumnLengths(
    cols: Vector[Column]
  ): Either[List[SchemaError], Unit] = {
    if (cols.isEmpty) {
      Right(())
    } else {
      val expectedLength = cols.head.length
      val mismatches = cols.zipWithIndex.collect {
        case (col, idx) if col.length != expectedLength =>
          SchemaError.ColumnLengthMismatch(idx, expectedLength, col.length)
      }

      if (mismatches.isEmpty) {
        Right(())
      } else {
        Left(mismatches.toList)
      }
    }
  }
}
