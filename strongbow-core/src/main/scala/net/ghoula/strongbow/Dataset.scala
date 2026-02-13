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
  case Map[A, B](parent: Dataset[A], func: A => B) extends Dataset[B]
  case FlatMap[A, B](parent: Dataset[A], func: A => Iterable[B]) extends Dataset[B]
  case Select[T, U](parent: Dataset[T], projection: T => U) extends Dataset[U]
  case SelectExprs[T](parent: Dataset[T], exprs: Vector[(String, Expr[T, Any], ColumnType)]) extends Dataset[T]
  case Distinct[T](parent: Dataset[T]) extends Dataset[T]
  case Limit[T](parent: Dataset[T], n: Int) extends Dataset[T]
  case Union[T](left: Dataset[T], right: Dataset[T]) extends Dataset[T]
  case Sort[T](parent: Dataset[T], ordering: Ordering[T]) extends Dataset[T]
  case SortBy[T, K](parent: Dataset[T], key: T => K, ordering: Ordering[K]) extends Dataset[T]
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

    inline def map[U](f: T => U): Dataset[U] = {
      Map(ds, f)
    }

    inline def flatMap[U](f: T => Iterable[U]): Dataset[U] = {
      FlatMap(ds, f)
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

    /** Join with another dataset on a condition.
      *
      * TODO: Implement join logic. For now throws to indicate unimplemented feature.
      */
    inline def join[U](
      other: Dataset[U],
      condition: (Dataset[T], Dataset[U]) => Expr[(T, U), Boolean]
    ): Dataset[(T, U)] = {
      throw new UnsupportedOperationException("Dataset joins not yet implemented") // scalafix:ok DisableSyntax.throw
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
