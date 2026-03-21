package net.ghoula.strongbow

import net.ghoula.strongbow.errors.{NonEmptyList, SchemaError}
import net.ghoula.strongbow.specs.{AggSpec, KeySpec, SortSpec, WindowExprSpec}

/** Immutable description of dataset transformations.
  *
  * Dataset[T] is pure data—just an enum describing computation. No execution happens until
  * interpreted.
  *
  * @tparam T
  *   The row type of this dataset
  */
enum Dataset[T] {
  case Root[T](source: DataSource, schema: Schema[T]) extends Dataset[T]
  case Filter[T](parent: Dataset[T], predicate: Expr[T, Boolean]) extends Dataset[T]
  case Map[A, B](parent: Dataset[A], func: A => B, schema: Schema[B]) extends Dataset[B]
  case FlatMap[A, B](parent: Dataset[A], func: A => Iterable[B], schema: Schema[B]) extends Dataset[B]
  case Select[T, U](parent: Dataset[T], projection: T => U, schema: Schema[U]) extends Dataset[U]
  case SelectExprs[In, Out](
    parent: Dataset[In],
    exprs: Vector[(String, Expr[In, Any], ColumnType)],
    schema: Schema[Out]
  ) extends Dataset[Out]
  case Distinct[T](parent: Dataset[T]) extends Dataset[T]
  case Limit[T](parent: Dataset[T], n: Int) extends Dataset[T]
  case Union[T](left: Dataset[T], right: Dataset[T]) extends Dataset[T]
  case InnerJoin[A, B](left: Dataset[A], right: Dataset[B], condition: (A, B) => Boolean) extends Dataset[(A, B)]
  case LeftJoin[A, B](left: Dataset[A], right: Dataset[B], condition: (A, B) => Boolean) extends Dataset[(A, Option[B])]
  case RightJoin[A, B](left: Dataset[A], right: Dataset[B], condition: (A, B) => Boolean)
      extends Dataset[(Option[A], B)]
  case FullJoin[A, B](left: Dataset[A], right: Dataset[B], condition: (A, B) => Boolean)
      extends Dataset[(Option[A], Option[B])]
  case LeftAntiJoin[A, B](left: Dataset[A], right: Dataset[B], condition: (A, B) => Boolean) extends Dataset[A]
  case InnerJoinOn[A, B, K](
    left: Dataset[A],
    right: Dataset[B],
    leftKey: Expr[A, K],
    rightKey: Expr[B, K],
    leftKeyType: ColumnType,
    rightKeyType: ColumnType
  ) extends Dataset[(A, B)]
  case LeftJoinOn[A, B, K](
    left: Dataset[A],
    right: Dataset[B],
    leftKey: Expr[A, K],
    rightKey: Expr[B, K],
    leftKeyType: ColumnType,
    rightKeyType: ColumnType
  ) extends Dataset[(A, Option[B])]
  case RightJoinOn[A, B, K](
    left: Dataset[A],
    right: Dataset[B],
    leftKey: Expr[A, K],
    rightKey: Expr[B, K],
    leftKeyType: ColumnType,
    rightKeyType: ColumnType
  ) extends Dataset[(Option[A], B)]
  case FullJoinOn[A, B, K](
    left: Dataset[A],
    right: Dataset[B],
    leftKey: Expr[A, K],
    rightKey: Expr[B, K],
    leftKeyType: ColumnType,
    rightKeyType: ColumnType
  ) extends Dataset[(Option[A], Option[B])]
  case LeftAntiJoinOn[A, B, K](
    left: Dataset[A],
    right: Dataset[B],
    leftKey: Expr[A, K],
    rightKey: Expr[B, K],
    leftKeyType: ColumnType,
    rightKeyType: ColumnType
  ) extends Dataset[A]
  case Intersect[T](left: Dataset[T], right: Dataset[T]) extends Dataset[T]
  case Except[T](left: Dataset[T], right: Dataset[T]) extends Dataset[T]
  case Sort[T](parent: Dataset[T], ordering: Ordering[T]) extends Dataset[T]
  case SortBy[T, K](parent: Dataset[T], key: T => K, ordering: Ordering[K]) extends Dataset[T]
  case SortByExpr[T, K](parent: Dataset[T], keyExpr: Expr[T, K], keyType: ColumnType, ordering: Ordering[K])
      extends Dataset[T]
  case Sample[T](
    parent: Dataset[T],
    fraction: Double,
    seed: Long,
    withReplacement: Boolean
  ) extends Dataset[T]
  case ZipWithIndex[T](parent: Dataset[T]) extends Dataset[(T, Long)]
  case ZipWithUniqueId[T](parent: Dataset[T]) extends Dataset[(T, Long)]

  case Persist[T](parent: Dataset[T]) extends Dataset[T]
  case Checkpoint[T](parent: Dataset[T]) extends Dataset[T]

  case Rebalance[T](parent: Dataset[T], numPartitions: Option[Int]) extends Dataset[T]

  case GroupByAgg[In, Out](
    parent: Dataset[In],
    keySpecs: Vector[KeySpec[In]],
    aggSpecs: Vector[AggSpec[In]],
    schema: Schema[Out]
  ) extends Dataset[Out]

  case SortByExprs[T](
    parent: Dataset[T],
    sortKeys: Vector[SortSpec[T]]
  ) extends Dataset[T]

  case LeftSemiJoinOn[A, B, K](
    left: Dataset[A],
    right: Dataset[B],
    leftKey: Expr[A, K],
    rightKey: Expr[B, K],
    leftKeyType: ColumnType,
    rightKeyType: ColumnType
  ) extends Dataset[A]

  case WithWindow[In, Out](
    parent: Dataset[In],
    windowExprs: Vector[WindowExprSpec[In]],
    windowSpec: WindowSpec[In],
    schema: Schema[Out]
  ) extends Dataset[Out]

  case Aggregate[T, R](
    parent: Dataset[T],
    aggSpecs: Vector[AggSpec[T]],
    resultSchema: Schema[R]
  ) extends Dataset[R]
}

object Dataset {

  /** Smart constructor with validation using Either (no deps).
    *
    * Returns Either[NonEmptyList[SchemaError], Dataset[T]] NonEmptyList ensures at least one error
    * on Left.
    */
  def fromColumns[T](
    cols: Vector[Column[?]],
    schema: Schema[T]
  ): Either[NonEmptyList[SchemaError], Dataset[T]] = {
    val validations = List(
      validateColumnCount(cols, schema),
      validateColumnTypes(cols, schema),
      validateColumnLengths(cols)
    )

    val errors = validations.collect { case Left(e) => e }.flatten
    if (errors.isEmpty) {
      Right(Root(InMemorySource(cols), schema))
    } else {
      Left(NonEmptyList.fromListUnsafe(errors))
    }
  }

  /** Unsafe constructor for internal use when validation already done. */
  private[strongbow] inline def unsafeRoot[T](cols: Vector[Column[?]], schema: Schema[T]): Dataset[T] = {
    Root(InMemorySource(cols), schema)
  }

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

    /** Sort by expression — avoids decoding rows, reads sort keys directly from columns. */
    inline def sortByExpr[K](keyExpr: Expr[T, K], keyType: ColumnType)(using ord: Ordering[K]): Dataset[T] = {
      SortByExpr(ds, keyExpr, keyType, ord)
    }

    /** Select columns by evaluating expressions. */
    inline def select(exprs: (String, Expr[T, Any], ColumnType)*)(using schema: Schema[T]): Dataset[T] = {
      SelectExprs(ds, exprs.toVector, schema)
    }

    /** Select columns, changing the output type. */
    inline def selectAs[U](exprs: (String, Expr[T, Any], ColumnType)*)(using schema: Schema[U]): Dataset[U] = {
      SelectExprs(ds, exprs.toVector, schema)
    }

    /** Sample fraction of rows.
      *
      * @param fraction
      *   Sampling fraction 0.0 to 1.0
      * @param seed
      *   Random seed for reproducibility
      * @param withReplacement
      *   Allow duplicate samples
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

    /** Zip dataset with unique IDs (not necessarily sequential).
      *
      * In-memory: produces sequential IDs (same as zipWithIndex). Spark: uses
      * monotonically_increasing_id() which guarantees uniqueness but not sequentiality.
      */
    inline def zipWithUniqueId: Dataset[(T, Long)] = {
      ZipWithUniqueId(ds)
    }

    /** Hint to cache the materialized result.
      *
      * In-memory: materializes parent (no caching layer). Spark: calls df.persist().
      */
    inline def persist: Dataset[T] = {
      Persist(ds)
    }

    /** Materialize and truncate the logical plan.
      *
      * In-memory: materializes into a fresh dataset. Spark: calls df.checkpoint().
      */
    inline def checkpoint: Dataset[T] = {
      Checkpoint(ds)
    }

    /** Repartition data for balanced parallelism.
      *
      * In-memory: no-op (no partitions). Spark: calls df.repartition().
      */
    inline def rebalance: Dataset[T] = {
      Rebalance(ds, None)
    }

    /** Repartition data into a specific number of partitions. */
    inline def rebalance(numPartitions: Int): Dataset[T] = {
      Rebalance(ds, Some(numPartitions))
    }

    /** Alias for union. */
    inline def ++(other: Dataset[T]): Dataset[T] = {
      union(other)
    }

    /** Inner join with another dataset on a condition.
      *
      * @param other
      *   The right dataset to join with
      * @param condition
      *   Join predicate evaluated on pairs of rows
      * @return
      *   Dataset of tuples (T, U) for matching rows
      */
    inline def join[U](other: Dataset[U], condition: (T, U) => Boolean): Dataset[(T, U)] = {
      InnerJoin(ds, other, condition)
    }

    /** Left outer join with another dataset.
      *
      * @param other
      *   The right dataset to join with
      * @param condition
      *   Join predicate evaluated on pairs of rows
      * @return
      *   Dataset of tuples (T, Option[U]) where U is None for unmatched left rows
      */
    inline def leftJoin[U](other: Dataset[U], condition: (T, U) => Boolean): Dataset[(T, Option[U])] = {
      LeftJoin(ds, other, condition)
    }

    /** Right outer join with another dataset.
      *
      * @param other
      *   The right dataset to join with
      * @param condition
      *   Join predicate evaluated on pairs of rows
      * @return
      *   Dataset of tuples (Option[T], U) where T is None for unmatched right rows
      */
    inline def rightJoin[U](other: Dataset[U], condition: (T, U) => Boolean): Dataset[(Option[T], U)] = {
      RightJoin(ds, other, condition)
    }

    /** Full outer join with another dataset.
      *
      * @param other
      *   The right dataset to join with
      * @param condition
      *   Join predicate evaluated on pairs of rows
      * @return
      *   Dataset of tuples (Option[T], Option[U]) where either side may be None for unmatched rows
      */
    inline def fullJoin[U](other: Dataset[U], condition: (T, U) => Boolean): Dataset[(Option[T], Option[U])] = {
      FullJoin(ds, other, condition)
    }

    /** Left anti join - returns rows from left with no match in right.
      *
      * @param other
      *   The right dataset to join with
      * @param condition
      *   Join predicate evaluated on pairs of rows
      * @return
      *   Dataset of T rows from left that have no matching right rows
      */
    inline def antiJoin[U](other: Dataset[U], condition: (T, U) => Boolean): Dataset[T] = {
      LeftAntiJoin(ds, other, condition)
    }

    /** Expression-based inner join — enables hash join in-memory and native equi-join in Spark. */
    inline def joinOn[U, K](
      other: Dataset[U],
      leftKey: Expr[T, K],
      rightKey: Expr[U, K],
      leftKeyType: ColumnType,
      rightKeyType: ColumnType
    ): Dataset[(T, U)] = {
      InnerJoinOn(ds, other, leftKey, rightKey, leftKeyType, rightKeyType)
    }

    /** Expression-based left join. */
    inline def leftJoinOn[U, K](
      other: Dataset[U],
      leftKey: Expr[T, K],
      rightKey: Expr[U, K],
      leftKeyType: ColumnType,
      rightKeyType: ColumnType
    ): Dataset[(T, Option[U])] = {
      LeftJoinOn(ds, other, leftKey, rightKey, leftKeyType, rightKeyType)
    }

    /** Expression-based right join. */
    inline def rightJoinOn[U, K](
      other: Dataset[U],
      leftKey: Expr[T, K],
      rightKey: Expr[U, K],
      leftKeyType: ColumnType,
      rightKeyType: ColumnType
    ): Dataset[(Option[T], U)] = {
      RightJoinOn(ds, other, leftKey, rightKey, leftKeyType, rightKeyType)
    }

    /** Expression-based full join. */
    inline def fullJoinOn[U, K](
      other: Dataset[U],
      leftKey: Expr[T, K],
      rightKey: Expr[U, K],
      leftKeyType: ColumnType,
      rightKeyType: ColumnType
    ): Dataset[(Option[T], Option[U])] = {
      FullJoinOn(ds, other, leftKey, rightKey, leftKeyType, rightKeyType)
    }

    /** Expression-based anti join. */
    inline def antiJoinOn[U, K](
      other: Dataset[U],
      leftKey: Expr[T, K],
      rightKey: Expr[U, K],
      leftKeyType: ColumnType,
      rightKeyType: ColumnType
    ): Dataset[T] = {
      LeftAntiJoinOn(ds, other, leftKey, rightKey, leftKeyType, rightKeyType)
    }

    /** Expression-based semi join — for IN/EXISTS subquery patterns. */
    inline def semiJoinOn[U, K](
      other: Dataset[U],
      leftKey: Expr[T, K],
      rightKey: Expr[U, K],
      leftKeyType: ColumnType,
      rightKeyType: ColumnType
    ): Dataset[T] = {
      LeftSemiJoinOn(ds, other, leftKey, rightKey, leftKeyType, rightKeyType)
    }

    /** GROUP BY with arbitrary keys and aggregations, producing a new Dataset.
      *
      * Uses Expr throughout — fully pushable to Spark. Supports any number of keys and
      * aggregations. HAVING is just `.filter()` on the result.
      */
    inline def groupByAgg[Out](
      keys: Vector[KeySpec[T]],
      aggs: Vector[AggSpec[T]]
    )(using schema: Schema[Out]): Dataset[Out] = {
      GroupByAgg(ds, keys, aggs, schema)
    }

    /** Multi-column ORDER BY with mixed ASC/DESC directions. */
    inline def sortByExprs(
      sortKeys: Vector[SortSpec[T]]
    ): Dataset[T] = {
      SortByExprs(ds, sortKeys)
    }

    /** Add window function columns to this dataset. */
    inline def withWindow[Out](
      windowExprs: Vector[WindowExprSpec[T]],
      windowSpec: WindowSpec[T]
    )(using schema: Schema[Out]): Dataset[Out] = {
      WithWindow(ds, windowExprs, windowSpec, schema)
    }

    /** Split dataset into two based on a predicate.
      *
      * Returns (matching, non-matching) — equivalent to `(ds.filter(f), ds.filter(!f))`.
      */
    inline def partition(predicate: Expr[T, Boolean]): (Dataset[T], Dataset[T]) = {
      (ds.filter(predicate), ds.filter(Expr.Not(predicate)))
    }

    /** Global aggregation without grouping keys.
      *
      * Evaluates aggregation expressions over the entire dataset, producing a single-row result.
      */
    inline def aggregate[R](aggSpecs: Vector[AggSpec[T]])(using schema: Schema[R]): Dataset[R] = {
      Aggregate(ds, aggSpecs, schema)
    }
  }

  private def validateColumnCount[T](
    cols: Vector[Column[?]],
    schema: Schema[T]
  ): Either[List[SchemaError], Unit] = {
    if (cols.length == schema.columnCount) {
      Right(())
    } else {
      Left(List(SchemaError.ColumnCountMismatch(schema.columnCount, cols.length)))
    }
  }

  private def validateColumnTypes[T](
    cols: Vector[Column[?]],
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
    cols: Vector[Column[?]]
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
