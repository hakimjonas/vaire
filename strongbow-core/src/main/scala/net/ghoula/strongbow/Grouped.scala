package net.ghoula.strongbow

/** Key-value pair dataset for grouping operations.
  *
  * Grouped[K, V] represents a dataset logically partitioned by key K, enabling type-safe joins,
  * aggregations, and reductions.
  *
  * @tparam K
  *   The key type
  * @tparam V
  *   The value type
  */
enum Grouped[K, +V] {
  case GroupBy[K, V](parent: Dataset[V], key: V => K) extends Grouped[K, V]
  case FromPairs[K, V](parent: Dataset[(K, V)]) extends Grouped[K, V]
  case MapValues[K, A, B](parent: Grouped[K, A], func: A => B) extends Grouped[K, B]
  case FlatMapValues[K, A, B](parent: Grouped[K, A], func: A => Iterable[B]) extends Grouped[K, B]
  case FilterKeys[K, V](parent: Grouped[K, V], predicate: K => Boolean) extends Grouped[K, V]
  case InnerJoin[K, V, U](left: Grouped[K, V], right: Grouped[K, U]) extends Grouped[K, (V, U)]
  case LeftJoin[K, V, U](left: Grouped[K, V], right: Grouped[K, U]) extends Grouped[K, (V, Option[U])]
  case RightJoin[K, V, U](left: Grouped[K, V], right: Grouped[K, U]) extends Grouped[K, (Option[V], U)]
  case FullJoin[K, V, U](left: Grouped[K, V], right: Grouped[K, U]) extends Grouped[K, (Option[V], Option[U])]
  case ReduceByKey[K, V](parent: Grouped[K, V], reduce: (V, V) => V) extends Grouped[K, V]
  case LeftAntiJoin[K, V, U](left: Grouped[K, V], right: Grouped[K, U]) extends Grouped[K, V]
  case SortByKey[K, V](parent: Grouped[K, V], ordering: Ordering[K]) extends Grouped[K, V]
  case UnionGrouped[K, V](left: Grouped[K, V], right: Grouped[K, V]) extends Grouped[K, V]
  case AggregateByKey2[K, V, A, B](
    parent: Grouped[K, V],
    agg1: V => A,
    agg2: V => B,
    reduce1: (A, A) => A,
    reduce2: (B, B) => B
  ) extends Grouped[K, (A, B)]
  case AggregateByKey3[K, V, A, B, C](
    parent: Grouped[K, V],
    agg1: V => A,
    agg2: V => B,
    agg3: V => C,
    reduce1: (A, A) => A,
    reduce2: (B, B) => B,
    reduce3: (C, C) => C
  ) extends Grouped[K, (A, B, C)]
  case AggregateByKey4[K, V, A, B, C, D](
    parent: Grouped[K, V],
    agg1: V => A,
    agg2: V => B,
    agg3: V => C,
    agg4: V => D,
    reduce1: (A, A) => A,
    reduce2: (B, B) => B,
    reduce3: (C, C) => C,
    reduce4: (D, D) => D
  ) extends Grouped[K, (A, B, C, D)]
  case AggregateByKey5[K, V, A, B, C, D, E](
    parent: Grouped[K, V],
    agg1: V => A,
    agg2: V => B,
    agg3: V => C,
    agg4: V => D,
    agg5: V => E,
    reduce1: (A, A) => A,
    reduce2: (B, B) => B,
    reduce3: (C, C) => C,
    reduce4: (D, D) => D,
    reduce5: (E, E) => E
  ) extends Grouped[K, (A, B, C, D, E)]
}

object Grouped {
  extension [K, V](grouped: Grouped[K, V]) {

    /** Extract only the values, discarding keys. */
    def values(using schemaV: Schema[V]): Dataset[V] = {
      Dataset.GroupedValues(grouped, schemaV)
    }

    /** Extract only the keys, discarding values. */
    def keys(using schemaK: Schema[K]): Dataset[K] = {
      Dataset.GroupedKeys(grouped, schemaK)
    }

    /** Convert to Dataset of (K, V) pairs. */
    def toPairs(using schemaK: Schema[K], schemaV: Schema[V]): Dataset[(K, V)] = {
      grouped match {
        case FromPairs(parent) => parent
        case _ => Dataset.GroupedToPairs(grouped, schemaK, schemaV)
      }
    }

    /** Transform values without changing keys. */
    inline def mapValues[U](f: V => U): Grouped[K, U] = {
      MapValues(grouped, f)
    }

    /** Transform values and flatten results. */
    inline def flatMapValues[U](f: V => Iterable[U]): Grouped[K, U] = {
      FlatMapValues(grouped, f)
    }

    /** Filter by key predicate. */
    inline def filterKeys(predicate: K => Boolean): Grouped[K, V] = {
      FilterKeys(grouped, predicate)
    }

    /** Inner join with another grouped dataset on the key. */
    inline def join[U](other: Grouped[K, U]): Grouped[K, (V, U)] = {
      InnerJoin(grouped, other)
    }

    /** Left outer join on the key. */
    inline def leftJoin[U](other: Grouped[K, U]): Grouped[K, (V, Option[U])] = {
      LeftJoin(grouped, other)
    }

    /** Right outer join on the key. */
    inline def rightJoin[U](other: Grouped[K, U]): Grouped[K, (Option[V], U)] = {
      RightJoin(grouped, other)
    }

    /** Full outer join on the key. */
    inline def fullJoin[U](other: Grouped[K, U]): Grouped[K, (Option[V], Option[U])] = {
      FullJoin(grouped, other)
    }

    /** Reduce values for each key using binary function. */
    inline def reduceByKey(f: (V, V) => V): Grouped[K, V] = {
      ReduceByKey(grouped, f)
    }

    /** Left anti join: keep rows from left where key not in right.
      *
      * Opposite of inner join - filters out matching keys.
      */
    inline def leftAntiJoin[U](other: Grouped[K, U]): Grouped[K, V] = {
      LeftAntiJoin(grouped, other)
    }

    /** Sort grouped dataset by key.
      *
      * Preserves grouping while ordering keys.
      */
    inline def sortByKey(using ord: Ordering[K]): Grouped[K, V] = {
      SortByKey(grouped, ord)
    }

    /** Union this grouped dataset with another.
      *
      * Combines all (key, value) pairs from both.
      */
    inline def union(other: Grouped[K, V]): Grouped[K, V] = {
      UnionGrouped(grouped, other)
    }

    /** Alias for union. */
    inline def ++(other: Grouped[K, V]): Grouped[K, V] = {
      union(other)
    }

    /** Aggregate by key with 2 aggregation functions. */
    inline def aggregateByKey[A, B](
      agg1: V => A,
      agg2: V => B,
      reduce1: (A, A) => A,
      reduce2: (B, B) => B
    ): Grouped[K, (A, B)] = {
      AggregateByKey2(grouped, agg1, agg2, reduce1, reduce2)
    }

    /** Aggregate by key with 3 aggregation functions. */
    inline def aggregateByKey[A, B, C](
      agg1: V => A,
      agg2: V => B,
      agg3: V => C,
      reduce1: (A, A) => A,
      reduce2: (B, B) => B,
      reduce3: (C, C) => C
    ): Grouped[K, (A, B, C)] = {
      AggregateByKey3(grouped, agg1, agg2, agg3, reduce1, reduce2, reduce3)
    }

    /** Aggregate by key with 4 aggregation functions. */
    inline def aggregateByKey[A, B, C, D](
      agg1: V => A,
      agg2: V => B,
      agg3: V => C,
      agg4: V => D,
      reduce1: (A, A) => A,
      reduce2: (B, B) => B,
      reduce3: (C, C) => C,
      reduce4: (D, D) => D
    ): Grouped[K, (A, B, C, D)] = {
      AggregateByKey4(grouped, agg1, agg2, agg3, agg4, reduce1, reduce2, reduce3, reduce4)
    }

    /** Aggregate by key with 5 aggregation functions. */
    inline def aggregateByKey[A, B, C, D, E](
      agg1: V => A,
      agg2: V => B,
      agg3: V => C,
      agg4: V => D,
      agg5: V => E,
      reduce1: (A, A) => A,
      reduce2: (B, B) => B,
      reduce3: (C, C) => C,
      reduce4: (D, D) => D,
      reduce5: (E, E) => E
    ): Grouped[K, (A, B, C, D, E)] = {
      AggregateByKey5(grouped, agg1, agg2, agg3, agg4, agg5, reduce1, reduce2, reduce3, reduce4, reduce5)
    }
  }

  // Conversion from Dataset[(K, V)] to Grouped[K, V]
  extension [K, V](ds: Dataset[(K, V)]) {
    inline def asGrouped: Grouped[K, V] = {
      FromPairs(ds)
    }
  }
}
