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
  case LeftJoin[K, V, U](left: Grouped[K, V], right: Grouped[K, U])
      extends Grouped[K, (V, Option[U])]
  case RightJoin[K, V, U](left: Grouped[K, V], right: Grouped[K, U])
      extends Grouped[K, (Option[V], U)]
  case FullJoin[K, V, U](left: Grouped[K, V], right: Grouped[K, U])
      extends Grouped[K, (Option[V], Option[U])]
  case ReduceByKey[K, V](parent: Grouped[K, V], reduce: (V, V) => V) extends Grouped[K, V]
}

object Grouped {
  extension [K, V](grouped: Grouped[K, V]) {
    /** Extract only the values, discarding keys. */
    def values: Dataset[V] = {
      Dataset.GroupedValues(grouped)
    }

    /** Extract only the keys, discarding values. */
    def keys: Dataset[K] = {
      Dataset.GroupedKeys(grouped)
    }

    /** Convert to Dataset of (K, V) pairs. */
    def toPairs: Dataset[(K, V)] = {
      grouped match {
        case FromPairs(parent) => parent
        case _ => Dataset.GroupedToPairs(grouped)
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
  }

  // Conversion from Dataset[(K, V)] to Grouped[K, V]
  extension [K, V](ds: Dataset[(K, V)]) {
    inline def asGrouped: Grouped[K, V] = {
      FromPairs(ds)
    }
  }
}
