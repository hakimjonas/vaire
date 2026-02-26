package net.ghoula.strongbow

/** Key-value pair dataset for grouping operations.
  *
  * Grouped[K, V] is a thin wrapper around Dataset[(K, V)] that provides type-safe joins,
  * aggregations, and reductions keyed by K.
  *
  * @tparam K
  *   The key type
  * @tparam V
  *   The value type
  */
final class Grouped[K, V](val underlying: Dataset[(K, V)])(using val keySchema: Schema[K], val valueSchema: Schema[V]) {

  /** Convert to Dataset of (K, V) pairs. */
  def toPairs: Dataset[(K, V)] = underlying

  /** Extract only the keys, discarding values. */
  def keys: Dataset[K] = underlying.map(_._1)(using keySchema)

  /** Extract only the values, discarding keys. */
  def values: Dataset[V] = underlying.map(_._2)(using valueSchema)

  /** Transform values without changing keys. */
  def mapValues[U](f: V => U)(using schemaU: Schema[U]): Grouped[K, U] = {
    given Schema[(K, U)] = Schema.tuple2Schema[K, U](using keySchema, schemaU)
    Grouped(underlying.map { case (k, v) => (k, f(v)) })
  }

  /** Transform values with access to the key. */
  def mapValuesWithKey[U](f: (K, V) => U)(using schemaU: Schema[U]): Grouped[K, U] = {
    given Schema[(K, U)] = Schema.tuple2Schema[K, U](using keySchema, schemaU)
    Grouped(underlying.map { case (k, v) => (k, f(k, v)) })
  }

  /** Transform values and flatten results. */
  def flatMapValues[U](f: V => Iterable[U])(using schemaU: Schema[U]): Grouped[K, U] = {
    given Schema[(K, U)] = Schema.tuple2Schema[K, U](using keySchema, schemaU)
    Grouped(underlying.flatMap { case (k, v) => f(v).map(u => (k, u)) })
  }

  /** Filter by key predicate. */
  def filterKeys(predicate: K => Boolean): Grouped[K, V] = {
    given Schema[(K, V)] = Schema.tuple2Schema[K, V](using keySchema, valueSchema)
    Grouped(underlying.flatMap { case (k, v) => if (predicate(k)) Some((k, v)) else None })
  }

  /** Inner join with another grouped dataset on the key. */
  def join[U](other: Grouped[K, U])(using schemaU: Schema[U]): Grouped[K, (V, U)] = {
    val joined = Dataset.InnerJoin[(K, V), (K, U)](
      underlying,
      other.underlying,
      (l, r) => l._1.equals(r._1)
    )
    given Schema[(V, U)] = Schema.tuple2Schema[V, U](using valueSchema, schemaU)
    given Schema[(K, (V, U))] = Schema.tuple2Schema[K, (V, U)](using keySchema, summon[Schema[(V, U)]])
    Grouped(joined.map { case ((k, v), (_, u)) => (k, (v, u)) })
  }

  /** Left outer join on the key. */
  def leftJoin[U](other: Grouped[K, U])(using schemaU: Schema[U]): Grouped[K, (V, Option[U])] = {
    val joined = Dataset.LeftJoin[(K, V), (K, U)](
      underlying,
      other.underlying,
      (l, r) => l._1.equals(r._1)
    )
    given Schema[Option[U]] = Schema.optionSchema[U](using schemaU)
    given Schema[(V, Option[U])] = Schema.tuple2Schema[V, Option[U]](using valueSchema, summon[Schema[Option[U]]])
    given Schema[(K, (V, Option[U]))] =
      Schema.tuple2Schema[K, (V, Option[U])](using keySchema, summon[Schema[(V, Option[U])]])
    Grouped(joined.map { case ((k, v), optKU) => (k, (v, optKU.map(_._2))) })
  }

  /** Right outer join on the key. */
  def rightJoin[U](other: Grouped[K, U])(using schemaU: Schema[U]): Grouped[K, (Option[V], U)] = {
    val joined = Dataset.RightJoin[(K, V), (K, U)](
      underlying,
      other.underlying,
      (l, r) => l._1.equals(r._1)
    )
    given Schema[Option[V]] = Schema.optionSchema[V](using valueSchema)
    given Schema[(Option[V], U)] = Schema.tuple2Schema[Option[V], U](using summon[Schema[Option[V]]], schemaU)
    given Schema[(K, (Option[V], U))] =
      Schema.tuple2Schema[K, (Option[V], U)](using keySchema, summon[Schema[(Option[V], U)]])
    Grouped(joined.map { case (optKV, (k, u)) => (k, (optKV.map(_._2), u)) })
  }

  /** Full outer join on the key. */
  def fullJoin[U](other: Grouped[K, U])(using schemaU: Schema[U]): Grouped[K, (Option[V], Option[U])] = {
    val joined = Dataset.FullJoin[(K, V), (K, U)](
      underlying,
      other.underlying,
      (l, r) => l._1.equals(r._1)
    )
    given optV: Schema[Option[V]] = Schema.optionSchema[V](using valueSchema)
    given optU: Schema[Option[U]] = Schema.optionSchema[U](using schemaU)
    given tupleOptVU: Schema[(Option[V], Option[U])] = Schema.tuple2Schema[Option[V], Option[U]](using optV, optU)
    given pairSchema: Schema[(K, (Option[V], Option[U]))] =
      Schema.tuple2Schema[K, (Option[V], Option[U])](using keySchema, tupleOptVU)
    Grouped(joined.map { case (optKV, optKU) =>
      val k = optKV.map(_._1).orElse(optKU.map(_._1)).get
      (k, (optKV.map(_._2), optKU.map(_._2)))
    })
  }

  /** Reduce values for each key using binary function. */
  def reduceByKey(f: (V, V) => V): Grouped[K, V] = {
    Grouped(Dataset.ReduceByKey(underlying, f, keySchema, valueSchema))
  }

  /** Left anti join: keep rows from left where key not in right. */
  def leftAntiJoin[U](other: Grouped[K, U]): Grouped[K, V] = {
    Grouped(
      Dataset.LeftAntiJoin[(K, V), (K, U)](
        underlying,
        other.underlying,
        (l, r) => l._1.equals(r._1)
      )
    )
  }

  /** Sort grouped dataset by key. */
  def sortByKey(using ord: Ordering[K]): Grouped[K, V] = {
    Grouped(underlying.sortBy(_._1))
  }

  /** Union this grouped dataset with another. */
  def union(other: Grouped[K, V]): Grouped[K, V] = {
    Grouped(underlying.union(other.underlying))
  }

  /** Alias for union. */
  def ++(other: Grouped[K, V]): Grouped[K, V] = union(other)

  /** Aggregate by key with 2 aggregation functions. */
  def aggregateByKey[A, B](
    agg1: V => A,
    agg2: V => B,
    reduce1: (A, A) => A,
    reduce2: (B, B) => B
  )(using schemaAB: Schema[(A, B)]): Grouped[K, (A, B)] = {
    val extractors: Vector[V => Any] = Vector(agg1, agg2)
    val reducers: Vector[(Any, Any) => Any] = Vector(
      (a: Any, b: Any) => reduce1(a.asInstanceOf[A], b.asInstanceOf[A]), // scalafix:ok DisableSyntax.asInstanceOf
      (a: Any, b: Any) => reduce2(a.asInstanceOf[B], b.asInstanceOf[B]) // scalafix:ok DisableSyntax.asInstanceOf
    )
    val assembler: Vector[Any] => (A, B) = v =>
      (v(0).asInstanceOf[A], v(1).asInstanceOf[B]) // scalafix:ok DisableSyntax.asInstanceOf
    Grouped(Dataset.AggregateByKey(underlying, extractors, reducers, assembler, keySchema, schemaAB))
  }

  /** Aggregate by key with 3 aggregation functions. */
  def aggregateByKey[A, B, C](
    agg1: V => A,
    agg2: V => B,
    agg3: V => C,
    reduce1: (A, A) => A,
    reduce2: (B, B) => B,
    reduce3: (C, C) => C
  )(using schemaABC: Schema[(A, B, C)]): Grouped[K, (A, B, C)] = {
    val extractors: Vector[V => Any] = Vector(agg1, agg2, agg3)
    val reducers: Vector[(Any, Any) => Any] = Vector(
      (a: Any, b: Any) => reduce1(a.asInstanceOf[A], b.asInstanceOf[A]), // scalafix:ok DisableSyntax.asInstanceOf
      (a: Any, b: Any) => reduce2(a.asInstanceOf[B], b.asInstanceOf[B]), // scalafix:ok DisableSyntax.asInstanceOf
      (a: Any, b: Any) => reduce3(a.asInstanceOf[C], b.asInstanceOf[C]) // scalafix:ok DisableSyntax.asInstanceOf
    )
    val assembler: Vector[Any] => (A, B, C) = v =>
      (v(0).asInstanceOf[A], v(1).asInstanceOf[B], v(2).asInstanceOf[C]) // scalafix:ok DisableSyntax.asInstanceOf
    Grouped(Dataset.AggregateByKey(underlying, extractors, reducers, assembler, keySchema, schemaABC))
  }

  /** Aggregate by key with 4 aggregation functions. */
  def aggregateByKey[A, B, C, D](
    agg1: V => A,
    agg2: V => B,
    agg3: V => C,
    agg4: V => D,
    reduce1: (A, A) => A,
    reduce2: (B, B) => B,
    reduce3: (C, C) => C,
    reduce4: (D, D) => D
  )(using schemaABCD: Schema[(A, B, C, D)]): Grouped[K, (A, B, C, D)] = {
    val extractors: Vector[V => Any] = Vector(agg1, agg2, agg3, agg4)
    val reducers: Vector[(Any, Any) => Any] = Vector(
      (a: Any, b: Any) => reduce1(a.asInstanceOf[A], b.asInstanceOf[A]), // scalafix:ok DisableSyntax.asInstanceOf
      (a: Any, b: Any) => reduce2(a.asInstanceOf[B], b.asInstanceOf[B]), // scalafix:ok DisableSyntax.asInstanceOf
      (a: Any, b: Any) => reduce3(a.asInstanceOf[C], b.asInstanceOf[C]), // scalafix:ok DisableSyntax.asInstanceOf
      (a: Any, b: Any) => reduce4(a.asInstanceOf[D], b.asInstanceOf[D]) // scalafix:ok DisableSyntax.asInstanceOf
    )
    val assembler: Vector[Any] => (A, B, C, D) = v =>
      (
        v(0).asInstanceOf[A], // scalafix:ok DisableSyntax.asInstanceOf
        v(1).asInstanceOf[B], // scalafix:ok DisableSyntax.asInstanceOf
        v(2).asInstanceOf[C], // scalafix:ok DisableSyntax.asInstanceOf
        v(3).asInstanceOf[D] // scalafix:ok DisableSyntax.asInstanceOf
      )
    Grouped(Dataset.AggregateByKey(underlying, extractors, reducers, assembler, keySchema, schemaABCD))
  }

  /** Aggregate by key with 5 aggregation functions. */
  def aggregateByKey[A, B, C, D, E](
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
  )(using schemaABCDE: Schema[(A, B, C, D, E)]): Grouped[K, (A, B, C, D, E)] = {
    val extractors: Vector[V => Any] = Vector(agg1, agg2, agg3, agg4, agg5)
    val reducers: Vector[(Any, Any) => Any] = Vector(
      (a: Any, b: Any) => reduce1(a.asInstanceOf[A], b.asInstanceOf[A]), // scalafix:ok DisableSyntax.asInstanceOf
      (a: Any, b: Any) => reduce2(a.asInstanceOf[B], b.asInstanceOf[B]), // scalafix:ok DisableSyntax.asInstanceOf
      (a: Any, b: Any) => reduce3(a.asInstanceOf[C], b.asInstanceOf[C]), // scalafix:ok DisableSyntax.asInstanceOf
      (a: Any, b: Any) => reduce4(a.asInstanceOf[D], b.asInstanceOf[D]), // scalafix:ok DisableSyntax.asInstanceOf
      (a: Any, b: Any) => reduce5(a.asInstanceOf[E], b.asInstanceOf[E]) // scalafix:ok DisableSyntax.asInstanceOf
    )
    val assembler: Vector[Any] => (A, B, C, D, E) = v =>
      (
        v(0).asInstanceOf[A], // scalafix:ok DisableSyntax.asInstanceOf
        v(1).asInstanceOf[B], // scalafix:ok DisableSyntax.asInstanceOf
        v(2).asInstanceOf[C], // scalafix:ok DisableSyntax.asInstanceOf
        v(3).asInstanceOf[D], // scalafix:ok DisableSyntax.asInstanceOf
        v(4).asInstanceOf[E] // scalafix:ok DisableSyntax.asInstanceOf
      )
    Grouped(Dataset.AggregateByKey(underlying, extractors, reducers, assembler, keySchema, schemaABCDE))
  }

  /** Aggregate by key with 6 aggregation functions. */
  def aggregateByKey[A, B, C, D, E, F](
    agg1: V => A,
    agg2: V => B,
    agg3: V => C,
    agg4: V => D,
    agg5: V => E,
    agg6: V => F,
    reduce1: (A, A) => A,
    reduce2: (B, B) => B,
    reduce3: (C, C) => C,
    reduce4: (D, D) => D,
    reduce5: (E, E) => E,
    reduce6: (F, F) => F
  )(using schemaABCDEF: Schema[(A, B, C, D, E, F)]): Grouped[K, (A, B, C, D, E, F)] = {
    val extractors: Vector[V => Any] = Vector(agg1, agg2, agg3, agg4, agg5, agg6)
    val reducers: Vector[(Any, Any) => Any] = Vector(
      (a: Any, b: Any) => reduce1(a.asInstanceOf[A], b.asInstanceOf[A]), // scalafix:ok DisableSyntax.asInstanceOf
      (a: Any, b: Any) => reduce2(a.asInstanceOf[B], b.asInstanceOf[B]), // scalafix:ok DisableSyntax.asInstanceOf
      (a: Any, b: Any) => reduce3(a.asInstanceOf[C], b.asInstanceOf[C]), // scalafix:ok DisableSyntax.asInstanceOf
      (a: Any, b: Any) => reduce4(a.asInstanceOf[D], b.asInstanceOf[D]), // scalafix:ok DisableSyntax.asInstanceOf
      (a: Any, b: Any) => reduce5(a.asInstanceOf[E], b.asInstanceOf[E]), // scalafix:ok DisableSyntax.asInstanceOf
      (a: Any, b: Any) => reduce6(a.asInstanceOf[F], b.asInstanceOf[F]) // scalafix:ok DisableSyntax.asInstanceOf
    )
    val assembler: Vector[Any] => (A, B, C, D, E, F) = v =>
      (
        v(0).asInstanceOf[A], // scalafix:ok DisableSyntax.asInstanceOf
        v(1).asInstanceOf[B], // scalafix:ok DisableSyntax.asInstanceOf
        v(2).asInstanceOf[C], // scalafix:ok DisableSyntax.asInstanceOf
        v(3).asInstanceOf[D], // scalafix:ok DisableSyntax.asInstanceOf
        v(4).asInstanceOf[E], // scalafix:ok DisableSyntax.asInstanceOf
        v(5).asInstanceOf[F] // scalafix:ok DisableSyntax.asInstanceOf
      )
    Grouped(Dataset.AggregateByKey(underlying, extractors, reducers, assembler, keySchema, schemaABCDEF))
  }

  /** Aggregate by key with 7 aggregation functions. */
  def aggregateByKey[A, B, C, D, E, F, G](
    agg1: V => A,
    agg2: V => B,
    agg3: V => C,
    agg4: V => D,
    agg5: V => E,
    agg6: V => F,
    agg7: V => G,
    reduce1: (A, A) => A,
    reduce2: (B, B) => B,
    reduce3: (C, C) => C,
    reduce4: (D, D) => D,
    reduce5: (E, E) => E,
    reduce6: (F, F) => F,
    reduce7: (G, G) => G
  )(using schemaABCDEFG: Schema[(A, B, C, D, E, F, G)]): Grouped[K, (A, B, C, D, E, F, G)] = {
    val extractors: Vector[V => Any] = Vector(agg1, agg2, agg3, agg4, agg5, agg6, agg7)
    val reducers: Vector[(Any, Any) => Any] = Vector(
      (a: Any, b: Any) => reduce1(a.asInstanceOf[A], b.asInstanceOf[A]), // scalafix:ok DisableSyntax.asInstanceOf
      (a: Any, b: Any) => reduce2(a.asInstanceOf[B], b.asInstanceOf[B]), // scalafix:ok DisableSyntax.asInstanceOf
      (a: Any, b: Any) => reduce3(a.asInstanceOf[C], b.asInstanceOf[C]), // scalafix:ok DisableSyntax.asInstanceOf
      (a: Any, b: Any) => reduce4(a.asInstanceOf[D], b.asInstanceOf[D]), // scalafix:ok DisableSyntax.asInstanceOf
      (a: Any, b: Any) => reduce5(a.asInstanceOf[E], b.asInstanceOf[E]), // scalafix:ok DisableSyntax.asInstanceOf
      (a: Any, b: Any) => reduce6(a.asInstanceOf[F], b.asInstanceOf[F]), // scalafix:ok DisableSyntax.asInstanceOf
      (a: Any, b: Any) => reduce7(a.asInstanceOf[G], b.asInstanceOf[G]) // scalafix:ok DisableSyntax.asInstanceOf
    )
    val assembler: Vector[Any] => (A, B, C, D, E, F, G) = v =>
      (
        v(0).asInstanceOf[A], // scalafix:ok DisableSyntax.asInstanceOf
        v(1).asInstanceOf[B], // scalafix:ok DisableSyntax.asInstanceOf
        v(2).asInstanceOf[C], // scalafix:ok DisableSyntax.asInstanceOf
        v(3).asInstanceOf[D], // scalafix:ok DisableSyntax.asInstanceOf
        v(4).asInstanceOf[E], // scalafix:ok DisableSyntax.asInstanceOf
        v(5).asInstanceOf[F], // scalafix:ok DisableSyntax.asInstanceOf
        v(6).asInstanceOf[G] // scalafix:ok DisableSyntax.asInstanceOf
      )
    Grouped(Dataset.AggregateByKey(underlying, extractors, reducers, assembler, keySchema, schemaABCDEFG))
  }
}

object Grouped {
  extension [K, V](ds: Dataset[(K, V)]) {
    def asGrouped(using Schema[K], Schema[V]): Grouped[K, V] = {
      Grouped(ds)
    }
  }
}
