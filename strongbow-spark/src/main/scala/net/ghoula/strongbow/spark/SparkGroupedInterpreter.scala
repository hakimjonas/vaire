package net.ghoula.strongbow.spark

import scala.collection.mutable

import net.ghoula.strongbow.{Dataset, Grouped}

/** Interpreter for Grouped operations within the Spark backend.
  *
  * Executes the parent Dataset via SparkInterpreter (distributed), then runs grouping logic on the
  * collected results using the same HashMap-based approach as GroupByInterpreter.
  *
  * This is correct and matches in-memory behavior. The parent execution is distributed (Spark
  * handles filter/join/union etc.), and only the final grouping collects to driver.
  */
class SparkGroupedInterpreter(sparkInterpreter: SparkInterpreter) {

  /** Execute a Grouped plan to produce key-value pairs. */
  def execute[K, V](grouped: Grouped[K, V]): Vector[(K, V)] = {
    grouped match {
      case Grouped.GroupBy(parent, key) =>
        val rows = executeDataset(parent)
        rows.map(row => (key(row), row))

      case Grouped.GroupByExpr(parent, keyExpr, keyType) =>
        // Execute parent via Spark, then materialize to columns for key expression eval
        import net.ghoula.strongbow.ExprInterpreter
        val mat = sparkInterpreter.execute(parent) match {
          case Right(ds) => ds
          case Left(err) =>
            throw new RuntimeException(s"GroupByExpr execution failed: $err") // scalafix:ok DisableSyntax.throw
        }
        val keyCol = ExprInterpreter.evalColumn(keyExpr, mat.columns, keyType) match {
          case Right(col) => col
          case Left(err) =>
            throw new RuntimeException(s"GroupByExpr key eval failed: $err") // scalafix:ok DisableSyntax.throw
        }
        val rows = mat.toVectorUnsafe
        rows.indices.iterator.map(i => (keyCol.getValue(i).asInstanceOf[K], rows(i))).toVector // scalafix:ok DisableSyntax.asInstanceOf

      case Grouped.FromPairs(parent) =>
        executeDataset(parent)

      case Grouped.MapValues(parent, func) =>
        execute(parent).map { case (k, v) => (k, func(v)) }

      case Grouped.FlatMapValues(parent, func) =>
        execute(parent).flatMap { case (k, v) =>
          func(v).map(newV => (k, newV)).toVector
        }

      case Grouped.FilterKeys(parent, predicate) =>
        execute(parent).filter { case (k, _) => predicate(k) }

      case Grouped.InnerJoin(left, right) =>
        innerJoin(execute(left), execute(right))

      case Grouped.LeftJoin(left, right) =>
        leftJoin(execute(left), execute(right))

      case Grouped.RightJoin(left, right) =>
        rightJoin(execute(left), execute(right))

      case Grouped.FullJoin(left, right) =>
        fullJoin(execute(left), execute(right))

      case Grouped.ReduceByKey(parent, reduce) =>
        reduceByKey(execute(parent), reduce)

      case Grouped.LeftAntiJoin(left, right) =>
        leftAntiJoin(execute(left), execute(right))

      case Grouped.SortByKey(parent, ordering) =>
        execute(parent).sortBy(_._1)(using ordering)

      case Grouped.UnionGrouped(left, right) =>
        execute(left) ++ execute(right)

      case agg: Grouped.AggregateByKey2[_, _, _, _] =>
        aggregateByKey2(execute(agg.parent), agg.agg1, agg.agg2, agg.reduce1, agg.reduce2)
          .asInstanceOf[Vector[(K, V)]] // scalafix:ok DisableSyntax.asInstanceOf

      case agg: Grouped.AggregateByKey3[_, _, _, _, _] =>
        aggregateByKey3(execute(agg.parent), agg.agg1, agg.agg2, agg.agg3, agg.reduce1, agg.reduce2, agg.reduce3)
          .asInstanceOf[Vector[(K, V)]] // scalafix:ok DisableSyntax.asInstanceOf

      case agg: Grouped.AggregateByKey4[_, _, _, _, _, _] =>
        aggregateByKey4(
          execute(agg.parent),
          agg.agg1,
          agg.agg2,
          agg.agg3,
          agg.agg4,
          agg.reduce1,
          agg.reduce2,
          agg.reduce3,
          agg.reduce4
        )
          .asInstanceOf[Vector[(K, V)]] // scalafix:ok DisableSyntax.asInstanceOf

      case agg: Grouped.AggregateByKey5[_, _, _, _, _, _, _] =>
        aggregateByKey5(
          execute(agg.parent),
          agg.agg1,
          agg.agg2,
          agg.agg3,
          agg.agg4,
          agg.agg5,
          agg.reduce1,
          agg.reduce2,
          agg.reduce3,
          agg.reduce4,
          agg.reduce5
        )
          .asInstanceOf[Vector[(K, V)]] // scalafix:ok DisableSyntax.asInstanceOf
    }
  }

  /** Execute a Dataset via the SparkInterpreter and collect results. */
  private def executeDataset[T](dataset: Dataset[T]): Vector[T] = {
    sparkInterpreter.execute(dataset) match {
      case Right(ds) => ds.toVectorUnsafe
      case Left(err) =>
        throw new RuntimeException(s"Spark grouped execution failed: $err") // scalafix:ok DisableSyntax.throw
    }
  }

  // --- Join helpers (same logic as GroupByInterpreter) ---

  private def innerJoin[K, V, U](
    left: Vector[(K, V)],
    right: Vector[(K, U)]
  ): Vector[(K, (V, U))] = {
    val rightMap = buildMultiMap(right)
    left.flatMap { case (k, v) =>
      rightMap.get(k).map(_.map(u => (k, (v, u)))).getOrElse(Vector.empty)
    }
  }

  private def leftJoin[K, V, U](
    left: Vector[(K, V)],
    right: Vector[(K, U)]
  ): Vector[(K, (V, Option[U]))] = {
    val rightMap = buildMultiMap(right)
    left.flatMap { case (k, v) =>
      rightMap.get(k) match {
        case Some(us) => us.map(u => (k, (v, Some(u))))
        case None => Vector((k, (v, None)))
      }
    }
  }

  private def rightJoin[K, V, U](
    left: Vector[(K, V)],
    right: Vector[(K, U)]
  ): Vector[(K, (Option[V], U))] = {
    val leftMap = buildMultiMap(left)
    right.flatMap { case (k, u) =>
      leftMap.get(k) match {
        case Some(vs) => vs.map(v => (k, (Some(v), u)))
        case None => Vector((k, (None, u)))
      }
    }
  }

  private def fullJoin[K, V, U](
    left: Vector[(K, V)],
    right: Vector[(K, U)]
  ): Vector[(K, (Option[V], Option[U]))] = {
    val leftMap = buildMultiMap(left)
    val rightMap = buildMultiMap(right)
    val allKeys = (leftMap.keySet ++ rightMap.keySet).toVector

    allKeys.flatMap { k =>
      val vs = leftMap.getOrElse(k, Vector.empty)
      val us = rightMap.getOrElse(k, Vector.empty)
      if (vs.isEmpty && us.nonEmpty) {
        us.map(u => (k, (None, Some(u))))
      } else if (us.isEmpty && vs.nonEmpty) {
        vs.map(v => (k, (Some(v), None)))
      } else {
        for { v <- vs; u <- us } yield (k, (Some(v), Some(u)))
      }
    }
  }

  private def leftAntiJoin[K, V, U](
    left: Vector[(K, V)],
    right: Vector[(K, U)]
  ): Vector[(K, V)] = {
    val rightKeys = right.map(_._1).toSet
    left.filterNot { case (k, _) => rightKeys.contains(k) }
  }

  private def reduceByKey[K, V](
    pairs: Vector[(K, V)],
    reduce: (V, V) => V
  ): Vector[(K, V)] = {
    val builder = mutable.HashMap.empty[K, V]
    pairs.foreach { case (k, v) =>
      builder.get(k) match {
        case Some(existing) => builder(k) = reduce(existing, v)
        case None => builder(k) = v
      }
    }
    builder.toVector
  }

  private def buildMultiMap[K, V](pairs: Vector[(K, V)]): Map[K, Vector[V]] = {
    val builder = mutable.HashMap.empty[K, mutable.ArrayBuffer[V]]
    pairs.foreach { case (k, v) =>
      builder.getOrElseUpdate(k, mutable.ArrayBuffer.empty) += v
    }
    builder.view.mapValues(_.toVector).toMap
  }

  // --- AggregateByKey helpers ---

  private def aggregateByKey2[K, V, A, B](
    pairs: Vector[(K, V)],
    agg1: V => A,
    agg2: V => B,
    reduce1: (A, A) => A,
    reduce2: (B, B) => B
  ): Vector[(K, (A, B))] = {
    val builder = mutable.HashMap.empty[K, (A, B)]
    pairs.foreach { case (k, v) =>
      val a = agg1(v); val b = agg2(v)
      builder.get(k) match {
        case Some((ea, eb)) => builder(k) = (reduce1(ea, a), reduce2(eb, b))
        case None => builder(k) = (a, b)
      }
    }
    builder.toVector
  }

  private def aggregateByKey3[K, V, A, B, C](
    pairs: Vector[(K, V)],
    agg1: V => A,
    agg2: V => B,
    agg3: V => C,
    reduce1: (A, A) => A,
    reduce2: (B, B) => B,
    reduce3: (C, C) => C
  ): Vector[(K, (A, B, C))] = {
    val builder = mutable.HashMap.empty[K, (A, B, C)]
    pairs.foreach { case (k, v) =>
      val a = agg1(v); val b = agg2(v); val c = agg3(v)
      builder.get(k) match {
        case Some((ea, eb, ec)) => builder(k) = (reduce1(ea, a), reduce2(eb, b), reduce3(ec, c))
        case None => builder(k) = (a, b, c)
      }
    }
    builder.toVector
  }

  private def aggregateByKey4[K, V, A, B, C, D](
    pairs: Vector[(K, V)],
    agg1: V => A,
    agg2: V => B,
    agg3: V => C,
    agg4: V => D,
    reduce1: (A, A) => A,
    reduce2: (B, B) => B,
    reduce3: (C, C) => C,
    reduce4: (D, D) => D
  ): Vector[(K, (A, B, C, D))] = {
    val builder = mutable.HashMap.empty[K, (A, B, C, D)]
    pairs.foreach { case (k, v) =>
      val a = agg1(v); val b = agg2(v); val c = agg3(v); val d = agg4(v)
      builder.get(k) match {
        case Some((ea, eb, ec, ed)) => builder(k) = (reduce1(ea, a), reduce2(eb, b), reduce3(ec, c), reduce4(ed, d))
        case None => builder(k) = (a, b, c, d)
      }
    }
    builder.toVector
  }

  private def aggregateByKey5[K, V, A, B, C, D, E](
    pairs: Vector[(K, V)],
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
  ): Vector[(K, (A, B, C, D, E))] = {
    val builder = mutable.HashMap.empty[K, (A, B, C, D, E)]
    pairs.foreach { case (k, v) =>
      val a = agg1(v); val b = agg2(v); val c = agg3(v); val d = agg4(v); val e = agg5(v)
      builder.get(k) match {
        case Some((ea, eb, ec, ed, ee)) =>
          builder(k) = (reduce1(ea, a), reduce2(eb, b), reduce3(ec, c), reduce4(ed, d), reduce5(ee, e))
        case None => builder(k) = (a, b, c, d, e)
      }
    }
    builder.toVector
  }
}
