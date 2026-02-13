package net.ghoula.strongbow

import scala.collection.mutable

/** Interpreter for Grouped operations with encapsulated mutation.
  *
  * Uses mutable builders internally for performance, but exposes immutable results. Handles
  * groupBy, joins, and reductions efficiently.
  */
object GroupByInterpreter {

  /** Execute a Grouped plan to produce key-value pairs. */
  def execute[K, V](grouped: Grouped[K, V]): Vector[(K, V)] = {
    grouped match {
      case Grouped.GroupBy(parent, key) =>
        val parentResult = DatasetInterpreter.execute(parent)
        val rows = parentResult.toVectorUnsafe
        rows.map(row => (key(row), row))

      case Grouped.FromPairs(parent) =>
        val parentResult = DatasetInterpreter.execute(parent)
        parentResult.toVectorUnsafe

      case Grouped.MapValues(parent, func) =>
        execute(parent).map { case (k, v) => (k, func(v)) }

      case Grouped.FlatMapValues(parent, func) =>
        execute(parent).flatMap { case (k, v) =>
          func(v).map(newV => (k, newV))
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
    }
  }

  /** Inner join: only keys present in both sides. */
  private def innerJoin[K, V, U](
      left: Vector[(K, V)],
      right: Vector[(K, U)]
  ): Vector[(K, (V, U))] = {
    val rightMap = buildMultiMap(right)

    left.flatMap { case (k, v) =>
      rightMap.get(k).map(_.map(u => (k, (v, u)))).getOrElse(Vector.empty)
    }
  }

  /** Left join: all keys from left, matching keys from right (or None). */
  private def leftJoin[K, V, U](
      left: Vector[(K, V)],
      right: Vector[(K, U)]
  ): Vector[(K, (V, Option[U]))] = {
    val rightMap = buildMultiMap(right)

    left.flatMap { case (k, v) =>
      rightMap.get(k) match {
        case Some(us) => us.map(u => (k, (v, Some(u))))
        case None     => Vector((k, (v, None)))
      }
    }
  }

  /** Right join: all keys from right, matching keys from left (or None). */
  private def rightJoin[K, V, U](
      left: Vector[(K, V)],
      right: Vector[(K, U)]
  ): Vector[(K, (Option[V], U))] = {
    val leftMap = buildMultiMap(left)

    right.flatMap { case (k, u) =>
      leftMap.get(k) match {
        case Some(vs) => vs.map(v => (k, (Some(v), u)))
        case None     => Vector((k, (None, u)))
      }
    }
  }

  /** Full join: all keys from both sides, None where missing. */
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
        for {
          v <- vs
          u <- us
        } yield (k, (Some(v), Some(u)))
      }
    }
  }

  /** Reduce values by key using binary function. */
  private def reduceByKey[K, V](
      pairs: Vector[(K, V)],
      reduce: (V, V) => V
  ): Vector[(K, V)] = {
    val builder = mutable.HashMap.empty[K, V]

    pairs.foreach { case (k, v) =>
      builder.get(k) match {
        case Some(existing) => builder(k) = reduce(existing, v)
        case None           => builder(k) = v
      }
    }

    builder.toVector
  }

  /** Build a multi-map from key-value pairs (one key -> multiple values). */
  private def buildMultiMap[K, V](pairs: Vector[(K, V)]): Map[K, Vector[V]] = {
    val builder = mutable.HashMap.empty[K, mutable.ArrayBuffer[V]]

    pairs.foreach { case (k, v) =>
      builder.getOrElseUpdate(k, mutable.ArrayBuffer.empty) += v
    }

    builder.view.mapValues(_.toVector).toMap
  }
}
