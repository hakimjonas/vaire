package net.ghoula.strongbow.agg

import scala.deriving.Mirror

import net.ghoula.strongbow.column.ColumnType
import net.ghoula.strongbow.expr.Expr
import net.ghoula.strongbow.internal.ExprCompiler
import net.ghoula.strongbow.params.AggSpec

/** Ergonomic builder methods for creating AggSpec instances using field-access lambdas.
  *
  * @example
  *   {{{
  * import net.ghoula.strongbow.agg.AggBuilders.*
  *
  * ds.groupByAgg[Result](keys, Vector(
  *   sumDouble[Sale](_.amount).as("totalAmount"),
  *   count[Sale].as("totalQty")
  * ))
  *   }}}
  */
object AggBuilders {

  /** SQL `SUM` over an Int field, as Long. */
  inline def sum[T](inline f: T => Int)(using m: Mirror.ProductOf[T]): AggSpec[T] = {
    val (expr, _) = ExprCompiler.column(f)
    AggSpec("sum", Expr.Sum(expr), ColumnType.LongType)
  }

  /** SQL `SUM` over a Long field. */
  inline def sumLong[T](inline f: T => Long)(using m: Mirror.ProductOf[T]): AggSpec[T] = {
    val (expr, _) = ExprCompiler.column(f)
    AggSpec("sumLong", Expr.SumLong(expr), ColumnType.LongType)
  }

  /** SQL `SUM` over a Double field. */
  inline def sumDouble[T](inline f: T => Double)(using m: Mirror.ProductOf[T]): AggSpec[T] = {
    val (expr, _) = ExprCompiler.column(f)
    AggSpec("sumDouble", Expr.SumDouble(expr), ColumnType.DoubleType)
  }

  /** SQL `AVG` over a Double field. */
  inline def avg[T](inline f: T => Double)(using m: Mirror.ProductOf[T]): AggSpec[T] = {
    val (expr, _) = ExprCompiler.column(f)
    AggSpec("avg", Expr.Avg(expr), ColumnType.DoubleType)
  }

  /** SQL `MAX` over an ordered field. */
  inline def max[T, A: Ordering](inline f: T => A)(using m: Mirror.ProductOf[T]): AggSpec[T] = {
    val (expr, _) = ExprCompiler.column(f)
    AggSpec("max", Expr.Max(expr, summon[Ordering[A]]), ColumnType.AnyType)
  }

  /** SQL `MIN` over an ordered field. */
  inline def min[T, A: Ordering](inline f: T => A)(using m: Mirror.ProductOf[T]): AggSpec[T] = {
    val (expr, _) = ExprCompiler.column(f)
    AggSpec("min", Expr.Min(expr, summon[Ordering[A]]), ColumnType.AnyType)
  }

  /** SQL `first` — the first value of the group. */
  inline def first[T, A](inline f: T => A)(using m: Mirror.ProductOf[T]): AggSpec[T] = {
    val (expr, _) = ExprCompiler.column(f)
    AggSpec("first", Expr.First(expr), ColumnType.AnyType)
  }

  /** SQL `COUNT(DISTINCT field)`. */
  inline def countDistinct[T, A](inline f: T => A)(using m: Mirror.ProductOf[T]): AggSpec[T] = {
    val (expr, _) = ExprCompiler.column(f)
    AggSpec("countDistinct", Expr.CountDistinct(expr), ColumnType.LongType)
  }

  /** SQL `stddev_samp` over a Double field. */
  inline def stdDev[T](inline f: T => Double)(using m: Mirror.ProductOf[T]): AggSpec[T] = {
    val (expr, _) = ExprCompiler.column(f)
    AggSpec("stdDev", Expr.StdDev(expr), ColumnType.DoubleType)
  }

  /** SQL `stddev_pop` over a Double field. */
  inline def stdDevPop[T](inline f: T => Double)(using m: Mirror.ProductOf[T]): AggSpec[T] = {
    val (expr, _) = ExprCompiler.column(f)
    AggSpec("stdDevPop", Expr.StdDevPop(expr), ColumnType.DoubleType)
  }

  /** SQL `COUNT(*)` — row count of the group. */
  def count[T]: AggSpec[T] = AggSpec("count", Expr.Count[T](), ColumnType.LongType)
}

extension [T](spec: AggSpec[T]) {

  /** Rename this aggregation spec. */
  def as(name: String): AggSpec[T] = AggSpec(name, spec.expr, spec.columnType)
}
