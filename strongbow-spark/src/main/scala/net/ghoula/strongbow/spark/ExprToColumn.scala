package net.ghoula.strongbow.spark

import org.apache.spark.sql.{Column => SparkColumn}
import org.apache.spark.sql.functions.{when => sparkWhen, *}

import net.ghoula.strongbow.{ColumnType, Expr}
import net.ghoula.strongbow.errors.ExecutionError

/** Translates Strongbow Expr cases to Spark SQL Column expressions.
  *
  * Row-level expressions (arithmetic, comparisons, boolean logic, string ops) translate to native
  * Spark Column operations for full Catalyst optimization.
  *
  * Aggregation expressions translate to Spark aggregate functions.
  */
object ExprToColumn {

  /** Convert a Strongbow Expr to a Spark Column paired with its output ColumnType.
    *
    * Returns Left for unsupported expressions.
    */
  def convert[Row, A](expr: Expr[Row, A]): Either[ExecutionError, (SparkColumn, ColumnType)] = {
    (expr: @unchecked) match {
      // Leaf nodes
      case cell: Expr.Cell[Row, _] =>
        Right((col(cell.name), ColumnType.AnyType))

      case c: Expr.Const[Row, _] =>
        Right((toLit(c.value), ColumnType.AnyType))

      case named: Expr.Named[Row, _] =>
        convert(named.expr).map { case (sparkCol, ct) => (sparkCol.as(named.name), ct) }

      // Numeric operations
      case add: Expr.Add[Row] =>
        for {
          (l, _) <- convert(add.left)
          (r, _) <- convert(add.right)
        } yield (l + r, ColumnType.IntType)

      case sub: Expr.Sub[Row] =>
        for {
          (l, _) <- convert(sub.left)
          (r, _) <- convert(sub.right)
        } yield (l - r, ColumnType.IntType)

      case mul: Expr.Mul[Row] =>
        for {
          (l, _) <- convert(mul.left)
          (r, _) <- convert(mul.right)
        } yield (l * r, ColumnType.IntType)

      case d: Expr.Div[Row] =>
        for {
          (l, _) <- convert(d.left)
          (r, _) <- convert(d.right)
        } yield ((l / r).cast("int"), ColumnType.IntType)

      // Comparisons
      case gt: Expr.Gt[Row, _] =>
        for {
          (l, _) <- convert(gt.left)
          (r, _) <- convert(gt.right)
        } yield (l > r, ColumnType.BooleanType)

      case gte: Expr.Gte[Row, _] =>
        for {
          (l, _) <- convert(gte.left)
          (r, _) <- convert(gte.right)
        } yield (l >= r, ColumnType.BooleanType)

      case lt: Expr.Lt[Row, _] =>
        for {
          (l, _) <- convert(lt.left)
          (r, _) <- convert(lt.right)
        } yield (l < r, ColumnType.BooleanType)

      case lte: Expr.Lte[Row, _] =>
        for {
          (l, _) <- convert(lte.left)
          (r, _) <- convert(lte.right)
        } yield (l <= r, ColumnType.BooleanType)

      case eq: Expr.Eq[Row, _] =>
        for {
          (l, _) <- convert(eq.left)
          (r, _) <- convert(eq.right)
        } yield (l === r, ColumnType.BooleanType)

      case neq: Expr.Neq[Row, _] =>
        for {
          (l, _) <- convert(neq.left)
          (r, _) <- convert(neq.right)
        } yield (l =!= r, ColumnType.BooleanType)

      // Boolean operations
      case and: Expr.And[Row] =>
        for {
          (l, _) <- convert(and.left)
          (r, _) <- convert(and.right)
        } yield (l && r, ColumnType.BooleanType)

      case or: Expr.Or[Row] =>
        for {
          (l, _) <- convert(or.left)
          (r, _) <- convert(or.right)
        } yield (l || r, ColumnType.BooleanType)

      case not: Expr.Not[Row] =>
        convert(not.expr).map { case (sparkCol, _) => (!sparkCol, ColumnType.BooleanType) }

      // Conditional
      case w: Expr.When[Row, _] =>
        for {
          (cond, _) <- convert(w.condition)
          (thenCol, thenType) <- convert(w.thenExpr)
          (elseCol, _) <- convert(w.elseExpr)
        } yield (sparkWhen(cond, thenCol).otherwise(elseCol), thenType)

      // String operations
      case cat: Expr.Concat[Row] =>
        for {
          (l, _) <- convert(cat.left)
          (r, _) <- convert(cat.right)
        } yield (concat(l, r), ColumnType.StringType)

      case len: Expr.Length[Row] =>
        convert(len.expr).map { case (sparkCol, _) => (length(sparkCol), ColumnType.IntType) }

      // Option operations
      case isDef: Expr.IsDefined[Row, _] =>
        convert(isDef.expr).map { case (sparkCol, _) => (sparkCol.isNotNull, ColumnType.BooleanType) }

      case goe: Expr.GetOrElse[Row, _] =>
        for {
          (e, ct) <- convert(goe.expr)
        } yield (sparkWhen(e.isNull, toLit(goe.default)).otherwise(e), ct)

      // Aggregations
      case s: Expr.Sum[Row] =>
        convert(s.expr).map { case (sparkCol, _) => (sum(sparkCol), ColumnType.IntType) }

      case _: Expr.Count[Row] =>
        Right((count(lit(1)), ColumnType.LongType))

      case mx: Expr.Max[Row, _] =>
        convert(mx.expr).map { case (sparkCol, ct) => (max(sparkCol), ct) }

      case mn: Expr.Min[Row, _] =>
        convert(mn.expr).map { case (sparkCol, ct) => (min(sparkCol), ct) }

      case av: Expr.Avg[Row] =>
        convert(av.expr).map { case (sparkCol, _) => (avg(sparkCol), ColumnType.DoubleType) }

      case cd: Expr.CountDistinct[Row, _] =>
        convert(cd.expr).map { case (sparkCol, _) => (countDistinct(sparkCol), ColumnType.LongType) }

      case ci: Expr.CountIf[Row] =>
        convert(ci.predicate).map { case (pred, _) =>
          (sum(sparkWhen(pred, 1).otherwise(0)), ColumnType.LongType)
        }

      case sd: Expr.StdDev[Row] =>
        convert(sd.expr).map { case (sparkCol, _) => (stddev(sparkCol), ColumnType.DoubleType) }

      case sdp: Expr.StdDevPop[Row] =>
        convert(sdp.expr).map { case (sparkCol, _) => (stddev_pop(sparkCol), ColumnType.DoubleType) }
    }
  }

  /** Convert a Scala value to a Spark lit Column, handling None/null. */
  private def toLit(value: Any): SparkColumn = value match {
    case None | null => lit(null) // scalafix:ok DisableSyntax.null
    case Some(v) => lit(v)
    case v => lit(v)
  }
}
