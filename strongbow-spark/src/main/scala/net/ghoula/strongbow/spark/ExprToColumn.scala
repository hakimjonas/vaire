package net.ghoula.strongbow.spark

import org.apache.spark.sql.{Column => SparkColumn}
import org.apache.spark.sql.functions.{when => sparkWhen, *}

import net.ghoula.strongbow.Expr
import net.ghoula.strongbow.errors.ExecutionError

/** Translates Strongbow Expr cases to Spark SQL Column expressions.
  *
  * Row-level expressions (arithmetic, comparisons, boolean logic, string ops) translate to native
  * Spark Column operations for full Catalyst optimization.
  *
  * Aggregation expressions translate to Spark aggregate functions.
  */
object ExprToColumn {

  /** Convert a Strongbow Expr to a Spark Column.
    *
    * Returns Left for unsupported expressions.
    */
  def convert[Row, A](expr: Expr[Row, A]): Either[ExecutionError, SparkColumn] = {
    (expr: @unchecked) match {
      // Leaf nodes
      case cell: Expr.Cell[Row, _] =>
        Right(col(cell.name))

      case c: Expr.Const[Row, _] =>
        Right(toLit(c.value))

      case named: Expr.Named[Row, _] =>
        convert(named.expr).map(_.as(named.name))

      // Numeric operations
      case add: Expr.Add[Row] =>
        for {
          l <- convert(add.left)
          r <- convert(add.right)
        } yield l + r

      case sub: Expr.Sub[Row] =>
        for {
          l <- convert(sub.left)
          r <- convert(sub.right)
        } yield l - r

      case mul: Expr.Mul[Row] =>
        for {
          l <- convert(mul.left)
          r <- convert(mul.right)
        } yield l * r

      case d: Expr.Div[Row] =>
        for {
          l <- convert(d.left)
          r <- convert(d.right)
        } yield l / r

      // Comparisons
      case gt: Expr.Gt[Row, _] =>
        for {
          l <- convert(gt.left)
          r <- convert(gt.right)
        } yield l > r

      case gte: Expr.Gte[Row, _] =>
        for {
          l <- convert(gte.left)
          r <- convert(gte.right)
        } yield l >= r

      case lt: Expr.Lt[Row, _] =>
        for {
          l <- convert(lt.left)
          r <- convert(lt.right)
        } yield l < r

      case lte: Expr.Lte[Row, _] =>
        for {
          l <- convert(lte.left)
          r <- convert(lte.right)
        } yield l <= r

      case eq: Expr.Eq[Row, _] =>
        for {
          l <- convert(eq.left)
          r <- convert(eq.right)
        } yield l === r

      case neq: Expr.Neq[Row, _] =>
        for {
          l <- convert(neq.left)
          r <- convert(neq.right)
        } yield l =!= r

      // Boolean operations
      case and: Expr.And[Row] =>
        for {
          l <- convert(and.left)
          r <- convert(and.right)
        } yield l && r

      case or: Expr.Or[Row] =>
        for {
          l <- convert(or.left)
          r <- convert(or.right)
        } yield l || r

      case not: Expr.Not[Row] =>
        convert(not.expr).map(!_)

      // Conditional
      case w: Expr.When[Row, _] =>
        for {
          cond <- convert(w.condition)
          thenCol <- convert(w.thenExpr)
          elseCol <- convert(w.elseExpr)
        } yield sparkWhen(cond, thenCol).otherwise(elseCol)

      // String operations
      case cat: Expr.Concat[Row] =>
        for {
          l <- convert(cat.left)
          r <- convert(cat.right)
        } yield concat(l, r)

      case len: Expr.Length[Row] =>
        convert(len.expr).map(length)

      // Option operations
      case isDef: Expr.IsDefined[Row, _] =>
        convert(isDef.expr).map(_.isNotNull)

      case goe: Expr.GetOrElse[Row, _] =>
        for {
          e <- convert(goe.expr)
        } yield sparkWhen(e.isNull, toLit(goe.default)).otherwise(e)

      // Aggregations
      case s: Expr.Sum[Row] =>
        convert(s.expr).map(sum)

      case _: Expr.Count[Row] =>
        Right(count(lit(1)))

      case mx: Expr.Max[Row, _] =>
        convert(mx.expr).map(max)

      case mn: Expr.Min[Row, _] =>
        convert(mn.expr).map(min)

      case av: Expr.Avg[Row] =>
        convert(av.expr).map(avg)

      case cd: Expr.CountDistinct[Row, _] =>
        convert(cd.expr).map(countDistinct(_))

      case ci: Expr.CountIf[Row] =>
        convert(ci.predicate).map { pred =>
          sum(sparkWhen(pred, 1).otherwise(0))
        }

      case sd: Expr.StdDev[Row] =>
        convert(sd.expr).map(stddev)

      case sdp: Expr.StdDevPop[Row] =>
        convert(sdp.expr).map(stddev_pop)
    }
  }

  /** Convert a Scala value to a Spark lit Column, handling None/null. */
  private def toLit(value: Any): SparkColumn = value match {
    case None | null => lit(null) // scalafix:ok DisableSyntax.null
    case Some(v) => lit(v)
    case v => lit(v)
  }
}
