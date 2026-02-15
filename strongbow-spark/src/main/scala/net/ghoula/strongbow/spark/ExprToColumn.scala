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

      // Long arithmetic
      case add: Expr.AddLong[Row] =>
        for {
          (l, _) <- convert(add.left)
          (r, _) <- convert(add.right)
        } yield (l + r, ColumnType.LongType)

      case sub: Expr.SubLong[Row] =>
        for {
          (l, _) <- convert(sub.left)
          (r, _) <- convert(sub.right)
        } yield (l - r, ColumnType.LongType)

      case mul: Expr.MulLong[Row] =>
        for {
          (l, _) <- convert(mul.left)
          (r, _) <- convert(mul.right)
        } yield (l * r, ColumnType.LongType)

      case d: Expr.DivLong[Row] =>
        for {
          (l, _) <- convert(d.left)
          (r, _) <- convert(d.right)
        } yield ((l / r).cast("long"), ColumnType.LongType)

      // Double arithmetic
      case add: Expr.AddDouble[Row] =>
        for {
          (l, _) <- convert(add.left)
          (r, _) <- convert(add.right)
        } yield (l + r, ColumnType.DoubleType)

      case sub: Expr.SubDouble[Row] =>
        for {
          (l, _) <- convert(sub.left)
          (r, _) <- convert(sub.right)
        } yield (l - r, ColumnType.DoubleType)

      case mul: Expr.MulDouble[Row] =>
        for {
          (l, _) <- convert(mul.left)
          (r, _) <- convert(mul.right)
        } yield (l * r, ColumnType.DoubleType)

      case d: Expr.DivDouble[Row] =>
        for {
          (l, _) <- convert(d.left)
          (r, _) <- convert(d.right)
        } yield (l / r, ColumnType.DoubleType)

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

      // String pattern matching
      case lk: Expr.Like[Row] =>
        convert(lk.expr).map { case (sparkCol, _) => (sparkCol.like(lk.pattern), ColumnType.BooleanType) }

      // Aggregations
      case s: Expr.Sum[Row] =>
        convert(s.expr).map { case (sparkCol, _) => (sum(sparkCol), ColumnType.IntType) }

      case sd: Expr.SumDouble[Row] =>
        convert(sd.expr).map { case (sparkCol, _) => (sum(sparkCol), ColumnType.DoubleType) }

      case sl: Expr.SumLong[Row] =>
        convert(sl.expr).map { case (sparkCol, _) => (sum(sparkCol), ColumnType.LongType) }

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

      // Date expressions
      case dad: Expr.DateAddDays[Row] =>
        for {
          (d, _) <- convert(dad.date)
          (n, _) <- convert(dad.days)
        } yield (date_add(d, n), ColumnType.DateType)

      case dsd: Expr.DateSubDays[Row] =>
        for {
          (d, _) <- convert(dsd.date)
          (n, _) <- convert(dsd.days)
        } yield (date_sub(d, n), ColumnType.DateType)

      case dam: Expr.DateAddMonths[Row] =>
        for {
          (d, _) <- convert(dam.date)
          (n, _) <- convert(dam.months)
        } yield (add_months(d, n), ColumnType.DateType)

      case dd: Expr.DateDiff[Row] =>
        for {
          (l, _) <- convert(dd.left)
          (r, _) <- convert(dd.right)
        } yield (datediff(l, r), ColumnType.IntType)

      case ey: Expr.ExtractYear[Row] =>
        convert(ey.date).map { case (d, _) => (year(d), ColumnType.IntType) }

      case em: Expr.ExtractMonth[Row] =>
        convert(em.date).map { case (d, _) => (month(d), ColumnType.IntType) }

      case ed: Expr.ExtractDay[Row] =>
        convert(ed.date).map { case (d, _) => (dayofmonth(d), ColumnType.IntType) }

      // Window functions — these produce Spark Column but need .over(windowSpec) at call site
      case _: Expr.RowNumber[Row] =>
        Right((row_number(), ColumnType.IntType))

      case _: Expr.Rank[Row] =>
        Right((rank(), ColumnType.IntType))

      case _: Expr.DenseRank[Row] =>
        Right((dense_rank(), ColumnType.IntType))

      case lagExpr: Expr.Lag[Row, _] =>
        convert(lagExpr.expr).map { case (sparkCol, ct) =>
          val lagCol = lagExpr.default match {
            case Some(d) => lag(sparkCol, lagExpr.offset, d)
            case scala.None => lag(sparkCol, lagExpr.offset)
          }
          (lagCol, ct)
        }

      case leadExpr: Expr.Lead[Row, _] =>
        convert(leadExpr.expr).map { case (sparkCol, ct) =>
          val leadCol = leadExpr.default match {
            case Some(d) => lead(sparkCol, leadExpr.offset, d)
            case scala.None => lead(sparkCol, leadExpr.offset)
          }
          (leadCol, ct)
        }
    }
  }

  /** Convert a Scala value to a Spark lit Column, handling None/null. */
  private def toLit(value: Any): SparkColumn = value match {
    case None | null => lit(null) // scalafix:ok DisableSyntax.null
    case Some(v) => lit(v)
    case v => lit(v)
  }
}
