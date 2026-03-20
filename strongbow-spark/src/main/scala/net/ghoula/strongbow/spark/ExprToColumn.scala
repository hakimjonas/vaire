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

      case cell: Expr.Cell[Row, _] =>
        Right((col(cell.name), ColumnType.AnyType))

      case c: Expr.Const[Row, _] =>
        Right((toLit(c.value), ColumnType.AnyType))

      case named: Expr.Named[Row, _] =>
        convert(named.expr).map { case (sparkCol, ct) => (sparkCol.as(named.name), ct) }

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

      case w: Expr.When[Row, _] =>
        for {
          (cond, _) <- convert(w.condition)
          (thenCol, thenType) <- convert(w.thenExpr)
          (elseCol, _) <- convert(w.elseExpr)
        } yield (sparkWhen(cond, thenCol).otherwise(elseCol), thenType)

      case cat: Expr.Concat[Row] =>
        for {
          (l, _) <- convert(cat.left)
          (r, _) <- convert(cat.right)
        } yield (concat(l, r), ColumnType.StringType)

      case len: Expr.Length[Row] =>
        convert(len.expr).map { case (sparkCol, _) => (length(sparkCol), ColumnType.IntType) }

      case isDef: Expr.IsDefined[Row, _] =>
        convert(isDef.expr).map { case (sparkCol, _) => (sparkCol.isNotNull, ColumnType.BooleanType) }

      case goe: Expr.GetOrElse[Row, _] =>
        for {
          (e, ct) <- convert(goe.expr)
        } yield (sparkWhen(e.isNull, toLit(goe.default)).otherwise(e), ct)

      case lk: Expr.Like[Row] =>
        convert(lk.expr).map { case (sparkCol, _) => (sparkCol.like(lk.pattern), ColumnType.BooleanType) }

      case lo: Expr.Lower[Row] =>
        convert(lo.expr).map { case (sparkCol, _) => (lower(sparkCol), ColumnType.StringType) }

      case up: Expr.Upper[Row] =>
        convert(up.expr).map { case (sparkCol, _) => (upper(sparkCol), ColumnType.StringType) }

      case tr: Expr.Trim[Row] =>
        convert(tr.expr).map { case (sparkCol, _) => (trim(sparkCol), ColumnType.StringType) }

      case lt: Expr.LTrim[Row] =>
        convert(lt.expr).map { case (sparkCol, _) => (ltrim(sparkCol), ColumnType.StringType) }

      case rt: Expr.RTrim[Row] =>
        convert(rt.expr).map { case (sparkCol, _) => (rtrim(sparkCol), ColumnType.StringType) }

      case ss: Expr.Substring[Row] =>
        convert(ss.expr).map { case (sparkCol, _) =>
          (substring(sparkCol, ss.pos, ss.len), ColumnType.StringType)
        }

      case sr: Expr.StringReplace[Row] =>
        convert(sr.expr).map { case (sparkCol, _) =>
          (regexp_replace(sparkCol, java.util.regex.Pattern.quote(sr.search), sr.replacement), ColumnType.StringType)
        }

      case rr: Expr.RegexpReplace[Row] =>
        convert(rr.expr).map { case (sparkCol, _) =>
          (regexp_replace(sparkCol, rr.pattern, rr.replacement), ColumnType.StringType)
        }

      case re: Expr.RegexpExtract[Row] =>
        convert(re.expr).map { case (sparkCol, _) =>
          (regexp_extract(sparkCol, re.pattern, re.groupIdx), ColumnType.StringType)
        }

      case sp: Expr.StringSplit[Row] =>
        convert(sp.expr).map { case (sparkCol, _) =>
          (split(sparkCol, sp.delimiter), ColumnType.AnyType)
        }

      case sw: Expr.StartsWith[Row] =>
        for {
          (e, _) <- convert(sw.expr)
          (p, _) <- convert(sw.prefix)
        } yield (e.startsWith(p), ColumnType.BooleanType)

      case ew: Expr.EndsWith[Row] =>
        for {
          (e, _) <- convert(ew.expr)
          (s, _) <- convert(ew.suffix)
        } yield (e.endsWith(s), ColumnType.BooleanType)

      case sc: Expr.StringContains[Row] =>
        for {
          (e, _) <- convert(sc.expr)
          (s, _) <- convert(sc.substr)
        } yield (e.contains(s), ColumnType.BooleanType)

      case cw: Expr.ConcatWs[Row] =>
        val results = cw.exprs.map(convert)
        val firstError = results.collectFirst { case Left(err) => err }
        firstError match {
          case Some(err) => Left(err)
          case scala.None =>
            val cols = results.collect { case Right((c, _)) => c }
            Right((concat_ws(cw.separator, cols*), ColumnType.StringType))
        }

      case co: Expr.Coalesce[Row, _] =>
        val results = co.exprs.map(convert)
        val firstError = results.collectFirst { case Left(err) => err }
        firstError match {
          case Some(err) => Left(err)
          case scala.None =>
            val cols = results.collect { case Right((c, _)) => c }
            val ct = results.collectFirst { case Right((_, t)) => t }.getOrElse(ColumnType.AnyType)
            Right((coalesce(cols*), ct))
        }

      case in: Expr.IsNull[Row, _] =>
        convert(in.expr).map { case (sparkCol, _) => (sparkCol.isNull, ColumnType.BooleanType) }

      case inn: Expr.IsNotNull[Row, _] =>
        convert(inn.expr).map { case (sparkCol, _) => (sparkCol.isNotNull, ColumnType.BooleanType) }

      case inV: Expr.In[Row, _] =>
        convert(inV.expr).map { case (sparkCol, _) =>
          val litValues = inV.values.map(v => toLit(v))
          (sparkCol.isin(litValues*), ColumnType.BooleanType)
        }

      case btw: Expr.Between[Row, _] =>
        for {
          (e, _) <- convert(btw.expr)
          (lo, _) <- convert(btw.lower)
          (hi, _) <- convert(btw.upper)
        } yield (e.between(lo, hi), ColumnType.BooleanType)

      case m: Expr.Mod[Row] =>
        for {
          (l, _) <- convert(m.left)
          (r, _) <- convert(m.right)
        } yield (l % r, ColumnType.IntType)

      case ml: Expr.ModLong[Row] =>
        for {
          (l, _) <- convert(ml.left)
          (r, _) <- convert(ml.right)
        } yield (l % r, ColumnType.LongType)

      case ab: Expr.Abs[Row] =>
        convert(ab.expr).map { case (sparkCol, _) => (abs(sparkCol), ColumnType.IntType) }

      case abl: Expr.AbsLong[Row] =>
        convert(abl.expr).map { case (sparkCol, _) => (abs(sparkCol), ColumnType.LongType) }

      case abd: Expr.AbsDouble[Row] =>
        convert(abd.expr).map { case (sparkCol, _) => (abs(sparkCol), ColumnType.DoubleType) }

      case neg: Expr.Negate[Row] =>
        convert(neg.expr).map { case (sparkCol, _) => (negate(sparkCol), ColumnType.IntType) }

      case negl: Expr.NegateLong[Row] =>
        convert(negl.expr).map { case (sparkCol, _) => (negate(sparkCol), ColumnType.LongType) }

      case negd: Expr.NegateDouble[Row] =>
        convert(negd.expr).map { case (sparkCol, _) => (negate(sparkCol), ColumnType.DoubleType) }

      case rnd: Expr.Round[Row] =>
        convert(rnd.expr).map { case (sparkCol, _) => (round(sparkCol, rnd.scale), ColumnType.DoubleType) }

      case fl: Expr.Floor[Row] =>
        convert(fl.expr).map { case (sparkCol, _) => (floor(sparkCol), ColumnType.DoubleType) }

      case cl: Expr.Ceil[Row] =>
        convert(cl.expr).map { case (sparkCol, _) => (ceil(sparkCol), ColumnType.DoubleType) }

      case ctl: Expr.CastToLong[Row] =>
        convert(ctl.expr).map { case (sparkCol, _) => (sparkCol.cast("long"), ColumnType.LongType) }

      case ctd: Expr.CastToDouble[Row] =>
        convert(ctd.expr).map { case (sparkCol, _) => (sparkCol.cast("double"), ColumnType.DoubleType) }

      case cltd: Expr.CastLongToDouble[Row] =>
        convert(cltd.expr).map { case (sparkCol, _) => (sparkCol.cast("double"), ColumnType.DoubleType) }

      case cts: Expr.CastToString[Row, _] =>
        convert(cts.expr).map { case (sparkCol, _) => (sparkCol.cast("string"), ColumnType.StringType) }

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

      case f: Expr.First[Row, _] =>
        convert(f.expr).map { case (sparkCol, ct) => (first(sparkCol), ct) }

      case c: Expr.Collect[Row, _] =>
        convert(c.expr).map { case (sparkCol, _) => (collect_list(sparkCol), ColumnType.AnyType) }

      case opt2iter: Expr.Option2Iterable[Row, _] =>
        convert(opt2iter.expr).map { case (sparkCol, _) =>
          (sparkWhen(sparkCol.isNotNull, array(sparkCol)).otherwise(array()), ColumnType.AnyType)
        }

      case pct: Expr.PercentileApprox[Row] =>
        convert(pct.expr).map { case (sparkCol, _) =>
          (percentile_approx(sparkCol, lit(pct.percentile), lit(pct.accuracy)), ColumnType.DoubleType)
        }

      case mb: Expr.MaxBy[Row, _, _] =>
        for {
          (valCol, _) <- convert(mb.valueExpr)
          (ordCol, _) <- convert(mb.orderExpr)
        } yield (max_by(valCol, ordCol), ColumnType.AnyType)

      case mb: Expr.MinBy[Row, _, _] =>
        for {
          (valCol, _) <- convert(mb.valueExpr)
          (ordCol, _) <- convert(mb.orderExpr)
        } yield (min_by(valCol, ordCol), ColumnType.AnyType)

      case mn: Expr.MaxN[Row, _] =>
        convert(mn.expr).map { case (sparkCol, _) =>
          (slice(sort_array(collect_list(sparkCol), asc = false), 1, mn.n), ColumnType.AnyType)
        }

      case mn: Expr.MinN[Row, _] =>
        convert(mn.expr).map { case (sparkCol, _) =>
          (slice(sort_array(collect_list(sparkCol), asc = true), 1, mn.n), ColumnType.AnyType)
        }

      case mbn: Expr.MaxByN[Row, _, _] =>
        for {
          (valCol, _) <- convert(mbn.valueExpr)
          (ordCol, _) <- convert(mbn.orderExpr)
        } yield {
          val zipped = arrays_zip(collect_list(valCol).as("v"), collect_list(ordCol).as("k"))
          val sorted = sort_array(zipped, asc = false)
          val sliced = slice(sorted, 1, mbn.n)
          (transform(sliced, (x: SparkColumn) => x.getField("v")), ColumnType.AnyType)
        }

      case mbn: Expr.MinByN[Row, _, _] =>
        for {
          (valCol, _) <- convert(mbn.valueExpr)
          (ordCol, _) <- convert(mbn.orderExpr)
        } yield {
          val zipped = arrays_zip(collect_list(valCol).as("v"), collect_list(ordCol).as("k"))
          val sorted = sort_array(zipped, asc = true)
          val sliced = slice(sorted, 1, mbn.n)
          (transform(sliced, (x: SparkColumn) => x.getField("v")), ColumnType.AnyType)
        }

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

      case sq: Expr.Sqrt[Row] =>
        convert(sq.expr).map { case (sparkCol, _) => (sqrt(sparkCol), ColumnType.DoubleType) }

      case pw: Expr.Pow[Row] =>
        for {
          (b, _) <- convert(pw.base)
          (e, _) <- convert(pw.exponent)
        } yield (pow(b, e), ColumnType.DoubleType)

      case lg: Expr.Log[Row] =>
        convert(lg.expr).map { case (sparkCol, _) => (log(sparkCol), ColumnType.DoubleType) }

      case lg10: Expr.Log10[Row] =>
        convert(lg10.expr).map { case (sparkCol, _) => (log10(sparkCol), ColumnType.DoubleType) }

      case lg2: Expr.Log2[Row] =>
        convert(lg2.expr).map { case (sparkCol, _) => (log2(sparkCol), ColumnType.DoubleType) }

      case ex: Expr.Exp[Row] =>
        convert(ex.expr).map { case (sparkCol, _) => (exp(sparkCol), ColumnType.DoubleType) }

      case sn: Expr.Sin[Row] =>
        convert(sn.expr).map { case (sparkCol, _) => (sin(sparkCol), ColumnType.DoubleType) }

      case cs: Expr.Cos[Row] =>
        convert(cs.expr).map { case (sparkCol, _) => (cos(sparkCol), ColumnType.DoubleType) }

      case tn: Expr.Tan[Row] =>
        convert(tn.expr).map { case (sparkCol, _) => (tan(sparkCol), ColumnType.DoubleType) }

      case asn: Expr.Asin[Row] =>
        convert(asn.expr).map { case (sparkCol, _) => (asin(sparkCol), ColumnType.DoubleType) }

      case acs: Expr.Acos[Row] =>
        convert(acs.expr).map { case (sparkCol, _) => (acos(sparkCol), ColumnType.DoubleType) }

      case atn: Expr.Atan[Row] =>
        convert(atn.expr).map { case (sparkCol, _) => (atan(sparkCol), ColumnType.DoubleType) }

      case atn2: Expr.Atan2[Row] =>
        for {
          (y, _) <- convert(atn2.y)
          (x, _) <- convert(atn2.x)
        } yield (atan2(y, x), ColumnType.DoubleType)

      case sg: Expr.Signum[Row] =>
        convert(sg.expr).map { case (sparkCol, _) => (signum(sparkCol), ColumnType.DoubleType) }

      case rnd: Expr.Rand[Row] =>
        Right((rand(rnd.seed), ColumnType.DoubleType))

      case dow: Expr.DayOfWeek[Row] =>
        convert(dow.date).map { case (d, _) => (dayofweek(d), ColumnType.IntType) }

      case doy: Expr.DayOfYear[Row] =>
        convert(doy.date).map { case (d, _) => (dayofyear(d), ColumnType.IntType) }

      case woy: Expr.WeekOfYear[Row] =>
        convert(woy.date).map { case (d, _) => (weekofyear(d), ColumnType.IntType) }

      case q: Expr.Quarter[Row] =>
        convert(q.date).map { case (d, _) => (quarter(d), ColumnType.IntType) }

      case ld: Expr.LastDay[Row] =>
        convert(ld.date).map { case (d, _) => (last_day(d), ColumnType.DateType) }

      case nd: Expr.NextDay[Row] =>
        convert(nd.date).map { case (d, _) => (next_day(d, nd.dayOfWeek), ColumnType.DateType) }

      case mb: Expr.MonthsBetween[Row] =>
        for {
          (e, _) <- convert(mb.end)
          (s, _) <- convert(mb.start)
        } yield (months_between(e, s), ColumnType.DoubleType)

      case dt: Expr.DateTrunc[Row] =>
        convert(dt.date).map { case (d, _) => (date_trunc(dt.unit, d), ColumnType.DateType) }

      case df: Expr.DateFormat[Row] =>
        convert(df.date).map { case (d, _) => (date_format(d, df.format), ColumnType.StringType) }

      case md: Expr.MakeDate[Row] =>
        for {
          (y, _) <- convert(md.year)
          (m, _) <- convert(md.month)
          (d, _) <- convert(md.day)
        } yield (make_date(y, m, d), ColumnType.DateType)

      case v: Expr.Variance[Row] =>
        convert(v.expr).map { case (sparkCol, _) => (var_samp(sparkCol), ColumnType.DoubleType) }

      case vp: Expr.VariancePop[Row] =>
        convert(vp.expr).map { case (sparkCol, _) => (var_pop(sparkCol), ColumnType.DoubleType) }

      case acd: Expr.ApproxCountDistinct[Row, _] =>
        convert(acd.expr).map { case (sparkCol, _) => (approx_count_distinct(sparkCol), ColumnType.LongType) }

      case cs: Expr.CollectSet[Row, _] =>
        convert(cs.expr).map { case (sparkCol, _) => (collect_set(sparkCol), ColumnType.AnyType) }

      case el: Expr.ExprLast[Row, _] =>
        convert(el.expr).map { case (sparkCol, ct) => (last(sparkCol), ct) }

      case av: Expr.AnyValue[Row, _] =>
        convert(av.expr).map { case (sparkCol, ct) => (any_value(sparkCol), ct) }

      case ba: Expr.BoolAnd[Row] =>
        convert(ba.expr).map { case (sparkCol, _) => (bool_and(sparkCol), ColumnType.BooleanType) }

      case bo: Expr.BoolOr[Row] =>
        convert(bo.expr).map { case (sparkCol, _) => (bool_or(sparkCol), ColumnType.BooleanType) }

      case cr: Expr.Corr[Row] =>
        for {
          (l, _) <- convert(cr.left)
          (r, _) <- convert(cr.right)
        } yield (corr(l, r), ColumnType.DoubleType)

      case cvs: Expr.CovarSamp[Row] =>
        for {
          (l, _) <- convert(cvs.left)
          (r, _) <- convert(cvs.right)
        } yield (covar_samp(l, r), ColumnType.DoubleType)

      case cvp: Expr.CovarPop[Row] =>
        for {
          (l, _) <- convert(cvp.left)
          (r, _) <- convert(cvp.right)
        } yield (covar_pop(l, r), ColumnType.DoubleType)

      case med: Expr.Median[Row] =>
        convert(med.expr).map { case (sparkCol, _) => (median(sparkCol), ColumnType.DoubleType) }

      case md: Expr.Mode[Row, _] =>
        convert(md.expr).map { case (sparkCol, ct) => (mode(sparkCol), ct) }

      case nt: Expr.NTile[Row] =>
        Right((ntile(nt.n), ColumnType.IntType))

      case _: Expr.CumeDist[Row] =>
        Right((cume_dist(), ColumnType.DoubleType))

      case _: Expr.PercentRank[Row] =>
        Right((percent_rank(), ColumnType.DoubleType))

      case nv: Expr.NthValue[Row, _] =>
        convert(nv.expr).map { case (sparkCol, ct) => (nth_value(sparkCol, nv.n), ct) }

      case fv: Expr.FirstValue[Row, _] =>
        convert(fv.expr).map { case (sparkCol, ct) => (first_value(sparkCol), ct) }

      case lv: Expr.LastValue[Row, _] =>
        convert(lv.expr).map { case (sparkCol, ct) => (last_value(sparkCol), ct) }

      case as: Expr.ArraySize[Row, _] =>
        convert(as.expr).map { case (sparkCol, _) => (size(sparkCol), ColumnType.IntType) }

      case ac: Expr.ArrayContains[Row, _] =>
        for {
          (arrCol, _) <- convert(ac.expr)
          (valCol, _) <- convert(ac.value)
        } yield (array_contains(arrCol, valCol), ColumnType.BooleanType)

      case ex: Expr.Explode[Row, _] =>
        convert(ex.expr).map { case (sparkCol, _) => (explode(sparkCol), ColumnType.AnyType) }

      case asrt: Expr.ArraySort[Row, _] =>
        convert(asrt.expr).map { case (sparkCol, _) => (sort_array(sparkCol), ColumnType.AnyType) }

      case ad: Expr.ArrayDistinct[Row, _] =>
        convert(ad.expr).map { case (sparkCol, _) => (array_distinct(sparkCol), ColumnType.AnyType) }

      case au: Expr.ArrayUnion[Row, _] =>
        for {
          (l, _) <- convert(au.left)
          (r, _) <- convert(au.right)
        } yield (array_union(l, r), ColumnType.AnyType)

      case ai: Expr.ArrayIntersect[Row, _] =>
        for {
          (l, _) <- convert(ai.left)
          (r, _) <- convert(ai.right)
        } yield (array_intersect(l, r), ColumnType.AnyType)

      case ae: Expr.ArrayExcept[Row, _] =>
        for {
          (l, _) <- convert(ae.left)
          (r, _) <- convert(ae.right)
        } yield (array_except(l, r), ColumnType.AnyType)

      case fl: Expr.Flatten[Row, _] =>
        convert(fl.expr).map { case (sparkCol, _) => (flatten(sparkCol), ColumnType.AnyType) }

      case ea: Expr.ElementAt[Row, _] =>
        for {
          (arrCol, _) <- convert(ea.expr)
          (idxCol, _) <- convert(ea.index)
        } yield (element_at(arrCol, idxCol), ColumnType.AnyType)

      case as: Expr.ArraySlice[Row, _] =>
        convert(as.expr).map { case (sparkCol, _) =>
          (slice(sparkCol, as.start, as.length), ColumnType.AnyType)
        }

      case mk: Expr.MapKeys[Row, _, _] =>
        convert(mk.expr).map { case (sparkCol, _) => (map_keys(sparkCol), ColumnType.AnyType) }

      case mv: Expr.MapValues[Row, _, _] =>
        convert(mv.expr).map { case (sparkCol, _) => (map_values(sparkCol), ColumnType.AnyType) }

      case mck: Expr.MapContainsKey[Row, _, _] =>
        for {
          (mapCol, _) <- convert(mck.expr)
          (keyCol, _) <- convert(mck.key)
        } yield (array_contains(map_keys(mapCol), keyCol), ColumnType.BooleanType)

      case me: Expr.MapEntries[Row, _, _] =>
        convert(me.expr).map { case (sparkCol, _) => (map_entries(sparkCol), ColumnType.AnyType) }

      case mfa: Expr.MapFromArrays[Row, _, _] =>
        for {
          (keysCol, _) <- convert(mfa.keys)
          (valsCol, _) <- convert(mfa.values)
        } yield (map_from_arrays(keysCol, valsCol), ColumnType.AnyType)

      case mc: Expr.MapConcat[Row, _, _] =>
        for {
          (l, _) <- convert(mc.left)
          (r, _) <- convert(mc.right)
        } yield (map_concat(l, r), ColumnType.AnyType)

      case m: Expr.Md5[Row] =>
        convert(m.expr).map { case (sparkCol, _) => (md5(sparkCol), ColumnType.StringType) }

      case s: Expr.Sha1[Row] =>
        convert(s.expr).map { case (sparkCol, _) => (sha1(sparkCol), ColumnType.StringType) }

      case s2: Expr.Sha2[Row] =>
        convert(s2.expr).map { case (sparkCol, _) => (sha2(sparkCol, s2.bitLength), ColumnType.StringType) }

      case ue: Expr.UrlEncode[Row] =>
        convert(ue.expr).map { case (sparkCol, _) => (url_encode(sparkCol), ColumnType.StringType) }

      case ud: Expr.UrlDecode[Row] =>
        convert(ud.expr).map { case (sparkCol, _) => (url_decode(sparkCol), ColumnType.StringType) }

      case b64e: Expr.Base64Encode[Row] =>
        convert(b64e.expr).map { case (sparkCol, _) => (base64(sparkCol), ColumnType.StringType) }

      case b64d: Expr.Base64Decode[Row] =>
        convert(b64d.expr).map { case (sparkCol, _) => (unbase64(sparkCol).cast("string"), ColumnType.StringType) }

      case hx: Expr.Hex[Row] =>
        convert(hx.expr).map { case (sparkCol, _) => (hex(sparkCol), ColumnType.StringType) }

      case gjo: Expr.GetJsonObject[Row] =>
        convert(gjo.expr).map { case (sparkCol, _) => (get_json_object(sparkCol, gjo.path), ColumnType.StringType) }
    }
  }

  /** Convert a Scala value to a Spark lit Column, handling None/null. */
  private def toLit(value: Any): SparkColumn = value match {
    case None | null => lit(null) // scalafix:ok DisableSyntax.null
    case Some(v) => lit(v)
    case v => lit(v)
  }
}
