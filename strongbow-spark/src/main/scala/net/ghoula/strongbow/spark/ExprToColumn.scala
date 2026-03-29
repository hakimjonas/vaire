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

  private def convertBinary[Row, A, B, C](
    left: Expr[Row, A],
    right: Expr[Row, B],
    op: (SparkColumn, SparkColumn) => SparkColumn,
    resultType: ColumnType
  ): Either[ExecutionError, (SparkColumn, ColumnType)] =
    for {
      (l, _) <- convert(left)
      (r, _) <- convert(right)
    } yield (op(l, r), resultType)

  private def convertUnary[Row, A, B](
    expr: Expr[Row, A],
    op: SparkColumn => SparkColumn,
    resultType: ColumnType
  ): Either[ExecutionError, (SparkColumn, ColumnType)] =
    convert(expr).map { case (sparkCol, _) => (op(sparkCol), resultType) }

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

      case add: Expr.Add[Row] => convertBinary(add.left, add.right, _ + _, ColumnType.IntType)
      case sub: Expr.Sub[Row] => convertBinary(sub.left, sub.right, _ - _, ColumnType.IntType)
      case mul: Expr.Mul[Row] => convertBinary(mul.left, mul.right, _ * _, ColumnType.IntType)
      case d: Expr.Div[Row] => convertBinary(d.left, d.right, (l, r) => (l / r).cast("int"), ColumnType.IntType)

      case add: Expr.AddLong[Row] => convertBinary(add.left, add.right, _ + _, ColumnType.LongType)
      case sub: Expr.SubLong[Row] => convertBinary(sub.left, sub.right, _ - _, ColumnType.LongType)
      case mul: Expr.MulLong[Row] => convertBinary(mul.left, mul.right, _ * _, ColumnType.LongType)
      case d: Expr.DivLong[Row] => convertBinary(d.left, d.right, (l, r) => (l / r).cast("long"), ColumnType.LongType)

      case add: Expr.AddDouble[Row] => convertBinary(add.left, add.right, _ + _, ColumnType.DoubleType)
      case sub: Expr.SubDouble[Row] => convertBinary(sub.left, sub.right, _ - _, ColumnType.DoubleType)
      case mul: Expr.MulDouble[Row] => convertBinary(mul.left, mul.right, _ * _, ColumnType.DoubleType)
      case d: Expr.DivDouble[Row] => convertBinary(d.left, d.right, _ / _, ColumnType.DoubleType)

      case gt: Expr.Gt[Row, _] => convertBinary(gt.left, gt.right, _ > _, ColumnType.BooleanType)
      case gte: Expr.Gte[Row, _] => convertBinary(gte.left, gte.right, _ >= _, ColumnType.BooleanType)
      case lt: Expr.Lt[Row, _] => convertBinary(lt.left, lt.right, _ < _, ColumnType.BooleanType)
      case lte: Expr.Lte[Row, _] => convertBinary(lte.left, lte.right, _ <= _, ColumnType.BooleanType)
      case eq: Expr.Eq[Row, _] => convertBinary(eq.left, eq.right, _ === _, ColumnType.BooleanType)
      case neq: Expr.Neq[Row, _] => convertBinary(neq.left, neq.right, _ =!= _, ColumnType.BooleanType)

      case and: Expr.And[Row] => convertBinary(and.left, and.right, _ && _, ColumnType.BooleanType)
      case or: Expr.Or[Row] => convertBinary(or.left, or.right, _ || _, ColumnType.BooleanType)
      case not: Expr.Not[Row] => convertUnary(not.expr, !_, ColumnType.BooleanType)

      case w: Expr.When[Row, _] =>
        for {
          (cond, _) <- convert(w.condition)
          (thenCol, thenType) <- convert(w.thenExpr)
          (elseCol, _) <- convert(w.elseExpr)
        } yield (sparkWhen(cond, thenCol).otherwise(elseCol), thenType)

      case cat: Expr.Concat[Row] => convertBinary(cat.left, cat.right, concat(_, _), ColumnType.StringType)
      case len: Expr.Length[Row] => convertUnary(len.expr, length, ColumnType.IntType)
      case isDef: Expr.IsDefined[Row, _] => convertUnary(isDef.expr, _.isNotNull, ColumnType.BooleanType)

      case goe: Expr.GetOrElse[Row, _] =>
        convert(goe.expr).map { case (e, ct) => (sparkWhen(e.isNull, toLit(goe.default)).otherwise(e), ct) }

      case lk: Expr.Like[Row] => convertUnary(lk.expr, _.like(lk.pattern), ColumnType.BooleanType)
      case lo: Expr.Lower[Row] => convertUnary(lo.expr, lower, ColumnType.StringType)
      case up: Expr.Upper[Row] => convertUnary(up.expr, upper, ColumnType.StringType)
      case tr: Expr.Trim[Row] => convertUnary(tr.expr, trim, ColumnType.StringType)
      case lt: Expr.LTrim[Row] => convertUnary(lt.expr, ltrim, ColumnType.StringType)
      case rt: Expr.RTrim[Row] => convertUnary(rt.expr, rtrim, ColumnType.StringType)

      case ss: Expr.Substring[Row] =>
        convertUnary(ss.expr, substring(_, ss.pos, ss.len), ColumnType.StringType)

      case sr: Expr.StringReplace[Row] =>
        convertUnary(
          sr.expr,
          regexp_replace(_, java.util.regex.Pattern.quote(sr.search), sr.replacement),
          ColumnType.StringType
        )

      case rr: Expr.RegexpReplace[Row] =>
        convertUnary(rr.expr, regexp_replace(_, rr.pattern, rr.replacement), ColumnType.StringType)

      case re: Expr.RegexpExtract[Row] =>
        convertUnary(re.expr, regexp_extract(_, re.pattern, re.groupIdx), ColumnType.StringType)

      case sp: Expr.StringSplit[Row] =>
        convertUnary(sp.expr, split(_, sp.delimiter), ColumnType.AnyType)

      case sw: Expr.StartsWith[Row] => convertBinary(sw.expr, sw.prefix, _.startsWith(_), ColumnType.BooleanType)
      case ew: Expr.EndsWith[Row] => convertBinary(ew.expr, ew.suffix, _.endsWith(_), ColumnType.BooleanType)
      case sc: Expr.StringContains[Row] => convertBinary(sc.expr, sc.substr, _.contains(_), ColumnType.BooleanType)

      case cw: Expr.ConcatWs[Row] =>
        convertSeq(cw.exprs, (cols, _) => concat_ws(cw.separator, cols*), ColumnType.StringType)

      case co: Expr.Coalesce[Row, _] =>
        convertSeq(
          co.exprs,
          (cols, _) => coalesce(cols*),
          ColumnType.AnyType
        ) // Note: actual type is the first non-null type from 'types'

      case in: Expr.IsNull[Row, _] => convertUnary(in.expr, _.isNull, ColumnType.BooleanType)
      case inn: Expr.IsNotNull[Row, _] => convertUnary(inn.expr, _.isNotNull, ColumnType.BooleanType)

      case inV: Expr.In[Row, _] =>
        convertUnary(inV.expr, _.isin(inV.values.map(toLit)*), ColumnType.BooleanType)

      case btw: Expr.Between[Row, _] =>
        for {
          (e, _) <- convert(btw.expr)
          (lo, _) <- convert(btw.lower)
          (hi, _) <- convert(btw.upper)
        } yield (e.between(lo, hi), ColumnType.BooleanType)

      case m: Expr.Mod[Row] => convertBinary(m.left, m.right, _ % _, ColumnType.IntType)
      case ml: Expr.ModLong[Row] => convertBinary(ml.left, ml.right, _ % _, ColumnType.LongType)

      case ab: Expr.Abs[Row] => convertUnary(ab.expr, abs, ColumnType.IntType)
      case abl: Expr.AbsLong[Row] => convertUnary(abl.expr, abs, ColumnType.LongType)
      case abd: Expr.AbsDouble[Row] => convertUnary(abd.expr, abs, ColumnType.DoubleType)

      case neg: Expr.Negate[Row] => convertUnary(neg.expr, negate, ColumnType.IntType)
      case negl: Expr.NegateLong[Row] => convertUnary(negl.expr, negate, ColumnType.LongType)
      case negd: Expr.NegateDouble[Row] => convertUnary(negd.expr, negate, ColumnType.DoubleType)

      case rnd: Expr.Round[Row] =>
        convertUnary(rnd.expr, round(_, rnd.scale), ColumnType.DoubleType)

      case fl: Expr.Floor[Row] => convertUnary(fl.expr, floor, ColumnType.DoubleType)
      case cl: Expr.Ceil[Row] => convertUnary(cl.expr, ceil, ColumnType.DoubleType)

      case ctl: Expr.CastToLong[Row] => convertUnary(ctl.expr, _.cast("long"), ColumnType.LongType)
      case ctd: Expr.CastToDouble[Row] => convertUnary(ctd.expr, _.cast("double"), ColumnType.DoubleType)
      case cltd: Expr.CastLongToDouble[Row] => convertUnary(cltd.expr, _.cast("double"), ColumnType.DoubleType)
      case cts: Expr.CastToString[Row, _] => convertUnary(cts.expr, _.cast("string"), ColumnType.StringType)

      case s: Expr.Sum[Row] => convertUnary(s.expr, sum, ColumnType.LongType)
      case sd: Expr.SumDouble[Row] => convertUnary(sd.expr, sum, ColumnType.DoubleType)
      case sl: Expr.SumLong[Row] => convertUnary(sl.expr, sum, ColumnType.LongType)

      case _: Expr.Count[Row] => Right((count(lit(1)), ColumnType.LongType))

      case mx: Expr.Max[Row, _] =>
        convert(mx.expr).map { case (sparkCol, ct) => (max(sparkCol), ct) }

      case mn: Expr.Min[Row, _] =>
        convert(mn.expr).map { case (sparkCol, ct) => (min(sparkCol), ct) }

      case av: Expr.Avg[Row] => convertUnary(av.expr, avg, ColumnType.DoubleType)
      case cd: Expr.CountDistinct[Row, _] => convertUnary(cd.expr, c => countDistinct(c), ColumnType.LongType)

      case ci: Expr.CountIf[Row] =>
        convertUnary(ci.predicate, pred => sum(sparkWhen(pred, 1).otherwise(0)), ColumnType.LongType)

      case sd: Expr.StdDev[Row] => convertUnary(sd.expr, stddev, ColumnType.DoubleType)
      case sdp: Expr.StdDevPop[Row] => convertUnary(sdp.expr, stddev_pop, ColumnType.DoubleType)

      case f: Expr.First[Row, _] =>
        convert(f.expr).map { case (sparkCol, ct) => (first(sparkCol), ct) }

      case c: Expr.Collect[Row, _] => convertUnary(c.expr, collect_list, ColumnType.AnyType)

      case opt2iter: Expr.Option2Iterable[Row, _] =>
        convertUnary(
          opt2iter.expr,
          sparkCol => sparkWhen(sparkCol.isNotNull, array(sparkCol)).otherwise(array()),
          ColumnType.AnyType
        )

      case pct: Expr.PercentileApprox[Row] =>
        convertUnary(pct.expr, percentile_approx(_, lit(pct.percentile), lit(pct.accuracy)), ColumnType.DoubleType)

      case mb: Expr.MaxBy[Row, _, _] => convertBinary(mb.valueExpr, mb.orderExpr, max_by, ColumnType.AnyType)
      case mb: Expr.MinBy[Row, _, _] => convertBinary(mb.valueExpr, mb.orderExpr, min_by, ColumnType.AnyType)

      case mn: Expr.MaxN[Row, _] =>
        convertUnary(mn.expr, sc => slice(sort_array(collect_list(sc), asc = false), 1, mn.n), ColumnType.AnyType)

      case mn: Expr.MinN[Row, _] =>
        convertUnary(mn.expr, sc => slice(sort_array(collect_list(sc), asc = true), 1, mn.n), ColumnType.AnyType)

      case mbn: Expr.MaxByN[Row, _, _] =>
        convertBinary(
          mbn.valueExpr,
          mbn.orderExpr,
          applyByN(_, _, mbn.n, asc = false),
          ColumnType.AnyType
        )

      case mbn: Expr.MinByN[Row, _, _] =>
        convertBinary(
          mbn.valueExpr,
          mbn.orderExpr,
          applyByN(_, _, mbn.n, asc = true),
          ColumnType.AnyType
        )

      case dad: Expr.DateAddDays[Row] => convertBinary(dad.date, dad.days, date_add, ColumnType.DateType)
      case dsd: Expr.DateSubDays[Row] => convertBinary(dsd.date, dsd.days, date_sub, ColumnType.DateType)
      case dam: Expr.DateAddMonths[Row] => convertBinary(dam.date, dam.months, add_months, ColumnType.DateType)
      case dd: Expr.DateDiff[Row] => convertBinary(dd.left, dd.right, datediff, ColumnType.IntType)

      case ey: Expr.ExtractYear[Row] => convertUnary(ey.date, year, ColumnType.IntType)
      case em: Expr.ExtractMonth[Row] => convertUnary(em.date, month, ColumnType.IntType)
      case ed: Expr.ExtractDay[Row] => convertUnary(ed.date, dayofmonth, ColumnType.IntType)

      case _: Expr.RowNumber[Row] => Right((row_number(), ColumnType.IntType))
      case _: Expr.Rank[Row] => Right((rank(), ColumnType.IntType))
      case _: Expr.DenseRank[Row] => Right((dense_rank(), ColumnType.IntType))

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

      case sq: Expr.Sqrt[Row] => convertUnary(sq.expr, sqrt, ColumnType.DoubleType)
      case pw: Expr.Pow[Row] => convertBinary(pw.base, pw.exponent, pow, ColumnType.DoubleType)

      case lg: Expr.Log[Row] => convertUnary(lg.expr, log, ColumnType.DoubleType)
      case lg10: Expr.Log10[Row] => convertUnary(lg10.expr, log10, ColumnType.DoubleType)
      case lg2: Expr.Log2[Row] => convertUnary(lg2.expr, log2, ColumnType.DoubleType)
      case ex: Expr.Exp[Row] => convertUnary(ex.expr, exp, ColumnType.DoubleType)

      case sn: Expr.Sin[Row] => convertUnary(sn.expr, sin, ColumnType.DoubleType)
      case cs: Expr.Cos[Row] => convertUnary(cs.expr, cos, ColumnType.DoubleType)
      case tn: Expr.Tan[Row] => convertUnary(tn.expr, tan, ColumnType.DoubleType)
      case asn: Expr.Asin[Row] => convertUnary(asn.expr, asin, ColumnType.DoubleType)
      case acs: Expr.Acos[Row] => convertUnary(acs.expr, acos, ColumnType.DoubleType)
      case atn: Expr.Atan[Row] => convertUnary(atn.expr, atan, ColumnType.DoubleType)
      case atn2: Expr.Atan2[Row] => convertBinary(atn2.y, atn2.x, atan2, ColumnType.DoubleType)

      case sg: Expr.Signum[Row] => convertUnary(sg.expr, signum, ColumnType.DoubleType)
      case rnd: Expr.Rand[Row] => Right((rand(rnd.seed), ColumnType.DoubleType))

      case dow: Expr.DayOfWeek[Row] => convertUnary(dow.date, dayofweek, ColumnType.IntType)
      case doy: Expr.DayOfYear[Row] => convertUnary(doy.date, dayofyear, ColumnType.IntType)
      case woy: Expr.WeekOfYear[Row] => convertUnary(woy.date, weekofyear, ColumnType.IntType)
      case q: Expr.Quarter[Row] => convertUnary(q.date, quarter, ColumnType.IntType)

      case ld: Expr.LastDay[Row] => convertUnary(ld.date, last_day, ColumnType.DateType)
      case nd: Expr.NextDay[Row] => convertUnary(nd.date, next_day(_, nd.dayOfWeek), ColumnType.DateType)

      case mb: Expr.MonthsBetween[Row] => convertBinary(mb.end, mb.start, months_between, ColumnType.DoubleType)

      case dt: Expr.DateTrunc[Row] => convertUnary(dt.date, date_trunc(dt.unit, _), ColumnType.DateType)
      case df: Expr.DateFormat[Row] => convertUnary(df.date, date_format(_, df.format), ColumnType.StringType)

      case md: Expr.MakeDate[Row] =>
        for {
          (y, _) <- convert(md.year)
          (m, _) <- convert(md.month)
          (d, _) <- convert(md.day)
        } yield (make_date(y, m, d), ColumnType.DateType)

      case v: Expr.Variance[Row] => convertUnary(v.expr, var_samp, ColumnType.DoubleType)
      case vp: Expr.VariancePop[Row] => convertUnary(vp.expr, var_pop, ColumnType.DoubleType)
      case acd: Expr.ApproxCountDistinct[Row, _] => convertUnary(acd.expr, approx_count_distinct, ColumnType.LongType)
      case cs: Expr.CollectSet[Row, _] => convertUnary(cs.expr, collect_set, ColumnType.AnyType)

      case el: Expr.ExprLast[Row, _] =>
        convert(el.expr).map { case (sparkCol, ct) => (last(sparkCol), ct) }

      case av: Expr.AnyValue[Row, _] =>
        convert(av.expr).map { case (sparkCol, ct) => (any_value(sparkCol), ct) }

      case ba: Expr.BoolAnd[Row] => convertUnary(ba.expr, bool_and, ColumnType.BooleanType)
      case bo: Expr.BoolOr[Row] => convertUnary(bo.expr, bool_or, ColumnType.BooleanType)

      case cr: Expr.Corr[Row] => convertBinary(cr.left, cr.right, corr, ColumnType.DoubleType)
      case cvs: Expr.CovarSamp[Row] => convertBinary(cvs.left, cvs.right, covar_samp, ColumnType.DoubleType)
      case cvp: Expr.CovarPop[Row] => convertBinary(cvp.left, cvp.right, covar_pop, ColumnType.DoubleType)

      case med: Expr.Median[Row] => convertUnary(med.expr, median, ColumnType.DoubleType)
      case md: Expr.Mode[Row, _] =>
        convert(md.expr).map { case (sparkCol, ct) => (mode(sparkCol), ct) }

      case nt: Expr.NTile[Row] => Right((ntile(nt.n), ColumnType.IntType))
      case _: Expr.CumeDist[Row] => Right((cume_dist(), ColumnType.DoubleType))
      case _: Expr.PercentRank[Row] => Right((percent_rank(), ColumnType.DoubleType))

      case nv: Expr.NthValue[Row, _] =>
        convertUnary(
          nv.expr,
          nth_value(_, nv.n),
          ColumnType.AnyType
        ) // Note: NthValue result type depends on input, AnyType is safe here

      case fv: Expr.FirstValue[Row, _] =>
        convert(fv.expr).map { case (sparkCol, ct) => (first_value(sparkCol), ct) }

      case lv: Expr.LastValue[Row, _] =>
        convert(lv.expr).map { case (sparkCol, ct) => (last_value(sparkCol), ct) }

      case as: Expr.ArraySize[Row, _] => convertUnary(as.expr, size, ColumnType.IntType)
      case ac: Expr.ArrayContains[Row, _] =>
        convertBinary(ac.expr, ac.value, array_contains(_, _), ColumnType.BooleanType)
      case ex: Expr.Explode[Row, _] => convertUnary(ex.expr, explode, ColumnType.AnyType)
      case asrt: Expr.ArraySort[Row, _] => convertUnary(asrt.expr, sort_array, ColumnType.AnyType)
      case ad: Expr.ArrayDistinct[Row, _] => convertUnary(ad.expr, array_distinct, ColumnType.AnyType)

      case au: Expr.ArrayUnion[Row, _] => convertBinary(au.left, au.right, array_union, ColumnType.AnyType)
      case ai: Expr.ArrayIntersect[Row, _] =>
        convertBinary(ai.left, ai.right, array_intersect, ColumnType.AnyType)
      case ae: Expr.ArrayExcept[Row, _] => convertBinary(ae.left, ae.right, array_except, ColumnType.AnyType)

      case fl: Expr.Flatten[Row, _] => convertUnary(fl.expr, flatten, ColumnType.AnyType)
      case ea: Expr.ElementAt[Row, _] => convertBinary(ea.expr, ea.index, element_at(_, _), ColumnType.AnyType)
      case as: Expr.ArraySlice[Row, _] => convertUnary(as.expr, slice(_, as.start, as.length), ColumnType.AnyType)

      case mk: Expr.MapKeys[Row, _, _] => convertUnary(mk.expr, map_keys, ColumnType.AnyType)
      case mv: Expr.MapValues[Row, _, _] => convertUnary(mv.expr, map_values, ColumnType.AnyType)
      case mck: Expr.MapContainsKey[Row, _, _] =>
        convertBinary(mck.expr, mck.key, (m, k) => array_contains(map_keys(m), k), ColumnType.BooleanType)
      case me: Expr.MapEntries[Row, _, _] => convertUnary(me.expr, map_entries, ColumnType.AnyType)

      case mfa: Expr.MapFromArrays[Row, _, _] =>
        convertBinary(mfa.keys, mfa.values, map_from_arrays, ColumnType.AnyType)
      case mc: Expr.MapConcat[Row, _, _] => convertBinary(mc.left, mc.right, map_concat(_, _), ColumnType.AnyType)

      case m: Expr.Md5[Row] => convertUnary(m.expr, md5, ColumnType.StringType)
      case s: Expr.Sha1[Row] => convertUnary(s.expr, sha1, ColumnType.StringType)
      case s2: Expr.Sha2[Row] => convertUnary(s2.expr, sha2(_, s2.bitLength), ColumnType.StringType)
      case ue: Expr.UrlEncode[Row] => convertUnary(ue.expr, url_encode, ColumnType.StringType)
      case ud: Expr.UrlDecode[Row] => convertUnary(ud.expr, url_decode, ColumnType.StringType)
      case b64e: Expr.Base64Encode[Row] => convertUnary(b64e.expr, base64, ColumnType.StringType)
      case b64d: Expr.Base64Decode[Row] =>
        convertUnary(b64d.expr, sc => unbase64(sc).cast("string"), ColumnType.StringType)
      case hx: Expr.Hex[Row] => convertUnary(hx.expr, hex, ColumnType.StringType)
      case gjo: Expr.GetJsonObject[Row] => convertUnary(gjo.expr, get_json_object(_, gjo.path), ColumnType.StringType)
    }
  }

  private def convertSeq[Row](
    exprs: Seq[Expr[Row, ?]],
    f: (Seq[SparkColumn], Seq[ColumnType]) => SparkColumn,
    defaultType: ColumnType
  ): Either[ExecutionError, (SparkColumn, ColumnType)] = {
    val results = exprs.map(convert)
    val firstError = results.collectFirst { case Left(err) => err }
    firstError match {
      case Some(err) => Left(err)
      case scala.None =>
        val cols = results.collect { case Right((c, _)) => c }
        val types = results.collect { case Right((_, t)) => t }
        val ct = if (defaultType == ColumnType.AnyType) types.headOption.getOrElse(ColumnType.AnyType) else defaultType
        Right((f(cols, types), ct))
    }
  }

  private def applyByN(valCol: SparkColumn, ordCol: SparkColumn, n: Int, asc: Boolean): SparkColumn = {
    val zipped = arrays_zip(collect_list(valCol).as("v"), collect_list(ordCol).as("k"))
    val sorted = sort_array(zipped, asc = asc)
    val sliced = slice(sorted, 1, n)
    transform(sliced, (x: SparkColumn) => x.getField("v"))
  }

  /** Convert a Scala value to a Spark Lit Column, handling None/null. */
  private def toLit(value: Any): SparkColumn = value match {
    case None | null => lit(null) // scalafix:ok DisableSyntax.null
    case Some(v) => lit(v)
    case v => lit(v)
  }
}
