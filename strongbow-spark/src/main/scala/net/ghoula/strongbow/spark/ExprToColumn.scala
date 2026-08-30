package net.ghoula.strongbow.spark

import org.apache.spark.sql.{Column => SparkColumn}
import org.apache.spark.sql.functions.{when => sparkWhen, *}

import net.ghoula.strongbow.column.ColumnType
import net.ghoula.strongbow.errors.ExecutionError
import net.ghoula.strongbow.expr.Expr

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

  /** Spark path for a literal json_tuple key. Spark's JsonPathParser has no escape mechanism: the
    * bracket form `$['k']` rejects single quotes, and the dot form `$.k` rejects dots and brackets
    * — a key containing both kinds of character is unaddressable.
    */
  private def jsonTuplePath(k: String): Either[ExecutionError, String] =
    if (!k.contains('\'') && !k.contains('[')) Right(s"$$['$k']")
    else if (!k.contains('.') && !k.contains('[')) Right(s"$$.$k")
    else
      Left(
        ExecutionError.UnsupportedOperation(
          s"json_tuple key cannot be addressed by a Spark get_json_object path: $k"
        )
      )

  /** Convert a Strongbow Expr to a Spark Column paired with its output ColumnType.
    *
    * Returns Left for unsupported expressions. `scope` carries the lambda-variable bindings of the
    * nearest enclosing higher-order expression; ordinary arms pass it through unchanged (implicit),
    * and higher-order arms extend it around body conversion.
    */
  def convert[Row, A](expr: Expr[Row, A])(implicit
    scope: LambdaColumnScope = LambdaColumnScope.empty
  ): Either[ExecutionError, (SparkColumn, ColumnType)] = {
    (expr: @unchecked) match {

      case cell: Expr.Cell[Row, _] =>
        Right((col(cell.name), ColumnType.AnyType))

      case lv: Expr.LambdaVar[Row, _] =>
        scope.get(lv.binder) match {
          case Some(sparkCol) => Right((sparkCol, ColumnType.AnyType))
          case None =>
            Left(ExecutionError.InvalidValue("Lambda variable used outside its binding expression"))
        }

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
      case crc: Expr.Crc32[Row] => convertUnary(crc.expr, crc32, ColumnType.LongType)
      case xx: Expr.XxHash64[Row] => convertUnary(xx.expr, (c: SparkColumn) => xxhash64(c), ColumnType.LongType)
      case hh: Expr.Hash[Row] => convertUnary(hh.expr, (c: SparkColumn) => hash(c), ColumnType.IntType)
      case ae: Expr.AesEncrypt[Row] => convertBinary(ae.expr, ae.key, aes_encrypt, ColumnType.BinaryType)
      case ad: Expr.AesDecrypt[Row] => convertBinary(ad.expr, ad.key, aes_decrypt, ColumnType.StringType)
      case tad: Expr.TryAesDecrypt[Row] => convertBinary(tad.expr, tad.key, try_aes_decrypt, ColumnType.StringType)
      case gjo: Expr.GetJsonObject[Row] => convertUnary(gjo.expr, get_json_object(_, gjo.path), ColumnType.StringType)
      case jt: Expr.JsonTuple[Row] =>
        val paths = jt.keys.map(jsonTuplePath)
        paths.collectFirst { case Left(err) => err } match {
          case Some(err) => Left(err)
          case None =>
            val ps = paths.collect { case Right(p) => p }
            convertUnary(
              jt.expr,
              jsonCol => array(ps.map(p => get_json_object(jsonCol, p))*),
              ColumnType.ArrayType(ColumnType.StringType)
            )
        }

      case st: Expr.Struct[Row, _] =>
        val fieldResults = st.fields.map { case (name, fieldExpr, _) =>
          convert(fieldExpr).map { case (sparkCol, _) => sparkCol.as(name) }
        }
        val firstError = fieldResults.collectFirst { case Left(err) => err }
        firstError match {
          case Some(err) => Left(err)
          case None =>
            val cols = fieldResults.collect { case Right(c) => c }
            Right(
              (
                struct(cols*),
                ColumnType.StructType(st.schema.columnNames.zip(st.schema.columnTypes))
              )
            )
        }

      case gf: Expr.GetField[Row, _, _] =>
        convert(gf.struct).map { case (sparkCol, _) => (sparkCol.getField(gf.fieldName), ColumnType.AnyType) }

      case pj: Expr.ParseJson[Row] =>
        convertUnary(pj.expr, parse_json, ColumnType.VariantType)

      case vg: Expr.VariantGet[Row] =>
        convertUnary(
          vg.expr,
          sparkCol => variant_get(sparkCol, vg.path, vg.targetType),
          ColumnType.VariantType
        )

      case tvg: Expr.TryVariantGet[Row] =>
        convertUnary(
          tvg.expr,
          sparkCol => try_variant_get(sparkCol, tvg.path, tvg.targetType),
          ColumnType.VariantType
        )

      case ivn: Expr.IsVariantNull[Row] =>
        convertUnary(ivn.expr, is_variant_null, ColumnType.BooleanType)

      case sov: Expr.SchemaOfVariant[Row] =>
        convertUnary(sov.expr, schema_of_variant, ColumnType.StringType)

      case ivv: Expr.IsValidVariant[Row] =>
        convertUnary(ivv.expr, sc => is_valid_variant(try_parse_json(sc)), ColumnType.BooleanType)

      case ve: Expr.VariantExplode[Row] =>
        convertUnary(ve.expr, call_function("variant_explode", _), ColumnType.VariantType)

      case gr: Expr.Greatest[Row, _] =>
        convertSeq(gr.exprs, (cols, _) => greatest(cols*), ColumnType.AnyType)

      case ls: Expr.Least[Row, _] =>
        convertSeq(ls.exprs, (cols, _) => least(cols*), ColumnType.AnyType)

      case ni: Expr.NullIf[Row, _] =>
        convert(ni.left).flatMap { case (l, lType) =>
          convert(ni.right).map { case (r, _) =>
            (sparkWhen(l === r, lit(null)).otherwise(l), lType) // scalafix:ok DisableSyntax.null
          }
        }

      case nv2: Expr.Nvl2[Row, _, _] =>
        for {
          (t, _) <- convert(nv2.test)
          (v, vType) <- convert(nv2.value)
          (a, _) <- convert(nv2.alt)
        } yield (nvl2(t, v, a), vType)

      case nv: Expr.Nanvl[Row] =>
        convertBinary(nv.left, nv.right, nanvl, ColumnType.DoubleType)

      case bc: Expr.BitCount[Row] => convertUnary(bc.expr, bit_count, ColumnType.IntType)
      case bg: Expr.BitGet[Row] =>
        convertBinary(bg.expr, bg.pos, bit_get, ColumnType.IntType)
      case sl: Expr.ShiftLeft[Row] =>
        convertBinary(sl.expr, sl.n, call_function("shiftleft", _, _), ColumnType.IntType)
      case sr: Expr.ShiftRight[Row] =>
        convertBinary(sr.expr, sr.n, call_function("shiftright", _, _), ColumnType.IntType)
      case sru: Expr.ShiftRightUnsigned[Row] =>
        convertBinary(sru.expr, sru.n, call_function("shiftrightunsigned", _, _), ColumnType.IntType)
      case bn: Expr.BitwiseNot[Row] => convertUnary(bn.expr, bitwise_not, ColumnType.IntType)
      case ba: Expr.BitAndAgg[Row] => convertUnary(ba.expr, bit_and, ColumnType.IntType)
      case bo: Expr.BitOrAgg[Row] => convertUnary(bo.expr, bit_or, ColumnType.IntType)
      case bx: Expr.BitXorAgg[Row] => convertUnary(bx.expr, bit_xor, ColumnType.IntType)

      case ic: Expr.Initcap[Row] => convertUnary(ic.expr, initcap, ColumnType.StringType)
      case ins: Expr.Instr[Row] =>
        convertBinary(ins.str, ins.substr, instr(_, _), ColumnType.IntType)
      case si: Expr.SubstringIndex[Row] =>
        convertUnary(si.str, substring_index(_, si.delim, si.count), ColumnType.StringType)
      case ls2: Expr.LeftStr[Row] =>
        convertBinary(ls2.str, ls2.n, left(_, _), ColumnType.StringType)
      case rs: Expr.RightStr[Row] =>
        convertBinary(rs.str, rs.n, right(_, _), ColumnType.StringType)
      case rp: Expr.Repeat[Row] =>
        convertBinary(rp.str, rp.n, repeat(_, _), ColumnType.StringType)
      case rv: Expr.Reverse[Row] => convertUnary(rv.str, reverse, ColumnType.StringType)
      case lp: Expr.Lpad[Row] =>
        convertBinary(lp.str, lp.len, (str, len) => lpad(str, len, lit(lp.pad)), ColumnType.StringType)
      case rp2: Expr.Rpad[Row] =>
        convertBinary(rp2.str, rp2.len, (str, len) => rpad(str, len, lit(rp2.pad)), ColumnType.StringType)
      case tr2: Expr.Translate[Row] =>
        convertUnary(tr2.str, translate(_, tr2.matching, tr2.replace), ColumnType.StringType)
      case fs: Expr.FormatString[Row] =>
        val argCols = fs.args.map(convert)
        val firstError = argCols.collectFirst { case Left(err) => err }
        firstError match {
          case Some(err) => Left(err)
          case None =>
            val cols = argCols.collect { case Right((c, _)) => c }
            Right((format_string(fs.format, cols*), ColumnType.StringType))
        }
      case asc: Expr.Ascii[Row] => convertUnary(asc.str, ascii, ColumnType.IntType)
      case ch: Expr.Chr[Row] => convertUnary(ch.expr, chr, ColumnType.StringType)
      case lv: Expr.Levenshtein[Row] =>
        convertBinary(lv.left, lv.right, levenshtein(_, _), ColumnType.IntType)
      case rl: Expr.Rlike[Row] =>
        convertUnary(rl.str, rlike(_, lit(rl.pattern)), ColumnType.BooleanType)
      case rea: Expr.RegexpExtractAll[Row] =>
        convertUnary(
          rea.str,
          regexp_extract_all(_, lit(rea.pattern), lit(rea.groupIdx)),
          ColumnType.ArrayType(ColumnType.StringType)
        )
      case sp2: Expr.SplitPart[Row] =>
        convertBinary(sp2.str, sp2.part, (str, part) => split_part(str, lit(sp2.delim), part), ColumnType.StringType)
      case pu: Expr.ParseUrl[Row] =>
        convertUnary(pu.url, parse_url(_, lit(pu.part)), ColumnType.StringType)
      case tc: Expr.NumberToChar[Row] =>
        convertUnary(tc.expr, to_char(_, lit(tc.format)), ColumnType.StringType)

      case cb: Expr.Cbrt[Row] => convertUnary(cb.expr, cbrt, ColumnType.DoubleType)
      case hy: Expr.Hypot[Row] => convertBinary(hy.left, hy.right, hypot(_, _), ColumnType.DoubleType)
      case bnn: Expr.Bin[Row] => convertUnary(bnn.expr, bin, ColumnType.StringType)
      case uh: Expr.Unhex[Row] =>
        convertUnary(uh.expr, unhex, ColumnType.BinaryType)
      case br: Expr.Bround[Row] =>
        convertUnary(br.expr, bround(_, br.scale), ColumnType.DoubleType)
      case cv: Expr.Conv[Row] =>
        convertUnary(cv.num, conv(_, cv.fromBase, cv.toBase), ColumnType.StringType)
      case fa: Expr.Factorial[Row] => convertUnary(fa.expr, factorial, ColumnType.LongType)
      case sn2: Expr.Sinh[Row] => convertUnary(sn2.expr, sinh, ColumnType.DoubleType)
      case cs2: Expr.Cosh[Row] => convertUnary(cs2.expr, cosh, ColumnType.DoubleType)
      case tn2: Expr.Tanh[Row] => convertUnary(tn2.expr, tanh, ColumnType.DoubleType)
      case as2: Expr.Asinh[Row] => convertUnary(as2.expr, asinh, ColumnType.DoubleType)
      case ac2: Expr.Acosh[Row] => convertUnary(ac2.expr, acosh, ColumnType.DoubleType)
      case at2: Expr.Atanh[Row] => convertUnary(at2.expr, atanh, ColumnType.DoubleType)
      case dg: Expr.Degrees[Row] => convertUnary(dg.expr, degrees, ColumnType.DoubleType)
      case rd2: Expr.Radians[Row] => convertUnary(rd2.expr, radians, ColumnType.DoubleType)
      case _: Expr.Pi[Row] => Right((pi(), ColumnType.DoubleType))
      case _: Expr.Euler[Row] => Right((e(), ColumnType.DoubleType))
      case wb: Expr.WidthBucket[Row] =>
        val quads = for {
          (v, _) <- convert(wb.value)
          (lo, _) <- convert(wb.min)
          (hi, _) <- convert(wb.max)
          (n, _) <- convert(wb.buckets)
        } yield call_function("width_bucket", v, lo, hi, n)
        quads.map((_, ColumnType.IntType))
      case rn2: Expr.Randn[Row] => Right((randn(rn2.seed), ColumnType.DoubleType))
      case pm: Expr.PmodInt[Row] =>
        convertBinary(pm.left, pm.right, pmod(_, _), ColumnType.IntType)
      case pml: Expr.PmodLong[Row] =>
        convertBinary(pml.left, pml.right, pmod(_, _), ColumnType.LongType)
      case l1p: Expr.Log1p[Row] => convertUnary(l1p.expr, log1p, ColumnType.DoubleType)
      case e1m: Expr.Expm1[Row] => convertUnary(e1m.expr, expm1, ColumnType.DoubleType)
      case tal: Expr.TryAddLong[Row] =>
        convertBinary(tal.left, tal.right, try_add(_, _), ColumnType.LongType)
      case tsl: Expr.TrySubtractLong[Row] =>
        convertBinary(tsl.left, tsl.right, try_subtract(_, _), ColumnType.LongType)
      case tml: Expr.TryMultiplyLong[Row] =>
        convertBinary(tml.left, tml.right, try_multiply(_, _), ColumnType.LongType)
      case tdl: Expr.TryDivideLong[Row] =>
        convertBinary(tdl.left, tdl.right, try_divide(_, _), ColumnType.DoubleType)
      case tdi: Expr.TryAddInt[Row] =>
        convertBinary(tdi.left, tdi.right, try_add(_, _), ColumnType.IntType)
      case tdd: Expr.TryDivideDouble[Row] =>
        convertBinary(tdd.left, tdd.right, try_divide(_, _), ColumnType.DoubleType)

      case ut: Expr.UnixTimestamp[Row] =>
        convertUnary(ut.expr, unix_timestamp, ColumnType.LongType)
      case fu: Expr.FromUnixtime[Row] =>
        convertUnary(fu.expr, from_unixtime, ColumnType.StringType)
      case tt: Expr.ToTimestamp[Row] =>
        convertUnary(tt.expr, to_timestamp, ColumnType.TimestampType)
      case td: Expr.ToDate[Row] =>
        convertUnary(td.expr, to_date, ColumnType.DateType)
      case _: Expr.CurrentDate[Row] => Right((current_date(), ColumnType.DateType))
      case _: Expr.Now[Row] => Right((now(), ColumnType.TimestampType))
      case tss: Expr.TimestampSeconds[Row] =>
        convertUnary(tss.expr, timestamp_seconds, ColumnType.TimestampType)
      case tsm: Expr.TimestampMillis[Row] =>
        convertUnary(tsm.expr, timestamp_millis, ColumnType.TimestampType)
      case tsmi: Expr.TimestampMicros[Row] =>
        convertUnary(tsmi.expr, timestamp_micros, ColumnType.TimestampType)
      case mt: Expr.MakeTimestamp[Row] =>
        for {
          (y, _) <- convert(mt.year)
          (mo, _) <- convert(mt.month)
          (d, _) <- convert(mt.day)
          (h, _) <- convert(mt.hour)
          (mi, _) <- convert(mt.minute)
          (sec, _) <- convert(mt.sec)
        } yield (make_timestamp(y, mo, d, h, mi, sec), ColumnType.TimestampType)
      case mdi: Expr.MakeDtInterval[Row] =>
        for {
          (d, _) <- convert(mdi.days)
          (h, _) <- convert(mdi.hours)
          (mi, _) <- convert(mdi.minutes)
          (sec, _) <- convert(mdi.seconds)
        } yield (make_dt_interval(d, h, mi, sec), ColumnType.DayTimeIntervalType)
      case myi: Expr.MakeYmInterval[Row] =>
        for {
          (y, _) <- convert(myi.years)
          (m, _) <- convert(myi.months)
        } yield (make_ym_interval(y, m), ColumnType.YearMonthIntervalType)
      case ho: Expr.HourOf[Row] => convertUnary(ho.expr, hour, ColumnType.IntType)
      case mo: Expr.MinuteOf[Row] => convertUnary(mo.expr, minute, ColumnType.IntType)
      case so: Expr.SecondOf[Row] => convertUnary(so.expr, second, ColumnType.IntType)
      case fut: Expr.FromUtcTimestamp[Row] =>
        convertUnary(fut.expr, from_utc_timestamp(_, fut.tz), ColumnType.TimestampType)
      case tut: Expr.ToUtcTimestamp[Row] =>
        convertUnary(tut.expr, to_utc_timestamp(_, tut.tz), ColumnType.TimestampType)
      case ta2: Expr.TimestampAdd[Row] =>
        convertBinary(
          ta2.qty,
          ta2.ts,
          (qty, ts) => timestampAdd(ts, ta2.unit, qty),
          ColumnType.TimestampType
        )
      case tdf: Expr.TimestampDiff[Row] =>
        val factor = timestampDiffFactor(tdf.unit)
        factor match {
          case Some(scale) =>
            convertBinary(
              tdf.start,
              tdf.end,
              (start, end) => (unix_micros(end) - unix_micros(start)).cast("double") / lit(scale.toDouble),
              ColumnType.LongType
            ).map { case (col, _) => (col.cast("long"), ColumnType.LongType) }
          case None =>
            Left(
              ExecutionError.UnsupportedOperation(
                s"timestampdiff unit ${tdf.unit} not supported; use MICROSECOND, MILLISECOND, SECOND, MINUTE, HOUR, DAY or WEEK"
              )
            )
        }
      case ctz: Expr.ConvertTimezone[Row] =>
        convertUnary(
          ctz.expr,
          convert_timezone(lit(ctz.fromTz), lit(ctz.toTz), _),
          ColumnType.TimestampType
        )
      case wk: Expr.Weekday[Row] => convertUnary(wk.expr, weekday, ColumnType.IntType)

      case ap: Expr.ArrayAppend[Row, _] =>
        convertBinary(ap.arr, ap.elem, array_append(_, _), ColumnType.AnyType)
      case ap2: Expr.ArrayPrepend[Row, _] =>
        convertBinary(ap2.arr, ap2.elem, array_prepend(_, _), ColumnType.AnyType)
      case ai2: Expr.ArrayInsert[Row, _] =>
        for {
          (arr, _) <- convert(ai2.arr)
          (pos, _) <- convert(ai2.pos)
          (elem, _) <- convert(ai2.elem)
        } yield (array_insert(arr, pos, elem), ColumnType.AnyType)
      case ar: Expr.ArrayRemove[Row, _] =>
        convertBinary(ar.arr, ar.elem, array_remove(_, _), ColumnType.AnyType)
      case arp: Expr.ArrayRepeat[Row, _] =>
        convertBinary(arp.elem, arp.count, array_repeat(_, _), ColumnType.AnyType)
      case aj: Expr.ArrayJoin[Row] =>
        convertUnary(
          aj.arr,
          sc =>
            aj.nullReplacement match {
              case Some(rep) => array_join(sc, aj.delimiter, rep)
              case None => array_join(sc, aj.delimiter)
            },
          ColumnType.StringType
        )
      case amx: Expr.ArrayMax[Row, _] => convertUnary(amx.arr, array_max, ColumnType.AnyType)
      case amn: Expr.ArrayMin[Row, _] => convertUnary(amn.arr, array_min, ColumnType.AnyType)
      case ac2: Expr.ArrayCompact[Row, _] =>
        convertUnary(ac2.arr, array_compact, ColumnType.AnyType)
      case apo: Expr.ArrayPosition[Row, _] =>
        convertBinary(apo.arr, apo.elem, array_position(_, _), ColumnType.IntType)
      case az: Expr.ArraysZip[Row] =>
        val zipCols = az.arrays.map(convert)
        val firstError = zipCols.collectFirst { case Left(err) => err }
        firstError match {
          case Some(err) => Left(err)
          case None =>
            val cols = zipCols.collect { case Right((c, _)) => c }
            Right((arrays_zip(cols*), ColumnType.AnyType))
        }
      case ao: Expr.ArraysOverlap[Row, _] =>
        convertBinary(ao.left, ao.right, arrays_overlap(_, _), ColumnType.BooleanType)
      case mfe: Expr.MapFromEntries[Row, _, _] =>
        convertUnary(mfe.arr, map_from_entries, ColumnType.AnyType)
      case ga: Expr.GetArray[Row, _] =>
        convertBinary(ga.arr, ga.index, call_function("get", _, _), ColumnType.AnyType)
      case px: Expr.Posexplode[Row] => convertUnary(px.expr, posexplode, ColumnType.AnyType)
      case eo: Expr.ExplodeOuter[Row] => convertUnary(eo.expr, explode_outer, ColumnType.AnyType)
      case in2: Expr.Inline[Row] => convertUnary(in2.expr, inline, ColumnType.AnyType)

      case rv: Expr.RegrAvgx[Row] => convertBinary(rv.y, rv.x, regr_avgx(_, _), ColumnType.DoubleType)
      case rv2: Expr.RegrAvgy[Row] => convertBinary(rv2.y, rv2.x, regr_avgy(_, _), ColumnType.DoubleType)
      case rc: Expr.RegrCount[Row] => convertBinary(rc.y, rc.x, regr_count(_, _), ColumnType.LongType)
      case ri: Expr.RegrIntercept[Row] =>
        convertBinary(ri.y, ri.x, regr_intercept(_, _), ColumnType.DoubleType)
      case rr: Expr.RegrR2[Row] => convertBinary(rr.y, rr.x, regr_r2(_, _), ColumnType.DoubleType)
      case rs: Expr.RegrSlope[Row] => convertBinary(rs.y, rs.x, regr_slope(_, _), ColumnType.DoubleType)
      case rsx: Expr.RegrSxx[Row] => convertBinary(rsx.y, rsx.x, regr_sxx(_, _), ColumnType.DoubleType)
      case rsy: Expr.RegrSxy[Row] => convertBinary(rsy.y, rsy.x, regr_sxy(_, _), ColumnType.DoubleType)
      case rsy2: Expr.RegrSyy[Row] => convertBinary(rsy2.y, rsy2.x, regr_syy(_, _), ColumnType.DoubleType)
      case kurt: Expr.Kurtosis[Row] => convertUnary(kurt.expr, kurtosis, ColumnType.DoubleType)
      case skw: Expr.Skewness[Row] => convertUnary(skw.expr, skewness, ColumnType.DoubleType)
      case pct: Expr.Percentile[Row] =>
        convertBinary(pct.expr, pct.percentage, percentile(_, _), ColumnType.DoubleType)
      case sdist: Expr.SumDistinct[Row] => convertUnary(sdist.expr, sum_distinct, ColumnType.LongType)
      case hist: Expr.HistogramNumeric[Row] =>
        convertBinary(hist.expr, hist.nBins, histogram_numeric(_, _), ColumnType.AnyType)
      case grp: Expr.Grouping[Row] => convertUnary(grp.expr, grouping, ColumnType.IntType)
      case gid: Expr.GroupingId[Row] =>
        val cols = gid.exprs.map(convert)
        val firstError = cols.collectFirst { case Left(err) => err }
        firstError match {
          case Some(err) => Left(err)
          case None =>
            val cs = cols.collect { case Right((c, _)) => c }
            Right((grouping_id(cs*), ColumnType.LongType))
        }

      case soj: Expr.SchemaOfJson[Row] =>
        val constantJson: Option[String] = soj.expr match {
          case c: Expr.Const[Row, ?] =>
            val v: Any | Null = c.value
            v match {
              case s: String => Some(s)
              case _ => None
            }
          case _ => None
        }
        constantJson match {
          case Some(s) => Right((schema_of_json(lit(s)), ColumnType.StringType))
          case None =>
            Left(
              ExecutionError.UnsupportedOperation(
                "schema_of_json requires a constant JSON string on Spark (foldable input)"
              )
            )
        }
      case jal: Expr.JsonArrayLength[Row] =>
        convertUnary(
          jal.expr,
          sc => json_array_length(get_json_object(sc, jal.path)),
          ColumnType.LongType
        )
      case jok: Expr.JsonObjectKeys[Row] =>
        convertUnary(
          jok.expr,
          sc => json_object_keys(get_json_object(sc, jok.path)),
          ColumnType.ArrayType(ColumnType.StringType)
        )

      case fj: Expr.FromJson[Row, _] =>
        convertUnary(fj.expr, from_json(_, lit(fj.schema)), ColumnType.AnyType)
      case tj: Expr.ToJson[Row, _] =>
        convertUnary(tj.expr, to_json, ColumnType.StringType)

      case ts: Expr.TimeToSeconds[Row] =>
        convertUnary(ts.expr, time_to_seconds, ColumnType.DecimalType(14, 6))
      case tm: Expr.TimeToMillis[Row] => convertUnary(tm.expr, time_to_millis, ColumnType.LongType)
      case tu: Expr.TimeToMicros[Row] => convertUnary(tu.expr, time_to_micros, ColumnType.LongType)

      case tfs: Expr.TimeFromSeconds[Row] =>
        convertUnary(tfs.expr, time_from_seconds, ColumnType.TimeType)
      case tfm: Expr.TimeFromMillis[Row] =>
        convertUnary(tfm.expr, time_from_millis, ColumnType.TimeType)
      case tfu: Expr.TimeFromMicros[Row] =>
        convertUnary(tfu.expr, time_from_micros, ColumnType.TimeType)

      case tb: Expr.TimeBucket[Row] =>
        for {
          bucket <- tb.bucketSize match {
            case c: Expr.Const[Row, _] =>
              c.value match {
                case l: Long => Right(lit(java.time.Duration.ofNanos(l * 1000L)))
                case _ => convert(tb.bucketSize).map(_._1)
              }
            case other => convert(other).map(_._1)
          }
          (tsCol, _) <- convert(tb.ts)
          origin <- tb.origin match {
            case c: Expr.Const[Row, _] =>
              c.value match {
                case l: Long =>
                  Right(
                    lit(
                      java.sql.Timestamp.valueOf(
                        java.time.LocalDateTime.ofInstant(
                          java.time.Instant.ofEpochSecond(l / 1000000L, (l % 1000000L) * 1000L),
                          java.time.ZoneOffset.UTC
                        )
                      )
                    )
                  )
                case _ => convert(tb.origin).map(_._1)
              }
            case other => convert(other).map(_._1)
          }
        } yield (time_bucket(bucket, tsCol, origin), ColumnType.TimestampType)

      case _: Expr.CurrentPath[Row] => Right((current_path(), ColumnType.StringType))

      case se: Expr.SketchEstimate[Row] =>
        convertUnary(se.sketch, call_function(se.fn, _), ColumnType.LongType)
      case ss: Expr.SketchSummary[Row] =>
        convertUnary(
          ss.sketch,
          sparkCol => ss.mode.fold(call_function(ss.fn, sparkCol))(m => call_function(ss.fn, sparkCol, lit(m))),
          ColumnType.DoubleType
        )
      case st: Expr.SketchTheta[Row] =>
        convertUnary(st.sketch, call_function(st.fn, _), ColumnType.DoubleType)
      case sb: Expr.SketchBinaryOp[Row] =>
        convertSketchBinaryOp(sb.fn, sb.left, sb.right, sb.lgNomEntries, sb.mode)
      case ta: Expr.TupleSketchAgg[Row] =>
        for {
          (key, _) <- convert(ta.key)
          (summary, _) <- convert(ta.summary)
        } yield {
          val withSize =
            ta.lgNomEntries.fold(call_function(ta.fn, key, summary))(n => call_function(ta.fn, key, summary, lit(n)))
          val result =
            ta.mode.fold(withSize)(m => call_function(ta.fn, key, summary, lit(ta.lgNomEntries.getOrElse(12)), lit(m)))
          (result, ColumnType.BinaryType)
        }
      case sa: Expr.SketchSetAgg[Row] =>
        convertUnary(
          sa.sketch,
          sparkCol =>
            (sa.lgNomEntries, sa.mode) match {
              case (Some(n), Some(m)) => call_function(sa.fn, sparkCol, lit(n), lit(m))
              case (Some(n), None) => call_function(sa.fn, sparkCol, lit(n))
              case (None, Some(m)) => call_function(sa.fn, sparkCol, lit(m))
              case (None, None) => call_function(sa.fn, sparkCol)
            },
          ColumnType.BinaryType
        )
      case ka: Expr.KllSketchAgg[Row] =>
        convertUnary(
          ka.value,
          sparkCol => ka.k.fold(call_function(ka.fn, sparkCol))(k => call_function(ka.fn, sparkCol, lit(k))),
          ColumnType.BinaryType
        )
      case kq: Expr.KllQuantile[Row] =>
        for {
          (sketch, _) <- convert(kq.sketch)
          (rank, _) <- convert(kq.rank)
        } yield (call_function(kq.fn, sketch, rank), ColumnType.AnyType)
      case kr: Expr.KllRank[Row] =>
        for {
          (sketch, _) <- convert(kr.sketch)
          (quantile, _) <- convert(kr.quantile)
        } yield (call_function(kr.fn, sketch, quantile), ColumnType.DoubleType)
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

  private def convertSketchBinaryOp[Row](
    fn: String,
    left: Expr[Row, net.ghoula.strongbow.types.Binary],
    right: Expr[Row, net.ghoula.strongbow.types.Binary],
    lgNomEntries: Option[Int],
    mode: Option[String]
  ): Either[ExecutionError, (SparkColumn, ColumnType)] =
    for {
      (l, _) <- convert(left)
      (r, _) <- convert(right)
    } yield {
      val base = (lgNomEntries, mode) match {
        case (None, None) => call_function(fn, l, r)
        case (Some(n), None) => call_function(fn, l, r, lit(n))
        case (Some(n), Some(m)) => call_function(fn, l, r, lit(n), lit(m))
        case (None, Some(m)) =>
          if (fn.contains("intersection")) call_function(fn, l, r, lit(m))
          else call_function(fn, l, r, lit(12), lit(m))
      }
      (base, ColumnType.BinaryType)
    }

  /** timestampadd via public APIs: month-family through add_months, the rest through day-time
    * intervals.
    */
  private def timestampAdd(ts: SparkColumn, unit: String, qty: SparkColumn): SparkColumn =
    unit.toUpperCase.nn match {
      case "MONTH" | "MONTHS" => add_months(ts, qty)
      case "YEAR" | "YEARS" => add_months(ts, qty * lit(12))
      case "SECOND" | "SECONDS" => ts + make_interval(lit(0), lit(0), lit(0), lit(0), lit(0), lit(0), qty)
      case "MILLISECOND" | "MILLISECONDS" =>
        ts + make_interval(lit(0), lit(0), lit(0), lit(0), lit(0), lit(0), qty / lit(1000.0))
      case "MINUTE" | "MINUTES" => ts + make_interval(lit(0), lit(0), lit(0), lit(0), lit(0), qty, lit(0))
      case "HOUR" | "HOURS" => ts + make_interval(lit(0), lit(0), lit(0), lit(0), qty, lit(0), lit(0))
      case "DAY" | "DAYS" => ts + make_interval(lit(0), lit(0), lit(0), qty, lit(0), lit(0), lit(0))
      case "WEEK" | "WEEKS" =>
        ts + make_interval(lit(0), lit(0), lit(0), qty * lit(7), lit(0), lit(0), lit(0))
      case other => sys.error(s"Unsupported timestampadd unit: $other")
    }

  private def timestampDiffFactor(unit: String): Option[Long] =
    unit.toUpperCase.nn match {
      case "MICROSECOND" | "MICROSECONDS" => Some(1L)
      case "MILLISECOND" | "MILLISECONDS" => Some(1000L)
      case "SECOND" | "SECONDS" => Some(1000000L)
      case "MINUTE" | "MINUTES" => Some(60000000L)
      case "HOUR" | "HOURS" => Some(3600000000L)
      case "DAY" | "DAYS" => Some(86400000000L)
      case "WEEK" | "WEEKS" => Some(604800000000L)
      case _ => None
    }

  private def applyByN(valCol: SparkColumn, ordCol: SparkColumn, n: Int, asc: Boolean): SparkColumn = {
    val zipped = arrays_zip(collect_list(valCol).as("v"), collect_list(ordCol).as("k"))
    val sorted = sort_array(zipped, asc = asc)
    val sliced = slice(sorted, 1, n)
    transform(sliced, (x: SparkColumn) => x.getField("v"))
  }

  /** Convert a Scala value to a Spark Lit Column, handling None/null, sequences and pairs. */
  private def toLit(value: Any): SparkColumn = value match {
    case None | null => lit(null) // scalafix:ok DisableSyntax.null
    case Some(v) => toLit(v)
    case s: scala.collection.Seq[?] => array(s.map(toLit).toIndexedSeq*)
    case p: Tuple2[?, ?] => struct(toLit(p._1), toLit(p._2))
    case v => lit(v)
  }
}
