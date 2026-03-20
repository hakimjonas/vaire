package net.ghoula.strongbow

import net.ghoula.sarati.ast.json.JsonValue
import parsers.json.{formatJson, parseJson}

import scala.collection.immutable.BitSet

import net.ghoula.strongbow.errors.ExecutionError
import net.ghoula.strongbow.types.{Date, RowIndex}

/** Zero-cast expression interpreter using typed columnar storage.
  *
  * Key architectural principle: ONE cast at the Cell evaluation boundary (where we access typed
  * arrays), then fully typed expression evaluation using GADT evidence.
  *
  * Unlike TypedDataset which casts everywhere (due to Spark's untyped Column), we exploit our typed
  * array storage (IntColumn, StringColumn, etc.) to minimize casts.
  */
object ExprInterpreter {

  /** Evaluate an expression for a specific row.
    *
    * GADT pattern matching refines types automatically. The only cast is at Cell evaluation
    * boundary where we transition from typed column storage to generic type A.
    */
  def eval[Row, A](
    expr: Expr[Row, A],
    columns: Vector[Column[?]],
    rowIdx: RowIndex
  ): Either[ExecutionError, A] = {
    (expr: @unchecked) match {
      case Expr.Const(value) =>
        Right(value)

      case named: Expr.Named[Row, _] =>
        eval(named.expr, columns, rowIdx)

      case cell: Expr.Cell[Row, a] =>
        if (cell.index.toInt >= columns.length) {
          Left(ExecutionError.IndexOutOfBounds(cell.index.toInt, columns.length))
        } else {
          val column = columns(cell.index.toInt)
          val idx = rowIdx.toInt

          if (idx >= column.length) {
            Left(ExecutionError.IndexOutOfBounds(idx, column.length))
          } else {
            val value: a = column.columnType match {
              case ColumnType.IntType => column.getInt(idx).asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
              case ColumnType.LongType => column.getLong(idx).asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
              case ColumnType.DoubleType =>
                column.getDouble(idx).asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
              case ColumnType.StringType =>
                column.getString(idx).asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
              case ColumnType.BooleanType =>
                column.getBoolean(idx).asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
              case ColumnType.DateType =>
                Date
                  .ofEpochDay(column.getDateEpochDay(idx).toLong)
                  .asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
              case ColumnType.AnyType | ColumnType.OptionType(_) | ColumnType.ArrayType(_) | ColumnType.MapType(_, _) =>
                column.getValue(idx).asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
            }
            Right(value)
          }
        }

      case add: Expr.Add[Row] =>
        for {
          l <- eval(add.left, columns, rowIdx)
          r <- eval(add.right, columns, rowIdx)
        } yield l + r

      case sub: Expr.Sub[Row] =>
        for {
          l <- eval(sub.left, columns, rowIdx)
          r <- eval(sub.right, columns, rowIdx)
        } yield l - r

      case mul: Expr.Mul[Row] =>
        for {
          l <- eval(mul.left, columns, rowIdx)
          r <- eval(mul.right, columns, rowIdx)
        } yield l * r

      case div: Expr.Div[Row] =>
        for {
          l <- eval(div.left, columns, rowIdx)
          r <- eval(div.right, columns, rowIdx)
          result <-
            if (r == 0) Left(ExecutionError.DivisionByZero(rowIdx.toInt))
            else Right(l / r)
        } yield result

      case add: Expr.AddLong[Row] =>
        for {
          l <- eval(add.left, columns, rowIdx)
          r <- eval(add.right, columns, rowIdx)
        } yield l + r

      case sub: Expr.SubLong[Row] =>
        for {
          l <- eval(sub.left, columns, rowIdx)
          r <- eval(sub.right, columns, rowIdx)
        } yield l - r

      case mul: Expr.MulLong[Row] =>
        for {
          l <- eval(mul.left, columns, rowIdx)
          r <- eval(mul.right, columns, rowIdx)
        } yield l * r

      case div: Expr.DivLong[Row] =>
        for {
          l <- eval(div.left, columns, rowIdx)
          r <- eval(div.right, columns, rowIdx)
          result <-
            if (r == 0L) Left(ExecutionError.DivisionByZero(rowIdx.toInt))
            else Right(l / r)
        } yield result

      case add: Expr.AddDouble[Row] =>
        for {
          l <- eval(add.left, columns, rowIdx)
          r <- eval(add.right, columns, rowIdx)
        } yield l + r

      case sub: Expr.SubDouble[Row] =>
        for {
          l <- eval(sub.left, columns, rowIdx)
          r <- eval(sub.right, columns, rowIdx)
        } yield l - r

      case mul: Expr.MulDouble[Row] =>
        for {
          l <- eval(mul.left, columns, rowIdx)
          r <- eval(mul.right, columns, rowIdx)
        } yield l * r

      case div: Expr.DivDouble[Row] =>
        for {
          l <- eval(div.left, columns, rowIdx)
          r <- eval(div.right, columns, rowIdx)
          result <-
            if (r == 0.0) Left(ExecutionError.DivisionByZero(rowIdx.toInt))
            else Right(l / r)
        } yield result

      case eq: Expr.Eq[Row, _] =>
        for {
          l <- eval(eq.left, columns, rowIdx)
          r <- eval(eq.right, columns, rowIdx)
        } yield java.util.Objects.equals(l, r)

      case gt: Expr.Gt[Row, _] =>
        for {
          l <- eval(gt.left, columns, rowIdx)
          r <- eval(gt.right, columns, rowIdx)
        } yield gt.ordering.gt(l, r)

      case lt: Expr.Lt[Row, _] =>
        for {
          l <- eval(lt.left, columns, rowIdx)
          r <- eval(lt.right, columns, rowIdx)
        } yield lt.ordering.lt(l, r)

      case gte: Expr.Gte[Row, _] =>
        for {
          l <- eval(gte.left, columns, rowIdx)
          r <- eval(gte.right, columns, rowIdx)
        } yield gte.ordering.gteq(l, r)

      case lte: Expr.Lte[Row, _] =>
        for {
          l <- eval(lte.left, columns, rowIdx)
          r <- eval(lte.right, columns, rowIdx)
        } yield lte.ordering.lteq(l, r)

      case neq: Expr.Neq[Row, _] =>
        for {
          l <- eval(neq.left, columns, rowIdx)
          r <- eval(neq.right, columns, rowIdx)
        } yield !java.util.Objects.equals(l, r)

      case when: Expr.When[Row, _] =>
        eval(when.condition, columns, rowIdx).flatMap { cond =>
          if (cond) eval(when.thenExpr, columns, rowIdx)
          else eval(when.elseExpr, columns, rowIdx)
        }

      case and: Expr.And[Row] =>
        for {
          l <- eval(and.left, columns, rowIdx)
          r <- eval(and.right, columns, rowIdx)
        } yield l && r

      case or: Expr.Or[Row] =>
        for {
          l <- eval(or.left, columns, rowIdx)
          r <- eval(or.right, columns, rowIdx)
        } yield l || r

      case not: Expr.Not[Row] =>
        for {
          v <- eval(not.expr, columns, rowIdx)
        } yield !v

      case concat: Expr.Concat[Row] =>
        for {
          l <- eval(concat.left, columns, rowIdx)
          r <- eval(concat.right, columns, rowIdx)
        } yield l + r

      case length: Expr.Length[Row] =>
        for {
          v <- eval(length.expr, columns, rowIdx)
        } yield v.length

      case isDefined: Expr.IsDefined[Row, _] =>
        eval(isDefined.expr, columns, rowIdx) match {
          case Right(Some(_)) => Right(true)
          case Right(None) => Right(false)
          case Left(err) => Left(err)
        }

      case getOrElse: Expr.GetOrElse[Row, _] =>
        eval(getOrElse.expr, columns, rowIdx) match {
          case Right(Some(value)) => Right(value)
          case Right(None) => Right(getOrElse.default)
          case Left(err) => Left(err)
        }

      case like: Expr.Like[Row] =>
        for {
          v <- eval(like.expr, columns, rowIdx)
        } yield likeToRegex(like.pattern).matches(v)

      case lo: Expr.Lower[Row] =>
        eval(lo.expr, columns, rowIdx).map(_.toLowerCase)

      case up: Expr.Upper[Row] =>
        eval(up.expr, columns, rowIdx).map(_.toUpperCase)

      case tr: Expr.Trim[Row] =>
        eval(tr.expr, columns, rowIdx).map(_.trim)

      case lt: Expr.LTrim[Row] =>
        eval(lt.expr, columns, rowIdx).map(_.stripLeading.nn)

      case rt: Expr.RTrim[Row] =>
        eval(rt.expr, columns, rowIdx).map(_.stripTrailing.nn)

      case ss: Expr.Substring[Row] =>
        eval(ss.expr, columns, rowIdx).map { s =>
          val start = Math.max(ss.pos - 1, 0)
          val end = Math.min(start + ss.len, s.length)
          if (start >= s.length) "" else s.substring(start, end)
        }

      case sr: Expr.StringReplace[Row] =>
        eval(sr.expr, columns, rowIdx).map(_.replace(sr.search, sr.replacement))

      case rr: Expr.RegexpReplace[Row] =>
        eval(rr.expr, columns, rowIdx).map(_.replaceAll(rr.pattern, rr.replacement))

      case re: Expr.RegexpExtract[Row] =>
        eval(re.expr, columns, rowIdx).map { s =>
          val m = java.util.regex.Pattern.compile(re.pattern).matcher(s)
          if (m.find()) m.group(re.groupIdx) else ""
        }

      case sp: Expr.StringSplit[Row] =>
        eval(sp.expr, columns, rowIdx).map(s => s.split(sp.delimiter, -1).toSeq)

      case sw: Expr.StartsWith[Row] =>
        for {
          v <- eval(sw.expr, columns, rowIdx)
          p <- eval(sw.prefix, columns, rowIdx)
        } yield v.startsWith(p)

      case ew: Expr.EndsWith[Row] =>
        for {
          v <- eval(ew.expr, columns, rowIdx)
          s <- eval(ew.suffix, columns, rowIdx)
        } yield v.endsWith(s)

      case sc: Expr.StringContains[Row] =>
        for {
          v <- eval(sc.expr, columns, rowIdx)
          s <- eval(sc.substr, columns, rowIdx)
        } yield v.contains(s)

      case cw: Expr.ConcatWs[Row] =>
        cw.exprs
          .foldLeft[Either[ExecutionError, Vector[String]]](Right(Vector.empty)) {
            case (Right(acc), e) => eval(e, columns, rowIdx).map(acc :+ _)
            case (err, _) => err
          }
          .map(_.mkString(cw.separator))

      case co: Expr.Coalesce[Row, _] =>
        co.exprs
          .foldLeft[Either[ExecutionError, Option[A]]](Right(None)) {
            case (Right(None), e) =>
              eval(e, columns, rowIdx).map(v => Option(v))
            case (found, _) => found
          }
          .flatMap {
            case Some(v) => Right(v)
            case None => Left(ExecutionError.UnsupportedOperation("Coalesce: all expressions were null"))
          }

      case in: Expr.IsNull[Row, _] =>
        eval(in.expr, columns, rowIdx).map(v => Option(v).isEmpty)

      case inn: Expr.IsNotNull[Row, _] =>
        eval(inn.expr, columns, rowIdx).map(v => Option(v).isDefined)

      case inV: Expr.In[Row, _] =>
        eval(inV.expr, columns, rowIdx).map(v => inV.values.contains(v))

      case btw: Expr.Between[Row, _] =>
        for {
          v <- eval(btw.expr, columns, rowIdx)
          lo <- eval(btw.lower, columns, rowIdx)
          hi <- eval(btw.upper, columns, rowIdx)
        } yield btw.ordering.gteq(v, lo) && btw.ordering.lteq(v, hi)

      case m: Expr.Mod[Row] =>
        for {
          l <- eval(m.left, columns, rowIdx)
          r <- eval(m.right, columns, rowIdx)
          result <-
            if (r == 0) Left(ExecutionError.DivisionByZero(rowIdx.toInt))
            else Right(l % r)
        } yield result

      case ml: Expr.ModLong[Row] =>
        for {
          l <- eval(ml.left, columns, rowIdx)
          r <- eval(ml.right, columns, rowIdx)
          result <-
            if (r == 0L) Left(ExecutionError.DivisionByZero(rowIdx.toInt))
            else Right(l % r)
        } yield result

      case ab: Expr.Abs[Row] =>
        eval(ab.expr, columns, rowIdx).map(v => Math.abs(v))

      case abl: Expr.AbsLong[Row] =>
        eval(abl.expr, columns, rowIdx).map(v => Math.abs(v))

      case abd: Expr.AbsDouble[Row] =>
        eval(abd.expr, columns, rowIdx).map(v => Math.abs(v))

      case neg: Expr.Negate[Row] =>
        eval(neg.expr, columns, rowIdx).map(v => -v)

      case negl: Expr.NegateLong[Row] =>
        eval(negl.expr, columns, rowIdx).map(v => -v)

      case negd: Expr.NegateDouble[Row] =>
        eval(negd.expr, columns, rowIdx).map(v => -v)

      case rnd: Expr.Round[Row] =>
        eval(rnd.expr, columns, rowIdx).map { v =>
          val bd = BigDecimal(v).setScale(rnd.scale, BigDecimal.RoundingMode.HALF_UP)
          bd.toDouble
        }

      case fl: Expr.Floor[Row] =>
        eval(fl.expr, columns, rowIdx).map(v => Math.floor(v))

      case cl: Expr.Ceil[Row] =>
        eval(cl.expr, columns, rowIdx).map(v => Math.ceil(v))

      case ctl: Expr.CastToLong[Row] =>
        eval(ctl.expr, columns, rowIdx).map(_.toLong)

      case ctd: Expr.CastToDouble[Row] =>
        eval(ctd.expr, columns, rowIdx).map(_.toDouble)

      case cltd: Expr.CastLongToDouble[Row] =>
        eval(cltd.expr, columns, rowIdx).map(_.toDouble)

      case cts: Expr.CastToString[Row, _] =>
        eval(cts.expr, columns, rowIdx).map(v => String.valueOf(v))

      case opt2iter: Expr.Option2Iterable[Row, _] =>
        eval(opt2iter.expr, columns, rowIdx).map(_.toList)

      case sq: Expr.Sqrt[Row] =>
        eval(sq.expr, columns, rowIdx).map(v => Math.sqrt(v))

      case pw: Expr.Pow[Row] =>
        for {
          b <- eval(pw.base, columns, rowIdx)
          e <- eval(pw.exponent, columns, rowIdx)
        } yield Math.pow(b, e)

      case lg: Expr.Log[Row] =>
        eval(lg.expr, columns, rowIdx).map(v => Math.log(v))

      case lg10: Expr.Log10[Row] =>
        eval(lg10.expr, columns, rowIdx).map(v => Math.log10(v))

      case lg2: Expr.Log2[Row] =>
        eval(lg2.expr, columns, rowIdx).map(v => Math.log(v) / Math.log(2.0))

      case ex: Expr.Exp[Row] =>
        eval(ex.expr, columns, rowIdx).map(v => Math.exp(v))

      case sn: Expr.Sin[Row] =>
        eval(sn.expr, columns, rowIdx).map(v => Math.sin(v))

      case cs: Expr.Cos[Row] =>
        eval(cs.expr, columns, rowIdx).map(v => Math.cos(v))

      case tn: Expr.Tan[Row] =>
        eval(tn.expr, columns, rowIdx).map(v => Math.tan(v))

      case asn: Expr.Asin[Row] =>
        eval(asn.expr, columns, rowIdx).map(v => Math.asin(v))

      case acs: Expr.Acos[Row] =>
        eval(acs.expr, columns, rowIdx).map(v => Math.acos(v))

      case atn: Expr.Atan[Row] =>
        eval(atn.expr, columns, rowIdx).map(v => Math.atan(v))

      case atn2: Expr.Atan2[Row] =>
        for {
          y <- eval(atn2.y, columns, rowIdx)
          x <- eval(atn2.x, columns, rowIdx)
        } yield Math.atan2(y, x)

      case sg: Expr.Signum[Row] =>
        eval(sg.expr, columns, rowIdx).map(v => Math.signum(v))

      case rnd: Expr.Rand[Row] =>
        Right(new java.util.Random(rnd.seed).nextDouble())

      case _: Expr.Sum[Row] | _: Expr.SumDouble[Row] | _: Expr.SumLong[Row] | _: Expr.Count[Row] | _: Expr.Max[Row, ?] |
          _: Expr.Min[Row, ?] | _: Expr.Avg[Row] | _: Expr.CountDistinct[Row, ?] | _: Expr.CountIf[Row] |
          _: Expr.StdDev[Row] | _: Expr.StdDevPop[Row] | _: Expr.First[Row, ?] | _: Expr.Collect[Row, ?] |
          _: Expr.PercentileApprox[Row] | _: Expr.MaxBy[Row, ?, ?] | _: Expr.MinBy[Row, ?, ?] | _: Expr.MaxN[Row, ?] |
          _: Expr.MinN[Row, ?] | _: Expr.MaxByN[Row, ?, ?] | _: Expr.MinByN[Row, ?, ?] | _: Expr.Variance[Row] |
          _: Expr.VariancePop[Row] | _: Expr.ApproxCountDistinct[Row, ?] | _: Expr.CollectSet[Row, ?] |
          _: Expr.ExprLast[Row, ?] | _: Expr.AnyValue[Row, ?] | _: Expr.BoolAnd[Row] | _: Expr.BoolOr[Row] |
          _: Expr.Corr[Row] | _: Expr.CovarSamp[Row] | _: Expr.CovarPop[Row] | _: Expr.Median[Row] |
          _: Expr.Mode[Row, ?] =>
        Left(ExecutionError.UnsupportedOperation("Aggregations not supported in row-level eval"))

      case dad: Expr.DateAddDays[Row] =>
        for {
          d <- eval(dad.date, columns, rowIdx)
          n <- eval(dad.days, columns, rowIdx)
        } yield d.plusDays(n.toLong)

      case dsd: Expr.DateSubDays[Row] =>
        for {
          d <- eval(dsd.date, columns, rowIdx)
          n <- eval(dsd.days, columns, rowIdx)
        } yield d.minusDays(n.toLong)

      case dam: Expr.DateAddMonths[Row] =>
        for {
          d <- eval(dam.date, columns, rowIdx)
          n <- eval(dam.months, columns, rowIdx)
        } yield d.plusMonths(n.toLong)

      case dd: Expr.DateDiff[Row] =>
        for {
          l <- eval(dd.left, columns, rowIdx)
          r <- eval(dd.right, columns, rowIdx)
        } yield java.time.temporal.ChronoUnit.DAYS.between(r.toLocalDate, l.toLocalDate).toInt

      case ey: Expr.ExtractYear[Row] =>
        eval(ey.date, columns, rowIdx).map(_.getYear)

      case em: Expr.ExtractMonth[Row] =>
        eval(em.date, columns, rowIdx).map(_.getMonthValue)

      case ed: Expr.ExtractDay[Row] =>
        eval(ed.date, columns, rowIdx).map(_.getDayOfMonth)

      case dow: Expr.DayOfWeek[Row] =>
        eval(dow.date, columns, rowIdx).map(d => d.getDayOfWeek.getValue % 7 + 1)

      case doy: Expr.DayOfYear[Row] =>
        eval(doy.date, columns, rowIdx).map(_.getDayOfYear)

      case woy: Expr.WeekOfYear[Row] =>
        eval(woy.date, columns, rowIdx).map { d =>
          d.toLocalDate.get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR)
        }

      case q: Expr.Quarter[Row] =>
        eval(q.date, columns, rowIdx).map(d => (d.getMonthValue - 1) / 3 + 1)

      case ld: Expr.LastDay[Row] =>
        eval(ld.date, columns, rowIdx).map(d => Date.fromLocalDate(d.toLocalDate.withDayOfMonth(d.lengthOfMonth)))

      case nd: Expr.NextDay[Row] =>
        eval(nd.date, columns, rowIdx).map { d =>
          val target = java.time.DayOfWeek.valueOf(nd.dayOfWeek.toUpperCase.nn)
          Date.fromLocalDate(d.toLocalDate.`with`(java.time.temporal.TemporalAdjusters.next(target)))
        }

      case mb: Expr.MonthsBetween[Row] =>
        for {
          e <- eval(mb.end, columns, rowIdx)
          s <- eval(mb.start, columns, rowIdx)
        } yield {
          val period = java.time.Period.between(s.toLocalDate, e.toLocalDate)
          period.toTotalMonths.toDouble + period.getDays.toDouble / 31.0
        }

      case dt: Expr.DateTrunc[Row] =>
        eval(dt.date, columns, rowIdx).map { d =>
          val ld = d.toLocalDate
          Date.fromLocalDate(dt.unit.toUpperCase.nn match {
            case "YEAR" => ld.withDayOfYear(1)
            case "MONTH" => ld.withDayOfMonth(1)
            case "WEEK" => ld.`with`(java.time.temporal.TemporalAdjusters.previousOrSame(java.time.DayOfWeek.MONDAY))
            case "QUARTER" =>
              val qMonth = (ld.getMonthValue - 1) / 3 * 3 + 1
              java.time.LocalDate.of(ld.getYear, qMonth, 1)
            case _ => ld
          })
        }

      case df: Expr.DateFormat[Row] =>
        eval(df.date, columns, rowIdx).map { d =>
          d.toLocalDate.format(java.time.format.DateTimeFormatter.ofPattern(df.format))
        }

      case md: Expr.MakeDate[Row] =>
        for {
          y <- eval(md.year, columns, rowIdx)
          m <- eval(md.month, columns, rowIdx)
          d <- eval(md.day, columns, rowIdx)
        } yield Date(y, m, d)

      case as: Expr.ArraySize[Row, _] =>
        eval(as.expr, columns, rowIdx).map(_.size)

      case ac: Expr.ArrayContains[Row, _] =>
        for {
          arr <- eval(ac.expr, columns, rowIdx)
          v <- eval(ac.value, columns, rowIdx)
        } yield arr.contains(v)

      case _: Expr.Explode[Row, _] =>
        Left(ExecutionError.UnsupportedOperation("Explode requires Dataset-level handling"))

      case asrt: Expr.ArraySort[Row, _] =>
        eval(asrt.expr, columns, rowIdx).map(_.sorted(using asrt.ordering))

      case ad: Expr.ArrayDistinct[Row, _] =>
        eval(ad.expr, columns, rowIdx).map(_.distinct)

      case au: Expr.ArrayUnion[Row, _] =>
        for {
          l <- eval(au.left, columns, rowIdx)
          r <- eval(au.right, columns, rowIdx)
        } yield (l ++ r).distinct

      case ai: Expr.ArrayIntersect[Row, _] =>
        for {
          l <- eval(ai.left, columns, rowIdx)
          r <- eval(ai.right, columns, rowIdx)
        } yield l.intersect(r)

      case ae: Expr.ArrayExcept[Row, _] =>
        for {
          l <- eval(ae.left, columns, rowIdx)
          r <- eval(ae.right, columns, rowIdx)
        } yield l.diff(r)

      case fl: Expr.Flatten[Row, _] =>
        eval(fl.expr, columns, rowIdx).map(_.flatten)

      case ea: Expr.ElementAt[Row, _] =>
        for {
          arr <- eval(ea.expr, columns, rowIdx)
          idx <- eval(ea.index, columns, rowIdx)
          resolved = if (idx > 0) idx - 1 else arr.size + idx
          result <-
            if (resolved >= 0 && resolved < arr.size) Right(arr(resolved))
            else Left(ExecutionError.IndexOutOfBounds(resolved, arr.size))
        } yield result

      case as: Expr.ArraySlice[Row, _] =>
        eval(as.expr, columns, rowIdx).map { arr =>
          val start = Math.max(as.start - 1, 0)
          arr.slice(start, start + as.length)
        }

      case mk: Expr.MapKeys[Row, _, _] =>
        eval(mk.expr, columns, rowIdx).map(_.keys.toSeq)

      case mv: Expr.MapValues[Row, _, _] =>
        eval(mv.expr, columns, rowIdx).map(_.values.toSeq)

      case mck: Expr.MapContainsKey[Row, _, _] =>
        for {
          m <- eval(mck.expr, columns, rowIdx)
          k <- eval(mck.key, columns, rowIdx)
        } yield m.contains(k)

      case me: Expr.MapEntries[Row, _, _] =>
        eval(me.expr, columns, rowIdx).map(_.toSeq)

      case mfa: Expr.MapFromArrays[Row, _, _] =>
        for {
          ks <- eval(mfa.keys, columns, rowIdx)
          vs <- eval(mfa.values, columns, rowIdx)
          result <-
            if (ks.size == vs.size) Right(ks.zip(vs).toMap)
            else Left(ExecutionError.InvalidValue(s"MapFromArrays: keys length ${ks.size} != values length ${vs.size}"))
        } yield result

      case mc: Expr.MapConcat[Row, _, _] =>
        for {
          l <- eval(mc.left, columns, rowIdx)
          r <- eval(mc.right, columns, rowIdx)
        } yield l ++ r

      case md: Expr.Md5[Row] =>
        eval(md.expr, columns, rowIdx).map(s =>
          hexEncode(java.security.MessageDigest.getInstance("MD5").nn.digest(s.getBytes("UTF-8")).nn)
        )

      case sh: Expr.Sha1[Row] =>
        eval(sh.expr, columns, rowIdx).map(s =>
          hexEncode(java.security.MessageDigest.getInstance("SHA-1").nn.digest(s.getBytes("UTF-8")).nn)
        )

      case sh2: Expr.Sha2[Row] =>
        eval(sh2.expr, columns, rowIdx).map { s =>
          val algo = sha2Algorithm(sh2.bitLength)
          hexEncode(java.security.MessageDigest.getInstance(algo).nn.digest(s.getBytes("UTF-8")).nn)
        }

      case ue: Expr.UrlEncode[Row] =>
        eval(ue.expr, columns, rowIdx).map(s => java.net.URLEncoder.encode(s, "UTF-8").nn)

      case ud: Expr.UrlDecode[Row] =>
        eval(ud.expr, columns, rowIdx).map(s => java.net.URLDecoder.decode(s, "UTF-8").nn)

      case b64e: Expr.Base64Encode[Row] =>
        eval(b64e.expr, columns, rowIdx).map(s => java.util.Base64.getEncoder.nn.encodeToString(s.getBytes("UTF-8")).nn)

      case b64d: Expr.Base64Decode[Row] =>
        eval(b64d.expr, columns, rowIdx).map(s => new String(java.util.Base64.getDecoder.nn.decode(s), "UTF-8"))

      case hx: Expr.Hex[Row] =>
        eval(hx.expr, columns, rowIdx).map(s => hexEncode(s.getBytes("UTF-8")))

      case gjo: Expr.GetJsonObject[Row] =>
        eval(gjo.expr, columns, rowIdx).flatMap(s => extractJsonPath(s, gjo.path))

      case _: Expr.RowNumber[Row] | _: Expr.Rank[Row] | _: Expr.DenseRank[Row] | _: Expr.Lag[Row, ?] |
          _: Expr.Lead[Row, ?] | _: Expr.NTile[Row] | _: Expr.CumeDist[Row] | _: Expr.PercentRank[Row] |
          _: Expr.NthValue[Row, ?] | _: Expr.FirstValue[Row, ?] | _: Expr.LastValue[Row, ?] =>
        Left(ExecutionError.UnsupportedOperation("Window functions not supported in row-level eval"))
    }
  }

  private[strongbow] def inferExprColumnType[Row, A](expr: Expr[Row, A], columns: Vector[Column[?]]): ColumnType = {
    (expr: @unchecked) match {
      case cell: Expr.Cell[_, _] => columns(cell.index.toInt).columnType
      case c: Expr.Const[_, _] =>
        c.value match {
          case _: Int => ColumnType.IntType
          case _: Long => ColumnType.LongType
          case _: Double => ColumnType.DoubleType
          case _: String => ColumnType.StringType
          case _: Boolean => ColumnType.BooleanType
          case _ => ColumnType.AnyType
        }
      case n: Expr.Named[_, _] => inferExprColumnType(n.expr, columns)
      case _: Expr.Add[_] | _: Expr.Sub[_] | _: Expr.Mul[_] | _: Expr.Div[_] | _: Expr.Length[_] | _: Expr.Mod[_] |
          _: Expr.Abs[_] | _: Expr.Negate[_] =>
        ColumnType.IntType
      case _: Expr.AddLong[_] | _: Expr.SubLong[_] | _: Expr.MulLong[_] | _: Expr.DivLong[_] | _: Expr.ModLong[_] |
          _: Expr.AbsLong[_] | _: Expr.NegateLong[_] | _: Expr.CastToLong[_] =>
        ColumnType.LongType
      case _: Expr.AddDouble[_] | _: Expr.SubDouble[_] | _: Expr.MulDouble[_] | _: Expr.DivDouble[_] |
          _: Expr.AbsDouble[_] | _: Expr.NegateDouble[_] | _: Expr.Round[_] | _: Expr.Floor[_] | _: Expr.Ceil[_] |
          _: Expr.CastToDouble[_] | _: Expr.CastLongToDouble[_] =>
        ColumnType.DoubleType
      case _: Expr.Concat[_] | _: Expr.Lower[_] | _: Expr.Upper[_] | _: Expr.Trim[_] | _: Expr.LTrim[_] |
          _: Expr.RTrim[_] | _: Expr.Substring[_] | _: Expr.StringReplace[_] | _: Expr.RegexpReplace[_] |
          _: Expr.RegexpExtract[_] | _: Expr.ConcatWs[_] | _: Expr.CastToString[_, _] =>
        ColumnType.StringType
      case _: Expr.StringSplit[_] => ColumnType.AnyType
      case _: Expr.Gt[_, _] | _: Expr.Lt[_, _] | _: Expr.Gte[_, _] | _: Expr.Lte[_, _] | _: Expr.Eq[_, _] |
          _: Expr.Neq[_, _] | _: Expr.And[_] | _: Expr.Or[_] | _: Expr.Not[_] | _: Expr.IsDefined[_, _] |
          _: Expr.Like[_] | _: Expr.StartsWith[_] | _: Expr.EndsWith[_] | _: Expr.StringContains[_] |
          _: Expr.IsNull[_, _] | _: Expr.IsNotNull[_, _] | _: Expr.In[_, _] | _: Expr.Between[_, _] =>
        ColumnType.BooleanType
      case c: Expr.Coalesce[_, _] =>
        c.exprs.headOption.map(e => inferExprColumnType(e, columns)).getOrElse(ColumnType.AnyType)
      case w: Expr.When[_, _] => inferExprColumnType(w.thenExpr, columns)
      case g: Expr.GetOrElse[_, _] => inferExprColumnType(g.expr, columns)
      case _: Expr.Sum[_] => ColumnType.IntType
      case _: Expr.SumDouble[_] | _: Expr.Avg[_] | _: Expr.StdDev[_] | _: Expr.StdDevPop[_] => ColumnType.DoubleType
      case _: Expr.SumLong[_] | _: Expr.Count[_] | _: Expr.CountDistinct[_, _] | _: Expr.CountIf[_] =>
        ColumnType.LongType
      case _: Expr.Max[_, _] | _: Expr.Min[_, _] | _: Expr.First[_, _] => ColumnType.AnyType
      case _: Expr.Collect[_, _] | _: Expr.Option2Iterable[_, _] => ColumnType.AnyType
      case _: Expr.PercentileApprox[_] => ColumnType.DoubleType
      case _: Expr.MaxBy[_, _, _] | _: Expr.MinBy[_, _, _] => ColumnType.AnyType
      case _: Expr.MaxN[_, _] | _: Expr.MinN[_, _] | _: Expr.MaxByN[_, _, _] | _: Expr.MinByN[_, _, _] =>
        ColumnType.AnyType
      case _: Expr.DateAddDays[_] | _: Expr.DateSubDays[_] | _: Expr.DateAddMonths[_] => ColumnType.DateType
      case _: Expr.DateDiff[_] | _: Expr.ExtractYear[_] | _: Expr.ExtractMonth[_] | _: Expr.ExtractDay[_] =>
        ColumnType.IntType
      case _: Expr.RowNumber[_] | _: Expr.Rank[_] | _: Expr.DenseRank[_] => ColumnType.IntType
      case _: Expr.Lag[_, _] | _: Expr.Lead[_, _] => ColumnType.AnyType
      case _: Expr.Sqrt[_] | _: Expr.Pow[_] | _: Expr.Log[_] | _: Expr.Log10[_] | _: Expr.Log2[_] | _: Expr.Exp[_] |
          _: Expr.Sin[_] | _: Expr.Cos[_] | _: Expr.Tan[_] | _: Expr.Asin[_] | _: Expr.Acos[_] | _: Expr.Atan[_] |
          _: Expr.Atan2[_] | _: Expr.Signum[_] | _: Expr.Rand[_] =>
        ColumnType.DoubleType
      case _: Expr.DayOfWeek[_] | _: Expr.DayOfYear[_] | _: Expr.WeekOfYear[_] | _: Expr.Quarter[_] =>
        ColumnType.IntType
      case _: Expr.LastDay[_] | _: Expr.NextDay[_] | _: Expr.DateTrunc[_] | _: Expr.MakeDate[_] =>
        ColumnType.DateType
      case _: Expr.MonthsBetween[_] => ColumnType.DoubleType
      case _: Expr.DateFormat[_] => ColumnType.StringType
      case _: Expr.Variance[_] | _: Expr.VariancePop[_] | _: Expr.Corr[_] | _: Expr.CovarSamp[_] | _: Expr.CovarPop[_] |
          _: Expr.Median[_] =>
        ColumnType.DoubleType
      case _: Expr.ApproxCountDistinct[_, _] => ColumnType.LongType
      case _: Expr.CollectSet[_, _] => ColumnType.AnyType
      case _: Expr.ExprLast[_, _] | _: Expr.AnyValue[_, _] | _: Expr.Mode[_, _] => ColumnType.AnyType
      case _: Expr.BoolAnd[_] | _: Expr.BoolOr[_] => ColumnType.BooleanType
      case _: Expr.NTile[_] => ColumnType.IntType
      case _: Expr.CumeDist[_] | _: Expr.PercentRank[_] => ColumnType.DoubleType
      case _: Expr.NthValue[_, _] | _: Expr.FirstValue[_, _] | _: Expr.LastValue[_, _] => ColumnType.AnyType
      case _: Expr.ArraySize[_, _] => ColumnType.IntType
      case _: Expr.ArrayContains[_, _] | _: Expr.MapContainsKey[_, _, _] => ColumnType.BooleanType
      case _: Expr.Explode[_, _] | _: Expr.ElementAt[_, _] => ColumnType.AnyType
      case _: Expr.ArraySort[_, _] | _: Expr.ArrayDistinct[_, _] | _: Expr.ArrayUnion[_, _] |
          _: Expr.ArrayIntersect[_, _] | _: Expr.ArrayExcept[_, _] | _: Expr.Flatten[_, _] | _: Expr.ArraySlice[_, _] |
          _: Expr.MapKeys[_, _, _] | _: Expr.MapValues[_, _, _] | _: Expr.MapEntries[_, _, _] =>
        ColumnType.AnyType
      case _: Expr.MapFromArrays[_, _, _] | _: Expr.MapConcat[_, _, _] => ColumnType.AnyType
      case _: Expr.Md5[_] | _: Expr.Sha1[_] | _: Expr.Sha2[_] | _: Expr.UrlEncode[_] | _: Expr.UrlDecode[_] |
          _: Expr.Base64Encode[_] | _: Expr.Base64Decode[_] | _: Expr.Hex[_] | _: Expr.GetJsonObject[_] =>
        ColumnType.StringType
    }
  }

  /** Evaluate expression for all rows, producing a new column.
    *
    * Vectorized: operates on entire arrays instead of row-by-row where possible. For Cell
    * references, returns the column directly (zero work). For arithmetic/comparisons/string ops,
    * uses while-loops on typed arrays. Falls back to row-by-row eval for unsupported patterns.
    */
  def evalColumn[Row, A](
    expr: Expr[Row, A],
    columns: Vector[Column[?]],
    columnType: ColumnType
  ): Either[ExecutionError, Column[?]] = {
    if (columns.isEmpty || columns.head.length == 0) {
      return Right(Column.empty(columnType)) // scalafix:ok DisableSyntax.return
    }

    val rowCount = columns.head.length

    (expr: @unchecked) match {
      case cell: Expr.Cell[_, _] =>
        Right(columns(cell.index.toInt))

      case named: Expr.Named[_, _] =>
        evalColumn(named.expr, columns, columnType)

      case c: Expr.Const[_, _] =>
        c.value match {
          case v: Int =>
            Right(Column.int(Array.fill(rowCount)(v)))
          case v: Long =>
            Right(Column.long(Array.fill(rowCount)(v)))
          case v: Double =>
            Right(Column.double(Array.fill(rowCount)(v)))
          case v: String =>
            Right(Column.string(Array.fill(rowCount)(v)))
          case v: Boolean =>
            Right(Column.boolean(Array.fill(rowCount)(v)))
          case v =>
            if (Option(v).isEmpty) {
              val allNulls = BitSet((0 until rowCount)*)
              Right(columnType match {
                case ColumnType.IntType => Column.int(new Array[Int](rowCount), allNulls)
                case ColumnType.LongType => Column.long(new Array[Long](rowCount), allNulls)
                case ColumnType.DoubleType => Column.double(new Array[Double](rowCount), allNulls)
                case ColumnType.StringType => Column.string(new Array[String](rowCount), allNulls)
                case ColumnType.BooleanType => Column.boolean(new Array[Boolean](rowCount), allNulls)
                case ColumnType.DateType => Column.date(new Array[Int](rowCount), allNulls)
                case _ => Column.any(new Array[Any](rowCount), allNulls)
              })
            } else if (columnType == ColumnType.DateType) {
              v match {
                case ld: java.time.LocalDate =>
                  Right(Column.date(Array.fill(rowCount)(ld.toEpochDay.toInt)))
                case _ =>
                  Left(ExecutionError.TypeMismatch("Date", v.getClass.getSimpleName, "evalColumn Const"))
              }
            } else {
              Right(Column.any(Array.fill[Any](rowCount)(v)))
            }
        }

      case add: Expr.Add[Row] =>
        vectorizedIntBinOp(add.left, add.right, columns, rowCount)(_ + _)

      case sub: Expr.Sub[Row] =>
        vectorizedIntBinOp(sub.left, sub.right, columns, rowCount)(_ - _)

      case mul: Expr.Mul[Row] =>
        vectorizedIntBinOp(mul.left, mul.right, columns, rowCount)(_ * _)

      case div: Expr.Div[Row] =>
        for {
          leftCol <- evalColumn(div.left, columns, ColumnType.IntType)
          rightCol <- evalColumn(div.right, columns, ColumnType.IntType)
          result <- (leftCol, rightCol) match {
            case (Column.IntColumn(ld, _), Column.IntColumn(rd, _)) =>
              vectorizedDiv(ld, rd, rowCount)
            case _ =>
              Left(ExecutionError.TypeMismatch("IntColumn", leftCol.columnType.toString, "evalColumn"))
          }
        } yield result

      case add: Expr.AddLong[Row] =>
        vectorizedLongBinOp(add.left, add.right, columns, rowCount)(_ + _)

      case sub: Expr.SubLong[Row] =>
        vectorizedLongBinOp(sub.left, sub.right, columns, rowCount)(_ - _)

      case mul: Expr.MulLong[Row] =>
        vectorizedLongBinOp(mul.left, mul.right, columns, rowCount)(_ * _)

      case div: Expr.DivLong[Row] =>
        for {
          leftCol <- evalColumn(div.left, columns, ColumnType.LongType)
          rightCol <- evalColumn(div.right, columns, ColumnType.LongType)
          result <- (leftCol, rightCol) match {
            case (Column.LongColumn(ld, _), Column.LongColumn(rd, _)) =>
              vectorizedLongDiv(ld, rd, rowCount)
            case _ =>
              Left(ExecutionError.TypeMismatch("LongColumn", leftCol.columnType.toString, "evalColumn"))
          }
        } yield result

      case add: Expr.AddDouble[Row] =>
        vectorizedDoubleBinOp(add.left, add.right, columns, rowCount)(_ + _)

      case sub: Expr.SubDouble[Row] =>
        vectorizedDoubleBinOp(sub.left, sub.right, columns, rowCount)(_ - _)

      case mul: Expr.MulDouble[Row] =>
        vectorizedDoubleBinOp(mul.left, mul.right, columns, rowCount)(_ * _)

      case div: Expr.DivDouble[Row] =>
        for {
          leftCol <- evalColumn(div.left, columns, ColumnType.DoubleType)
          rightCol <- evalColumn(div.right, columns, ColumnType.DoubleType)
          result <- (leftCol, rightCol) match {
            case (Column.DoubleColumn(ld, _), Column.DoubleColumn(rd, _)) =>
              vectorizedDoubleDiv(ld, rd, rowCount)
            case _ =>
              Left(ExecutionError.TypeMismatch("DoubleColumn", leftCol.columnType.toString, "evalColumn"))
          }
        } yield result

      case gt: Expr.Gt[Row, _] =>
        typedComparison(gt.left, gt.right, columns, rowCount)(
          (a: Int, b: Int) => a > b,
          (a: Long, b: Long) => a > b,
          (a: Double, b: Double) => a > b,
          (a: String, b: String) => a.compareTo(b) > 0
        )

      case gte: Expr.Gte[Row, _] =>
        typedComparison(gte.left, gte.right, columns, rowCount)(
          (a: Int, b: Int) => a >= b,
          (a: Long, b: Long) => a >= b,
          (a: Double, b: Double) => a >= b,
          (a: String, b: String) => a.compareTo(b) >= 0
        )

      case lt: Expr.Lt[Row, _] =>
        typedComparison(lt.left, lt.right, columns, rowCount)(
          (a: Int, b: Int) => a < b,
          (a: Long, b: Long) => a < b,
          (a: Double, b: Double) => a < b,
          (a: String, b: String) => a.compareTo(b) < 0
        )

      case lte: Expr.Lte[Row, _] =>
        typedComparison(lte.left, lte.right, columns, rowCount)(
          (a: Int, b: Int) => a <= b,
          (a: Long, b: Long) => a <= b,
          (a: Double, b: Double) => a <= b,
          (a: String, b: String) => a.compareTo(b) <= 0
        )

      case eq: Expr.Eq[Row, _] =>
        equalityComparison(eq.left, eq.right, columns, rowCount)(java.util.Objects.equals)

      case neq: Expr.Neq[Row, _] =>
        equalityComparison(neq.left, neq.right, columns, rowCount)((a, b) => !java.util.Objects.equals(a, b))

      case and: Expr.And[Row] =>
        for {
          leftCol <- evalColumn(and.left, columns, ColumnType.BooleanType)
          rightCol <- evalColumn(and.right, columns, ColumnType.BooleanType)
        } yield {
          (leftCol, rightCol) match {
            case (Column.BooleanColumn(ld, ln), Column.BooleanColumn(rd, rn)) =>
              val out = new Array[Boolean](rowCount)
              var i = 0 // scalafix:ok DisableSyntax.var
              while (i < rowCount) { out(i) = ld(i) && rd(i); i += 1 }
              Column.boolean(out, ln | rn)
            case _ =>
              Column.boolean(Array.empty[Boolean])
          }
        }

      case or: Expr.Or[Row] =>
        for {
          leftCol <- evalColumn(or.left, columns, ColumnType.BooleanType)
          rightCol <- evalColumn(or.right, columns, ColumnType.BooleanType)
        } yield {
          (leftCol, rightCol) match {
            case (Column.BooleanColumn(ld, ln), Column.BooleanColumn(rd, rn)) =>
              val out = new Array[Boolean](rowCount)
              var i = 0 // scalafix:ok DisableSyntax.var
              while (i < rowCount) { out(i) = ld(i) || rd(i); i += 1 }
              Column.boolean(out, ln | rn)
            case _ =>
              Column.boolean(Array.empty[Boolean])
          }
        }

      case not: Expr.Not[Row] =>
        evalColumn(not.expr, columns, ColumnType.BooleanType).map {
          case Column.BooleanColumn(data, nulls) =>
            val out = new Array[Boolean](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) { out(i) = !data(i); i += 1 }
            Column.boolean(out, nulls)
          case _ =>
            Column.boolean(Array.empty[Boolean])
        }

      case concat: Expr.Concat[Row] =>
        for {
          leftCol <- evalColumn(concat.left, columns, ColumnType.StringType)
          rightCol <- evalColumn(concat.right, columns, ColumnType.StringType)
        } yield {
          (leftCol, rightCol) match {
            case (Column.StringColumn(ld, ln), Column.StringColumn(rd, rn)) =>
              val out = new Array[String](rowCount)
              var i = 0 // scalafix:ok DisableSyntax.var
              while (i < rowCount) { out(i) = ld(i) + rd(i); i += 1 }
              Column.string(out, ln | rn)
            case _ =>
              Column.string(Array.empty[String])
          }
        }

      case length: Expr.Length[Row] =>
        evalColumn(length.expr, columns, ColumnType.StringType).map {
          case Column.StringColumn(data, nulls) =>
            val out = new Array[Int](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) { out(i) = if (nulls.contains(i)) 0 else data(i).length; i += 1 }
            Column.int(out, nulls)
          case _ =>
            Column.int(Array.empty[Int])
        }

      case when: Expr.When[Row, _] =>
        for {
          condCol <- evalColumn(when.condition, columns, ColumnType.BooleanType)
          thenCol <- evalColumn(when.thenExpr, columns, columnType)
          elseCol <- evalColumn(when.elseExpr, columns, columnType)
        } yield {
          condCol match {
            case Column.BooleanColumn(cond, _) =>
              (thenCol, elseCol) match {
                case (Column.IntColumn(td, tn), Column.IntColumn(ed, en)) =>
                  val out = new Array[Int](rowCount)
                  var i = 0 // scalafix:ok DisableSyntax.var
                  while (i < rowCount) { out(i) = if (cond(i)) td(i) else ed(i); i += 1 }
                  Column.int(out, tn | en)
                case (Column.LongColumn(td, tn), Column.LongColumn(ed, en)) =>
                  val out = new Array[Long](rowCount)
                  var i = 0 // scalafix:ok DisableSyntax.var
                  while (i < rowCount) { out(i) = if (cond(i)) td(i) else ed(i); i += 1 }
                  Column.long(out, tn | en)
                case (Column.DoubleColumn(td, tn), Column.DoubleColumn(ed, en)) =>
                  val out = new Array[Double](rowCount)
                  var i = 0 // scalafix:ok DisableSyntax.var
                  while (i < rowCount) { out(i) = if (cond(i)) td(i) else ed(i); i += 1 }
                  Column.double(out, tn | en)
                case (Column.StringColumn(td, tn), Column.StringColumn(ed, en)) =>
                  val out = new Array[String](rowCount)
                  var i = 0 // scalafix:ok DisableSyntax.var
                  while (i < rowCount) { out(i) = if (cond(i)) td(i) else ed(i); i += 1 }
                  Column.string(out, tn | en)
                case (Column.BooleanColumn(td, tn), Column.BooleanColumn(ed, en)) =>
                  val out = new Array[Boolean](rowCount)
                  var i = 0 // scalafix:ok DisableSyntax.var
                  while (i < rowCount) { out(i) = if (cond(i)) td(i) else ed(i); i += 1 }
                  Column.boolean(out, tn | en)
                case (Column.DateColumn(td, tn), Column.DateColumn(ed, en)) =>
                  val out = new Array[Int](rowCount)
                  var i = 0 // scalafix:ok DisableSyntax.var
                  while (i < rowCount) { out(i) = if (cond(i)) td(i) else ed(i); i += 1 }
                  Column.date(out, tn | en)
                case _ =>
                  val out = new Array[Any](rowCount)
                  var i = 0 // scalafix:ok DisableSyntax.var
                  while (i < rowCount) {
                    out(i) = if (cond(i)) thenCol.getValue(i) else elseCol.getValue(i)
                    i += 1
                  }
                  Column.fromValues(out.toVector, columnType) match {
                    case Right(c) => c
                    case Left(_) => Column.any(out)
                  }
              }
            case _ =>
              Column.any(Array.empty[Any])
          }
        }

      case like: Expr.Like[Row] =>
        evalColumn(like.expr, columns, ColumnType.StringType).map {
          case Column.StringColumn(data, nulls) =>
            val regex = likeToRegex(like.pattern)
            val out = new Array[Boolean](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) = if (nulls.contains(i)) false else regex.matches(data(i))
              i += 1
            }
            Column.boolean(out, nulls)
          case _ => Column.boolean(Array.empty[Boolean])
        }

      case lo: Expr.Lower[Row] =>
        evalColumn(lo.expr, columns, ColumnType.StringType).map {
          case Column.StringColumn(data, nulls) =>
            val out = new Array[String](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) = if (nulls.contains(i)) null else data(i).toLowerCase // scalafix:ok DisableSyntax.null
              i += 1
            }
            Column.string(out, nulls)
          case _ => Column.string(Array.empty[String])
        }

      case up: Expr.Upper[Row] =>
        evalColumn(up.expr, columns, ColumnType.StringType).map {
          case Column.StringColumn(data, nulls) =>
            val out = new Array[String](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) = if (nulls.contains(i)) null else data(i).toUpperCase // scalafix:ok DisableSyntax.null
              i += 1
            }
            Column.string(out, nulls)
          case _ => Column.string(Array.empty[String])
        }

      case tr: Expr.Trim[Row] =>
        evalColumn(tr.expr, columns, ColumnType.StringType).map {
          case Column.StringColumn(data, nulls) =>
            val out = new Array[String](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) = if (nulls.contains(i)) null else data(i).trim // scalafix:ok DisableSyntax.null
              i += 1
            }
            Column.string(out, nulls)
          case _ => Column.string(Array.empty[String])
        }

      case lt: Expr.LTrim[Row] =>
        evalColumn(lt.expr, columns, ColumnType.StringType).map {
          case Column.StringColumn(data, nulls) =>
            val out = new Array[String](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) = if (nulls.contains(i)) null else data(i).stripLeading.nn // scalafix:ok DisableSyntax.null
              i += 1
            }
            Column.string(out, nulls)
          case _ => Column.string(Array.empty[String])
        }

      case rt: Expr.RTrim[Row] =>
        evalColumn(rt.expr, columns, ColumnType.StringType).map {
          case Column.StringColumn(data, nulls) =>
            val out = new Array[String](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) = if (nulls.contains(i)) null else data(i).stripTrailing.nn // scalafix:ok DisableSyntax.null
              i += 1
            }
            Column.string(out, nulls)
          case _ => Column.string(Array.empty[String])
        }

      case ss: Expr.Substring[Row] =>
        evalColumn(ss.expr, columns, ColumnType.StringType).map {
          case Column.StringColumn(data, nulls) =>
            val out = new Array[String](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              if (nulls.contains(i)) {
                out(i) = null // scalafix:ok DisableSyntax.null
              } else {
                val s = data(i)
                val start = Math.max(ss.pos - 1, 0)
                val end = Math.min(start + ss.len, s.length)
                out(i) = if (start >= s.length) "" else s.substring(start, end)
              }
              i += 1
            }
            Column.string(out, nulls)
          case _ => Column.string(Array.empty[String])
        }

      case sr: Expr.StringReplace[Row] =>
        evalColumn(sr.expr, columns, ColumnType.StringType).map {
          case Column.StringColumn(data, nulls) =>
            val out = new Array[String](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) =
                if (nulls.contains(i)) null
                else data(i).replace(sr.search, sr.replacement) // scalafix:ok DisableSyntax.null
              i += 1
            }
            Column.string(out, nulls)
          case _ => Column.string(Array.empty[String])
        }

      case rr: Expr.RegexpReplace[Row] =>
        evalColumn(rr.expr, columns, ColumnType.StringType).map {
          case Column.StringColumn(data, nulls) =>
            val compiled = java.util.regex.Pattern.compile(rr.pattern)
            val out = new Array[String](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) =
                if (nulls.contains(i)) null // scalafix:ok DisableSyntax.null
                else compiled.matcher(data(i)).replaceAll(rr.replacement).nn
              i += 1
            }
            Column.string(out, nulls)
          case _ => Column.string(Array.empty[String])
        }

      case re: Expr.RegexpExtract[Row] =>
        evalColumn(re.expr, columns, ColumnType.StringType).map {
          case Column.StringColumn(data, nulls) =>
            val compiled = java.util.regex.Pattern.compile(re.pattern)
            val out = new Array[String](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              if (nulls.contains(i)) {
                out(i) = null // scalafix:ok DisableSyntax.null
              } else {
                val m = compiled.matcher(data(i))
                out(i) = if (m.find()) m.group(re.groupIdx) else ""
              }
              i += 1
            }
            Column.string(out, nulls)
          case _ => Column.string(Array.empty[String])
        }

      case sp: Expr.StringSplit[Row] =>
        evalColumn(sp.expr, columns, ColumnType.StringType).map {
          case Column.StringColumn(data, nulls) =>
            val out = new Array[Any](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) =
                if (nulls.contains(i)) null // scalafix:ok DisableSyntax.null
                else data(i).split(sp.delimiter, -1).toSeq
              i += 1
            }
            Column.any(out, nulls)
          case _ => Column.any(Array.empty[Any])
        }

      case sw: Expr.StartsWith[Row] =>
        for {
          exprCol <- evalColumn(sw.expr, columns, ColumnType.StringType)
          prefixCol <- evalColumn(sw.prefix, columns, ColumnType.StringType)
        } yield {
          (exprCol, prefixCol) match {
            case (Column.StringColumn(ed, en), Column.StringColumn(pd, pn)) =>
              val combined = en | pn
              val out = new Array[Boolean](rowCount)
              var i = 0 // scalafix:ok DisableSyntax.var
              while (i < rowCount) {
                out(i) = if (combined.contains(i)) false else ed(i).startsWith(pd(i))
                i += 1
              }
              Column.boolean(out, combined)
            case _ => Column.boolean(Array.empty[Boolean])
          }
        }

      case ew: Expr.EndsWith[Row] =>
        for {
          exprCol <- evalColumn(ew.expr, columns, ColumnType.StringType)
          suffixCol <- evalColumn(ew.suffix, columns, ColumnType.StringType)
        } yield {
          (exprCol, suffixCol) match {
            case (Column.StringColumn(ed, en), Column.StringColumn(sd, sn)) =>
              val combined = en | sn
              val out = new Array[Boolean](rowCount)
              var i = 0 // scalafix:ok DisableSyntax.var
              while (i < rowCount) {
                out(i) = if (combined.contains(i)) false else ed(i).endsWith(sd(i))
                i += 1
              }
              Column.boolean(out, combined)
            case _ => Column.boolean(Array.empty[Boolean])
          }
        }

      case sc: Expr.StringContains[Row] =>
        for {
          exprCol <- evalColumn(sc.expr, columns, ColumnType.StringType)
          substrCol <- evalColumn(sc.substr, columns, ColumnType.StringType)
        } yield {
          (exprCol, substrCol) match {
            case (Column.StringColumn(ed, en), Column.StringColumn(sd, sn)) =>
              val combined = en | sn
              val out = new Array[Boolean](rowCount)
              var i = 0 // scalafix:ok DisableSyntax.var
              while (i < rowCount) {
                out(i) = if (combined.contains(i)) false else ed(i).contains(sd(i))
                i += 1
              }
              Column.boolean(out, combined)
            case _ => Column.boolean(Array.empty[Boolean])
          }
        }

      case cw: Expr.ConcatWs[Row] =>
        val colResults = cw.exprs.map(e => evalColumn(e, columns, ColumnType.StringType))
        val firstErr = colResults.collectFirst { case Left(err) => err }
        firstErr match {
          case Some(err) => Left(err)
          case None =>
            val cols = colResults.collect { case Right(c) => c }
            val out = new Array[String](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              val parts = cols.collect {
                case Column.StringColumn(data, nulls) if !nulls.contains(i) => data(i)
              }
              out(i) = parts.mkString(cw.separator)
              i += 1
            }
            Right(Column.string(out))
        }

      case co: Expr.Coalesce[Row, _] =>
        val colResults = co.exprs.map(e => evalColumn(e, columns, columnType))
        val firstErr = colResults.collectFirst { case Left(err) => err }
        firstErr match {
          case Some(err) => Left(err)
          case None =>
            val cols = colResults.collect { case Right(c) => c }
            if (cols.isEmpty) {
              Left(ExecutionError.UnsupportedOperation("Coalesce: no expressions provided"))
            } else {
              val out = new Array[Any](rowCount)
              val outNulls = scala.collection.mutable.BitSet.empty
              var i = 0 // scalafix:ok DisableSyntax.var
              while (i < rowCount) {
                var found = false // scalafix:ok DisableSyntax.var
                var j = 0 // scalafix:ok DisableSyntax.var
                while (j < cols.length && !found) {
                  if (!cols(j).isNull(RowIndex(i))) {
                    out(i) = cols(j).getValue(i)
                    found = true
                  }
                  j += 1
                }
                if (!found) {
                  out(i) = null // scalafix:ok DisableSyntax.null
                  outNulls += i
                }
                i += 1
              }
              Column.fromValues(out.toVector, columnType)
            }
        }

      case in: Expr.IsNull[Row, _] =>
        val innerType = inferExprColumnType(in.expr, columns)
        evalColumn(in.expr, columns, innerType).map { col =>
          val out = new Array[Boolean](rowCount)
          val nulls = col match {
            case Column.IntColumn(_, n) => n
            case Column.LongColumn(_, n) => n
            case Column.DoubleColumn(_, n) => n
            case Column.StringColumn(_, n) => n
            case Column.BooleanColumn(_, n) => n
            case Column.DateColumn(_, n) => n
            case Column.AnyColumn(_, n) => n
          }
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            out(i) = nulls.contains(i)
            i += 1
          }
          Column.boolean(out)
        }

      case inn: Expr.IsNotNull[Row, _] =>
        val innerType = inferExprColumnType(inn.expr, columns)
        evalColumn(inn.expr, columns, innerType).map { col =>
          val out = new Array[Boolean](rowCount)
          val nulls = col match {
            case Column.IntColumn(_, n) => n
            case Column.LongColumn(_, n) => n
            case Column.DoubleColumn(_, n) => n
            case Column.StringColumn(_, n) => n
            case Column.BooleanColumn(_, n) => n
            case Column.DateColumn(_, n) => n
            case Column.AnyColumn(_, n) => n
          }
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            out(i) = !nulls.contains(i)
            i += 1
          }
          Column.boolean(out)
        }

      case inV: Expr.In[Row, _] =>
        val innerType = inferExprColumnType(inV.expr, columns)
        evalColumn(inV.expr, columns, innerType).map { col =>
          val valSet: Set[Any] = inV.values.map(v => v: Any).toSet
          val out = new Array[Boolean](rowCount)
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            out(i) = valSet.contains(col.getValue(i))
            i += 1
          }
          Column.boolean(out)
        }

      case btw: Expr.Between[Row, _] =>
        val innerType = inferExprColumnType(btw.expr, columns)
        for {
          exprCol <- evalColumn(btw.expr, columns, innerType)
          lowerCol <- evalColumn(btw.lower, columns, innerType)
          upperCol <- evalColumn(btw.upper, columns, innerType)
          result <- {
            val out = new Array[Boolean](rowCount)
            (exprCol, lowerCol, upperCol) match {
              case (Column.IntColumn(vd, vn), Column.IntColumn(ld, ln), Column.IntColumn(ud, un)) =>
                val combined = vn | ln | un
                var i = 0 // scalafix:ok DisableSyntax.var
                while (i < rowCount) {
                  out(i) = if (combined.contains(i)) false else vd(i) >= ld(i) && vd(i) <= ud(i)
                  i += 1
                }
                Right(Column.boolean(out, combined))
              case (Column.LongColumn(vd, vn), Column.LongColumn(ld, ln), Column.LongColumn(ud, un)) =>
                val combined = vn | ln | un
                var i = 0 // scalafix:ok DisableSyntax.var
                while (i < rowCount) {
                  out(i) = if (combined.contains(i)) false else vd(i) >= ld(i) && vd(i) <= ud(i)
                  i += 1
                }
                Right(Column.boolean(out, combined))
              case (Column.DoubleColumn(vd, vn), Column.DoubleColumn(ld, ln), Column.DoubleColumn(ud, un)) =>
                val combined = vn | ln | un
                var i = 0 // scalafix:ok DisableSyntax.var
                while (i < rowCount) {
                  out(i) = if (combined.contains(i)) false else vd(i) >= ld(i) && vd(i) <= ud(i)
                  i += 1
                }
                Right(Column.boolean(out, combined))
              case _ =>
                Left(ExecutionError.UnsupportedOperation("Between not supported for untyped columns"))
            }
          }
        } yield result

      case m: Expr.Mod[Row] =>
        for {
          leftCol <- evalColumn(m.left, columns, ColumnType.IntType)
          rightCol <- evalColumn(m.right, columns, ColumnType.IntType)
          result <- (leftCol, rightCol) match {
            case (Column.IntColumn(ld, _), Column.IntColumn(rd, _)) =>
              vectorizedIntMod(ld, rd, rowCount)
            case _ =>
              Left(ExecutionError.TypeMismatch("IntColumn", leftCol.columnType.toString, "evalColumn"))
          }
        } yield result

      case ml: Expr.ModLong[Row] =>
        for {
          leftCol <- evalColumn(ml.left, columns, ColumnType.LongType)
          rightCol <- evalColumn(ml.right, columns, ColumnType.LongType)
          result <- (leftCol, rightCol) match {
            case (Column.LongColumn(ld, _), Column.LongColumn(rd, _)) =>
              vectorizedLongMod(ld, rd, rowCount)
            case _ =>
              Left(ExecutionError.TypeMismatch("LongColumn", leftCol.columnType.toString, "evalColumn"))
          }
        } yield result

      case ab: Expr.Abs[Row] =>
        evalColumn(ab.expr, columns, ColumnType.IntType).map {
          case Column.IntColumn(data, nulls) =>
            val out = new Array[Int](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) { out(i) = Math.abs(data(i)); i += 1 }
            Column.int(out, nulls)
          case _ => Column.int(Array.empty[Int])
        }

      case abl: Expr.AbsLong[Row] =>
        evalColumn(abl.expr, columns, ColumnType.LongType).map {
          case Column.LongColumn(data, nulls) =>
            val out = new Array[Long](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) { out(i) = Math.abs(data(i)); i += 1 }
            Column.long(out, nulls)
          case _ => Column.long(Array.empty[Long])
        }

      case abd: Expr.AbsDouble[Row] =>
        evalColumn(abd.expr, columns, ColumnType.DoubleType).map {
          case Column.DoubleColumn(data, nulls) =>
            val out = new Array[Double](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) { out(i) = Math.abs(data(i)); i += 1 }
            Column.double(out, nulls)
          case _ => Column.double(Array.empty[Double])
        }

      case neg: Expr.Negate[Row] =>
        evalColumn(neg.expr, columns, ColumnType.IntType).map {
          case Column.IntColumn(data, nulls) =>
            val out = new Array[Int](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) { out(i) = -data(i); i += 1 }
            Column.int(out, nulls)
          case _ => Column.int(Array.empty[Int])
        }

      case negl: Expr.NegateLong[Row] =>
        evalColumn(negl.expr, columns, ColumnType.LongType).map {
          case Column.LongColumn(data, nulls) =>
            val out = new Array[Long](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) { out(i) = -data(i); i += 1 }
            Column.long(out, nulls)
          case _ => Column.long(Array.empty[Long])
        }

      case negd: Expr.NegateDouble[Row] =>
        evalColumn(negd.expr, columns, ColumnType.DoubleType).map {
          case Column.DoubleColumn(data, nulls) =>
            val out = new Array[Double](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) { out(i) = -data(i); i += 1 }
            Column.double(out, nulls)
          case _ => Column.double(Array.empty[Double])
        }

      case rnd: Expr.Round[Row] =>
        evalColumn(rnd.expr, columns, ColumnType.DoubleType).map {
          case Column.DoubleColumn(data, nulls) =>
            val out = new Array[Double](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) = BigDecimal(data(i)).setScale(rnd.scale, BigDecimal.RoundingMode.HALF_UP).toDouble
              i += 1
            }
            Column.double(out, nulls)
          case _ => Column.double(Array.empty[Double])
        }

      case fl: Expr.Floor[Row] =>
        evalColumn(fl.expr, columns, ColumnType.DoubleType).map {
          case Column.DoubleColumn(data, nulls) =>
            val out = new Array[Double](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) { out(i) = Math.floor(data(i)); i += 1 }
            Column.double(out, nulls)
          case _ => Column.double(Array.empty[Double])
        }

      case cl: Expr.Ceil[Row] =>
        evalColumn(cl.expr, columns, ColumnType.DoubleType).map {
          case Column.DoubleColumn(data, nulls) =>
            val out = new Array[Double](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) { out(i) = Math.ceil(data(i)); i += 1 }
            Column.double(out, nulls)
          case _ => Column.double(Array.empty[Double])
        }

      case ctl: Expr.CastToLong[Row] =>
        evalColumn(ctl.expr, columns, ColumnType.IntType).map {
          case Column.IntColumn(data, nulls) =>
            val out = new Array[Long](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) { out(i) = data(i).toLong; i += 1 }
            Column.long(out, nulls)
          case _ => Column.long(Array.empty[Long])
        }

      case ctd: Expr.CastToDouble[Row] =>
        evalColumn(ctd.expr, columns, ColumnType.IntType).map {
          case Column.IntColumn(data, nulls) =>
            val out = new Array[Double](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) { out(i) = data(i).toDouble; i += 1 }
            Column.double(out, nulls)
          case _ => Column.double(Array.empty[Double])
        }

      case cltd: Expr.CastLongToDouble[Row] =>
        evalColumn(cltd.expr, columns, ColumnType.LongType).map {
          case Column.LongColumn(data, nulls) =>
            val out = new Array[Double](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) { out(i) = data(i).toDouble; i += 1 }
            Column.double(out, nulls)
          case _ => Column.double(Array.empty[Double])
        }

      case cts: Expr.CastToString[Row, _] =>
        val innerType = inferExprColumnType(cts.expr, columns)
        evalColumn(cts.expr, columns, innerType).map { col =>
          val out = new Array[String](rowCount)
          val nulls = col match {
            case Column.IntColumn(_, n) => n
            case Column.LongColumn(_, n) => n
            case Column.DoubleColumn(_, n) => n
            case Column.StringColumn(_, n) => n
            case Column.BooleanColumn(_, n) => n
            case Column.DateColumn(_, n) => n
            case Column.AnyColumn(_, n) => n
          }
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            out(i) = if (nulls.contains(i)) null else String.valueOf(col.getValue(i)) // scalafix:ok DisableSyntax.null
            i += 1
          }
          Column.string(out, nulls)
        }

      case isDefined: Expr.IsDefined[Row, _] =>
        val innerType = inferExprColumnType(isDefined.expr, columns)
        evalColumn(isDefined.expr, columns, innerType).map { col =>
          val out = new Array[Boolean](rowCount)
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            col.getValue(i) match {
              case Some(_) => out(i) = true
              case _ => out(i) = false
            }
            i += 1
          }
          Column.boolean(out)
        }

      case getOrElse: Expr.GetOrElse[Row, _] =>
        val innerType = inferExprColumnType(getOrElse.expr, columns)
        evalColumn(getOrElse.expr, columns, innerType).flatMap { col =>
          val out = new Array[Any](rowCount)
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            col.getValue(i) match {
              case Some(value) => out(i) = value
              case _ => out(i) = getOrElse.default
            }
            i += 1
          }
          Column.fromValues(out.toVector, columnType)
        }

      case opt2iter: Expr.Option2Iterable[Row, _] =>
        val innerType = inferExprColumnType(opt2iter.expr, columns)
        evalColumn(opt2iter.expr, columns, innerType).map { col =>
          val out = new Array[Any](rowCount)
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            col.getValue(i) match {
              case Some(value) => out(i) = List(value)
              case opt: Option[?] if opt.isEmpty => out(i) = List.empty
              case other => out(i) = List(other)
            }
            i += 1
          }
          Column.any(out)
        }

      case sq: Expr.Sqrt[Row] =>
        vectorizedDoubleUnaryOp(sq.expr, columns, rowCount)(Math.sqrt)

      case pw: Expr.Pow[Row] =>
        vectorizedDoubleBinOp(pw.base, pw.exponent, columns, rowCount)(Math.pow)

      case lg: Expr.Log[Row] =>
        vectorizedDoubleUnaryOp(lg.expr, columns, rowCount)(Math.log)

      case lg10: Expr.Log10[Row] =>
        vectorizedDoubleUnaryOp(lg10.expr, columns, rowCount)(Math.log10)

      case lg2: Expr.Log2[Row] =>
        vectorizedDoubleUnaryOp(lg2.expr, columns, rowCount)(v => Math.log(v) / Math.log(2.0))

      case ex: Expr.Exp[Row] =>
        vectorizedDoubleUnaryOp(ex.expr, columns, rowCount)(Math.exp)

      case sn: Expr.Sin[Row] =>
        vectorizedDoubleUnaryOp(sn.expr, columns, rowCount)(Math.sin)

      case cs: Expr.Cos[Row] =>
        vectorizedDoubleUnaryOp(cs.expr, columns, rowCount)(Math.cos)

      case tn: Expr.Tan[Row] =>
        vectorizedDoubleUnaryOp(tn.expr, columns, rowCount)(Math.tan)

      case asn: Expr.Asin[Row] =>
        vectorizedDoubleUnaryOp(asn.expr, columns, rowCount)(Math.asin)

      case acs: Expr.Acos[Row] =>
        vectorizedDoubleUnaryOp(acs.expr, columns, rowCount)(Math.acos)

      case atn: Expr.Atan[Row] =>
        vectorizedDoubleUnaryOp(atn.expr, columns, rowCount)(Math.atan)

      case atn2: Expr.Atan2[Row] =>
        vectorizedDoubleBinOp(atn2.y, atn2.x, columns, rowCount)(Math.atan2)

      case sg: Expr.Signum[Row] =>
        vectorizedDoubleUnaryOp(sg.expr, columns, rowCount)(Math.signum)

      case rnd: Expr.Rand[Row] =>
        val random = new java.util.Random(rnd.seed)
        val out = new Array[Double](rowCount)
        var i = 0 // scalafix:ok DisableSyntax.var
        while (i < rowCount) { out(i) = random.nextDouble(); i += 1 }
        Right(Column.double(out))

      case dad: Expr.DateAddDays[Row] =>
        for {
          dateCol <- evalColumn(dad.date, columns, ColumnType.DateType)
          daysCol <- evalColumn(dad.days, columns, ColumnType.IntType)
        } yield {
          (dateCol, daysCol) match {
            case (Column.DateColumn(dd, dn), Column.IntColumn(nd, nn)) =>
              val combined = dn | nn
              val out = new Array[Int](rowCount)
              var i = 0 // scalafix:ok DisableSyntax.var
              while (i < rowCount) {
                out(i) =
                  if (combined.contains(i)) 0
                  else {
                    val d = java.time.LocalDate.ofEpochDay(dd(i).toLong)
                    d.plusDays(nd(i).toLong).toEpochDay.toInt
                  }
                i += 1
              }
              Column.date(out, combined)
            case _ => Column.date(Array.empty[Int])
          }
        }

      case dsd: Expr.DateSubDays[Row] =>
        for {
          dateCol <- evalColumn(dsd.date, columns, ColumnType.DateType)
          daysCol <- evalColumn(dsd.days, columns, ColumnType.IntType)
        } yield {
          (dateCol, daysCol) match {
            case (Column.DateColumn(dd, dn), Column.IntColumn(nd, nn)) =>
              val combined = dn | nn
              val out = new Array[Int](rowCount)
              var i = 0 // scalafix:ok DisableSyntax.var
              while (i < rowCount) {
                out(i) =
                  if (combined.contains(i)) 0
                  else {
                    val d = java.time.LocalDate.ofEpochDay(dd(i).toLong)
                    d.minusDays(nd(i).toLong).toEpochDay.toInt
                  }
                i += 1
              }
              Column.date(out, combined)
            case _ => Column.date(Array.empty[Int])
          }
        }

      case dam: Expr.DateAddMonths[Row] =>
        for {
          dateCol <- evalColumn(dam.date, columns, ColumnType.DateType)
          monthsCol <- evalColumn(dam.months, columns, ColumnType.IntType)
        } yield {
          (dateCol, monthsCol) match {
            case (Column.DateColumn(dd, dn), Column.IntColumn(md, mn)) =>
              val combined = dn | mn
              val out = new Array[Int](rowCount)
              var i = 0 // scalafix:ok DisableSyntax.var
              while (i < rowCount) {
                out(i) =
                  if (combined.contains(i)) 0
                  else {
                    val d = java.time.LocalDate.ofEpochDay(dd(i).toLong)
                    d.plusMonths(md(i).toLong).toEpochDay.toInt
                  }
                i += 1
              }
              Column.date(out, combined)
            case _ => Column.date(Array.empty[Int])
          }
        }

      case dd: Expr.DateDiff[Row] =>
        for {
          leftCol <- evalColumn(dd.left, columns, ColumnType.DateType)
          rightCol <- evalColumn(dd.right, columns, ColumnType.DateType)
        } yield {
          (leftCol, rightCol) match {
            case (Column.DateColumn(ld, ln), Column.DateColumn(rd, rn)) =>
              val combined = ln | rn
              val out = new Array[Int](rowCount)
              var i = 0 // scalafix:ok DisableSyntax.var
              while (i < rowCount) {
                out(i) =
                  if (combined.contains(i)) 0
                  else {
                    val l = java.time.LocalDate.ofEpochDay(ld(i).toLong)
                    val r = java.time.LocalDate.ofEpochDay(rd(i).toLong)
                    java.time.temporal.ChronoUnit.DAYS.between(r, l).toInt
                  }
                i += 1
              }
              Column.int(out, combined)
            case _ => Column.int(Array.empty[Int])
          }
        }

      case ey: Expr.ExtractYear[Row] =>
        evalColumn(ey.date, columns, ColumnType.DateType).map {
          case Column.DateColumn(data, nulls) =>
            val out = new Array[Int](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) = if (nulls.contains(i)) 0 else java.time.LocalDate.ofEpochDay(data(i).toLong).getYear
              i += 1
            }
            Column.int(out, nulls)
          case _ => Column.int(Array.empty[Int])
        }

      case em: Expr.ExtractMonth[Row] =>
        evalColumn(em.date, columns, ColumnType.DateType).map {
          case Column.DateColumn(data, nulls) =>
            val out = new Array[Int](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) = if (nulls.contains(i)) 0 else java.time.LocalDate.ofEpochDay(data(i).toLong).getMonthValue
              i += 1
            }
            Column.int(out, nulls)
          case _ => Column.int(Array.empty[Int])
        }

      case ed: Expr.ExtractDay[Row] =>
        evalColumn(ed.date, columns, ColumnType.DateType).map {
          case Column.DateColumn(data, nulls) =>
            val out = new Array[Int](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) = if (nulls.contains(i)) 0 else java.time.LocalDate.ofEpochDay(data(i).toLong).getDayOfMonth
              i += 1
            }
            Column.int(out, nulls)
          case _ => Column.int(Array.empty[Int])
        }

      case dow: Expr.DayOfWeek[Row] =>
        evalColumn(dow.date, columns, ColumnType.DateType).map {
          case Column.DateColumn(data, nulls) =>
            val out = new Array[Int](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) =
                if (nulls.contains(i)) 0
                else java.time.LocalDate.ofEpochDay(data(i).toLong).getDayOfWeek.getValue % 7 + 1
              i += 1
            }
            Column.int(out, nulls)
          case _ => Column.int(Array.empty[Int])
        }

      case doy: Expr.DayOfYear[Row] =>
        evalColumn(doy.date, columns, ColumnType.DateType).map {
          case Column.DateColumn(data, nulls) =>
            val out = new Array[Int](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) = if (nulls.contains(i)) 0 else java.time.LocalDate.ofEpochDay(data(i).toLong).getDayOfYear
              i += 1
            }
            Column.int(out, nulls)
          case _ => Column.int(Array.empty[Int])
        }

      case woy: Expr.WeekOfYear[Row] =>
        evalColumn(woy.date, columns, ColumnType.DateType).map {
          case Column.DateColumn(data, nulls) =>
            val out = new Array[Int](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) =
                if (nulls.contains(i)) 0
                else
                  java.time.LocalDate
                    .ofEpochDay(data(i).toLong)
                    .get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR)
              i += 1
            }
            Column.int(out, nulls)
          case _ => Column.int(Array.empty[Int])
        }

      case q: Expr.Quarter[Row] =>
        evalColumn(q.date, columns, ColumnType.DateType).map {
          case Column.DateColumn(data, nulls) =>
            val out = new Array[Int](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) =
                if (nulls.contains(i)) 0
                else {
                  val m = java.time.LocalDate.ofEpochDay(data(i).toLong).getMonthValue
                  (m - 1) / 3 + 1
                }
              i += 1
            }
            Column.int(out, nulls)
          case _ => Column.int(Array.empty[Int])
        }

      case ld: Expr.LastDay[Row] =>
        evalColumn(ld.date, columns, ColumnType.DateType).map {
          case Column.DateColumn(data, nulls) =>
            val out = new Array[Int](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) =
                if (nulls.contains(i)) 0
                else {
                  val d = java.time.LocalDate.ofEpochDay(data(i).toLong)
                  d.withDayOfMonth(d.lengthOfMonth()).toEpochDay.toInt
                }
              i += 1
            }
            Column.date(out, nulls)
          case _ => Column.date(Array.empty[Int])
        }

      case nd: Expr.NextDay[Row] =>
        evalColumn(nd.date, columns, ColumnType.DateType).map {
          case Column.DateColumn(data, nulls) =>
            val target = java.time.DayOfWeek.valueOf(nd.dayOfWeek.toUpperCase.nn)
            val out = new Array[Int](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) =
                if (nulls.contains(i)) 0
                else {
                  val d = java.time.LocalDate.ofEpochDay(data(i).toLong)
                  d.`with`(java.time.temporal.TemporalAdjusters.next(target)).toEpochDay.toInt
                }
              i += 1
            }
            Column.date(out, nulls)
          case _ => Column.date(Array.empty[Int])
        }

      case mb: Expr.MonthsBetween[Row] =>
        for {
          endCol <- evalColumn(mb.end, columns, ColumnType.DateType)
          startCol <- evalColumn(mb.start, columns, ColumnType.DateType)
        } yield {
          (endCol, startCol) match {
            case (Column.DateColumn(ed, en), Column.DateColumn(sd, sn)) =>
              val combined = en | sn
              val out = new Array[Double](rowCount)
              var i = 0 // scalafix:ok DisableSyntax.var
              while (i < rowCount) {
                out(i) =
                  if (combined.contains(i)) 0.0
                  else {
                    val e = java.time.LocalDate.ofEpochDay(ed(i).toLong)
                    val s = java.time.LocalDate.ofEpochDay(sd(i).toLong)
                    val period = java.time.Period.between(s, e)
                    period.toTotalMonths.toDouble + period.getDays.toDouble / 31.0
                  }
                i += 1
              }
              Column.double(out, combined)
            case _ => Column.double(Array.empty[Double])
          }
        }

      case dt: Expr.DateTrunc[Row] =>
        evalColumn(dt.date, columns, ColumnType.DateType).map {
          case Column.DateColumn(data, nulls) =>
            val out = new Array[Int](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) =
                if (nulls.contains(i)) 0
                else {
                  val d = java.time.LocalDate.ofEpochDay(data(i).toLong)
                  val truncated = dt.unit.toUpperCase.nn match {
                    case "YEAR" => d.withDayOfYear(1)
                    case "MONTH" => d.withDayOfMonth(1)
                    case "WEEK" =>
                      d.`with`(java.time.temporal.TemporalAdjusters.previousOrSame(java.time.DayOfWeek.MONDAY))
                    case "QUARTER" =>
                      val qMonth = (d.getMonthValue - 1) / 3 * 3 + 1
                      java.time.LocalDate.of(d.getYear, qMonth, 1)
                    case _ => d
                  }
                  truncated.toEpochDay.toInt
                }
              i += 1
            }
            Column.date(out, nulls)
          case _ => Column.date(Array.empty[Int])
        }

      case df: Expr.DateFormat[Row] =>
        evalColumn(df.date, columns, ColumnType.DateType).map {
          case Column.DateColumn(data, nulls) =>
            val formatter = java.time.format.DateTimeFormatter.ofPattern(df.format)
            val out = new Array[String](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) =
                if (nulls.contains(i)) null // scalafix:ok DisableSyntax.null
                else java.time.LocalDate.ofEpochDay(data(i).toLong).format(formatter)
              i += 1
            }
            Column.string(out, nulls)
          case _ => Column.string(Array.empty[String])
        }

      case md: Expr.MakeDate[Row] =>
        for {
          yearCol <- evalColumn(md.year, columns, ColumnType.IntType)
          monthCol <- evalColumn(md.month, columns, ColumnType.IntType)
          dayCol <- evalColumn(md.day, columns, ColumnType.IntType)
        } yield {
          (yearCol, monthCol, dayCol) match {
            case (Column.IntColumn(yd, yn), Column.IntColumn(mdata, mn), Column.IntColumn(dd, dn)) =>
              val combined = yn | mn | dn
              val out = new Array[Int](rowCount)
              var i = 0 // scalafix:ok DisableSyntax.var
              while (i < rowCount) {
                out(i) =
                  if (combined.contains(i)) 0
                  else java.time.LocalDate.of(yd(i), mdata(i), dd(i)).toEpochDay.toInt
                i += 1
              }
              Column.date(out, combined)
            case _ => Column.date(Array.empty[Int])
          }
        }

      case as: Expr.ArraySize[Row, _] =>
        val innerType = inferExprColumnType(as.expr, columns)
        evalColumn(as.expr, columns, innerType).map { col =>
          val out = new Array[Int](rowCount)
          val nulls = col match {
            case Column.AnyColumn(_, n) => n
            case _ => BitSet.empty
          }
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            out(i) =
              if (nulls.contains(i)) 0
              else
                col.getValue(i) match {
                  case s: Seq[?] => s.size
                  case _ => 0
                }
            i += 1
          }
          Column.int(out, nulls)
        }

      case ac: Expr.ArrayContains[Row, _] =>
        val arrType = inferExprColumnType(ac.expr, columns)
        val valType = inferExprColumnType(ac.value, columns)
        for {
          arrCol <- evalColumn(ac.expr, columns, arrType)
          valCol <- evalColumn(ac.value, columns, valType)
        } yield {
          val out = new Array[Boolean](rowCount)
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            val arr = arrCol.getValue(i)
            val v = valCol.getValue(i)
            out(i) = arr match {
              case s: Seq[?] => s.contains(v)
              case _ => false
            }
            i += 1
          }
          Column.boolean(out)
        }

      case _: Expr.Explode[Row, _] =>
        Left(ExecutionError.UnsupportedOperation("Explode requires Dataset-level handling"))

      case _: Expr.ArraySort[Row, _] =>
        Left(ExecutionError.UnsupportedOperation("ArraySort on untyped columns not supported"))

      case ad: Expr.ArrayDistinct[Row, _] =>
        val innerType = inferExprColumnType(ad.expr, columns)
        evalColumn(ad.expr, columns, innerType).map { col =>
          val out = new Array[Any](rowCount)
          val nulls = col match {
            case Column.AnyColumn(_, n) => n
            case _ => BitSet.empty
          }
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            out(i) =
              if (nulls.contains(i)) null // scalafix:ok DisableSyntax.null
              else
                col.getValue(i) match {
                  case s: Seq[?] => s.distinct
                  case other => other
                }
            i += 1
          }
          Column.any(out, nulls)
        }

      case au: Expr.ArrayUnion[Row, _] =>
        val innerType = inferExprColumnType(au.left, columns)
        for {
          leftCol <- evalColumn(au.left, columns, innerType)
          rightCol <- evalColumn(au.right, columns, innerType)
        } yield {
          val out = new Array[Any](rowCount)
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            (leftCol.getValue(i), rightCol.getValue(i)) match {
              case (l: Seq[?], r: Seq[?]) => out(i) = (l ++ r).distinct
              case _ => out(i) = Seq.empty
            }
            i += 1
          }
          Column.any(out)
        }

      case ai: Expr.ArrayIntersect[Row, _] =>
        val innerType = inferExprColumnType(ai.left, columns)
        for {
          leftCol <- evalColumn(ai.left, columns, innerType)
          rightCol <- evalColumn(ai.right, columns, innerType)
        } yield {
          val out = new Array[Any](rowCount)
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            (leftCol.getValue(i), rightCol.getValue(i)) match {
              case (l: Seq[?], r: Seq[?]) => out(i) = l.intersect(r)
              case _ => out(i) = Seq.empty
            }
            i += 1
          }
          Column.any(out)
        }

      case ae: Expr.ArrayExcept[Row, _] =>
        val innerType = inferExprColumnType(ae.left, columns)
        for {
          leftCol <- evalColumn(ae.left, columns, innerType)
          rightCol <- evalColumn(ae.right, columns, innerType)
        } yield {
          val out = new Array[Any](rowCount)
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            (leftCol.getValue(i), rightCol.getValue(i)) match {
              case (l: Seq[?], r: Seq[?]) => out(i) = l.diff(r)
              case _ => out(i) = Seq.empty
            }
            i += 1
          }
          Column.any(out)
        }

      case fl: Expr.Flatten[Row, _] =>
        val innerType = inferExprColumnType(fl.expr, columns)
        evalColumn(fl.expr, columns, innerType).map { col =>
          val out = new Array[Any](rowCount)
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            col.getValue(i) match {
              case s: Seq[Seq[?] @unchecked] => out(i) = s.flatten
              case other => out(i) = other
            }
            i += 1
          }
          Column.any(out)
        }

      case ea: Expr.ElementAt[Row, _] =>
        val arrType = inferExprColumnType(ea.expr, columns)
        for {
          arrCol <- evalColumn(ea.expr, columns, arrType)
          idxCol <- evalColumn(ea.index, columns, ColumnType.IntType)
        } yield {
          val out = new Array[Any](rowCount)
          val outNulls = scala.collection.mutable.BitSet.empty
          idxCol match {
            case Column.IntColumn(idxData, _) =>
              var i = 0 // scalafix:ok DisableSyntax.var
              while (i < rowCount) {
                arrCol.getValue(i) match {
                  case s: Seq[?] =>
                    val idx = idxData(i)
                    val resolved = if (idx > 0) idx - 1 else s.size + idx
                    if (resolved >= 0 && resolved < s.size) {
                      out(i) = s(resolved)
                    } else {
                      out(i) = null // scalafix:ok DisableSyntax.null
                      outNulls += i
                    }
                  case _ =>
                    out(i) = null // scalafix:ok DisableSyntax.null
                    outNulls += i
                }
                i += 1
              }
            case _ => ()
          }
          Column.any(out, BitSet.empty ++ outNulls)
        }

      case as: Expr.ArraySlice[Row, _] =>
        val innerType = inferExprColumnType(as.expr, columns)
        evalColumn(as.expr, columns, innerType).map { col =>
          val out = new Array[Any](rowCount)
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            col.getValue(i) match {
              case s: Seq[?] =>
                val start = Math.max(as.start - 1, 0)
                out(i) = s.slice(start, start + as.length)
              case other => out(i) = other
            }
            i += 1
          }
          Column.any(out)
        }

      case mk: Expr.MapKeys[Row, _, _] =>
        val innerType = inferExprColumnType(mk.expr, columns)
        evalColumn(mk.expr, columns, innerType).map { col =>
          val out = new Array[Any](rowCount)
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            col.getValue(i) match {
              case m: Map[?, ?] => out(i) = m.keys.toSeq
              case _ => out(i) = Seq.empty
            }
            i += 1
          }
          Column.any(out)
        }

      case mv: Expr.MapValues[Row, _, _] =>
        val innerType = inferExprColumnType(mv.expr, columns)
        evalColumn(mv.expr, columns, innerType).map { col =>
          val out = new Array[Any](rowCount)
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            col.getValue(i) match {
              case m: Map[?, ?] => out(i) = m.values.toSeq
              case _ => out(i) = Seq.empty
            }
            i += 1
          }
          Column.any(out)
        }

      case mck: Expr.MapContainsKey[Row, _, _] =>
        val mapType = inferExprColumnType(mck.expr, columns)
        val keyType = inferExprColumnType(mck.key, columns)
        for {
          mapCol <- evalColumn(mck.expr, columns, mapType)
          keyCol <- evalColumn(mck.key, columns, keyType)
        } yield {
          val out = new Array[Boolean](rowCount)
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            mapCol.getValue(i) match {
              case m: Map[?, ?] =>
                out(i) = m.keys.exists(java.util.Objects.equals(_, keyCol.getValue(i)))
              case _ => out(i) = false
            }
            i += 1
          }
          Column.boolean(out)
        }

      case me: Expr.MapEntries[Row, _, _] =>
        val innerType = inferExprColumnType(me.expr, columns)
        evalColumn(me.expr, columns, innerType).map { col =>
          val out = new Array[Any](rowCount)
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            col.getValue(i) match {
              case m: Map[?, ?] => out(i) = m.toSeq
              case _ => out(i) = Seq.empty
            }
            i += 1
          }
          Column.any(out)
        }

      case mfa: Expr.MapFromArrays[Row, _, _] =>
        val keysType = inferExprColumnType(mfa.keys, columns)
        val valsType = inferExprColumnType(mfa.values, columns)
        for {
          keysCol <- evalColumn(mfa.keys, columns, keysType)
          valsCol <- evalColumn(mfa.values, columns, valsType)
        } yield {
          val out = new Array[Any](rowCount)
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            (keysCol.getValue(i), valsCol.getValue(i)) match {
              case (ks: Seq[?], vs: Seq[?]) => out(i) = ks.zip(vs).toMap
              case _ => out(i) = Map.empty
            }
            i += 1
          }
          Column.any(out)
        }

      case mc: Expr.MapConcat[Row, _, _] =>
        val innerType = inferExprColumnType(mc.left, columns)
        for {
          leftCol <- evalColumn(mc.left, columns, innerType)
          rightCol <- evalColumn(mc.right, columns, innerType)
        } yield {
          val out = new Array[Any](rowCount)
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            (leftCol.getValue(i), rightCol.getValue(i)) match {
              case (l: Map[?, ?], r: Map[?, ?]) =>
                out(i) = (l.toSeq ++ r.toSeq).toMap
              case _ => out(i) = Map.empty
            }
            i += 1
          }
          Column.any(out)
        }

      case md: Expr.Md5[Row] =>
        vectorizedStringHash(md.expr, columns, rowCount)("MD5")

      case sh: Expr.Sha1[Row] =>
        vectorizedStringHash(sh.expr, columns, rowCount)("SHA-1")

      case sh2: Expr.Sha2[Row] =>
        vectorizedStringHash(sh2.expr, columns, rowCount)(sha2Algorithm(sh2.bitLength))

      case ue: Expr.UrlEncode[Row] =>
        evalColumn(ue.expr, columns, ColumnType.StringType).map {
          case Column.StringColumn(data, nulls) =>
            val out = new Array[String](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) =
                if (nulls.contains(i)) null // scalafix:ok DisableSyntax.null
                else java.net.URLEncoder.encode(data(i), "UTF-8").nn
              i += 1
            }
            Column.string(out, nulls)
          case _ => Column.string(Array.empty[String])
        }

      case ud: Expr.UrlDecode[Row] =>
        evalColumn(ud.expr, columns, ColumnType.StringType).map {
          case Column.StringColumn(data, nulls) =>
            val out = new Array[String](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) =
                if (nulls.contains(i)) null // scalafix:ok DisableSyntax.null
                else java.net.URLDecoder.decode(data(i), "UTF-8").nn
              i += 1
            }
            Column.string(out, nulls)
          case _ => Column.string(Array.empty[String])
        }

      case b64e: Expr.Base64Encode[Row] =>
        evalColumn(b64e.expr, columns, ColumnType.StringType).map {
          case Column.StringColumn(data, nulls) =>
            val encoder = java.util.Base64.getEncoder.nn
            val out = new Array[String](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) =
                if (nulls.contains(i)) null // scalafix:ok DisableSyntax.null
                else encoder.encodeToString(data(i).getBytes("UTF-8")).nn
              i += 1
            }
            Column.string(out, nulls)
          case _ => Column.string(Array.empty[String])
        }

      case b64d: Expr.Base64Decode[Row] =>
        evalColumn(b64d.expr, columns, ColumnType.StringType).map {
          case Column.StringColumn(data, nulls) =>
            val decoder = java.util.Base64.getDecoder.nn
            val out = new Array[String](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) =
                if (nulls.contains(i)) null // scalafix:ok DisableSyntax.null
                else new String(decoder.decode(data(i)), "UTF-8")
              i += 1
            }
            Column.string(out, nulls)
          case _ => Column.string(Array.empty[String])
        }

      case hx: Expr.Hex[Row] =>
        evalColumn(hx.expr, columns, ColumnType.StringType).map {
          case Column.StringColumn(data, nulls) =>
            val out = new Array[String](rowCount)
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) =
                if (nulls.contains(i)) null // scalafix:ok DisableSyntax.null
                else hexEncode(data(i).getBytes("UTF-8"))
              i += 1
            }
            Column.string(out, nulls)
          case _ => Column.string(Array.empty[String])
        }

      case gjo: Expr.GetJsonObject[Row] =>
        evalColumn(gjo.expr, columns, ColumnType.StringType).map {
          case Column.StringColumn(data, nulls) =>
            val out = new Array[String](rowCount)
            val outNulls = scala.collection.mutable.BitSet.empty
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              if (nulls.contains(i)) {
                out(i) = null // scalafix:ok DisableSyntax.null
                outNulls += i
              } else {
                extractJsonPath(data(i), gjo.path) match {
                  case Right(v) =>
                    if (Option(v).isEmpty) {
                      out(i) = null // scalafix:ok DisableSyntax.null
                      outNulls += i
                    } else {
                      out(i) = v
                    }
                  case Left(_) =>
                    out(i) = null // scalafix:ok DisableSyntax.null
                    outNulls += i
                }
              }
              i += 1
            }
            Column.string(out, BitSet.empty ++ outNulls)
          case _ => Column.string(Array.empty[String])
        }

      case _: Expr.Sum[Row] | _: Expr.SumDouble[Row] | _: Expr.SumLong[Row] | _: Expr.Count[Row] | _: Expr.Max[Row, ?] |
          _: Expr.Min[Row, ?] | _: Expr.Avg[Row] | _: Expr.CountDistinct[Row, ?] | _: Expr.CountIf[Row] |
          _: Expr.StdDev[Row] | _: Expr.StdDevPop[Row] | _: Expr.First[Row, ?] | _: Expr.Collect[Row, ?] |
          _: Expr.PercentileApprox[Row] | _: Expr.MaxBy[Row, ?, ?] | _: Expr.MinBy[Row, ?, ?] | _: Expr.MaxN[Row, ?] |
          _: Expr.MinN[Row, ?] | _: Expr.MaxByN[Row, ?, ?] | _: Expr.MinByN[Row, ?, ?] | _: Expr.Variance[Row] |
          _: Expr.VariancePop[Row] | _: Expr.ApproxCountDistinct[Row, ?] | _: Expr.CollectSet[Row, ?] |
          _: Expr.ExprLast[Row, ?] | _: Expr.AnyValue[Row, ?] | _: Expr.BoolAnd[Row] | _: Expr.BoolOr[Row] |
          _: Expr.Corr[Row] | _: Expr.CovarSamp[Row] | _: Expr.CovarPop[Row] | _: Expr.Median[Row] |
          _: Expr.Mode[Row, ?] =>
        Left(ExecutionError.UnsupportedOperation("Aggregations not supported in columnar evalColumn"))

      case _: Expr.RowNumber[Row] | _: Expr.Rank[Row] | _: Expr.DenseRank[Row] | _: Expr.Lag[Row, ?] |
          _: Expr.Lead[Row, ?] | _: Expr.NTile[Row] | _: Expr.CumeDist[Row] | _: Expr.PercentRank[Row] |
          _: Expr.NthValue[Row, ?] | _: Expr.FirstValue[Row, ?] | _: Expr.LastValue[Row, ?] =>
        Left(ExecutionError.UnsupportedOperation("Window functions not supported in columnar evalColumn"))
    }
  }

  private def vectorizedIntBinOp[Row](
    left: Expr[Row, Int],
    right: Expr[Row, Int],
    columns: Vector[Column[?]],
    rowCount: Int
  )(op: (Int, Int) => Int): Either[ExecutionError, Column[?]] = {
    for {
      leftCol <- evalColumn(left, columns, ColumnType.IntType)
      rightCol <- evalColumn(right, columns, ColumnType.IntType)
    } yield {
      (leftCol, rightCol) match {
        case (Column.IntColumn(ld, ln), Column.IntColumn(rd, rn)) =>
          val out = new Array[Int](rowCount)
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) { out(i) = op(ld(i), rd(i)); i += 1 }
          Column.int(out, ln | rn)
        case _ =>
          Column.int(Array.empty[Int])
      }
    }
  }

  private def vectorizedDiv(left: Array[Int], right: Array[Int], rowCount: Int): Either[ExecutionError, Column[?]] = {
    val out = new Array[Int](rowCount)
    var i = 0 // scalafix:ok DisableSyntax.var
    while (i < rowCount) {
      if (right(i) == 0) return Left(ExecutionError.DivisionByZero(i)) // scalafix:ok DisableSyntax.return
      out(i) = left(i) / right(i)
      i += 1
    }
    Right(Column.int(out))
  }

  private def vectorizedLongBinOp[Row](
    left: Expr[Row, Long],
    right: Expr[Row, Long],
    columns: Vector[Column[?]],
    rowCount: Int
  )(op: (Long, Long) => Long): Either[ExecutionError, Column[?]] = {
    for {
      leftCol <- evalColumn(left, columns, ColumnType.LongType)
      rightCol <- evalColumn(right, columns, ColumnType.LongType)
    } yield {
      (leftCol, rightCol) match {
        case (Column.LongColumn(ld, ln), Column.LongColumn(rd, rn)) =>
          val out = new Array[Long](rowCount)
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) { out(i) = op(ld(i), rd(i)); i += 1 }
          Column.long(out, ln | rn)
        case _ =>
          Column.long(Array.empty[Long])
      }
    }
  }

  private def vectorizedLongDiv(
    left: Array[Long],
    right: Array[Long],
    rowCount: Int
  ): Either[ExecutionError, Column[?]] = {
    val out = new Array[Long](rowCount)
    var i = 0 // scalafix:ok DisableSyntax.var
    while (i < rowCount) {
      if (right(i) == 0L) return Left(ExecutionError.DivisionByZero(i)) // scalafix:ok DisableSyntax.return
      out(i) = left(i) / right(i)
      i += 1
    }
    Right(Column.long(out))
  }

  private def vectorizedDoubleBinOp[Row](
    left: Expr[Row, Double],
    right: Expr[Row, Double],
    columns: Vector[Column[?]],
    rowCount: Int
  )(op: (Double, Double) => Double): Either[ExecutionError, Column[?]] = {
    for {
      leftCol <- evalColumn(left, columns, ColumnType.DoubleType)
      rightCol <- evalColumn(right, columns, ColumnType.DoubleType)
    } yield {
      (leftCol, rightCol) match {
        case (Column.DoubleColumn(ld, ln), Column.DoubleColumn(rd, rn)) =>
          val out = new Array[Double](rowCount)
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) { out(i) = op(ld(i), rd(i)); i += 1 }
          Column.double(out, ln | rn)
        case _ =>
          Column.double(Array.empty[Double])
      }
    }
  }

  private def vectorizedDoubleDiv(
    left: Array[Double],
    right: Array[Double],
    rowCount: Int
  ): Either[ExecutionError, Column[?]] = {
    val out = new Array[Double](rowCount)
    var i = 0 // scalafix:ok DisableSyntax.var
    while (i < rowCount) {
      if (right(i) == 0.0) return Left(ExecutionError.DivisionByZero(i)) // scalafix:ok DisableSyntax.return
      out(i) = left(i) / right(i)
      i += 1
    }
    Right(Column.double(out))
  }

  private def typedComparison[Row, A](
    left: Expr[Row, A],
    right: Expr[Row, A],
    columns: Vector[Column[?]],
    rowCount: Int
  )(
    intCmp: (Int, Int) => Boolean,
    longCmp: (Long, Long) => Boolean,
    doubleCmp: (Double, Double) => Boolean,
    stringCmp: (String, String) => Boolean
  ): Either[ExecutionError, Column[?]] = {
    val opType = inferExprColumnType(left, columns)
    for {
      leftCol <- evalColumn(left, columns, opType)
      rightCol <- evalColumn(right, columns, opType)
      result <- {
        val out = new Array[Boolean](rowCount)
        (leftCol, rightCol) match {
          case (Column.IntColumn(ld, ln), Column.IntColumn(rd, rn)) =>
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) { out(i) = intCmp(ld(i), rd(i)); i += 1 }
            Right(Column.boolean(out, ln | rn))
          case (Column.LongColumn(ld, ln), Column.LongColumn(rd, rn)) =>
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) { out(i) = longCmp(ld(i), rd(i)); i += 1 }
            Right(Column.boolean(out, ln | rn))
          case (Column.DoubleColumn(ld, ln), Column.DoubleColumn(rd, rn)) =>
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) { out(i) = doubleCmp(ld(i), rd(i)); i += 1 }
            Right(Column.boolean(out, ln | rn))
          case (Column.StringColumn(ld, ln), Column.StringColumn(rd, rn)) =>
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) {
              out(i) = if (ln.contains(i) || rn.contains(i)) false else stringCmp(ld(i), rd(i))
              i += 1
            }
            Right(Column.boolean(out, ln | rn))
          case (Column.DateColumn(ld, ln), Column.DateColumn(rd, rn)) =>
            var i = 0 // scalafix:ok DisableSyntax.var
            while (i < rowCount) { out(i) = intCmp(ld(i), rd(i)); i += 1 }
            Right(Column.boolean(out, ln | rn))
          case _ =>
            Left(ExecutionError.UnsupportedOperation("Comparison not supported for untyped columns"))
        }
      }
    } yield result
  }

  private def equalityComparison[Row, A](
    left: Expr[Row, A],
    right: Expr[Row, A],
    columns: Vector[Column[?]],
    rowCount: Int
  )(cmp: (Any, Any) => Boolean): Either[ExecutionError, Column[?]] = {
    val opType = inferExprColumnType(left, columns)
    for {
      leftCol <- evalColumn(left, columns, opType)
      rightCol <- evalColumn(right, columns, opType)
    } yield {
      val out = new Array[Boolean](rowCount)
      var i = 0 // scalafix:ok DisableSyntax.var
      while (i < rowCount) {
        out(i) = cmp(leftCol.getValue(i), rightCol.getValue(i))
        i += 1
      }
      Column.boolean(out)
    }
  }

  private def vectorizedIntMod(
    left: Array[Int],
    right: Array[Int],
    rowCount: Int
  ): Either[ExecutionError, Column[?]] = {
    val out = new Array[Int](rowCount)
    var i = 0 // scalafix:ok DisableSyntax.var
    while (i < rowCount) {
      if (right(i) == 0) return Left(ExecutionError.DivisionByZero(i)) // scalafix:ok DisableSyntax.return
      out(i) = left(i) % right(i)
      i += 1
    }
    Right(Column.int(out))
  }

  private def vectorizedLongMod(
    left: Array[Long],
    right: Array[Long],
    rowCount: Int
  ): Either[ExecutionError, Column[?]] = {
    val out = new Array[Long](rowCount)
    var i = 0 // scalafix:ok DisableSyntax.var
    while (i < rowCount) {
      if (right(i) == 0L) return Left(ExecutionError.DivisionByZero(i)) // scalafix:ok DisableSyntax.return
      out(i) = left(i) % right(i)
      i += 1
    }
    Right(Column.long(out))
  }

  private def vectorizedDoubleUnaryOp[Row](
    expr: Expr[Row, Double],
    columns: Vector[Column[?]],
    rowCount: Int
  )(op: Double => Double): Either[ExecutionError, Column[?]] = {
    evalColumn(expr, columns, ColumnType.DoubleType).map {
      case Column.DoubleColumn(data, nulls) =>
        val out = new Array[Double](rowCount)
        var i = 0 // scalafix:ok DisableSyntax.var
        while (i < rowCount) { out(i) = op(data(i)); i += 1 }
        Column.double(out, nulls)
      case _ =>
        Column.double(Array.empty[Double])
    }
  }

  private def vectorizedStringHash[Row](
    expr: Expr[Row, String],
    columns: Vector[Column[?]],
    rowCount: Int
  )(algorithm: String): Either[ExecutionError, Column[?]] = {
    evalColumn(expr, columns, ColumnType.StringType).map {
      case Column.StringColumn(data, nulls) =>
        val digest = java.security.MessageDigest.getInstance(algorithm).nn
        val out = new Array[String](rowCount)
        var i = 0 // scalafix:ok DisableSyntax.var
        while (i < rowCount) {
          if (nulls.contains(i)) {
            out(i) = null // scalafix:ok DisableSyntax.null
          } else {
            digest.reset()
            out(i) = hexEncode(digest.digest(data(i).getBytes("UTF-8")).nn)
          }
          i += 1
        }
        Column.string(out, nulls)
      case _ =>
        Column.string(Array.empty[String])
    }
  }

  /** Evaluate aggregation expression over entire dataset.
    *
    * Aggregations operate on all rows to produce a single value. GADT pattern matching refines the
    * return type A for each case — e.g. matching Expr.Sum[Row] refines A to Int, so Right(total)
    * where total: Int typechecks as Either[ExecutionError, A] without any cast.
    *
    * Fixed-type aggregations (Sum, Avg, StdDev, etc.) operate on typed columns directly via
    * evalColumn for columnar performance. Generic aggregations (Max, Collect, etc.) use per-row
    * eval to preserve GADT type evidence through the existential type parameter.
    *
    * Zero asInstanceOf — all type safety comes from GADT refinement.
    */
  def evalAggregation[Row, A](
    expr: Expr[Row, A],
    columns: Vector[Column[?]]
  ): Either[ExecutionError, A] = {
    if (columns.isEmpty || columns.head.length == 0) {
      (expr: @unchecked) match {
        case _: Expr.Count[Row] => Right(0L)
        case _: Expr.Sum[Row] => Right(0)
        case _: Expr.SumDouble[Row] => Right(0.0)
        case _: Expr.SumLong[Row] => Right(0L)
        case _: Expr.Avg[Row] => Right(0.0)
        case _: Expr.Max[Row, ?] => Right(None)
        case _: Expr.Min[Row, ?] => Right(None)
        case _: Expr.CountDistinct[Row, ?] => Right(0L)
        case _: Expr.CountIf[Row] => Right(0L)
        case _: Expr.StdDev[Row] => Right(0.0)
        case _: Expr.StdDevPop[Row] => Right(0.0)
        case _: Expr.First[Row, ?] => Right(None)
        case _: Expr.Collect[Row, a] => Right(Seq.empty[a])
        case _: Expr.PercentileApprox[Row] => Right(0.0)
        case _: Expr.MaxBy[Row, ?, ?] => Right(None)
        case _: Expr.MinBy[Row, ?, ?] => Right(None)
        case _: Expr.MaxN[Row, a] => Right(Seq.empty[a])
        case _: Expr.MinN[Row, a] => Right(Seq.empty[a])
        case _: Expr.MaxByN[Row, a, ?] => Right(Seq.empty[a])
        case _: Expr.MinByN[Row, a, ?] => Right(Seq.empty[a])
        case _: Expr.Variance[Row] => Right(0.0)
        case _: Expr.VariancePop[Row] => Right(0.0)
        case _: Expr.ApproxCountDistinct[Row, ?] => Right(0L)
        case _: Expr.CollectSet[Row, a] => Right(Seq.empty[a])
        case _: Expr.ExprLast[Row, ?] => Right(None)
        case _: Expr.AnyValue[Row, ?] => Right(None)
        case _: Expr.BoolAnd[Row] => Right(true)
        case _: Expr.BoolOr[Row] => Right(false)
        case _: Expr.Corr[Row] => Right(0.0)
        case _: Expr.CovarSamp[Row] => Right(0.0)
        case _: Expr.CovarPop[Row] => Right(0.0)
        case _: Expr.Median[Row] => Right(0.0)
        case _: Expr.Mode[Row, ?] => Right(None)
      }
    } else {
      val rowCount = columns.head.length
      (expr: @unchecked) match {
        case Expr.Count() =>
          Right(rowCount.toLong)

        case sum: Expr.Sum[Row] =>
          evalColumn(sum.expr, columns, ColumnType.IntType).map { col =>
            col match {
              case Column.IntColumn(data, nulls) =>
                var total = 0 // scalafix:ok DisableSyntax.var
                var i = 0 // scalafix:ok DisableSyntax.var
                while (i < data.length) {
                  if (!nulls.contains(i)) total += data(i)
                  i += 1
                }
                total
              case _ => 0
            }
          }

        case sumD: Expr.SumDouble[Row] =>
          evalColumn(sumD.expr, columns, ColumnType.DoubleType).map { col =>
            col match {
              case Column.DoubleColumn(data, nulls) =>
                var total = 0.0 // scalafix:ok DisableSyntax.var
                var i = 0 // scalafix:ok DisableSyntax.var
                while (i < data.length) {
                  if (!nulls.contains(i)) total += data(i)
                  i += 1
                }
                total
              case _ => 0.0
            }
          }

        case sumL: Expr.SumLong[Row] =>
          evalColumn(sumL.expr, columns, ColumnType.LongType).map { col =>
            col match {
              case Column.LongColumn(data, nulls) =>
                var total = 0L // scalafix:ok DisableSyntax.var
                var i = 0 // scalafix:ok DisableSyntax.var
                while (i < data.length) {
                  if (!nulls.contains(i)) total += data(i)
                  i += 1
                }
                total
              case _ => 0L
            }
          }

        case avg: Expr.Avg[Row] =>
          evalColumn(avg.expr, columns, ColumnType.DoubleType).map { col =>
            col match {
              case Column.DoubleColumn(data, nulls) =>
                var total = 0.0 // scalafix:ok DisableSyntax.var
                var count = 0 // scalafix:ok DisableSyntax.var
                var i = 0 // scalafix:ok DisableSyntax.var
                while (i < data.length) {
                  if (!nulls.contains(i)) { total += data(i); count += 1 }
                  i += 1
                }
                if (count == 0) 0.0 else total / count
              case _ => 0.0
            }
          }

        case countIf: Expr.CountIf[Row] =>
          evalColumn(countIf.predicate, columns, ColumnType.BooleanType).map { col =>
            col match {
              case Column.BooleanColumn(data, nulls) =>
                var count = 0L // scalafix:ok DisableSyntax.var
                var i = 0 // scalafix:ok DisableSyntax.var
                while (i < data.length) {
                  if (!nulls.contains(i) && data(i)) count += 1L
                  i += 1
                }
                count
              case _ => 0L
            }
          }

        case sd: Expr.StdDev[Row] =>
          aggregateDoubleExpr(sd.expr, columns) { (data, nulls) =>
            val (sum, count) = sumAndCount(data, nulls)
            if (count <= 1) 0.0
            else {
              val mean = sum / count
              var variance = 0.0 // scalafix:ok DisableSyntax.var
              var i = 0 // scalafix:ok DisableSyntax.var
              while (i < data.length) {
                if (!nulls.contains(i)) { val d = data(i) - mean; variance += d * d }
                i += 1
              }
              math.sqrt(variance / (count - 1))
            }
          }

        case sdp: Expr.StdDevPop[Row] =>
          aggregateDoubleExpr(sdp.expr, columns) { (data, nulls) =>
            val (sum, count) = sumAndCount(data, nulls)
            if (count == 0) 0.0
            else {
              val mean = sum / count
              var variance = 0.0 // scalafix:ok DisableSyntax.var
              var i = 0 // scalafix:ok DisableSyntax.var
              while (i < data.length) {
                if (!nulls.contains(i)) { val d = data(i) - mean; variance += d * d }
                i += 1
              }
              math.sqrt(variance / count)
            }
          }

        case pct: Expr.PercentileApprox[Row] =>
          aggregateDoubleExpr(pct.expr, columns) { (data, nulls) =>
            val nonNull = collectNonNullDoubles(data, nulls)
            if (nonNull.isEmpty) 0.0
            else {
              java.util.Arrays.sort(nonNull)
              val idx = math.min((nonNull.length * pct.percentile).toInt, nonNull.length - 1)
              nonNull(idx)
            }
          }

        case ba: Expr.BoolAnd[Row] =>
          evalColumn(ba.expr, columns, ColumnType.BooleanType).map { col =>
            col match {
              case Column.BooleanColumn(data, nulls) =>
                var result = true // scalafix:ok DisableSyntax.var
                var i = 0 // scalafix:ok DisableSyntax.var
                while (i < data.length && result) {
                  if (!nulls.contains(i) && !data(i)) result = false
                  i += 1
                }
                result
              case _ => true
            }
          }

        case bo: Expr.BoolOr[Row] =>
          evalColumn(bo.expr, columns, ColumnType.BooleanType).map { col =>
            col match {
              case Column.BooleanColumn(data, nulls) =>
                var result = false // scalafix:ok DisableSyntax.var
                var i = 0 // scalafix:ok DisableSyntax.var
                while (i < data.length && !result) {
                  if (!nulls.contains(i) && data(i)) result = true
                  i += 1
                }
                result
              case _ => false
            }
          }

        case v: Expr.Variance[Row] =>
          aggregateDoubleExpr(v.expr, columns) { (data, nulls) =>
            val (sum, count) = sumAndCount(data, nulls)
            if (count <= 1) 0.0
            else {
              val mean = sum / count
              var variance = 0.0 // scalafix:ok DisableSyntax.var
              var i = 0 // scalafix:ok DisableSyntax.var
              while (i < data.length) {
                if (!nulls.contains(i)) { val d = data(i) - mean; variance += d * d }
                i += 1
              }
              variance / (count - 1)
            }
          }

        case vp: Expr.VariancePop[Row] =>
          aggregateDoubleExpr(vp.expr, columns) { (data, nulls) =>
            val (sum, count) = sumAndCount(data, nulls)
            if (count == 0) 0.0
            else {
              val mean = sum / count
              var variance = 0.0 // scalafix:ok DisableSyntax.var
              var i = 0 // scalafix:ok DisableSyntax.var
              while (i < data.length) {
                if (!nulls.contains(i)) { val d = data(i) - mean; variance += d * d }
                i += 1
              }
              variance / count
            }
          }

        case corr: Expr.Corr[Row] =>
          for {
            leftCol <- evalColumn(corr.left, columns, ColumnType.DoubleType)
            rightCol <- evalColumn(corr.right, columns, ColumnType.DoubleType)
          } yield {
            (leftCol, rightCol) match {
              case (Column.DoubleColumn(xData, xNulls), Column.DoubleColumn(yData, yNulls)) =>
                val combinedNulls = xNulls | yNulls
                val (xSum, n) = sumAndCount(xData, combinedNulls)
                val (ySum, _) = sumAndCount(yData, combinedNulls)
                if (n <= 1) 0.0
                else {
                  val xMean = xSum / n
                  val yMean = ySum / n
                  var cov = 0.0 // scalafix:ok DisableSyntax.var
                  var xVar = 0.0 // scalafix:ok DisableSyntax.var
                  var yVar = 0.0 // scalafix:ok DisableSyntax.var
                  var i = 0 // scalafix:ok DisableSyntax.var
                  while (i < xData.length) {
                    if (!combinedNulls.contains(i)) {
                      val dx = xData(i) - xMean
                      val dy = yData(i) - yMean
                      cov += dx * dy
                      xVar += dx * dx
                      yVar += dy * dy
                    }
                    i += 1
                  }
                  val denom = math.sqrt(xVar * yVar)
                  if (denom == 0.0) 0.0 else cov / denom
                }
              case _ => 0.0
            }
          }

        case cs: Expr.CovarSamp[Row] =>
          for {
            leftCol <- evalColumn(cs.left, columns, ColumnType.DoubleType)
            rightCol <- evalColumn(cs.right, columns, ColumnType.DoubleType)
          } yield {
            (leftCol, rightCol) match {
              case (Column.DoubleColumn(xData, xNulls), Column.DoubleColumn(yData, yNulls)) =>
                val combinedNulls = xNulls | yNulls
                val (xSum, n) = sumAndCount(xData, combinedNulls)
                val (ySum, _) = sumAndCount(yData, combinedNulls)
                if (n <= 1) 0.0
                else {
                  val xMean = xSum / n
                  val yMean = ySum / n
                  var cov = 0.0 // scalafix:ok DisableSyntax.var
                  var i = 0 // scalafix:ok DisableSyntax.var
                  while (i < xData.length) {
                    if (!combinedNulls.contains(i)) cov += (xData(i) - xMean) * (yData(i) - yMean)
                    i += 1
                  }
                  cov / (n - 1)
                }
              case _ => 0.0
            }
          }

        case cp: Expr.CovarPop[Row] =>
          for {
            leftCol <- evalColumn(cp.left, columns, ColumnType.DoubleType)
            rightCol <- evalColumn(cp.right, columns, ColumnType.DoubleType)
          } yield {
            (leftCol, rightCol) match {
              case (Column.DoubleColumn(xData, xNulls), Column.DoubleColumn(yData, yNulls)) =>
                val combinedNulls = xNulls | yNulls
                val (xSum, n) = sumAndCount(xData, combinedNulls)
                val (ySum, _) = sumAndCount(yData, combinedNulls)
                if (n == 0) 0.0
                else {
                  val xMean = xSum / n
                  val yMean = ySum / n
                  var cov = 0.0 // scalafix:ok DisableSyntax.var
                  var i = 0 // scalafix:ok DisableSyntax.var
                  while (i < xData.length) {
                    if (!combinedNulls.contains(i)) cov += (xData(i) - xMean) * (yData(i) - yMean)
                    i += 1
                  }
                  cov / n
                }
              case _ => 0.0
            }
          }

        case med: Expr.Median[Row] =>
          aggregateDoubleExpr(med.expr, columns) { (data, nulls) =>
            val nonNull = collectNonNullDoubles(data, nulls)
            if (nonNull.isEmpty) 0.0
            else {
              java.util.Arrays.sort(nonNull)
              val mid = nonNull.length / 2
              if (nonNull.length % 2 == 0) (nonNull(mid - 1) + nonNull(mid)) / 2.0
              else nonNull(mid)
            }
          }

        case countDist: Expr.CountDistinct[Row, a] =>
          val values = scala.collection.mutable.HashSet.empty[a]
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            eval(countDist.expr, columns, RowIndex(i)) match {
              case Right(v) => values += v
              case _ => ()
            }
            i += 1
          }
          Right(values.size.toLong)

        case acd: Expr.ApproxCountDistinct[Row, a] =>
          val values = scala.collection.mutable.HashSet.empty[a]
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            eval(acd.expr, columns, RowIndex(i)) match {
              case Right(v) => values += v
              case _ => ()
            }
            i += 1
          }
          Right(values.size.toLong)

        case max: Expr.Max[Row, a] =>
          var best: Option[a] = None // scalafix:ok DisableSyntax.var
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            eval(max.expr, columns, RowIndex(i)) match {
              case Right(value) =>
                best = best match {
                  case None => Some(value)
                  case Some(m) => Some(if (max.ordering.gt(value, m)) value else m)
                }
              case _ => ()
            }
            i += 1
          }
          Right(best)

        case min: Expr.Min[Row, a] =>
          var best: Option[a] = None // scalafix:ok DisableSyntax.var
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            eval(min.expr, columns, RowIndex(i)) match {
              case Right(value) =>
                best = best match {
                  case None => Some(value)
                  case Some(m) => Some(if (min.ordering.lt(value, m)) value else m)
                }
              case _ => ()
            }
            i += 1
          }
          Right(best)

        case first: Expr.First[Row, a] =>
          var result: Option[a] = None // scalafix:ok DisableSyntax.var
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount && result.isEmpty) {
            eval(first.expr, columns, RowIndex(i)) match {
              case Right(v) => result = Some(v)
              case _ => ()
            }
            i += 1
          }
          Right(result)

        case last: Expr.ExprLast[Row, a] =>
          var result: Option[a] = None // scalafix:ok DisableSyntax.var
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            eval(last.expr, columns, RowIndex(i)) match {
              case Right(v) => result = Some(v)
              case _ => ()
            }
            i += 1
          }
          Right(result)

        case anyVal: Expr.AnyValue[Row, a] =>
          var result: Option[a] = None // scalafix:ok DisableSyntax.var
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount && result.isEmpty) {
            eval(anyVal.expr, columns, RowIndex(i)) match {
              case Right(v) => result = Some(v)
              case _ => ()
            }
            i += 1
          }
          Right(result)

        case mode: Expr.Mode[Row, a] =>
          val counts = scala.collection.mutable.LinkedHashMap.empty[a, Int]
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            eval(mode.expr, columns, RowIndex(i)) match {
              case Right(v) => counts(v) = counts.getOrElse(v, 0) + 1
              case _ => ()
            }
            i += 1
          }
          Right(if (counts.isEmpty) None else Some(counts.maxBy(_._2)._1))

        case collect: Expr.Collect[Row, a] =>
          val builder = Vector.newBuilder[a]
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            eval(collect.expr, columns, RowIndex(i)) match {
              case Right(v) => builder += v
              case _ => ()
            }
            i += 1
          }
          Right(builder.result().toSeq)

        case cs: Expr.CollectSet[Row, a] =>
          val set = scala.collection.mutable.LinkedHashSet.empty[a]
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            eval(cs.expr, columns, RowIndex(i)) match {
              case Right(v) => set += v
              case _ => ()
            }
            i += 1
          }
          Right(set.toSeq)

        case mb: Expr.MaxBy[Row, a, k] =>
          var bestValue: Option[a] = None // scalafix:ok DisableSyntax.var
          var bestKey: Option[k] = None // scalafix:ok DisableSyntax.var
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            (eval(mb.valueExpr, columns, RowIndex(i)), eval(mb.orderExpr, columns, RowIndex(i))) match {
              case (Right(value), Right(key)) =>
                bestKey match {
                  case None =>
                    bestValue = Some(value)
                    bestKey = Some(key)
                  case Some(bk) if mb.ordering.gt(key, bk) =>
                    bestValue = Some(value)
                    bestKey = Some(key)
                  case _ => ()
                }
              case _ => ()
            }
            i += 1
          }
          Right(bestValue)

        case mb: Expr.MinBy[Row, a, k] =>
          var bestValue: Option[a] = None // scalafix:ok DisableSyntax.var
          var bestKey: Option[k] = None // scalafix:ok DisableSyntax.var
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            (eval(mb.valueExpr, columns, RowIndex(i)), eval(mb.orderExpr, columns, RowIndex(i))) match {
              case (Right(value), Right(key)) =>
                bestKey match {
                  case None =>
                    bestValue = Some(value)
                    bestKey = Some(key)
                  case Some(bk) if mb.ordering.lt(key, bk) =>
                    bestValue = Some(value)
                    bestKey = Some(key)
                  case _ => ()
                }
              case _ => ()
            }
            i += 1
          }
          Right(bestValue)

        case mn: Expr.MaxN[Row, a] =>
          val values = Vector.newBuilder[a]
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            eval(mn.expr, columns, RowIndex(i)) match {
              case Right(v) => values += v
              case _ => ()
            }
            i += 1
          }
          Right(values.result().sorted(using mn.ordering.reverse).take(mn.n).toSeq)

        case mn: Expr.MinN[Row, a] =>
          val values = Vector.newBuilder[a]
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            eval(mn.expr, columns, RowIndex(i)) match {
              case Right(v) => values += v
              case _ => ()
            }
            i += 1
          }
          Right(values.result().sorted(using mn.ordering).take(mn.n).toSeq)

        case mbn: Expr.MaxByN[Row, a, k] =>
          val pairs = Vector.newBuilder[(a, k)]
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            (eval(mbn.valueExpr, columns, RowIndex(i)), eval(mbn.orderExpr, columns, RowIndex(i))) match {
              case (Right(value), Right(key)) => pairs += ((value, key))
              case _ => ()
            }
            i += 1
          }
          Right(pairs.result().sortBy(_._2)(using mbn.ordering.reverse).take(mbn.n).map(_._1).toSeq)

        case mbn: Expr.MinByN[Row, a, k] =>
          val pairs = Vector.newBuilder[(a, k)]
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            (eval(mbn.valueExpr, columns, RowIndex(i)), eval(mbn.orderExpr, columns, RowIndex(i))) match {
              case (Right(value), Right(key)) => pairs += ((value, key))
              case _ => ()
            }
            i += 1
          }
          Right(pairs.result().sortBy(_._2)(using mbn.ordering).take(mbn.n).map(_._1).toSeq)
      }
    }
  }

  /** Helper: evaluate a Double-typed sub-expression to a DoubleColumn and aggregate it.
    *
    * Takes the inner sub-expression (not the aggregation wrapper), evaluates to DoubleColumn, then
    * applies the aggregation function on the raw Array[Double] and BitSet nulls.
    */
  private def aggregateDoubleExpr[Row](
    subExpr: Expr[Row, Double],
    columns: Vector[Column[?]]
  )(f: (Array[Double], BitSet) => Double): Either[ExecutionError, Double] = {
    evalColumn(subExpr, columns, ColumnType.DoubleType).map { col =>
      col match {
        case Column.DoubleColumn(data, nulls) => f(data, nulls)
        case _ => 0.0
      }
    }
  }

  /** Sum non-null values and count them in a single pass. */
  private def sumAndCount(data: Array[Double], nulls: BitSet): (Double, Int) = {
    var sum = 0.0 // scalafix:ok DisableSyntax.var
    var count = 0 // scalafix:ok DisableSyntax.var
    var i = 0 // scalafix:ok DisableSyntax.var
    while (i < data.length) {
      if (!nulls.contains(i)) { sum += data(i); count += 1 }
      i += 1
    }
    (sum, count)
  }

  /** Collect non-null doubles into a new array for sorting. */
  private def collectNonNullDoubles(data: Array[Double], nulls: BitSet): Array[Double] = {
    val builder = Array.newBuilder[Double]
    var i = 0 // scalafix:ok DisableSyntax.var
    while (i < data.length) {
      if (!nulls.contains(i)) builder += data(i)
      i += 1
    }
    builder.result()
  }

  private def hexEncode(bytes: Array[Byte]): String =
    bytes.map(b => String.format("%02x", b)).mkString

  private def sha2Algorithm(bitLength: Int): String = bitLength match {
    case 0 | 256 => "SHA-256"
    case 224 => "SHA-224"
    case 384 => "SHA-384"
    case 512 => "SHA-512"
    case _ => "SHA-256"
  }

  private def extractJsonPath(jsonStr: String, path: String): Either[ExecutionError, String] = {
    parseJson(jsonStr) match {
      case parser.core.Result.Success(jsonValue, _) =>
        walkJsonPath(jsonValue, parseJsonDotPath(path)) match {
          case Some(JsonValue.Str(s)) => Right(s)
          case Some(JsonValue.Null) =>
            Right(null) // scalafix:ok DisableSyntax.null
          case Some(JsonValue.Bool(b)) => Right(b.toString)
          case Some(JsonValue.Number(n)) =>
            Right(if (n == n.toLong.toDouble) n.toLong.toString else n.toString)
          case Some(compound) => Right(formatJson(compound))
          case None => Right(null) // scalafix:ok DisableSyntax.null
        }
      case _ =>
        Left(ExecutionError.InvalidValue(s"Invalid JSON: ${jsonStr.take(100)}"))
    }
  }

  private def parseJsonDotPath(path: String): List[String] = {
    val stripped = if (path.startsWith("$.")) path.drop(2) else if (path.startsWith("$")) path.drop(1) else path
    stripped.split('.').filter(_.nonEmpty).toList
  }

  private def walkJsonPath(value: JsonValue, segments: List[String]): Option[JsonValue] = {
    segments match {
      case Nil => Some(value)
      case head :: tail =>
        value match {
          case JsonValue.Object(fields) => fields.get(head).flatMap(walkJsonPath(_, tail))
          case _ => None
        }
    }
  }

  /** Convert a SQL LIKE pattern to a regex. `%` → `.*`, `_` → `.`, others escaped. */
  private def likeToRegex(pattern: String): scala.util.matching.Regex = {
    val sb = new StringBuilder("(?s)") // DOTALL so `.` matches newlines
    var i = 0 // scalafix:ok DisableSyntax.var
    while (i < pattern.length) {
      pattern.charAt(i) match {
        case '%' => sb.append(".*")
        case '_' => sb.append('.')
        case c =>
          if ("\\[]{}()^$.|*+?".indexOf(c) >= 0) sb.append('\\')
          sb.append(c)
      }
      i += 1
    }
    sb.toString.r
  }
}
