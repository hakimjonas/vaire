package net.ghoula.strongbow

import net.ghoula.strongbow.errors.ExecutionError
import net.ghoula.strongbow.types.{ColumnIndex, RowIndex}

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
    columns: Vector[Column],
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
                java.time.LocalDate
                  .ofEpochDay(column.getDateEpochDay(idx).toLong)
                  .asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
              case ColumnType.AnyType | ColumnType.OptionType(_) =>
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

      case _: Expr.Sum[Row] | _: Expr.SumDouble[Row] | _: Expr.SumLong[Row] | _: Expr.Count[Row] | _: Expr.Max[Row, ?] |
          _: Expr.Min[Row, ?] | _: Expr.Avg[Row] | _: Expr.CountDistinct[Row, ?] | _: Expr.CountIf[Row] |
          _: Expr.StdDev[Row] | _: Expr.StdDevPop[Row] | _: Expr.First[Row, ?] | _: Expr.Collect[Row, ?] |
          _: Expr.PercentileApprox[Row] | _: Expr.MaxBy[Row, ?, ?] | _: Expr.MinBy[Row, ?, ?] | _: Expr.MaxN[Row, ?] |
          _: Expr.MinN[Row, ?] | _: Expr.MaxByN[Row, ?, ?] | _: Expr.MinByN[Row, ?, ?] =>
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
        } yield java.time.temporal.ChronoUnit.DAYS.between(r, l).toInt

      case ey: Expr.ExtractYear[Row] =>
        eval(ey.date, columns, rowIdx).map(_.getYear)

      case em: Expr.ExtractMonth[Row] =>
        eval(em.date, columns, rowIdx).map(_.getMonthValue)

      case ed: Expr.ExtractDay[Row] =>
        eval(ed.date, columns, rowIdx).map(_.getDayOfMonth)

      case _: Expr.RowNumber[Row] | _: Expr.Rank[Row] | _: Expr.DenseRank[Row] | _: Expr.Lag[Row, ?] |
          _: Expr.Lead[Row, ?] =>
        Left(ExecutionError.UnsupportedOperation("Window functions not supported in row-level eval"))
    }
  }

  /** Fast-path boolean evaluation without Either wrapping.
    *
    * Eliminates the per-row Either allocation that is pure waste for filter predicates. For
    * comparison and boolean cases, calls typed accessors directly and returns primitive boolean.
    * Short-circuits And/Or (current `eval` evaluates both sides unconditionally).
    *
    * Throws on actual errors (programming bugs in filter predicates, not data quality issues).
    */
  def evalBoolean[Row](
    expr: Expr[Row, Boolean],
    columns: Vector[Column],
    rowIdx: RowIndex
  ): Boolean = {
    (expr: @unchecked) match {
      case Expr.Const(value) =>
        value

      case named: Expr.Named[Row, _] =>
        evalBoolean(
          named.expr.asInstanceOf[Expr[Row, Boolean]],
          columns,
          rowIdx
        ) // scalafix:ok DisableSyntax.asInstanceOf

      case cell: Expr.Cell[Row, _] =>
        val column = columns(cell.index.toInt)
        column.getBoolean(rowIdx.toInt)

      case eq: Expr.Eq[Row, _] =>
        val l = evalAny(eq.left, columns, rowIdx)
        val r = evalAny(eq.right, columns, rowIdx)
        java.util.Objects.equals(l, r)

      case neq: Expr.Neq[Row, _] =>
        val l = evalAny(neq.left, columns, rowIdx)
        val r = evalAny(neq.right, columns, rowIdx)
        !java.util.Objects.equals(l, r)

      case gt: Expr.Gt[Row, a] =>
        val l = evalAny(gt.left, columns, rowIdx).asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
        val r = evalAny(gt.right, columns, rowIdx).asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
        gt.ordering.gt(l, r)

      case lt: Expr.Lt[Row, a] =>
        val l = evalAny(lt.left, columns, rowIdx).asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
        val r = evalAny(lt.right, columns, rowIdx).asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
        lt.ordering.lt(l, r)

      case gte: Expr.Gte[Row, a] =>
        val l = evalAny(gte.left, columns, rowIdx).asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
        val r = evalAny(gte.right, columns, rowIdx).asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
        gte.ordering.gteq(l, r)

      case lte: Expr.Lte[Row, a] =>
        val l = evalAny(lte.left, columns, rowIdx).asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
        val r = evalAny(lte.right, columns, rowIdx).asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
        lte.ordering.lteq(l, r)

      case and: Expr.And[Row] =>
        evalBoolean(and.left, columns, rowIdx) && evalBoolean(and.right, columns, rowIdx)

      case or: Expr.Or[Row] =>
        evalBoolean(or.left, columns, rowIdx) || evalBoolean(or.right, columns, rowIdx)

      case not: Expr.Not[Row] =>
        !evalBoolean(not.expr, columns, rowIdx)

      case isDefined: Expr.IsDefined[Row, _] =>
        evalAny(isDefined.expr, columns, rowIdx)
          .asInstanceOf[Option[Any]]
          .isDefined // scalafix:ok DisableSyntax.asInstanceOf

      case when: Expr.When[Row, Boolean] =>
        val cond = evalBoolean(when.condition, columns, rowIdx)
        if (cond) evalBoolean(when.thenExpr, columns, rowIdx)
        else evalBoolean(when.elseExpr, columns, rowIdx)

      case like: Expr.Like[Row] =>
        val v = evalAny(like.expr, columns, rowIdx).asInstanceOf[String] // scalafix:ok DisableSyntax.asInstanceOf
        likeToRegex(like.pattern).matches(v)

      case sw: Expr.StartsWith[Row] =>
        val v = evalAny(sw.expr, columns, rowIdx).asInstanceOf[String] // scalafix:ok DisableSyntax.asInstanceOf
        val p = evalAny(sw.prefix, columns, rowIdx).asInstanceOf[String] // scalafix:ok DisableSyntax.asInstanceOf
        v.startsWith(p)

      case ew: Expr.EndsWith[Row] =>
        val v = evalAny(ew.expr, columns, rowIdx).asInstanceOf[String] // scalafix:ok DisableSyntax.asInstanceOf
        val s = evalAny(ew.suffix, columns, rowIdx).asInstanceOf[String] // scalafix:ok DisableSyntax.asInstanceOf
        v.endsWith(s)

      case sc: Expr.StringContains[Row] =>
        val v = evalAny(sc.expr, columns, rowIdx).asInstanceOf[String] // scalafix:ok DisableSyntax.asInstanceOf
        val s = evalAny(sc.substr, columns, rowIdx).asInstanceOf[String] // scalafix:ok DisableSyntax.asInstanceOf
        v.contains(s)

      case in: Expr.IsNull[Row, _] =>
        Option(evalAny(in.expr, columns, rowIdx)).isEmpty

      case inn: Expr.IsNotNull[Row, _] =>
        Option(evalAny(inn.expr, columns, rowIdx)).isDefined

      case inV: Expr.In[Row, _] =>
        val v = evalAny(inV.expr, columns, rowIdx)
        inV.values.contains(v)

      case btw: Expr.Between[Row, a] =>
        val v = evalAny(btw.expr, columns, rowIdx).asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
        val lo = evalAny(btw.lower, columns, rowIdx).asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
        val hi = evalAny(btw.upper, columns, rowIdx).asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
        btw.ordering.gteq(v, lo) && btw.ordering.lteq(v, hi)
    }
  }

  private def evalAny[Row, A](
    expr: Expr[Row, A],
    columns: Vector[Column],
    rowIdx: RowIndex
  ): Any = {
    (expr: @unchecked) match {
      case Expr.Const(value) => value

      case named: Expr.Named[Row, _] =>
        evalAny(named.expr, columns, rowIdx)

      case cell: Expr.Cell[Row, _] =>
        val column = columns(cell.index.toInt)
        val idx = rowIdx.toInt
        column.columnType match {
          case ColumnType.IntType => column.getInt(idx)
          case ColumnType.LongType => column.getLong(idx)
          case ColumnType.DoubleType => column.getDouble(idx)
          case ColumnType.StringType => column.getString(idx)
          case ColumnType.BooleanType => column.getBoolean(idx)
          case ColumnType.DateType => java.time.LocalDate.ofEpochDay(column.getDateEpochDay(idx).toLong)
          case ColumnType.AnyType | ColumnType.OptionType(_) => column.getValue(idx)
        }

      case add: Expr.Add[Row] =>
        evalAny(add.left, columns, rowIdx).asInstanceOf[Int] + evalAny(add.right, columns, rowIdx)
          .asInstanceOf[Int] // scalafix:ok DisableSyntax.asInstanceOf

      case sub: Expr.Sub[Row] =>
        evalAny(sub.left, columns, rowIdx).asInstanceOf[Int] - evalAny(sub.right, columns, rowIdx)
          .asInstanceOf[Int] // scalafix:ok DisableSyntax.asInstanceOf

      case mul: Expr.Mul[Row] =>
        evalAny(mul.left, columns, rowIdx).asInstanceOf[Int] * evalAny(mul.right, columns, rowIdx)
          .asInstanceOf[Int] // scalafix:ok DisableSyntax.asInstanceOf

      case div: Expr.Div[Row] =>
        val l = evalAny(div.left, columns, rowIdx).asInstanceOf[Int] // scalafix:ok DisableSyntax.asInstanceOf
        val r = evalAny(div.right, columns, rowIdx).asInstanceOf[Int] // scalafix:ok DisableSyntax.asInstanceOf
        if (r == 0)
          throw new ArithmeticException(s"Division by zero at row ${rowIdx.toInt}") // scalafix:ok DisableSyntax.throw
        l / r

      case add: Expr.AddLong[Row] =>
        evalAny(add.left, columns, rowIdx).asInstanceOf[Long] + evalAny(add.right, columns, rowIdx)
          .asInstanceOf[Long] // scalafix:ok DisableSyntax.asInstanceOf

      case sub: Expr.SubLong[Row] =>
        evalAny(sub.left, columns, rowIdx).asInstanceOf[Long] - evalAny(sub.right, columns, rowIdx)
          .asInstanceOf[Long] // scalafix:ok DisableSyntax.asInstanceOf

      case mul: Expr.MulLong[Row] =>
        evalAny(mul.left, columns, rowIdx).asInstanceOf[Long] * evalAny(mul.right, columns, rowIdx)
          .asInstanceOf[Long] // scalafix:ok DisableSyntax.asInstanceOf

      case div: Expr.DivLong[Row] =>
        val l = evalAny(div.left, columns, rowIdx).asInstanceOf[Long] // scalafix:ok DisableSyntax.asInstanceOf
        val r = evalAny(div.right, columns, rowIdx).asInstanceOf[Long] // scalafix:ok DisableSyntax.asInstanceOf
        if (r == 0L)
          throw new ArithmeticException(s"Division by zero at row ${rowIdx.toInt}") // scalafix:ok DisableSyntax.throw
        l / r

      case add: Expr.AddDouble[Row] =>
        evalAny(add.left, columns, rowIdx).asInstanceOf[Double] + evalAny(add.right, columns, rowIdx)
          .asInstanceOf[Double] // scalafix:ok DisableSyntax.asInstanceOf

      case sub: Expr.SubDouble[Row] =>
        evalAny(sub.left, columns, rowIdx).asInstanceOf[Double] - evalAny(sub.right, columns, rowIdx)
          .asInstanceOf[Double] // scalafix:ok DisableSyntax.asInstanceOf

      case mul: Expr.MulDouble[Row] =>
        evalAny(mul.left, columns, rowIdx).asInstanceOf[Double] * evalAny(mul.right, columns, rowIdx)
          .asInstanceOf[Double] // scalafix:ok DisableSyntax.asInstanceOf

      case div: Expr.DivDouble[Row] =>
        val l = evalAny(div.left, columns, rowIdx).asInstanceOf[Double] // scalafix:ok DisableSyntax.asInstanceOf
        val r = evalAny(div.right, columns, rowIdx).asInstanceOf[Double] // scalafix:ok DisableSyntax.asInstanceOf
        if (r == 0.0)
          throw new ArithmeticException(s"Division by zero at row ${rowIdx.toInt}") // scalafix:ok DisableSyntax.throw
        l / r

      case concat: Expr.Concat[Row] =>
        evalAny(concat.left, columns, rowIdx).asInstanceOf[String] + evalAny(concat.right, columns, rowIdx)
          .asInstanceOf[String] // scalafix:ok DisableSyntax.asInstanceOf

      case length: Expr.Length[Row] =>
        evalAny(length.expr, columns, rowIdx).asInstanceOf[String].length // scalafix:ok DisableSyntax.asInstanceOf

      case getOrElse: Expr.GetOrElse[Row, _] =>
        evalAny(getOrElse.expr, columns, rowIdx)
          .asInstanceOf[Option[Any]] match { // scalafix:ok DisableSyntax.asInstanceOf
          case Some(value) => value
          case scala.None => getOrElse.default
        }

      case when: Expr.When[Row, _] =>
        val cond = evalBoolean(when.condition, columns, rowIdx)
        if (cond) evalAny(when.thenExpr, columns, rowIdx)
        else evalAny(when.elseExpr, columns, rowIdx)

      case lo: Expr.Lower[Row] =>
        evalAny(lo.expr, columns, rowIdx).asInstanceOf[String].toLowerCase // scalafix:ok DisableSyntax.asInstanceOf

      case up: Expr.Upper[Row] =>
        evalAny(up.expr, columns, rowIdx).asInstanceOf[String].toUpperCase // scalafix:ok DisableSyntax.asInstanceOf

      case tr: Expr.Trim[Row] =>
        evalAny(tr.expr, columns, rowIdx).asInstanceOf[String].trim // scalafix:ok DisableSyntax.asInstanceOf

      case lt: Expr.LTrim[Row] =>
        evalAny(lt.expr, columns, rowIdx).asInstanceOf[String].stripLeading.nn // scalafix:ok DisableSyntax.asInstanceOf

      case rt: Expr.RTrim[Row] =>
        evalAny(rt.expr, columns, rowIdx)
          .asInstanceOf[String]
          .stripTrailing
          .nn // scalafix:ok DisableSyntax.asInstanceOf

      case ss: Expr.Substring[Row] =>
        val s = evalAny(ss.expr, columns, rowIdx).asInstanceOf[String] // scalafix:ok DisableSyntax.asInstanceOf
        val start = Math.max(ss.pos - 1, 0)
        val end = Math.min(start + ss.len, s.length)
        if (start >= s.length) "" else s.substring(start, end)

      case sr: Expr.StringReplace[Row] =>
        evalAny(sr.expr, columns, rowIdx)
          .asInstanceOf[String]
          .replace(sr.search, sr.replacement) // scalafix:ok DisableSyntax.asInstanceOf

      case rr: Expr.RegexpReplace[Row] =>
        evalAny(rr.expr, columns, rowIdx)
          .asInstanceOf[String]
          .replaceAll(rr.pattern, rr.replacement) // scalafix:ok DisableSyntax.asInstanceOf

      case re: Expr.RegexpExtract[Row] =>
        val s = evalAny(re.expr, columns, rowIdx).asInstanceOf[String] // scalafix:ok DisableSyntax.asInstanceOf
        val m = java.util.regex.Pattern.compile(re.pattern).matcher(s)
        if (m.find()) m.group(re.groupIdx) else ""

      case sp: Expr.StringSplit[Row] =>
        val s = evalAny(sp.expr, columns, rowIdx).asInstanceOf[String] // scalafix:ok DisableSyntax.asInstanceOf
        s.split(sp.delimiter, -1).toSeq

      case cw: Expr.ConcatWs[Row] =>
        val strs =
          cw.exprs.map(e => evalAny(e, columns, rowIdx).asInstanceOf[String]) // scalafix:ok DisableSyntax.asInstanceOf
        strs.mkString(cw.separator)

      case co: Expr.Coalesce[Row, _] =>
        co.exprs.iterator
          .map(e => evalAny(e, columns, rowIdx))
          .find(v => Option(v).isDefined)
          .orNull // scalafix:ok DisableSyntax.null

      case m: Expr.Mod[Row] =>
        val l = evalAny(m.left, columns, rowIdx).asInstanceOf[Int] // scalafix:ok DisableSyntax.asInstanceOf
        val r = evalAny(m.right, columns, rowIdx).asInstanceOf[Int] // scalafix:ok DisableSyntax.asInstanceOf
        if (r == 0)
          throw new ArithmeticException(s"Division by zero at row ${rowIdx.toInt}") // scalafix:ok DisableSyntax.throw
        l % r

      case ml: Expr.ModLong[Row] =>
        val l = evalAny(ml.left, columns, rowIdx).asInstanceOf[Long] // scalafix:ok DisableSyntax.asInstanceOf
        val r = evalAny(ml.right, columns, rowIdx).asInstanceOf[Long] // scalafix:ok DisableSyntax.asInstanceOf
        if (r == 0L)
          throw new ArithmeticException(s"Division by zero at row ${rowIdx.toInt}") // scalafix:ok DisableSyntax.throw
        l % r

      case ab: Expr.Abs[Row] =>
        Math.abs(evalAny(ab.expr, columns, rowIdx).asInstanceOf[Int]) // scalafix:ok DisableSyntax.asInstanceOf

      case abl: Expr.AbsLong[Row] =>
        Math.abs(evalAny(abl.expr, columns, rowIdx).asInstanceOf[Long]) // scalafix:ok DisableSyntax.asInstanceOf

      case abd: Expr.AbsDouble[Row] =>
        Math.abs(evalAny(abd.expr, columns, rowIdx).asInstanceOf[Double]) // scalafix:ok DisableSyntax.asInstanceOf

      case neg: Expr.Negate[Row] =>
        -evalAny(neg.expr, columns, rowIdx).asInstanceOf[Int] // scalafix:ok DisableSyntax.asInstanceOf

      case negl: Expr.NegateLong[Row] =>
        -evalAny(negl.expr, columns, rowIdx).asInstanceOf[Long] // scalafix:ok DisableSyntax.asInstanceOf

      case negd: Expr.NegateDouble[Row] =>
        -evalAny(negd.expr, columns, rowIdx).asInstanceOf[Double] // scalafix:ok DisableSyntax.asInstanceOf

      case rnd: Expr.Round[Row] =>
        val v = evalAny(rnd.expr, columns, rowIdx).asInstanceOf[Double] // scalafix:ok DisableSyntax.asInstanceOf
        BigDecimal(v).setScale(rnd.scale, BigDecimal.RoundingMode.HALF_UP).toDouble

      case fl: Expr.Floor[Row] =>
        Math.floor(evalAny(fl.expr, columns, rowIdx).asInstanceOf[Double]) // scalafix:ok DisableSyntax.asInstanceOf

      case cl: Expr.Ceil[Row] =>
        Math.ceil(evalAny(cl.expr, columns, rowIdx).asInstanceOf[Double]) // scalafix:ok DisableSyntax.asInstanceOf

      case ctl: Expr.CastToLong[Row] =>
        evalAny(ctl.expr, columns, rowIdx).asInstanceOf[Int].toLong // scalafix:ok DisableSyntax.asInstanceOf

      case ctd: Expr.CastToDouble[Row] =>
        evalAny(ctd.expr, columns, rowIdx).asInstanceOf[Int].toDouble // scalafix:ok DisableSyntax.asInstanceOf

      case cltd: Expr.CastLongToDouble[Row] =>
        evalAny(cltd.expr, columns, rowIdx).asInstanceOf[Long].toDouble // scalafix:ok DisableSyntax.asInstanceOf

      case cts: Expr.CastToString[Row, _] =>
        String.valueOf(evalAny(cts.expr, columns, rowIdx))

      case boolExpr: (Expr.Gt[Row, _] | Expr.Lt[Row, _] | Expr.Gte[Row, _] | Expr.Lte[Row, _] | Expr.Eq[Row, _] |
            Expr.Neq[Row, _] | Expr.And[Row] | Expr.Or[Row] | Expr.Not[Row] | Expr.IsDefined[Row, _] | Expr.Like[Row] |
            Expr.StartsWith[Row] | Expr.EndsWith[Row] | Expr.StringContains[Row] | Expr.IsNull[Row, _] |
            Expr.IsNotNull[Row, _] | Expr.In[Row, _] | Expr.Between[Row, _]) =>
        evalBoolean(
          boolExpr.asInstanceOf[Expr[Row, Boolean]],
          columns,
          rowIdx
        ) // scalafix:ok DisableSyntax.asInstanceOf

      case dad: Expr.DateAddDays[Row] =>
        val d =
          evalAny(dad.date, columns, rowIdx).asInstanceOf[java.time.LocalDate] // scalafix:ok DisableSyntax.asInstanceOf
        val n = evalAny(dad.days, columns, rowIdx).asInstanceOf[Int] // scalafix:ok DisableSyntax.asInstanceOf
        d.plusDays(n.toLong)

      case dsd: Expr.DateSubDays[Row] =>
        val d =
          evalAny(dsd.date, columns, rowIdx).asInstanceOf[java.time.LocalDate] // scalafix:ok DisableSyntax.asInstanceOf
        val n = evalAny(dsd.days, columns, rowIdx).asInstanceOf[Int] // scalafix:ok DisableSyntax.asInstanceOf
        d.minusDays(n.toLong)

      case dam: Expr.DateAddMonths[Row] =>
        val d =
          evalAny(dam.date, columns, rowIdx).asInstanceOf[java.time.LocalDate] // scalafix:ok DisableSyntax.asInstanceOf
        val n = evalAny(dam.months, columns, rowIdx).asInstanceOf[Int] // scalafix:ok DisableSyntax.asInstanceOf
        d.plusMonths(n.toLong)

      case dd: Expr.DateDiff[Row] =>
        val l =
          evalAny(dd.left, columns, rowIdx).asInstanceOf[java.time.LocalDate] // scalafix:ok DisableSyntax.asInstanceOf
        val r =
          evalAny(dd.right, columns, rowIdx).asInstanceOf[java.time.LocalDate] // scalafix:ok DisableSyntax.asInstanceOf
        java.time.temporal.ChronoUnit.DAYS.between(r, l).toInt

      case ey: Expr.ExtractYear[Row] =>
        evalAny(ey.date, columns, rowIdx)
          .asInstanceOf[java.time.LocalDate]
          .getYear // scalafix:ok DisableSyntax.asInstanceOf

      case em: Expr.ExtractMonth[Row] =>
        evalAny(em.date, columns, rowIdx)
          .asInstanceOf[java.time.LocalDate]
          .getMonthValue // scalafix:ok DisableSyntax.asInstanceOf

      case ed: Expr.ExtractDay[Row] =>
        evalAny(ed.date, columns, rowIdx)
          .asInstanceOf[java.time.LocalDate]
          .getDayOfMonth // scalafix:ok DisableSyntax.asInstanceOf

      case opt2iter: Expr.Option2Iterable[Row, _] =>
        evalAny(opt2iter.expr, columns, rowIdx)
          .asInstanceOf[Option[Any]]
          .toList // scalafix:ok DisableSyntax.asInstanceOf
    }
  }

  private def inferExprColumnType[Row, A](expr: Expr[Row, A], columns: Vector[Column]): ColumnType = {
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
    columns: Vector[Column],
    columnType: ColumnType
  ): Either[ExecutionError, Column] = {
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
        columnType match {
          case ColumnType.IntType =>
            Right(Column.int(Array.fill(rowCount)(c.value.asInstanceOf[Int]))) // scalafix:ok DisableSyntax.asInstanceOf
          case ColumnType.LongType =>
            Right(
              Column.long(Array.fill(rowCount)(c.value.asInstanceOf[Long]))
            ) // scalafix:ok DisableSyntax.asInstanceOf
          case ColumnType.DoubleType =>
            Right(
              Column.double(Array.fill(rowCount)(c.value.asInstanceOf[Double]))
            ) // scalafix:ok DisableSyntax.asInstanceOf
          case ColumnType.StringType =>
            Right(
              Column.string(Array.fill(rowCount)(c.value.asInstanceOf[String]))
            ) // scalafix:ok DisableSyntax.asInstanceOf
          case ColumnType.BooleanType =>
            Right(
              Column.boolean(Array.fill(rowCount)(c.value.asInstanceOf[Boolean]))
            ) // scalafix:ok DisableSyntax.asInstanceOf
          case ColumnType.DateType =>
            val epochDay =
              c.value.asInstanceOf[java.time.LocalDate].toEpochDay.toInt // scalafix:ok DisableSyntax.asInstanceOf
            Right(Column.date(Array.fill(rowCount)(epochDay)))
          case _ =>
            Right(Column.any(Array.fill(rowCount)(c.value.asInstanceOf[Any]))) // scalafix:ok DisableSyntax.asInstanceOf
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
          result <- vectorizedDiv(
            leftCol.asInstanceOf[Column.IntColumn].data, // scalafix:ok DisableSyntax.asInstanceOf
            rightCol.asInstanceOf[Column.IntColumn].data, // scalafix:ok DisableSyntax.asInstanceOf
            rowCount
          )
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
          result <- vectorizedLongDiv(
            leftCol.asInstanceOf[Column.LongColumn].data, // scalafix:ok DisableSyntax.asInstanceOf
            rightCol.asInstanceOf[Column.LongColumn].data, // scalafix:ok DisableSyntax.asInstanceOf
            rowCount
          )
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
          result <- vectorizedDoubleDiv(
            leftCol.asInstanceOf[Column.DoubleColumn].data, // scalafix:ok DisableSyntax.asInstanceOf
            rightCol.asInstanceOf[Column.DoubleColumn].data, // scalafix:ok DisableSyntax.asInstanceOf
            rowCount
          )
        } yield result

      case gt: Expr.Gt[Row, _] =>
        vectorizedComparison(gt.left, gt.right, columns, rowCount)(
          gt.ordering.asInstanceOf[Ordering[Any]].gt
        ) // scalafix:ok DisableSyntax.asInstanceOf

      case gte: Expr.Gte[Row, _] =>
        vectorizedComparison(gte.left, gte.right, columns, rowCount)(
          gte.ordering.asInstanceOf[Ordering[Any]].gteq
        ) // scalafix:ok DisableSyntax.asInstanceOf

      case lt: Expr.Lt[Row, _] =>
        vectorizedComparison(lt.left, lt.right, columns, rowCount)(
          lt.ordering.asInstanceOf[Ordering[Any]].lt
        ) // scalafix:ok DisableSyntax.asInstanceOf

      case lte: Expr.Lte[Row, _] =>
        vectorizedComparison(lte.left, lte.right, columns, rowCount)(
          lte.ordering.asInstanceOf[Ordering[Any]].lteq
        ) // scalafix:ok DisableSyntax.asInstanceOf

      case eq: Expr.Eq[Row, _] =>
        vectorizedComparison(eq.left, eq.right, columns, rowCount)((a, b) => java.util.Objects.equals(a, b))

      case neq: Expr.Neq[Row, _] =>
        vectorizedComparison(neq.left, neq.right, columns, rowCount)((a, b) => !java.util.Objects.equals(a, b))

      case and: Expr.And[Row] =>
        for {
          leftCol <- evalColumn(and.left, columns, ColumnType.BooleanType)
          rightCol <- evalColumn(and.right, columns, ColumnType.BooleanType)
        } yield {
          val ld = leftCol.asInstanceOf[Column.BooleanColumn].data // scalafix:ok DisableSyntax.asInstanceOf
          val rd = rightCol.asInstanceOf[Column.BooleanColumn].data // scalafix:ok DisableSyntax.asInstanceOf
          val out = new Array[Boolean](rowCount)
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) { out(i) = ld(i) && rd(i); i += 1 }
          Column.boolean(out)
        }

      case or: Expr.Or[Row] =>
        for {
          leftCol <- evalColumn(or.left, columns, ColumnType.BooleanType)
          rightCol <- evalColumn(or.right, columns, ColumnType.BooleanType)
        } yield {
          val ld = leftCol.asInstanceOf[Column.BooleanColumn].data // scalafix:ok DisableSyntax.asInstanceOf
          val rd = rightCol.asInstanceOf[Column.BooleanColumn].data // scalafix:ok DisableSyntax.asInstanceOf
          val out = new Array[Boolean](rowCount)
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) { out(i) = ld(i) || rd(i); i += 1 }
          Column.boolean(out)
        }

      case not: Expr.Not[Row] =>
        evalColumn(not.expr, columns, ColumnType.BooleanType).map { col =>
          val data = col.asInstanceOf[Column.BooleanColumn].data // scalafix:ok DisableSyntax.asInstanceOf
          val out = new Array[Boolean](rowCount)
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) { out(i) = !data(i); i += 1 }
          Column.boolean(out)
        }

      case concat: Expr.Concat[Row] =>
        for {
          leftCol <- evalColumn(concat.left, columns, ColumnType.StringType)
          rightCol <- evalColumn(concat.right, columns, ColumnType.StringType)
        } yield {
          val ld = leftCol.asInstanceOf[Column.StringColumn].data // scalafix:ok DisableSyntax.asInstanceOf
          val rd = rightCol.asInstanceOf[Column.StringColumn].data // scalafix:ok DisableSyntax.asInstanceOf
          val out = new Array[String](rowCount)
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) { out(i) = ld(i) + rd(i); i += 1 }
          Column.string(out)
        }

      case length: Expr.Length[Row] =>
        evalColumn(length.expr, columns, ColumnType.StringType).map { col =>
          val data = col.asInstanceOf[Column.StringColumn].data // scalafix:ok DisableSyntax.asInstanceOf
          val out = new Array[Int](rowCount)
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) { out(i) = data(i).length; i += 1 }
          Column.int(out)
        }

      case when: Expr.When[Row, _] =>
        for {
          condCol <- evalColumn(when.condition, columns, ColumnType.BooleanType)
          thenCol <- evalColumn(when.thenExpr, columns, columnType)
          elseCol <- evalColumn(when.elseExpr, columns, columnType)
        } yield {
          val cond = condCol.asInstanceOf[Column.BooleanColumn].data // scalafix:ok DisableSyntax.asInstanceOf
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
        evalColumnRowByRow(expr, columns, columnType, rowCount)
    }
  }

  private def vectorizedIntBinOp[Row](
    left: Expr[Row, Int],
    right: Expr[Row, Int],
    columns: Vector[Column],
    rowCount: Int
  )(op: (Int, Int) => Int): Either[ExecutionError, Column] = {
    for {
      leftCol <- evalColumn(left, columns, ColumnType.IntType)
      rightCol <- evalColumn(right, columns, ColumnType.IntType)
    } yield {
      val ld = leftCol.asInstanceOf[Column.IntColumn].data // scalafix:ok DisableSyntax.asInstanceOf
      val rd = rightCol.asInstanceOf[Column.IntColumn].data // scalafix:ok DisableSyntax.asInstanceOf
      val out = new Array[Int](rowCount)
      var i = 0 // scalafix:ok DisableSyntax.var
      while (i < rowCount) { out(i) = op(ld(i), rd(i)); i += 1 }
      Column.int(out)
    }
  }

  private def vectorizedDiv(left: Array[Int], right: Array[Int], rowCount: Int): Either[ExecutionError, Column] = {
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
    columns: Vector[Column],
    rowCount: Int
  )(op: (Long, Long) => Long): Either[ExecutionError, Column] = {
    for {
      leftCol <- evalColumn(left, columns, ColumnType.LongType)
      rightCol <- evalColumn(right, columns, ColumnType.LongType)
    } yield {
      val ld = leftCol.asInstanceOf[Column.LongColumn].data // scalafix:ok DisableSyntax.asInstanceOf
      val rd = rightCol.asInstanceOf[Column.LongColumn].data // scalafix:ok DisableSyntax.asInstanceOf
      val out = new Array[Long](rowCount)
      var i = 0 // scalafix:ok DisableSyntax.var
      while (i < rowCount) { out(i) = op(ld(i), rd(i)); i += 1 }
      Column.long(out)
    }
  }

  private def vectorizedLongDiv(
    left: Array[Long],
    right: Array[Long],
    rowCount: Int
  ): Either[ExecutionError, Column] = {
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
    columns: Vector[Column],
    rowCount: Int
  )(op: (Double, Double) => Double): Either[ExecutionError, Column] = {
    for {
      leftCol <- evalColumn(left, columns, ColumnType.DoubleType)
      rightCol <- evalColumn(right, columns, ColumnType.DoubleType)
    } yield {
      val ld = leftCol.asInstanceOf[Column.DoubleColumn].data // scalafix:ok DisableSyntax.asInstanceOf
      val rd = rightCol.asInstanceOf[Column.DoubleColumn].data // scalafix:ok DisableSyntax.asInstanceOf
      val out = new Array[Double](rowCount)
      var i = 0 // scalafix:ok DisableSyntax.var
      while (i < rowCount) { out(i) = op(ld(i), rd(i)); i += 1 }
      Column.double(out)
    }
  }

  private def vectorizedDoubleDiv(
    left: Array[Double],
    right: Array[Double],
    rowCount: Int
  ): Either[ExecutionError, Column] = {
    val out = new Array[Double](rowCount)
    var i = 0 // scalafix:ok DisableSyntax.var
    while (i < rowCount) {
      if (right(i) == 0.0) return Left(ExecutionError.DivisionByZero(i)) // scalafix:ok DisableSyntax.return
      out(i) = left(i) / right(i)
      i += 1
    }
    Right(Column.double(out))
  }

  private def vectorizedComparison[Row, A](
    left: Expr[Row, A],
    right: Expr[Row, A],
    columns: Vector[Column],
    rowCount: Int
  )(cmp: (Any, Any) => Boolean): Either[ExecutionError, Column] = {
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

  private def evalColumnRowByRow[Row, A](
    expr: Expr[Row, A],
    columns: Vector[Column],
    columnType: ColumnType,
    rowCount: Int
  ): Either[ExecutionError, Column] = {
    val valuesOrError = (0 until rowCount).foldLeft[Either[ExecutionError, Vector[Any]]](
      Right(Vector.empty)
    ) { (acc, rowIdx) =>
      acc.flatMap { values =>
        eval(expr, columns, RowIndex(rowIdx)).map(values :+ _)
      }
    }
    valuesOrError.flatMap(values => Column.fromValues(values, columnType))
  }

  /** Evaluate aggregation expression over entire dataset.
    *
    * Aggregations operate on all rows to produce a single value. GADT pattern matching ensures
    * type-safe aggregation logic without casts.
    */
  def evalAggregation[Row, A](
    expr: Expr[Row, A],
    columns: Vector[Column]
  ): Either[ExecutionError, A] = {
    if (columns.isEmpty || columns.head.length == 0) {
      (expr: @unchecked) match {
        case _: Expr.Count[Row] =>
          Right(0L)
        case _: Expr.Sum[Row] =>
          Right(0)
        case _: Expr.SumDouble[Row] =>
          Right(0.0)
        case _: Expr.SumLong[Row] =>
          Right(0L)
        case _: Expr.Avg[Row] =>
          Right(0.0)
        case _: Expr.Max[Row, ?] =>
          Right(None)
        case _: Expr.Min[Row, ?] =>
          Right(None)
        case _: Expr.CountDistinct[Row, ?] =>
          Right(0L)
        case _: Expr.CountIf[Row] =>
          Right(0L)
        case _: Expr.StdDev[Row] =>
          Right(0.0)
        case _: Expr.StdDevPop[Row] =>
          Right(0.0)
        case _: Expr.First[Row, ?] =>
          Right(None)
        case _: Expr.Collect[Row, ?] =>
          Right(Seq.empty.asInstanceOf[A]) // scalafix:ok DisableSyntax.asInstanceOf
        case _: Expr.PercentileApprox[Row] =>
          Right(0.0)
        case _: Expr.MaxBy[Row, ?, ?] =>
          Right(None)
        case _: Expr.MinBy[Row, ?, ?] =>
          Right(None)
        case _: Expr.MaxN[Row, ?] =>
          Right(Seq.empty.asInstanceOf[A]) // scalafix:ok DisableSyntax.asInstanceOf
        case _: Expr.MinN[Row, ?] =>
          Right(Seq.empty.asInstanceOf[A]) // scalafix:ok DisableSyntax.asInstanceOf
        case _: Expr.MaxByN[Row, ?, ?] =>
          Right(Seq.empty.asInstanceOf[A]) // scalafix:ok DisableSyntax.asInstanceOf
        case _: Expr.MinByN[Row, ?, ?] =>
          Right(Seq.empty.asInstanceOf[A]) // scalafix:ok DisableSyntax.asInstanceOf
      }
    } else {
      (expr: @unchecked) match {
        case Expr.Count() =>
          Right(columns.head.length.toLong)

        case sum: Expr.Sum[Row] =>
          val rowCount = columns.head.length
          (0 until rowCount).foldLeft[Either[ExecutionError, Int]](Right(0)) { (acc, rowIdx) =>
            acc.flatMap { currentSum =>
              eval(sum.expr, columns, RowIndex(rowIdx)).map(currentSum + _)
            }
          }

        case sumD: Expr.SumDouble[Row] =>
          val rowCount = columns.head.length
          (0 until rowCount).foldLeft[Either[ExecutionError, Double]](Right(0.0)) { (acc, rowIdx) =>
            acc.flatMap { currentSum =>
              eval(sumD.expr, columns, RowIndex(rowIdx)).map(currentSum + _)
            }
          }

        case sumL: Expr.SumLong[Row] =>
          val rowCount = columns.head.length
          (0 until rowCount).foldLeft[Either[ExecutionError, Long]](Right(0L)) { (acc, rowIdx) =>
            acc.flatMap { currentSum =>
              eval(sumL.expr, columns, RowIndex(rowIdx)).map(currentSum + _)
            }
          }

        case avg: Expr.Avg[Row] =>
          val rowCount = columns.head.length
          val sumResult = (0 until rowCount).foldLeft[Either[ExecutionError, Double]](Right(0.0)) { (acc, rowIdx) =>
            acc.flatMap { currentSum =>
              eval(avg.expr, columns, RowIndex(rowIdx)).map(currentSum + _)
            }
          }
          sumResult.map(_ / rowCount)

        case max: Expr.Max[Row, a] =>
          val rowCount = columns.head.length
          (0 until rowCount).foldLeft[Either[ExecutionError, Option[a]]](Right(None)) { (acc, rowIdx) =>
            acc.flatMap { currentMax =>
              eval(max.expr, columns, RowIndex(rowIdx)).map { value =>
                currentMax match {
                  case None => Some(value)
                  case Some(m) => Some(if (max.ordering.gt(value, m)) value else m)
                }
              }
            }
          }

        case min: Expr.Min[Row, a] =>
          val rowCount = columns.head.length
          (0 until rowCount).foldLeft[Either[ExecutionError, Option[a]]](Right(None)) { (acc, rowIdx) =>
            acc.flatMap { currentMin =>
              eval(min.expr, columns, RowIndex(rowIdx)).map { value =>
                currentMin match {
                  case None => Some(value)
                  case Some(m) => Some(if (min.ordering.lt(value, m)) value else m)
                }
              }
            }
          }

        case countDist: Expr.CountDistinct[Row, _] =>
          val rowCount = columns.head.length
          val values = (0 until rowCount).flatMap { rowIdx =>
            eval(countDist.expr, columns, RowIndex(rowIdx)).toOption
          }
          Right(values.distinct.size.toLong.asInstanceOf[A]) // scalafix:ok DisableSyntax.asInstanceOf

        case countIf: Expr.CountIf[Row] =>
          val rowCount = columns.head.length
          val count = (0 until rowCount).count { rowIdx =>
            eval(countIf.predicate, columns, RowIndex(rowIdx)) == Right(true)
          }
          Right(count.toLong.asInstanceOf[A]) // scalafix:ok DisableSyntax.asInstanceOf

        case stddev: Expr.StdDev[Row] =>
          val rowCount = columns.head.length
          val values = (0 until rowCount).flatMap { rowIdx =>
            eval(stddev.expr, columns, RowIndex(rowIdx)).toOption
          }
          if (values.isEmpty || values.length == 1) {
            Right(0.0.asInstanceOf[A]) // scalafix:ok DisableSyntax.asInstanceOf
          } else {
            val mean = values.sum / values.length
            val variance = values.map(v => math.pow(v - mean, 2)).sum / (values.length - 1)
            Right(math.sqrt(variance).asInstanceOf[A]) // scalafix:ok DisableSyntax.asInstanceOf
          }

        case stddevPop: Expr.StdDevPop[Row] =>
          val rowCount = columns.head.length
          val values = (0 until rowCount).flatMap { rowIdx =>
            eval(stddevPop.expr, columns, RowIndex(rowIdx)).toOption
          }
          if (values.isEmpty) {
            Right(0.0.asInstanceOf[A]) // scalafix:ok DisableSyntax.asInstanceOf
          } else {
            val mean = values.sum / values.length
            val variance = values.map(v => math.pow(v - mean, 2)).sum / values.length
            Right(math.sqrt(variance).asInstanceOf[A]) // scalafix:ok DisableSyntax.asInstanceOf
          }

        case first: Expr.First[Row, _] =>
          val rowCount = columns.head.length
          val firstValue = (0 until rowCount).iterator.flatMap { rowIdx =>
            eval(first.expr, columns, RowIndex(rowIdx)).toOption
          }.nextOption()
          Right(firstValue.asInstanceOf[A]) // scalafix:ok DisableSyntax.asInstanceOf

        case collect: Expr.Collect[Row, _] =>
          val rowCount = columns.head.length
          val values = (0 until rowCount).flatMap { rowIdx =>
            eval(collect.expr, columns, RowIndex(rowIdx)).toOption
          }
          Right(values.toSeq.asInstanceOf[A]) // scalafix:ok DisableSyntax.asInstanceOf

        case pct: Expr.PercentileApprox[Row] =>
          val rowCount = columns.head.length
          val values = (0 until rowCount).flatMap { rowIdx =>
            eval(pct.expr, columns, RowIndex(rowIdx)).toOption
          }
          if (values.isEmpty) {
            Right(0.0.asInstanceOf[A]) // scalafix:ok DisableSyntax.asInstanceOf
          } else {
            val sorted = values.sorted
            val idx = math.min((sorted.length * pct.percentile).toInt, sorted.length - 1)
            Right(sorted(idx).asInstanceOf[A]) // scalafix:ok DisableSyntax.asInstanceOf
          }

        case mb: Expr.MaxBy[Row, a, k] =>
          val rowCount = columns.head.length
          var bestValue: Option[a] = None // scalafix:ok DisableSyntax.var
          var bestKey: Option[k] = None // scalafix:ok DisableSyntax.var
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            val v = eval(mb.valueExpr, columns, RowIndex(i))
            val kv = eval(mb.orderExpr, columns, RowIndex(i))
            (v, kv) match {
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
          Right(bestValue.asInstanceOf[A]) // scalafix:ok DisableSyntax.asInstanceOf

        case mb: Expr.MinBy[Row, a, k] =>
          val rowCount = columns.head.length
          var bestValue: Option[a] = None // scalafix:ok DisableSyntax.var
          var bestKey: Option[k] = None // scalafix:ok DisableSyntax.var
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < rowCount) {
            val v = eval(mb.valueExpr, columns, RowIndex(i))
            val kv = eval(mb.orderExpr, columns, RowIndex(i))
            (v, kv) match {
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
          Right(bestValue.asInstanceOf[A]) // scalafix:ok DisableSyntax.asInstanceOf

        case mn: Expr.MaxN[Row, _] =>
          val rowCount = columns.head.length
          val values = (0 until rowCount).flatMap { rowIdx =>
            eval(mn.expr, columns, RowIndex(rowIdx)).toOption
          }
          val sorted = values.sorted(using mn.ordering.reverse)
          Right(sorted.take(mn.n).toSeq.asInstanceOf[A]) // scalafix:ok DisableSyntax.asInstanceOf

        case mn: Expr.MinN[Row, _] =>
          val rowCount = columns.head.length
          val values = (0 until rowCount).flatMap { rowIdx =>
            eval(mn.expr, columns, RowIndex(rowIdx)).toOption
          }
          val sorted = values.sorted(using mn.ordering)
          Right(sorted.take(mn.n).toSeq.asInstanceOf[A]) // scalafix:ok DisableSyntax.asInstanceOf

        case mbn: Expr.MaxByN[Row, a, k] =>
          val rowCount = columns.head.length
          val pairs = (0 until rowCount).flatMap { rowIdx =>
            val v = eval(mbn.valueExpr, columns, RowIndex(rowIdx))
            val kv = eval(mbn.orderExpr, columns, RowIndex(rowIdx))
            (v, kv) match {
              case (Right(value), Right(key)) => Some((value, key))
              case _ => None
            }
          }
          val sorted = pairs.sortBy(_._2)(using mbn.ordering.reverse)
          Right(sorted.take(mbn.n).map(_._1).toSeq.asInstanceOf[A]) // scalafix:ok DisableSyntax.asInstanceOf

        case mbn: Expr.MinByN[Row, a, k] =>
          val rowCount = columns.head.length
          val pairs = (0 until rowCount).flatMap { rowIdx =>
            val v = eval(mbn.valueExpr, columns, RowIndex(rowIdx))
            val kv = eval(mbn.orderExpr, columns, RowIndex(rowIdx))
            (v, kv) match {
              case (Right(value), Right(key)) => Some((value, key))
              case _ => None
            }
          }
          val sorted = pairs.sortBy(_._2)(using mbn.ordering)
          Right(sorted.take(mbn.n).map(_._1).toSeq.asInstanceOf[A]) // scalafix:ok DisableSyntax.asInstanceOf
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
