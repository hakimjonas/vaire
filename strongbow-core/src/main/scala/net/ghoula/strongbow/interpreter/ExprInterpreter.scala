package net.ghoula.strongbow.interpreter

import net.ghoula.sarati.ast.json.JsonValue
import parsers.json.{formatJson, parseJson}

import scala.collection.immutable.BitSet

import net.ghoula.strongbow.column.{Column, ColumnType}
import net.ghoula.strongbow.errors.ExecutionError
import net.ghoula.strongbow.expr.Expr
import net.ghoula.strongbow.types.{Date, DayTimeInterval, RowIndex, Time, YearMonthInterval}

/** Zero-cast expression interpreter using typed columnar storage.
  *
  * All evaluation goes through evalColumn (vectorized column operations). No asInstanceOf at the
  * Cell evaluation boundary.
  *
  * Column[+A] GADT refinement provides typed array access (IntColumn, StringColumn, etc.) without
  * casts. Aggregations use typed dispatch for Ordering-based operations and getValue for generic
  * value collection.
  */
object ExprInterpreter {

  private[strongbow] def inferExprColumnType[Row, A](expr: Expr[Row, A], columns: Vector[Column[?]]): ColumnType = {
    expr.outputType.getOrElse {
      expr match {
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
        case _ => ColumnType.AnyType
      }
    }
  }

  /** Evaluate expression for all rows, producing a new column.
    *
    * Vectorized: operates on entire arrays instead of row-by-row where possible. For Cell
    * references, returns the column directly (zero work). For arithmetic/comparisons/string ops, It
    * uses while-loops on typed arrays.
    *
    * `lambdaScope` carries the lambda-variable bindings of the nearest enclosing higher-order
    * expression; ordinary arms pass it through unchanged (implicit), and higher-order arms extend
    * it around body evaluation.
    */
  def evalColumn[Row, A](
    expr: Expr[Row, A],
    columns: Vector[Column[?]],
    columnType: ColumnType
  )(implicit lambdaScope: LambdaScope = LambdaScope.empty): Either[ExecutionError, Column[?]] = {
    if (columns.isEmpty || columns.head.length == 0) {
      Right(Column.empty(columnType))
    } else {

      val rowCount = columns.head.length

      (expr: @unchecked) match {
        case cell: Expr.Cell[_, _] =>
          Right(columns(cell.index.toInt))

        case named: Expr.Named[_, _] =>
          evalColumn(named.expr, columns, columnType)

        case lv: Expr.LambdaVar[Row, _] =>
          lambdaScope.get(lv.binder) match {
            case Some(col) => Right(col)
            case None =>
              Left(ExecutionError.InvalidValue("Lambda variable used outside its binding expression"))
          }

        case c: Expr.Const[_, _] =>
          c.value match {
            case v: Int =>
              Right(Column.int(Array.fill(rowCount)(v)))
            case v: Long =>
              Right(columnType match {
                case ColumnType.TimestampType => Column.timestamp(Array.fill(rowCount)(v))
                case ColumnType.TimestampNTZType => Column.timestampNTZ(Array.fill(rowCount)(v))
                case ColumnType.TimeType => Column.time(Array.fill(rowCount)(v))
                case ColumnType.DayTimeIntervalType => Column.dayTimeInterval(Array.fill(rowCount)(v))
                case _ => Column.long(Array.fill(rowCount)(v))
              })
            case v: Double =>
              Right(Column.double(Array.fill(rowCount)(v)))
            case v: String =>
              Right(Column.string(Array.fill(rowCount)(v)))
            case v: Boolean =>
              Right(Column.boolean(Array.fill(rowCount)(v)))
            case v =>
              if (Option(v).isEmpty) {
                val allNulls = BitSet(0 until rowCount*)
                Right(columnType match {
                  case ColumnType.IntType => Column.int(new Array[Int](rowCount), allNulls)
                  case ColumnType.LongType => Column.long(new Array[Long](rowCount), allNulls)
                  case ColumnType.DoubleType => Column.double(new Array[Double](rowCount), allNulls)
                  case ColumnType.StringType => Column.string(new Array[String | Null](rowCount), allNulls)
                  case ColumnType.BooleanType => Column.boolean(new Array[Boolean](rowCount), allNulls)
                  case ColumnType.DateType => Column.date(new Array[Int](rowCount), allNulls)
                  case _ => Column.any(new Array[Any | Null](rowCount), allNulls)
                })
              } else if (columnType == ColumnType.DateType) {
                v match {
                  case ld: java.time.LocalDate =>
                    Right(Column.date(Array.fill(rowCount)(ld.toEpochDay.toInt)))
                  case _ =>
                    Left(ExecutionError.TypeMismatch("Date", v.getClass.getSimpleName, "evalColumn Const"))
                }
              } else if (columnType == ColumnType.TimeType) {
                v match {
                  case lt: java.time.LocalTime =>
                    Right(Column.time(Array.fill(rowCount)(Time.fromLocalTime(lt).toMicros)))
                  case _ =>
                    Left(ExecutionError.TypeMismatch("Time", v.getClass.getSimpleName, "evalColumn Const"))
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
                Column.boolean(Array.tabulate(rowCount)(i => ld(i) && rd(i)), ln | rn)
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
                Column.boolean(Array.tabulate(rowCount)(i => ld(i) || rd(i)), ln | rn)
              case _ =>
                Column.boolean(Array.empty[Boolean])
            }
          }

        case not: Expr.Not[Row] =>
          evalColumn(not.expr, columns, ColumnType.BooleanType).map {
            case Column.BooleanColumn(data, nulls) =>
              Column.boolean(Array.tabulate(rowCount)(i => !data(i)), nulls)
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
                Column.string(Array.tabulate(rowCount)(i => ld(i).nn.concat(rd(i))), ln | rn)
              case _ =>
                Column.string(Array.empty[String | Null])
            }
          }

        case length: Expr.Length[Row] =>
          evalColumn(length.expr, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              Column.int(Array.tabulate(rowCount)(i => if (nulls.contains(i)) 0 else data(i).nn.length), nulls)
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
                    Column.int(Array.tabulate(rowCount)(i => if (cond(i)) td(i) else ed(i)), tn | en)
                  case (Column.LongColumn(td, tn), Column.LongColumn(ed, en)) =>
                    Column.long(Array.tabulate(rowCount)(i => if (cond(i)) td(i) else ed(i)), tn | en)
                  case (Column.DoubleColumn(td, tn), Column.DoubleColumn(ed, en)) =>
                    Column.double(Array.tabulate(rowCount)(i => if (cond(i)) td(i) else ed(i)), tn | en)
                  case (Column.StringColumn(td, tn), Column.StringColumn(ed, en)) =>
                    Column.string(Array.tabulate(rowCount)(i => if (cond(i)) td(i) else ed(i)), tn | en)
                  case (Column.BooleanColumn(td, tn), Column.BooleanColumn(ed, en)) =>
                    Column.boolean(Array.tabulate(rowCount)(i => if (cond(i)) td(i) else ed(i)), tn | en)
                  case (Column.DateColumn(td, tn), Column.DateColumn(ed, en)) =>
                    Column.date(Array.tabulate(rowCount)(i => if (cond(i)) td(i) else ed(i)), tn | en)
                  case _ =>
                    val out =
                      Array.tabulate[Any](rowCount)(i => if (cond(i)) thenCol.getValue(i) else elseCol.getValue(i))
                    Column.fromValues(out.toVector, columnType) match {
                      case Right(c) => c
                      case Left(_) => Column.any(out)
                    }
                }
              case _ =>
                Column.any(Array.empty[Any | Null])
            }
          }

        case like: Expr.Like[Row] =>
          evalColumn(like.expr, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              val regex = likeToRegex(like.pattern)
              Column.boolean(
                Array.tabulate(rowCount)(i => if (nulls.contains(i)) false else regex.matches(data(i).nn)),
                nulls
              )
            case _ => Column.boolean(Array.empty[Boolean])
          }

        case lo: Expr.Lower[Row] =>
          evalColumn(lo.expr, columns, ColumnType.StringType).map {
            case sc: Column.StringColumn => mapString(sc)(_.toLowerCase)
            case _ => Column.string(Array.empty[String | Null])
          }

        case up: Expr.Upper[Row] =>
          evalColumn(up.expr, columns, ColumnType.StringType).map {
            case sc: Column.StringColumn => mapString(sc)(_.toUpperCase)
            case _ => Column.string(Array.empty[String | Null])
          }

        case tr: Expr.Trim[Row] =>
          evalColumn(tr.expr, columns, ColumnType.StringType).map {
            case sc: Column.StringColumn => mapString(sc)(_.trim)
            case _ => Column.string(Array.empty[String | Null])
          }

        case lt: Expr.LTrim[Row] =>
          evalColumn(lt.expr, columns, ColumnType.StringType).map {
            case sc: Column.StringColumn => mapString(sc)(_.stripLeading.nn)
            case _ => Column.string(Array.empty[String | Null])
          }

        case rt: Expr.RTrim[Row] =>
          evalColumn(rt.expr, columns, ColumnType.StringType).map {
            case sc: Column.StringColumn => mapString(sc)(_.stripTrailing.nn)
            case _ => Column.string(Array.empty[String | Null])
          }

        case ss: Expr.Substring[Row] =>
          evalColumn(ss.expr, columns, ColumnType.StringType).map {
            case sc: Column.StringColumn =>
              mapString(sc) { s =>
                val start = Math.max(ss.pos - 1, 0)
                val end = Math.min(start + ss.len, s.length)
                if (start >= s.length) "" else s.substring(start, end)
              }
            case _ => Column.string(Array.empty[String | Null])
          }

        case sr: Expr.StringReplace[Row] =>
          evalColumn(sr.expr, columns, ColumnType.StringType).map {
            case sc: Column.StringColumn => mapString(sc)(_.replace(sr.search, sr.replacement))
            case _ => Column.string(Array.empty[String | Null])
          }

        case rr: Expr.RegexpReplace[Row] =>
          evalColumn(rr.expr, columns, ColumnType.StringType).map {
            case sc: Column.StringColumn =>
              val compiled = java.util.regex.Pattern.compile(rr.pattern)
              mapString(sc)(s => compiled.matcher(s).replaceAll(rr.replacement).nn)
            case _ => Column.string(Array.empty[String | Null])
          }

        case re: Expr.RegexpExtract[Row] =>
          evalColumn(re.expr, columns, ColumnType.StringType).map {
            case sc: Column.StringColumn =>
              val compiled = java.util.regex.Pattern.compile(re.pattern)
              mapString(sc) { s =>
                val m = compiled.matcher(s)
                if (m.find()) m.group(re.groupIdx) else ""
              }
            case _ => Column.string(Array.empty[String | Null])
          }

        case sp: Expr.StringSplit[Row] =>
          evalColumn(sp.expr, columns, ColumnType.StringType).map {
            case sc: Column.StringColumn => mapAny(sc)(s => String.valueOf(s).nn.split(sp.delimiter, -1).toSeq)
            case _ => Column.any(Array.empty[Any | Null])
          }

        case sw: Expr.StartsWith[Row] =>
          for {
            exprCol <- evalColumn(sw.expr, columns, ColumnType.StringType)
            prefixCol <- evalColumn(sw.prefix, columns, ColumnType.StringType)
          } yield {
            (exprCol, prefixCol) match {
              case (Column.StringColumn(ed, en), Column.StringColumn(pd, pn)) =>
                val combined = en | pn
                Column.boolean(
                  Array.tabulate(rowCount)(i => if (combined.contains(i)) false else ed(i).nn.startsWith(pd(i))),
                  combined
                )
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
                Column.boolean(
                  Array.tabulate(rowCount)(i => if (combined.contains(i)) false else ed(i).nn.endsWith(sd(i))),
                  combined
                )
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
                Column.boolean(
                  Array.tabulate(rowCount)(i => if (combined.contains(i)) false else ed(i).nn.contains(sd(i))),
                  combined
                )
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
              Right(Column.string(Array.tabulate(rowCount) { i =>
                val parts = cols.collect {
                  case Column.StringColumn(data, nulls) if !nulls.contains(i) => data(i)
                }
                parts.mkString(cw.separator)
              }))
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
                val outNulls = scala.collection.mutable.BitSet.empty
                val out = Array.tabulate[Any](rowCount) { i =>
                  cols.find(c => !c.isNull(RowIndex(i))) match {
                    case Some(c) => c.getValue(i)
                    case None =>
                      outNulls += i
                      null // scalafix:ok DisableSyntax.null
                  }
                }
                Column.fromValues(out.toVector, columnType)
              }
          }

        case in: Expr.IsNull[Row, _] =>
          val innerType = inferExprColumnType(in.expr, columns)
          evalColumn(in.expr, columns, innerType).map { col =>
            val nulls = col.nullSet
            Column.boolean(Array.tabulate(rowCount)(i => nulls.contains(i)))
          }

        case inn: Expr.IsNotNull[Row, _] =>
          val innerType = inferExprColumnType(inn.expr, columns)
          evalColumn(inn.expr, columns, innerType).map { col =>
            val nulls = col.nullSet
            Column.boolean(Array.tabulate(rowCount)(i => !nulls.contains(i)))
          }

        case inV: Expr.In[Row, _] =>
          val innerType = inferExprColumnType(inV.expr, columns)
          evalColumn(inV.expr, columns, innerType).map { col =>
            val valSet: Set[Any] = inV.values.map(v => v: Any).toSet
            Column.boolean(Array.tabulate(rowCount)(i => valSet.contains(col.getValue(i))))
          }

        case btw: Expr.Between[Row, _] =>
          val innerType = inferExprColumnType(btw.expr, columns)
          for {
            exprCol <- evalColumn(btw.expr, columns, innerType)
            lowerCol <- evalColumn(btw.lower, columns, innerType)
            upperCol <- evalColumn(btw.upper, columns, innerType)
            result <- {
              def between[T](vd: Array[T], vn: BitSet, ld: Array[T], ln: BitSet, ud: Array[T], un: BitSet)(
                gte: (T, T) => Boolean,
                lte: (T, T) => Boolean
              ): Either[ExecutionError, Column[?]] = {
                val combined = vn | ln | un
                Right(
                  Column.boolean(
                    Array.tabulate(rowCount)(i =>
                      if (combined.contains(i)) false else gte(vd(i), ld(i)) && lte(vd(i), ud(i))
                    ),
                    combined
                  )
                )
              }

              (exprCol, lowerCol, upperCol) match {
                case (Column.IntColumn(vd, vn), Column.IntColumn(ld, ln), Column.IntColumn(ud, un)) =>
                  between(vd, vn, ld, ln, ud, un)(_ >= _, _ <= _)
                case (Column.LongColumn(vd, vn), Column.LongColumn(ld, ln), Column.LongColumn(ud, un)) =>
                  between(vd, vn, ld, ln, ud, un)(_ >= _, _ <= _)
                case (Column.DoubleColumn(vd, vn), Column.DoubleColumn(ld, ln), Column.DoubleColumn(ud, un)) =>
                  between(vd, vn, ld, ln, ud, un)(_ >= _, _ <= _)
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
              Column.int(Array.tabulate(rowCount)(i => Math.abs(data(i))), nulls)
            case _ => Column.int(Array.empty[Int])
          }

        case abl: Expr.AbsLong[Row] =>
          evalColumn(abl.expr, columns, ColumnType.LongType).map {
            case Column.LongColumn(data, nulls) =>
              Column.long(Array.tabulate(rowCount)(i => Math.abs(data(i))), nulls)
            case _ => Column.long(Array.empty[Long])
          }

        case abd: Expr.AbsDouble[Row] =>
          evalColumn(abd.expr, columns, ColumnType.DoubleType).map {
            case Column.DoubleColumn(data, nulls) =>
              Column.double(Array.tabulate(rowCount)(i => Math.abs(data(i))), nulls)
            case _ => Column.double(Array.empty[Double])
          }

        case neg: Expr.Negate[Row] =>
          evalColumn(neg.expr, columns, ColumnType.IntType).map {
            case Column.IntColumn(data, nulls) =>
              Column.int(Array.tabulate(rowCount)(i => -data(i)), nulls)
            case _ => Column.int(Array.empty[Int])
          }

        case negl: Expr.NegateLong[Row] =>
          evalColumn(negl.expr, columns, ColumnType.LongType).map {
            case Column.LongColumn(data, nulls) =>
              Column.long(Array.tabulate(rowCount)(i => -data(i)), nulls)
            case _ => Column.long(Array.empty[Long])
          }

        case negd: Expr.NegateDouble[Row] =>
          evalColumn(negd.expr, columns, ColumnType.DoubleType).map {
            case Column.DoubleColumn(data, nulls) =>
              Column.double(Array.tabulate(rowCount)(i => -data(i)), nulls)
            case _ => Column.double(Array.empty[Double])
          }

        case rnd: Expr.Round[Row] =>
          evalColumn(rnd.expr, columns, ColumnType.DoubleType).map {
            case Column.DoubleColumn(data, nulls) =>
              Column.double(
                Array.tabulate(rowCount)(i =>
                  BigDecimal(data(i)).setScale(rnd.scale, BigDecimal.RoundingMode.HALF_UP).toDouble
                ),
                nulls
              )
            case _ => Column.double(Array.empty[Double])
          }

        case fl: Expr.Floor[Row] =>
          evalColumn(fl.expr, columns, ColumnType.DoubleType).map {
            case Column.DoubleColumn(data, nulls) =>
              Column.double(Array.tabulate(rowCount)(i => Math.floor(data(i))), nulls)
            case _ => Column.double(Array.empty[Double])
          }

        case cl: Expr.Ceil[Row] =>
          evalColumn(cl.expr, columns, ColumnType.DoubleType).map {
            case Column.DoubleColumn(data, nulls) =>
              Column.double(Array.tabulate(rowCount)(i => Math.ceil(data(i))), nulls)
            case _ => Column.double(Array.empty[Double])
          }

        case ctl: Expr.CastToLong[Row] =>
          evalColumn(ctl.expr, columns, ColumnType.IntType).map {
            case Column.IntColumn(data, nulls) =>
              Column.long(Array.tabulate(rowCount)(i => data(i).toLong), nulls)
            case _ => Column.long(Array.empty[Long])
          }

        case ctd: Expr.CastToDouble[Row] =>
          evalColumn(ctd.expr, columns, ColumnType.IntType).map {
            case Column.IntColumn(data, nulls) =>
              Column.double(Array.tabulate(rowCount)(i => data(i).toDouble), nulls)
            case _ => Column.double(Array.empty[Double])
          }

        case cltd: Expr.CastLongToDouble[Row] =>
          evalColumn(cltd.expr, columns, ColumnType.LongType).map {
            case Column.LongColumn(data, nulls) =>
              Column.double(Array.tabulate(rowCount)(i => data(i).toDouble), nulls)
            case _ => Column.double(Array.empty[Double])
          }

        case cts: Expr.CastToString[Row, _] =>
          val innerType = inferExprColumnType(cts.expr, columns)
          evalColumn(cts.expr, columns, innerType).map { col =>
            mapAny(col)(v => String.valueOf(v).nn)
          }

        case isDefined: Expr.IsDefined[Row, _] =>
          val innerType = inferExprColumnType(isDefined.expr, columns)
          evalColumn(isDefined.expr, columns, innerType).map { col =>
            Column.boolean(Array.tabulate(rowCount) { i =>
              col.getValue(i) match {
                case Some(_) => true
                case _ => false
              }
            })
          }

        case getOrElse: Expr.GetOrElse[Row, _] =>
          val innerType = inferExprColumnType(getOrElse.expr, columns)
          evalColumn(getOrElse.expr, columns, innerType).flatMap { col =>
            val out = Array.tabulate[Any](rowCount) { i =>
              col.getValue(i) match {
                case Some(value) => value
                case _ => getOrElse.default
              }
            }
            Column.fromValues(out.toVector, columnType)
          }

        case opt2iter: Expr.Option2Iterable[Row, _] =>
          val innerType = inferExprColumnType(opt2iter.expr, columns)
          evalColumn(opt2iter.expr, columns, innerType).map { col =>
            Column.any(Array.tabulate[Any](rowCount) { i =>
              col.getValue(i) match {
                case Some(value) => List(value)
                case opt: Option[?] if opt.isEmpty => List.empty
                case other => List(other)
              }
            })
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
          Right(Column.double(Array.tabulate(rowCount)(_ => random.nextDouble())))

        case dad: Expr.DateAddDays[Row] =>
          for {
            dateCol <- evalColumn(dad.date, columns, ColumnType.DateType)
            daysCol <- evalColumn(dad.days, columns, ColumnType.IntType)
          } yield {
            (dateCol, daysCol) match {
              case (Column.DateColumn(dd, dn), Column.IntColumn(nd, nn)) =>
                val combined = dn | nn
                Column.date(
                  Array.tabulate(rowCount)(i =>
                    if (combined.contains(i)) 0
                    else java.time.LocalDate.ofEpochDay(dd(i).toLong).plusDays(nd(i).toLong).toEpochDay.toInt
                  ),
                  combined
                )
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
                Column.date(
                  Array.tabulate(rowCount)(i =>
                    if (combined.contains(i)) 0
                    else java.time.LocalDate.ofEpochDay(dd(i).toLong).minusDays(nd(i).toLong).toEpochDay.toInt
                  ),
                  combined
                )
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
                Column.date(
                  Array.tabulate(rowCount)(i =>
                    if (combined.contains(i)) 0
                    else java.time.LocalDate.ofEpochDay(dd(i).toLong).plusMonths(md(i).toLong).toEpochDay.toInt
                  ),
                  combined
                )
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
                Column.int(
                  Array.tabulate(rowCount) { i =>
                    if (combined.contains(i)) 0
                    else {
                      val l = java.time.LocalDate.ofEpochDay(ld(i).toLong)
                      val r = java.time.LocalDate.ofEpochDay(rd(i).toLong)
                      java.time.temporal.ChronoUnit.DAYS.between(r, l).toInt
                    }
                  },
                  combined
                )
              case _ => Column.int(Array.empty[Int])
            }
          }

        case ey: Expr.ExtractYear[Row] =>
          evalColumn(ey.date, columns, ColumnType.DateType).map {
            case Column.DateColumn(data, nulls) =>
              Column.int(
                Array.tabulate(rowCount)(i =>
                  if (nulls.contains(i)) 0 else java.time.LocalDate.ofEpochDay(data(i).toLong).getYear
                ),
                nulls
              )
            case _ => Column.int(Array.empty[Int])
          }

        case em: Expr.ExtractMonth[Row] =>
          evalColumn(em.date, columns, ColumnType.DateType).map {
            case Column.DateColumn(data, nulls) =>
              Column.int(
                Array.tabulate(rowCount)(i =>
                  if (nulls.contains(i)) 0 else java.time.LocalDate.ofEpochDay(data(i).toLong).getMonthValue
                ),
                nulls
              )
            case _ => Column.int(Array.empty[Int])
          }

        case ed: Expr.ExtractDay[Row] =>
          evalColumn(ed.date, columns, ColumnType.DateType).map {
            case Column.DateColumn(data, nulls) =>
              Column.int(
                Array.tabulate(rowCount)(i =>
                  if (nulls.contains(i)) 0 else java.time.LocalDate.ofEpochDay(data(i).toLong).getDayOfMonth
                ),
                nulls
              )
            case _ => Column.int(Array.empty[Int])
          }

        case dow: Expr.DayOfWeek[Row] =>
          evalColumn(dow.date, columns, ColumnType.DateType).map {
            case Column.DateColumn(data, nulls) =>
              Column.int(
                Array.tabulate(rowCount)(i =>
                  if (nulls.contains(i)) 0
                  else java.time.LocalDate.ofEpochDay(data(i).toLong).getDayOfWeek.getValue % 7 + 1
                ),
                nulls
              )
            case _ => Column.int(Array.empty[Int])
          }

        case doy: Expr.DayOfYear[Row] =>
          evalColumn(doy.date, columns, ColumnType.DateType).map {
            case Column.DateColumn(data, nulls) =>
              Column.int(
                Array.tabulate(rowCount)(i =>
                  if (nulls.contains(i)) 0 else java.time.LocalDate.ofEpochDay(data(i).toLong).getDayOfYear
                ),
                nulls
              )
            case _ => Column.int(Array.empty[Int])
          }

        case woy: Expr.WeekOfYear[Row] =>
          evalColumn(woy.date, columns, ColumnType.DateType).map {
            case Column.DateColumn(data, nulls) =>
              Column.int(
                Array.tabulate(rowCount)(i =>
                  if (nulls.contains(i)) 0
                  else
                    java.time.LocalDate
                      .ofEpochDay(data(i).toLong)
                      .get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR)
                ),
                nulls
              )
            case _ => Column.int(Array.empty[Int])
          }

        case q: Expr.Quarter[Row] =>
          evalColumn(q.date, columns, ColumnType.DateType).map {
            case Column.DateColumn(data, nulls) =>
              Column.int(
                Array.tabulate(rowCount) { i =>
                  if (nulls.contains(i)) 0
                  else { val m = java.time.LocalDate.ofEpochDay(data(i).toLong).getMonthValue; (m - 1) / 3 + 1 }
                },
                nulls
              )
            case _ => Column.int(Array.empty[Int])
          }

        case ld: Expr.LastDay[Row] =>
          evalColumn(ld.date, columns, ColumnType.DateType).map {
            case Column.DateColumn(data, nulls) =>
              Column.date(
                Array.tabulate(rowCount) { i =>
                  if (nulls.contains(i)) 0
                  else {
                    val d = java.time.LocalDate.ofEpochDay(data(i).toLong)
                    d.withDayOfMonth(d.lengthOfMonth()).toEpochDay.toInt
                  }
                },
                nulls
              )
            case _ => Column.date(Array.empty[Int])
          }

        case nd: Expr.NextDay[Row] =>
          evalColumn(nd.date, columns, ColumnType.DateType).map {
            case Column.DateColumn(data, nulls) =>
              val target = java.time.DayOfWeek.valueOf(nd.dayOfWeek.toUpperCase)
              Column.date(
                Array.tabulate(rowCount) { i =>
                  if (nulls.contains(i)) 0
                  else
                    java.time.LocalDate
                      .ofEpochDay(data(i).toLong)
                      .`with`(java.time.temporal.TemporalAdjusters.next(target))
                      .toEpochDay
                      .toInt
                },
                nulls
              )
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
                Column.double(
                  Array.tabulate(rowCount) { i =>
                    if (combined.contains(i)) 0.0
                    else {
                      val e = java.time.LocalDate.ofEpochDay(ed(i).toLong)
                      val s = java.time.LocalDate.ofEpochDay(sd(i).toLong)
                      val period = java.time.Period.between(s, e)
                      period.toTotalMonths.toDouble + period.getDays.toDouble / 31.0
                    }
                  },
                  combined
                )
              case _ => Column.double(Array.empty[Double])
            }
          }

        case dt: Expr.DateTrunc[Row] =>
          evalColumn(dt.date, columns, ColumnType.DateType).map {
            case Column.DateColumn(data, nulls) =>
              Column.date(
                Array.tabulate(rowCount) { i =>
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
                },
                nulls
              )
            case _ => Column.date(Array.empty[Int])
          }

        case df: Expr.DateFormat[Row] =>
          evalColumn(df.date, columns, ColumnType.DateType).map { dateCol =>
            val formatter = java.time.format.DateTimeFormatter.ofPattern(df.format)
            mapAny(dateCol) {
              case ld: java.time.LocalDate => ld.format(formatter)
              case other => String.valueOf(other)
            }
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
                Column.date(
                  Array.tabulate(rowCount)(i =>
                    if (combined.contains(i)) 0
                    else java.time.LocalDate.of(yd(i), mdata(i), dd(i)).toEpochDay.toInt
                  ),
                  combined
                )
              case _ => Column.date(Array.empty[Int])
            }
          }

        case as: Expr.ArraySize[Row, _] =>
          val innerType = inferExprColumnType(as.expr, columns)
          evalColumn(as.expr, columns, innerType).map { col =>
            val nulls = col match { case Column.AnyColumn(_, n) => n; case _ => BitSet.empty }
            Column.int(
              Array.tabulate(rowCount)(i =>
                if (nulls.contains(i)) 0
                else col.getValue(i) match { case s: Seq[?] => s.size; case _ => 0 }
              ),
              nulls
            )
          }

        case ac: Expr.ArrayContains[Row, _] =>
          val arrType = inferExprColumnType(ac.expr, columns)
          val valType = inferExprColumnType(ac.value, columns)
          for {
            arrCol <- evalColumn(ac.expr, columns, arrType)
            valCol <- evalColumn(ac.value, columns, valType)
          } yield {
            Column.boolean(Array.tabulate(rowCount) { i =>
              arrCol.getValue(i) match { case s: Seq[?] => s.contains(valCol.getValue(i)); case _ => false }
            })
          }

        case _: Expr.Explode[Row, _] =>
          Left(ExecutionError.UnsupportedOperation("Explode requires Dataset-level handling"))

        case _: Expr.ArraySort[Row, _] =>
          Left(ExecutionError.UnsupportedOperation("ArraySort on untyped columns not supported"))

        case ad: Expr.ArrayDistinct[Row, _] =>
          val innerType = inferExprColumnType(ad.expr, columns)
          evalColumn(ad.expr, columns, innerType).map { col =>
            mapSeq(col)(_.distinct)
          }

        case au: Expr.ArrayUnion[Row, _] =>
          val innerType = inferExprColumnType(au.left, columns)
          for {
            leftCol <- evalColumn(au.left, columns, innerType)
            rightCol <- evalColumn(au.right, columns, innerType)
          } yield {
            Column.any(Array.tabulate[Any](rowCount) { i =>
              (leftCol.getValue(i), rightCol.getValue(i)) match {
                case (l: Seq[?], r: Seq[?]) => (l ++ r).distinct
                case _ => Seq.empty
              }
            })
          }

        case ai: Expr.ArrayIntersect[Row, _] =>
          val innerType = inferExprColumnType(ai.left, columns)
          for {
            leftCol <- evalColumn(ai.left, columns, innerType)
            rightCol <- evalColumn(ai.right, columns, innerType)
          } yield {
            Column.any(Array.tabulate[Any](rowCount) { i =>
              (leftCol.getValue(i), rightCol.getValue(i)) match {
                case (l: Seq[?], r: Seq[?]) => l.intersect(r)
                case _ => Seq.empty
              }
            })
          }

        case ae: Expr.ArrayExcept[Row, _] =>
          val innerType = inferExprColumnType(ae.left, columns)
          for {
            leftCol <- evalColumn(ae.left, columns, innerType)
            rightCol <- evalColumn(ae.right, columns, innerType)
          } yield {
            Column.any(Array.tabulate[Any](rowCount) { i =>
              (leftCol.getValue(i), rightCol.getValue(i)) match {
                case (l: Seq[?], r: Seq[?]) => l.diff(r)
                case _ => Seq.empty
              }
            })
          }

        case fl: Expr.Flatten[Row, _] =>
          val innerType = inferExprColumnType(fl.expr, columns)
          evalColumn(fl.expr, columns, innerType).map { col =>
            mapSeq(col)(_.flatMap {
              case inner: Seq[?] => inner
              case other => Vector(other)
            })
          }

        case ea: Expr.ElementAt[Row, _] =>
          val arrType = inferExprColumnType(ea.expr, columns)
          for {
            arrCol <- evalColumn(ea.expr, columns, arrType)
            idxCol <- evalColumn(ea.index, columns, ColumnType.IntType)
          } yield {
            val outNulls = scala.collection.mutable.BitSet.empty
            val out = idxCol match {
              case Column.IntColumn(idxData, _) =>
                Array.tabulate[Any](rowCount) { i =>
                  arrCol.getValue(i) match {
                    case s: Seq[?] =>
                      val idx = idxData(i)
                      val resolved = if (idx > 0) idx - 1 else s.size + idx
                      if (resolved >= 0 && resolved < s.size) s(resolved)
                      else { outNulls += i; null } // scalafix:ok DisableSyntax.null
                    case _ => outNulls += i; null // scalafix:ok DisableSyntax.null
                  }
                }
              case _ => new Array[Any](rowCount)
            }
            Column.any(out, BitSet.empty ++ outNulls)
          }

        case as: Expr.ArraySlice[Row, _] =>
          val innerType = inferExprColumnType(as.expr, columns)
          evalColumn(as.expr, columns, innerType).map { col =>
            Column.any(Array.tabulate[Any](rowCount) { i =>
              col.getValue(i) match {
                case s: Seq[?] => val start = Math.max(as.start - 1, 0); s.slice(start, start + as.length)
                case other => other
              }
            })
          }

        case mk: Expr.MapKeys[Row, _, _] =>
          val innerType = inferExprColumnType(mk.expr, columns)
          evalColumn(mk.expr, columns, innerType).map { col =>
            Column.any(
              Array.tabulate[Any](rowCount)(i =>
                col.getValue(i) match { case m: Map[?, ?] => m.keys.toSeq; case _ => Seq.empty }
              )
            )
          }

        case mv: Expr.MapValues[Row, _, _] =>
          val innerType = inferExprColumnType(mv.expr, columns)
          evalColumn(mv.expr, columns, innerType).map { col =>
            Column.any(
              Array.tabulate[Any](rowCount)(i =>
                col.getValue(i) match { case m: Map[?, ?] => m.values.toSeq; case _ => Seq.empty }
              )
            )
          }

        case mck: Expr.MapContainsKey[Row, _, _] =>
          val mapType = inferExprColumnType(mck.expr, columns)
          val keyType = inferExprColumnType(mck.key, columns)
          for {
            mapCol <- evalColumn(mck.expr, columns, mapType)
            keyCol <- evalColumn(mck.key, columns, keyType)
          } yield {
            Column.boolean(Array.tabulate(rowCount) { i =>
              mapCol.getValue(i) match {
                case m: Map[?, ?] => m.keys.exists(java.util.Objects.equals(_, keyCol.getValue(i)))
                case _ => false
              }
            })
          }

        case me: Expr.MapEntries[Row, _, _] =>
          val innerType = inferExprColumnType(me.expr, columns)
          evalColumn(me.expr, columns, innerType).map { col =>
            Column.any(
              Array.tabulate[Any](rowCount)(i =>
                col.getValue(i) match { case m: Map[?, ?] => m.toSeq; case _ => Seq.empty }
              )
            )
          }

        case mfa: Expr.MapFromArrays[Row, _, _] =>
          val keysType = inferExprColumnType(mfa.keys, columns)
          val valsType = inferExprColumnType(mfa.values, columns)
          for {
            keysCol <- evalColumn(mfa.keys, columns, keysType)
            valsCol <- evalColumn(mfa.values, columns, valsType)
          } yield {
            Column.any(Array.tabulate[Any](rowCount) { i =>
              (keysCol.getValue(i), valsCol.getValue(i)) match {
                case (ks: Seq[?], vs: Seq[?]) => ks.zip(vs).toMap
                case _ => Map.empty
              }
            })
          }

        case mc: Expr.MapConcat[Row, _, _] =>
          val innerType = inferExprColumnType(mc.left, columns)
          for {
            leftCol <- evalColumn(mc.left, columns, innerType)
            rightCol <- evalColumn(mc.right, columns, innerType)
          } yield {
            Column.any(Array.tabulate[Any](rowCount) { i =>
              (leftCol.getValue(i), rightCol.getValue(i)) match {
                case (l: Map[?, ?], r: Map[?, ?]) => (l.toSeq ++ r.toSeq).toMap
                case _ => Map.empty
              }
            })
          }

        case md: Expr.Md5[Row] =>
          vectorizedStringHash(md.expr, columns)("MD5")

        case sh: Expr.Sha1[Row] =>
          vectorizedStringHash(sh.expr, columns)("SHA-1")

        case sh2: Expr.Sha2[Row] =>
          vectorizedStringHash(sh2.expr, columns)(sha2Algorithm(sh2.bitLength))

        case crc: Expr.Crc32[Row] =>
          evalColumn(crc.expr, columns, ColumnType.StringType).map {
            case sc: Column.StringColumn => mapStringToLong(sc)(s => crc32Value(s))
            case _ => Column.long(Array.empty[Long])
          }

        case xx: Expr.XxHash64[Row] =>
          evalColumn(xx.expr, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              Column.long(
                Array.tabulate(data.length)(i => if (nulls.contains(i)) 42L else xxHash64Value(data(i).nn)),
                BitSet.empty
              )
            case _ => Column.long(Array.empty[Long])
          }

        case hh: Expr.Hash[Row] =>
          evalColumn(hh.expr, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              Column.int(
                Array.tabulate(data.length)(i =>
                  if (nulls.contains(i)) 42 else murmur3Hash(data(i).nn.getBytes("UTF-8"))
                ),
                BitSet.empty
              )
            case _ => Column.int(Array.empty[Int])
          }

        case ue: Expr.UrlEncode[Row] =>
          evalColumn(ue.expr, columns, ColumnType.StringType).map {
            case sc: Column.StringColumn => mapString(sc)(s => java.net.URLEncoder.encode(s, "UTF-8").nn)
            case _ => Column.string(Array.empty[String | Null])
          }

        case ud: Expr.UrlDecode[Row] =>
          evalColumn(ud.expr, columns, ColumnType.StringType).map {
            case sc: Column.StringColumn => mapString(sc)(s => java.net.URLDecoder.decode(s, "UTF-8").nn)
            case _ => Column.string(Array.empty[String | Null])
          }

        case b64e: Expr.Base64Encode[Row] =>
          evalColumn(b64e.expr, columns, ColumnType.StringType).map {
            case sc: Column.StringColumn =>
              val encoder = java.util.Base64.getEncoder.nn
              mapString(sc)(s => encoder.encodeToString(s.getBytes("UTF-8")).nn)
            case _ => Column.string(Array.empty[String | Null])
          }

        case b64d: Expr.Base64Decode[Row] =>
          evalColumn(b64d.expr, columns, ColumnType.StringType).map {
            case sc: Column.StringColumn =>
              val decoder = java.util.Base64.getDecoder.nn
              mapString(sc)(s => String(decoder.decode(s), "UTF-8"))
            case _ => Column.string(Array.empty[String | Null])
          }

        case hx: Expr.Hex[Row] =>
          evalColumn(hx.expr, columns, ColumnType.StringType).map {
            case sc: Column.StringColumn => mapString(sc)(s => hexEncode(s.getBytes("UTF-8")))
            case _ => Column.string(Array.empty[String | Null])
          }

        case gjo: Expr.GetJsonObject[Row] =>
          evalColumn(gjo.expr, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              val outNulls = scala.collection.mutable.BitSet.empty
              val out = Array.tabulate(rowCount) { i =>
                if (nulls.contains(i)) { outNulls += i; null } // scalafix:ok DisableSyntax.null
                else
                  extractJsonPath(data(i).nn, gjo.path) match {
                    case Right(v) if Option(v).nonEmpty => v
                    case _ => outNulls += i; null // scalafix:ok DisableSyntax.null
                  }
              }
              Column.string(out, BitSet.empty ++ outNulls)
            case _ => Column.string(Array.empty[String | Null])
          }

        case jt: Expr.JsonTuple[Row] =>
          evalColumn(jt.expr, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              Column.any(
                Array.tabulate[Any | Null](rowCount) { i =>
                  if (nulls.contains(i)) jsonTupleNulls(jt.keys.size)
                  else
                    parseJson(data(i).nn) match {
                      case parser.core.Result.Success(v: JsonValue, _) =>
                        jt.keys.map(key => jsonTupleScalar(walkJsonPath(v, List(key))))
                      case _ => jsonTupleNulls(jt.keys.size)
                    }
                }
              )
            case _ => Column.any(Array.empty[Any | Null])
          }

        case st: Expr.Struct[Row, _] =>
          val fieldResults = st.fields.map { case (_, fieldExpr, fieldCt) =>
            evalColumn(fieldExpr, columns, fieldCt)
          }
          val firstErr = fieldResults.collectFirst { case Left(err) => err }
          firstErr match {
            case Some(err) => Left(err)
            case None =>
              val flattened = fieldResults.collect { case Right(col) => col }
                .zip(st.fields.map(_._3))
                .flatMap { case (col, ct) => flattenStructColumn(col, ct) }
              if (flattened.size != st.schema.columnCount) {
                Left(
                  ExecutionError.TypeMismatch(
                    s"struct schema with ${st.schema.columnCount} columns",
                    s"${flattened.size} evaluated columns",
                    "evalColumn Struct"
                  )
                )
              } else {
                Right(Column.struct(flattened, st.schema))
              }
          }

        case gf: Expr.GetField[Row, _, _] =>
          val structCt = inferExprColumnType(gf.struct, columns)
          evalColumn(gf.struct, columns, structCt).flatMap {
            case Column.StructColumn(inner, _, _) =>
              val idx = gf.fieldIndex.toInt
              if (idx >= 0 && idx < inner.length) Right(inner(idx))
              else
                Left(
                  ExecutionError.InvalidValue(
                    s"Field index $idx out of bounds for struct with ${inner.length} columns"
                  )
                )
            case other =>
              Left(ExecutionError.TypeMismatch("StructColumn", other.columnType.toString, "evalColumn GetField"))
          }

        case ts2: Expr.TimeToSeconds[Row] =>
          evalColumn(ts2.expr, columns, ColumnType.TimeType).map {
            case Column.TimeColumn(data, nulls) =>
              val unscaled = data
              Column.decimal(unscaled, 14, 6, nulls)
            case _ => Column.decimal(Array.empty[Long], 14, 6)
          }

        case tm: Expr.TimeToMillis[Row] =>
          evalColumn(tm.expr, columns, ColumnType.TimeType).map {
            case Column.TimeColumn(data, nulls) =>
              Column.long(
                Array.tabulate(rowCount)(i => Math.floorDiv(data(i), 1000L)),
                nulls
              )
            case _ => Column.long(Array.empty[Long])
          }

        case tu: Expr.TimeToMicros[Row] =>
          evalColumn(tu.expr, columns, ColumnType.TimeType).map {
            case Column.TimeColumn(data, nulls) => Column.long(data, nulls)
            case _ => Column.long(Array.empty[Long])
          }

        case tfs: Expr.TimeFromSeconds[Row] =>
          evalColumn(tfs.expr, columns, ColumnType.DoubleType).flatMap {
            case Column.DoubleColumn(data, nulls) =>
              val invalid = data.indices.find(i => !nulls.contains(i) && (data(i).isNaN || data(i).isInfinite))
              invalid match {
                case Some(i) =>
                  Left(ExecutionError.InvalidValue(s"Cannot convert ${data(i)} to TIME at row $i"))
                case None =>
                  Right(
                    Column.time(
                      Array.tabulate(rowCount)(i => (data(i) * 1e9).toLong / 1000L),
                      nulls
                    )
                  )
              }
            case _ => Right(Column.time(Array.empty[Long]))
          }

        case tfm: Expr.TimeFromMillis[Row] =>
          evalColumn(tfm.expr, columns, ColumnType.LongType).map {
            case Column.LongColumn(data, nulls) =>
              Column.time(Array.tabulate(rowCount)(i => data(i) * 1000L), nulls)
            case _ => Column.time(Array.empty[Long])
          }

        case tfu: Expr.TimeFromMicros[Row] =>
          evalColumn(tfu.expr, columns, ColumnType.LongType).map {
            case Column.LongColumn(data, nulls) => Column.time(data, nulls)
            case _ => Column.time(Array.empty[Long])
          }

        case tb: Expr.TimeBucket[Row] =>
          for {
            bucketCol <- evalColumn(tb.bucketSize, columns, ColumnType.DayTimeIntervalType)
            tsCol <- evalColumn(tb.ts, columns, ColumnType.TimestampType)
            originCol <- evalColumn(tb.origin, columns, ColumnType.TimestampType)
            result <- (bucketCol, tsCol, originCol) match {
              case (
                    Column.DayTimeIntervalColumn(bd, bn),
                    Column.TimestampColumn(td, tn),
                    Column.TimestampColumn(od, on)
                  ) =>
                val combined = bn | tn | on
                val nonPositive = bd.indices.find(i => !bn.contains(i) && bd(i) <= 0L)
                nonPositive match {
                  case Some(i) =>
                    Left(ExecutionError.InvalidValue(s"Bucket size must be positive, found ${bd(i)} at row $i"))
                  case None =>
                    Right(
                      Column.timestamp(
                        Array.tabulate(rowCount) { i =>
                          val bucket = bd(i)
                          val diff = td(i) - od(i)
                          val k = Math.floorDiv(diff, bucket)
                          od(i) + k * bucket
                        },
                        combined
                      )
                    )
                }
              case _ => Right(Column.timestamp(Array.empty[Long]))
            }
          } yield result

        case pj: Expr.ParseJson[Row] =>
          evalColumn(pj.expr, columns, ColumnType.StringType).flatMap {
            case Column.StringColumn(data, nulls) =>
              val out = new Array[Any | Null](rowCount)
              val errors = data.indices
                .filterNot(nulls.contains)
                .foldLeft(Option.empty[ExecutionError]) { (err, i) =>
                  err match {
                    case Some(_) => err
                    case None =>
                      parseJson(data(i).nn) match {
                        case parser.core.Result.Success(v, _) =>
                          out(i) = v
                          None
                        case _ =>
                          Some(ExecutionError.InvalidValue(s"Invalid JSON: ${data(i).nn.take(100)}"))
                      }
                  }
                }
              errors match {
                case Some(e) => Left(e)
                case None =>
                  (0 until rowCount).foreach { i =>
                    if (nulls.contains(i)) out(i) = null // scalafix:ok DisableSyntax.null
                  }
                  Right(Column.any(out, nulls))
              }
            case other => Left(ExecutionError.TypeMismatch("StringColumn", other.columnType.toString, "ParseJson"))
          }

        case vg: Expr.VariantGet[Row] =>
          variantGet(vg.expr, vg.path, vg.targetType, columns, rowCount, tryMode = false)

        case tvg: Expr.TryVariantGet[Row] =>
          variantGet(tvg.expr, tvg.path, tvg.targetType, columns, rowCount, tryMode = true)

        case ivn: Expr.IsVariantNull[Row] =>
          val innerType = inferExprColumnType(ivn.expr, columns)
          evalColumn(ivn.expr, columns, innerType).map { col =>
            Column.boolean(
              Array.tabulate(rowCount)(i =>
                if (col.isNull(RowIndex(i))) false
                else
                  col.getValue(i) match {
                    case v: JsonValue =>
                      v match {
                        case JsonValue.Null => true
                        case _ => false
                      }
                    case _ => false
                  }
              )
            )
          }

        case sov: Expr.SchemaOfVariant[Row] =>
          val innerType = inferExprColumnType(sov.expr, columns)
          evalColumn(sov.expr, columns, innerType).flatMap { col =>
            val out = new Array[Any | Null](rowCount)
            val errors = (0 until rowCount)
              .filterNot(i => col.isNull(RowIndex(i)))
              .foldLeft(Option.empty[ExecutionError]) { (err, i) =>
                err match {
                  case Some(_) => err
                  case None =>
                    col.getValue(i) match {
                      case v: JsonValue =>
                        jsonValueToSqlType(v) match {
                          case Some(t) => out(i) = t; None
                          case None => Some(ExecutionError.InvalidValue(s"Unsupported variant value at row $i"))
                        }
                      case other =>
                        Some(ExecutionError.TypeMismatch("JsonValue", other.getClass.getSimpleName, "SchemaOfVariant"))
                    }
                }
              }
            errors match {
              case Some(e) => Left(e)
              case None =>
                (0 until rowCount).foreach { i =>
                  if (col.isNull(RowIndex(i))) out(i) = null // scalafix:ok DisableSyntax.null
                }
                Right(Column.any(out, col.nullSet))
            }
          }

        case ivv: Expr.IsValidVariant[Row] =>
          val innerType = inferExprColumnType(ivv.expr, columns)
          evalColumn(ivv.expr, columns, innerType).map { col =>
            val outNulls = scala.collection.mutable.BitSet.empty
            val out = Array.fill(rowCount)(false)
            (0 until rowCount).foreach { i =>
              if (col.isNull(RowIndex(i))) outNulls += i
              else
                col.getValue(i) match {
                  case s: String =>
                    parseJson(s) match {
                      case _: parser.core.Result.Success[?, ?] => out(i) = true
                      case _ => outNulls += i
                    }
                  case _ => outNulls += i
                }
            }
            Column.boolean(out, col.nullSet | (BitSet.empty ++ outNulls))
          }

        case _: Expr.VariantExplode[Row] =>
          Left(ExecutionError.UnsupportedOperation("VariantExplode requires Dataset-level handling"))

        case gr: Expr.Greatest[Row, _] =>
          rowWiseExtreme(gr.exprs, gr.ordering, columns, rowCount, isMax = true)

        case ls: Expr.Least[Row, _] =>
          rowWiseExtreme(ls.exprs, ls.ordering, columns, rowCount, isMax = false)

        case ni: Expr.NullIf[Row, _] =>
          for {
            leftCol <- evalColumn(ni.left, columns, inferExprColumnType(ni.left, columns))
            rightCol <- evalColumn(ni.right, columns, inferExprColumnType(ni.right, columns))
          } yield {
            val out = Array.tabulate[Any | Null](rowCount) { i =>
              if (java.util.Objects.equals(leftCol.getValue(i), rightCol.getValue(i)))
                null // scalafix:ok DisableSyntax.null
              else leftCol.getValue(i)
            }
            val nulls = leftCol.nullSet | BitSet.fromSpecific(
              (0 until rowCount).filter(i => java.util.Objects.equals(leftCol.getValue(i), rightCol.getValue(i)))
            )
            Column.fromValues(out.toVector, inferExprColumnType(ni.left, columns)) match {
              case Right(col) => col
              case Left(_) => Column.any(out, nulls)
            }
          }

        case nv2: Expr.Nvl2[Row, _, _] =>
          for {
            testCol <- evalColumn(nv2.test, columns, inferExprColumnType(nv2.test, columns))
            valueCol <- evalColumn(nv2.value, columns, inferExprColumnType(nv2.value, columns))
            altCol <- evalColumn(nv2.alt, columns, inferExprColumnType(nv2.alt, columns))
          } yield {
            val out = Array.tabulate[Any](rowCount) { i =>
              if (testCol.isNull(RowIndex(i))) altCol.getValue(i) else valueCol.getValue(i)
            }
            val nulls = valueCol.nullSet &~ testCol.nullSet | (altCol.nullSet & testCol.nullSet)
            Column.fromValues(out.toVector, inferExprColumnType(nv2.value, columns)) match {
              case Right(col) => col
              case Left(_) => Column.any(out, nulls)
            }
          }

        case nv: Expr.Nanvl[Row] =>
          for {
            leftCol <- evalColumn(nv.left, columns, ColumnType.DoubleType)
            rightCol <- evalColumn(nv.right, columns, ColumnType.DoubleType)
          } yield {
            (leftCol, rightCol) match {
              case (Column.DoubleColumn(ld, ln), Column.DoubleColumn(rd, rn)) =>
                Column.double(
                  Array.tabulate(rowCount)(i => if (ld(i).isNaN) rd(i) else ld(i)),
                  ln &~ BitSet.fromSpecific((0 until rowCount).filter(i => ld(i).isNaN)) | rn
                )
              case _ => Column.double(Array.empty[Double])
            }
          }

        case bc: Expr.BitCount[Row] =>
          evalColumn(bc.expr, columns, ColumnType.IntType).map {
            case Column.IntColumn(data, nulls) =>
              Column.int(Array.tabulate(rowCount)(i => Integer.bitCount(data(i))), nulls)
            case _ => Column.int(Array.empty[Int])
          }

        case bg: Expr.BitGet[Row] =>
          for {
            exprCol <- evalColumn(bg.expr, columns, ColumnType.IntType)
            posCol <- evalColumn(bg.pos, columns, ColumnType.IntType)
          } yield {
            (exprCol, posCol) match {
              case (Column.IntColumn(ed, en), Column.IntColumn(pd, pn)) =>
                val combined = en | pn
                Column.int(
                  Array.tabulate(rowCount)(i => (ed(i) >> pd(i)) & 1),
                  combined
                )
              case _ => Column.int(Array.empty[Int])
            }
          }

        case sl: Expr.ShiftLeft[Row] =>
          for {
            exprCol <- evalColumn(sl.expr, columns, ColumnType.IntType)
            nCol <- evalColumn(sl.n, columns, ColumnType.IntType)
          } yield {
            (exprCol, nCol) match {
              case (Column.IntColumn(ed, en), Column.IntColumn(nd, nn)) =>
                Column.int(Array.tabulate(rowCount)(i => ed(i) << nd(i)), en | nn)
              case _ => Column.int(Array.empty[Int])
            }
          }

        case sr: Expr.ShiftRight[Row] =>
          for {
            exprCol <- evalColumn(sr.expr, columns, ColumnType.IntType)
            nCol <- evalColumn(sr.n, columns, ColumnType.IntType)
          } yield {
            (exprCol, nCol) match {
              case (Column.IntColumn(ed, en), Column.IntColumn(nd, nn)) =>
                Column.int(Array.tabulate(rowCount)(i => ed(i) >> nd(i)), en | nn)
              case _ => Column.int(Array.empty[Int])
            }
          }

        case sru: Expr.ShiftRightUnsigned[Row] =>
          for {
            exprCol <- evalColumn(sru.expr, columns, ColumnType.IntType)
            nCol <- evalColumn(sru.n, columns, ColumnType.IntType)
          } yield {
            (exprCol, nCol) match {
              case (Column.IntColumn(ed, en), Column.IntColumn(nd, nn)) =>
                Column.int(Array.tabulate(rowCount)(i => ed(i) >>> nd(i)), en | nn)
              case _ => Column.int(Array.empty[Int])
            }
          }

        case bn: Expr.BitwiseNot[Row] =>
          evalColumn(bn.expr, columns, ColumnType.IntType).map {
            case Column.IntColumn(data, nulls) =>
              Column.int(Array.tabulate(rowCount)(i => ~data(i)), nulls)
            case _ => Column.int(Array.empty[Int])
          }

        case ic: Expr.Initcap[Row] =>
          evalColumn(ic.expr, columns, ColumnType.StringType).map {
            case sc: Column.StringColumn => mapString(sc)(initcapWord)
            case _ => Column.string(Array.empty[String | Null])
          }

        case ins: Expr.Instr[Row] =>
          for {
            strCol <- evalColumn(ins.str, columns, ColumnType.StringType)
            subCol <- evalColumn(ins.substr, columns, ColumnType.StringType)
          } yield {
            (strCol, subCol) match {
              case (Column.StringColumn(sd, sn), Column.StringColumn(xd, xn)) =>
                val combined = sn | xn
                Column.int(
                  Array.tabulate(rowCount) { i =>
                    if (combined.contains(i)) 0
                    else {
                      val idx = sd(i).nn.indexOf(xd(i).nn)
                      idx + 1
                    }
                  },
                  combined
                )
              case _ => Column.int(Array.empty[Int])
            }
          }

        case ins: Expr.Instr[Row] =>
          for {
            strCol <- evalColumn(ins.str, columns, ColumnType.StringType)
            subCol <- evalColumn(ins.substr, columns, ColumnType.StringType)
          } yield {
            (strCol, subCol) match {
              case (Column.StringColumn(sd, sn), Column.StringColumn(xd, xn)) =>
                val combined = sn | xn
                Column.int(
                  Array.tabulate(rowCount) { i =>
                    if (combined.contains(i)) 0
                    else {
                      val idx = sd(i).nn.indexOf(xd(i).nn)
                      idx + 1
                    }
                  },
                  combined
                )
              case _ => Column.int(Array.empty[Int])
            }
          }

        case si: Expr.SubstringIndex[Row] =>
          evalColumn(si.str, columns, ColumnType.StringType).map {
            case sc: Column.StringColumn => mapString(sc)(substringIndex(_, si.delim, si.count))
            case _ => Column.string(Array.empty[String | Null])
          }

        case ls2: Expr.LeftStr[Row] =>
          strIntBin(ls2.str, ls2.n, columns, rowCount)((str, n) => if (n <= 0) "" else str.take(n))

        case rs: Expr.RightStr[Row] =>
          strIntBin(rs.str, rs.n, columns, rowCount)((str, n) => if (n <= 0) "" else str.takeRight(n))

        case rp: Expr.Repeat[Row] =>
          strIntBin(rp.str, rp.n, columns, rowCount)((str, n) => if (n <= 0) "" else str * n)

        case rv: Expr.Reverse[Row] =>
          evalColumn(rv.str, columns, ColumnType.StringType).map {
            case sc: Column.StringColumn => mapString(sc)(_.reverse)
            case _ => Column.string(Array.empty[String | Null])
          }

        case lp: Expr.Lpad[Row] =>
          strIntPad(lp.str, lp.len, lp.pad, columns, rowCount, isLpad = true)

        case rp2: Expr.Rpad[Row] =>
          strIntPad(rp2.str, rp2.len, rp2.pad, columns, rowCount, isLpad = false)

        case tr2: Expr.Translate[Row] =>
          evalColumn(tr2.str, columns, ColumnType.StringType).map {
            case sc: Column.StringColumn =>
              val mapping = tr2.matching.zipAll(tr2.replace, ' ', ' ').toMap
              mapString(sc)(s => s.map(c => mapping.getOrElse(c, c)).mkString)
            case _ => Column.string(Array.empty[String | Null])
          }

        case fs: Expr.FormatString[Row] =>
          val argCols = fs.args.map(e => evalColumn(e, columns, inferExprColumnType(e, columns)))
          val firstErr = argCols.collectFirst { case Left(err) => err }
          firstErr match {
            case Some(err) => Left(err)
            case None =>
              val cols = argCols.collect { case Right(c) => c }
              Right(
                Column.string(
                  Array.tabulate(rowCount) { i =>
                    val args: Seq[AnyRef] = cols.map { c =>
                      val v = c.getValue(i)
                      v match {
                        case b: java.lang.Boolean => b
                        case n: java.lang.Integer => n
                        case n: java.lang.Long => n
                        case n: java.lang.Double => n
                        case s: String => s
                        case other => String.valueOf(other)
                      }
                    }
                    String.format(fs.format, args*)
                  }
                )
              )
          }

        case asc: Expr.Ascii[Row] =>
          evalColumn(asc.str, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              Column.int(
                Array.tabulate(rowCount) { i =>
                  if (nulls.contains(i)) 0
                  else if (data(i).nn.isEmpty) 0
                  else data(i).nn.charAt(0).toInt
                },
                nulls
              )
            case _ => Column.int(Array.empty[Int])
          }

        case ch: Expr.Chr[Row] =>
          evalColumn(ch.expr, columns, ColumnType.IntType).map {
            case ic: Column.IntColumn => mapAny(ic)(v => String.valueOf(String.valueOf(v).toInt.toChar))
            case _ => Column.string(Array.empty[String | Null])
          }

        case lv: Expr.Levenshtein[Row] =>
          for {
            leftCol <- evalColumn(lv.left, columns, ColumnType.StringType)
            rightCol <- evalColumn(lv.right, columns, ColumnType.StringType)
          } yield {
            (leftCol, rightCol) match {
              case (Column.StringColumn(ld, ln), Column.StringColumn(rd, rn)) =>
                val combined = ln | rn
                Column.int(
                  Array.tabulate(rowCount)(i => if (combined.contains(i)) 0 else levenshtein(ld(i).nn, rd(i).nn)),
                  combined
                )
              case _ => Column.int(Array.empty[Int])
            }
          }

        case rl: Expr.Rlike[Row] =>
          evalColumn(rl.str, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              val compiled = java.util.regex.Pattern.compile(rl.pattern)
              Column.boolean(
                Array.tabulate(rowCount)(i => if (nulls.contains(i)) false else compiled.matcher(data(i)).matches()),
                nulls
              )
            case _ => Column.boolean(Array.empty[Boolean])
          }

        case rea: Expr.RegexpExtractAll[Row] =>
          evalColumn(rea.str, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              val compiled = java.util.regex.Pattern.compile(rea.pattern)
              Column.any(
                Array.tabulate[Any | Null](rowCount) { i =>
                  if (nulls.contains(i)) null // scalafix:ok DisableSyntax.null
                  else {
                    val m = compiled.matcher(data(i))
                    val matches = List.newBuilder[String]
                    while (m.find()) {
                      val g = m.group(rea.groupIdx)
                      if (g != null) matches += g // scalafix:ok DisableSyntax.null
                    }
                    matches.result()
                  }
                },
                nulls
              )
            case _ => Column.any(Array.empty[Any | Null])
          }

        case sp2: Expr.SplitPart[Row] =>
          for {
            strCol <- evalColumn(sp2.str, columns, ColumnType.StringType)
            partCol <- evalColumn(sp2.part, columns, ColumnType.IntType)
            result <- (strCol, partCol) match {
              case (Column.StringColumn(sd, sn), Column.IntColumn(pd, pn)) =>
                val combined = sn | pn
                val zeroRow = (0 until rowCount).find(i => !combined.contains(i) && pd(i) == 0)
                zeroRow match {
                  case Some(i) => Left(ExecutionError.InvalidValue(s"partNum must not be 0 at row $i"))
                  case None =>
                    val out = new Array[String | Null](rowCount)
                    (0 until rowCount).foreach { i =>
                      if (combined.contains(i)) out(i) = null // scalafix:ok DisableSyntax.null
                      else {
                        val parts = sd(i).nn.split(java.util.regex.Pattern.quote(sp2.delim), -1).toIndexedSeq
                        val idx = pd(i)
                        val resolved = if (idx > 0) idx - 1 else parts.size + idx
                        out(i) =
                          if (resolved < 0 || resolved >= parts.size) ""
                          else parts(resolved)
                      }
                    }
                    Right(Column.string(out, combined))
                }
              case _ => Right(Column.string(Array.empty[String | Null]))
            }
          } yield result

        case pu: Expr.ParseUrl[Row] =>
          evalColumn(pu.url, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              Column.string(
                Array.tabulate(rowCount) { i =>
                  if (nulls.contains(i)) null // scalafix:ok DisableSyntax.null
                  else urlPart(data(i).nn, pu.part)
                },
                nulls
              )
            case _ => Column.string(Array.empty[String | Null])
          }

        case tc: Expr.NumberToChar[Row] =>
          evalColumn(tc.expr, columns, ColumnType.DoubleType).map {
            case dc: Column.DoubleColumn =>
              val formatter = new java.text.DecimalFormat(tc.format)
              mapAny(dc)(v => formatter.format(String.valueOf(v).toDouble))
            case _ => Column.string(Array.empty[String | Null])
          }

        case cb: Expr.Cbrt[Row] =>
          vectorizedDoubleUnaryOp(cb.expr, columns, rowCount)(Math.cbrt)

        case hy: Expr.Hypot[Row] =>
          vectorizedDoubleBinOp(hy.left, hy.right, columns, rowCount)(Math.hypot)

        case bnn: Expr.Bin[Row] =>
          evalColumn(bnn.expr, columns, ColumnType.LongType).map {
            case lc: Column.LongColumn => mapAny(lc)(v => java.lang.Long.toBinaryString(String.valueOf(v).toLong))
            case _ => Column.string(Array.empty[String | Null])
          }

        case uh: Expr.Unhex[Row] =>
          evalColumn(uh.expr, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              val byteArrays = Array.tabulate(rowCount) { i =>
                if (nulls.contains(i)) None
                else unhexString(data(i).nn)
              }
              val nulls2 = BitSet.fromSpecific(
                (0 until rowCount).filter(i => byteArrays(i).isEmpty)
              )
              Column.binaryFromArrays(
                byteArrays.map(_.getOrElse(Array.empty[Byte])),
                nulls2
              )
            case _ => Column.binary(Array.empty[Byte], Array(0))
          }

        case br: Expr.Bround[Row] =>
          evalColumn(br.expr, columns, ColumnType.DoubleType).map {
            case Column.DoubleColumn(data, nulls) =>
              Column.double(
                Array.tabulate(rowCount)(i =>
                  BigDecimal(data(i)).setScale(br.scale, BigDecimal.RoundingMode.HALF_EVEN).toDouble
                ),
                nulls
              )
            case _ => Column.double(Array.empty[Double])
          }

        case cv: Expr.Conv[Row] =>
          evalColumn(cv.num, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              Column.string(
                Array.tabulate(rowCount) { i =>
                  if (nulls.contains(i)) null // scalafix:ok DisableSyntax.null
                  else
                    try {
                      val parsed = java.lang.Long.parseLong(data(i), cv.fromBase)
                      java.lang.Long.toString(parsed, cv.toBase)
                    } catch {
                      case _: NumberFormatException => null // scalafix:ok DisableSyntax.null
                    }
                },
                nulls
              )
            case _ => Column.string(Array.empty[String | Null])
          }

        case fa: Expr.Factorial[Row] =>
          evalColumn(fa.expr, columns, ColumnType.IntType).map {
            case Column.IntColumn(data, nulls) =>
              val out = new Array[Long](rowCount)
              val outNulls = scala.collection.mutable.BitSet.empty
              (0 until rowCount).foreach { i =>
                if (nulls.contains(i) || data(i) < 0) outNulls += i
                else out(i) = (1L to data(i).toLong).product
              }
              Column.long(out, nulls | (BitSet.empty ++ outNulls))
            case _ => Column.long(Array.empty[Long])
          }

        case sn2: Expr.Sinh[Row] => vectorizedDoubleUnaryOp(sn2.expr, columns, rowCount)(Math.sinh)
        case cs2: Expr.Cosh[Row] => vectorizedDoubleUnaryOp(cs2.expr, columns, rowCount)(Math.cosh)
        case tn2: Expr.Tanh[Row] => vectorizedDoubleUnaryOp(tn2.expr, columns, rowCount)(Math.tanh)
        case as2: Expr.Asinh[Row] =>
          vectorizedDoubleUnaryOp(as2.expr, columns, rowCount)(x => Math.log(x + Math.sqrt(x * x + 1.0)))
        case ac2: Expr.Acosh[Row] =>
          vectorizedDoubleUnaryOp(ac2.expr, columns, rowCount)(x => Math.log(x + Math.sqrt(x * x - 1.0)))
        case at2: Expr.Atanh[Row] =>
          vectorizedDoubleUnaryOp(at2.expr, columns, rowCount)(x => 0.5 * Math.log((1.0 + x) / (1.0 - x)))
        case dg: Expr.Degrees[Row] =>
          vectorizedDoubleUnaryOp(dg.expr, columns, rowCount)(x => x * 180.0 / Math.PI)
        case rd2: Expr.Radians[Row] => vectorizedDoubleUnaryOp(rd2.expr, columns, rowCount)(Math.toRadians)
        case l1p: Expr.Log1p[Row] => vectorizedDoubleUnaryOp(l1p.expr, columns, rowCount)(Math.log1p)
        case e1m: Expr.Expm1[Row] => vectorizedDoubleUnaryOp(e1m.expr, columns, rowCount)(Math.expm1)

        case _: Expr.Pi[Row] =>
          Right(Column.double(Array.fill(rowCount)(Math.PI)))
        case _: Expr.Euler[Row] =>
          Right(Column.double(Array.fill(rowCount)(Math.E)))

        case wb: Expr.WidthBucket[Row] =>
          for {
            vCol <- evalColumn(wb.value, columns, ColumnType.DoubleType)
            minCol <- evalColumn(wb.min, columns, ColumnType.DoubleType)
            maxCol <- evalColumn(wb.max, columns, ColumnType.DoubleType)
            bCol <- evalColumn(wb.buckets, columns, ColumnType.IntType)
          } yield {
            (vCol, minCol, maxCol, bCol) match {
              case (
                    Column.DoubleColumn(vd, vn),
                    Column.DoubleColumn(mind, minn),
                    Column.DoubleColumn(maxd, maxn),
                    Column.IntColumn(bd, bn)
                  ) =>
                val combined = vn | minn | maxn | bn
                Column.int(
                  Array.tabulate(rowCount) { i =>
                    if (combined.contains(i)) 0
                    else {
                      val v = vd(i); val lo = mind(i); val hi = maxd(i); val n = bd(i)
                      if (n <= 0 || hi < lo) 0
                      else if (v < lo) 0
                      else if (v > hi) n + 1
                      else ((v - lo) / ((hi - lo) / n)).toInt + 1
                    }
                  },
                  combined
                )
              case _ => Column.int(Array.empty[Int])
            }
          }

        case rn2: Expr.Randn[Row] =>
          val random = new java.util.Random(rn2.seed)
          Right(Column.double(Array.tabulate(rowCount)(_ => random.nextGaussian())))

        case pm: Expr.PmodInt[Row] =>
          for {
            leftCol <- evalColumn(pm.left, columns, ColumnType.IntType)
            rightCol <- evalColumn(pm.right, columns, ColumnType.IntType)
          } yield {
            (leftCol, rightCol) match {
              case (Column.IntColumn(ld, ln), Column.IntColumn(rd, rn)) =>
                val combined = ln | rn | BitSet.fromSpecific(
                  (0 until rowCount).filter(i => rd(i) == 0)
                )
                Column.int(
                  Array.tabulate(rowCount)(i => if (rd(i) == 0) 0 else Math.floorMod(ld(i), rd(i))),
                  combined
                )
              case _ => Column.int(Array.empty[Int])
            }
          }

        case pml: Expr.PmodLong[Row] =>
          for {
            leftCol <- evalColumn(pml.left, columns, ColumnType.LongType)
            rightCol <- evalColumn(pml.right, columns, ColumnType.LongType)
          } yield {
            (leftCol, rightCol) match {
              case (Column.LongColumn(ld, ln), Column.LongColumn(rd, rn)) =>
                val combined = ln | rn | BitSet.fromSpecific(
                  (0 until rowCount).filter(i => rd(i) == 0L)
                )
                Column.long(
                  Array.tabulate(rowCount)(i => if (rd(i) == 0L) 0L else Math.floorMod(ld(i), rd(i))),
                  combined
                )
              case _ => Column.long(Array.empty[Long])
            }
          }

        case ta: Expr.TryAddInt[Row] =>
          for {
            leftCol <- evalColumn(ta.left, columns, ColumnType.IntType)
            rightCol <- evalColumn(ta.right, columns, ColumnType.IntType)
          } yield {
            (leftCol, rightCol) match {
              case (Column.IntColumn(ld, ln), Column.IntColumn(rd, rn)) =>
                val out = new Array[Int](rowCount)
                val outNulls = scala.collection.mutable.BitSet.empty
                outNulls ++= ln
                outNulls ++= rn
                (0 until rowCount).foreach { i =>
                  val r = ld(i).toLong + rd(i).toLong
                  if (r > Int.MaxValue.toLong || r < Int.MinValue.toLong) outNulls += i
                  else out(i) = r.toInt
                }
                Column.int(out, BitSet.empty ++ outNulls)
              case _ => Column.int(Array.empty[Int])
            }
          }

        case tal: Expr.TryAddLong[Row] =>
          tryLongOp(tal.left, tal.right, columns, rowCount) { (a, b) =>
            try Some(Math.addExact(a, b))
            catch { case _: ArithmeticException => None }
          }

        case tsl: Expr.TrySubtractLong[Row] =>
          tryLongOp(tsl.left, tsl.right, columns, rowCount) { (a, b) =>
            try Some(Math.subtractExact(a, b))
            catch { case _: ArithmeticException => None }
          }

        case tml: Expr.TryMultiplyLong[Row] =>
          tryLongOp(tml.left, tml.right, columns, rowCount) { (a, b) =>
            try Some(Math.multiplyExact(a, b))
            catch { case _: ArithmeticException => None }
          }

        case tdl: Expr.TryDivideLong[Row] =>
          for {
            leftCol <- evalColumn(tdl.left, columns, ColumnType.LongType)
            rightCol <- evalColumn(tdl.right, columns, ColumnType.LongType)
          } yield (leftCol, rightCol) match {
            case (Column.LongColumn(ld, ln), Column.LongColumn(rd, rn)) =>
              val combined = ln | rn | BitSet.fromSpecific(
                (0 until rowCount).filter(i => rd(i) == 0L)
              )
              Column.double(
                Array.tabulate(rowCount)(i => if (rd(i) == 0L) 0.0 else ld(i).toDouble / rd(i).toDouble),
                combined
              )
            case _ => Column.double(Array.empty[Double])
          }

        case tdd: Expr.TryDivideDouble[Row] =>
          for {
            leftCol <- evalColumn(tdd.left, columns, ColumnType.DoubleType)
            rightCol <- evalColumn(tdd.right, columns, ColumnType.DoubleType)
          } yield {
            (leftCol, rightCol) match {
              case (Column.DoubleColumn(ld, ln), Column.DoubleColumn(rd, rn)) =>
                val combined = ln | rn | BitSet.fromSpecific(
                  (0 until rowCount).filter(i => rd(i) == 0.0)
                )
                Column.double(
                  Array.tabulate(rowCount)(i => if (rd(i) == 0.0) 0.0 else ld(i) / rd(i)),
                  combined
                )
              case _ => Column.double(Array.empty[Double])
            }
          }

        case ut: Expr.UnixTimestamp[Row] =>
          evalColumn(ut.expr, columns, ColumnType.TimestampType).map {
            case Column.TimestampColumn(data, nulls) =>
              Column.long(Array.tabulate(rowCount)(i => Math.floorDiv(data(i), 1000000L)), nulls)
            case _ => Column.long(Array.empty[Long])
          }

        case fu: Expr.FromUnixtime[Row] =>
          evalColumn(fu.expr, columns, ColumnType.LongType).map {
            case lc: Column.LongColumn =>
              val formatter = java.time.format.DateTimeFormatter
                .ofPattern("yyyy-MM-dd HH:mm:ss")
                .withZone(java.time.ZoneOffset.UTC)
              mapAny(lc)(v => formatter.format(java.time.Instant.ofEpochSecond(String.valueOf(v).toLong)))
            case _ => Column.string(Array.empty[String | Null])
          }

        case tt: Expr.ToTimestamp[Row] =>
          evalColumn(tt.expr, columns, ColumnType.StringType).flatMap {
            case Column.StringColumn(data, nulls) =>
              val out = new Array[Long](rowCount)
              val outNulls = scala.collection.mutable.BitSet.empty
              val error = (0 until rowCount).foldLeft(Option.empty[ExecutionError]) { (err, i) =>
                err match {
                  case some @ Some(_) => some
                  case None =>
                    if (nulls.contains(i)) { outNulls += i; None }
                    else
                      parseIsoTimestamp(data(i).nn) match {
                        case Some(micros) => out(i) = micros; None
                        case None => { outNulls += i; None }
                      }
                }
              }
              error match {
                case Some(e) => Left(e)
                case None => Right(Column.timestamp(out, nulls | (BitSet.empty ++ outNulls)))
              }
            case other => Left(ExecutionError.TypeMismatch("StringColumn", other.columnType.toString, "ToTimestamp"))
          }

        case td: Expr.ToDate[Row] =>
          evalColumn(td.expr, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              val out = new Array[Int](rowCount)
              val outNulls = scala.collection.mutable.BitSet.empty
              (0 until rowCount).foreach { i =>
                if (nulls.contains(i)) outNulls += i
                else
                  try out(i) = java.time.LocalDate.parse(data(i)).toEpochDay.toInt
                  catch { case _: java.time.format.DateTimeParseException => outNulls += i }
              }
              Column.date(out, nulls | (BitSet.empty ++ outNulls))
            case _ => Column.date(Array.empty[Int])
          }

        case _: Expr.CurrentDate[Row] =>
          Right(Column.date(Array.fill(rowCount)(java.time.LocalDate.now().nn.toEpochDay.toInt)))

        case _: Expr.Now[Row] =>
          Right(Column.timestamp(Array.fill(rowCount)(System.currentTimeMillis() * 1000L)))

        case tss: Expr.TimestampSeconds[Row] =>
          evalColumn(tss.expr, columns, ColumnType.DoubleType).map {
            case Column.DoubleColumn(data, nulls) =>
              Column.timestamp(
                Array.tabulate(rowCount)(i => Math.round(data(i) * 1000000.0)),
                nulls
              )
            case _ => Column.timestamp(Array.empty[Long])
          }

        case tsm: Expr.TimestampMillis[Row] =>
          evalColumn(tsm.expr, columns, ColumnType.LongType).map {
            case Column.LongColumn(data, nulls) => Column.timestamp(data.map(_ * 1000L), nulls)
            case _ => Column.timestamp(Array.empty[Long])
          }

        case tsmi: Expr.TimestampMicros[Row] =>
          evalColumn(tsmi.expr, columns, ColumnType.LongType).map {
            case Column.LongColumn(data, nulls) => Column.timestamp(data, nulls)
            case _ => Column.timestamp(Array.empty[Long])
          }

        case mt: Expr.MakeTimestamp[Row] =>
          for {
            yCol <- evalColumn(mt.year, columns, ColumnType.IntType)
            moCol <- evalColumn(mt.month, columns, ColumnType.IntType)
            dCol <- evalColumn(mt.day, columns, ColumnType.IntType)
            hCol <- evalColumn(mt.hour, columns, ColumnType.IntType)
            miCol <- evalColumn(mt.minute, columns, ColumnType.IntType)
            sCol <- evalColumn(mt.sec, columns, ColumnType.DoubleType)
          } yield {
            (yCol, moCol, dCol, hCol, miCol, sCol) match {
              case (
                    Column.IntColumn(yd, yn),
                    Column.IntColumn(mod, mon),
                    Column.IntColumn(dd, dn),
                    Column.IntColumn(hd, hn),
                    Column.IntColumn(mid, minn),
                    Column.DoubleColumn(sd2, sn)
                  ) =>
                val combined = yn | mon | dn | hn | minn | sn
                Column.timestamp(
                  Array.tabulate(rowCount) { i =>
                    val secs = sd2(i).toLong
                    val nanos = ((sd2(i) - secs) * 1e9).toLong
                    java.time.LocalDateTime
                      .of(yd(i), mod(i), dd(i), hd(i), mid(i), secs.toInt, nanos.toInt)
                      .toEpochSecond(java.time.ZoneOffset.UTC) * 1000000L + nanos / 1000L
                  },
                  combined
                )
              case _ => Column.timestamp(Array.empty[Long])
            }
          }

        case mdi: Expr.MakeDtInterval[Row] =>
          for {
            dCol <- evalColumn(mdi.days, columns, ColumnType.LongType)
            hCol <- evalColumn(mdi.hours, columns, ColumnType.IntType)
            miCol <- evalColumn(mdi.minutes, columns, ColumnType.IntType)
            sCol <- evalColumn(mdi.seconds, columns, ColumnType.DoubleType)
          } yield {
            (dCol, hCol, miCol, sCol) match {
              case (
                    Column.LongColumn(dd, dn),
                    Column.IntColumn(hd, hn),
                    Column.IntColumn(mid, minn),
                    Column.DoubleColumn(sd2, sn)
                  ) =>
                val combined = dn | hn | minn | sn
                Column.dayTimeInterval(
                  Array.tabulate(rowCount) { i =>
                    dd(i) * 86400000000L + hd(i) * 3600000000L + mid(i) * 60000000L +
                      Math.round(sd2(i) * 1000000.0)
                  },
                  combined
                )
              case _ => Column.dayTimeInterval(Array.empty[Long])
            }
          }

        case myi: Expr.MakeYmInterval[Row] =>
          for {
            yCol <- evalColumn(myi.years, columns, ColumnType.IntType)
            mCol <- evalColumn(myi.months, columns, ColumnType.IntType)
          } yield {
            (yCol, mCol) match {
              case (Column.IntColumn(yd, yn), Column.IntColumn(md, mn)) =>
                Column.yearMonthInterval(Array.tabulate(rowCount)(i => yd(i) * 12 + md(i)), yn | mn)
              case _ => Column.yearMonthInterval(Array.empty[Int])
            }
          }

        case ho: Expr.HourOf[Row] =>
          evalColumn(ho.expr, columns, ColumnType.TimestampType).map {
            case Column.TimestampColumn(data, nulls) =>
              Column.int(Array.tabulate(rowCount)(i => epochMicrosToUtcDateTime(data(i)).getHour), nulls)
            case _ => Column.int(Array.empty[Int])
          }

        case mo: Expr.MinuteOf[Row] =>
          evalColumn(mo.expr, columns, ColumnType.TimestampType).map {
            case Column.TimestampColumn(data, nulls) =>
              Column.int(Array.tabulate(rowCount)(i => epochMicrosToUtcDateTime(data(i)).getMinute), nulls)
            case _ => Column.int(Array.empty[Int])
          }

        case so: Expr.SecondOf[Row] =>
          evalColumn(so.expr, columns, ColumnType.TimestampType).map {
            case Column.TimestampColumn(data, nulls) =>
              Column.int(Array.tabulate(rowCount)(i => epochMicrosToUtcDateTime(data(i)).getSecond), nulls)
            case _ => Column.int(Array.empty[Int])
          }

        case fut: Expr.FromUtcTimestamp[Row] =>
          evalColumn(fut.expr, columns, ColumnType.TimestampType).map {
            case Column.TimestampColumn(data, nulls) =>
              Column.timestamp(
                Array.tabulate(rowCount)(i => fromUtcMicros(data(i), fut.tz)),
                nulls
              )
            case _ => Column.timestamp(Array.empty[Long])
          }

        case tut: Expr.ToUtcTimestamp[Row] =>
          evalColumn(tut.expr, columns, ColumnType.TimestampType).map {
            case Column.TimestampColumn(data, nulls) =>
              Column.timestamp(
                Array.tabulate(rowCount)(i => shiftWallClock(data(i), java.time.ZoneId.of(tut.tz), "UTC")),
                nulls
              )
            case _ => Column.timestamp(Array.empty[Long])
          }

        case ctz: Expr.ConvertTimezone[Row] =>
          evalColumn(ctz.expr, columns, ColumnType.TimestampType).map {
            case Column.TimestampColumn(data, nulls) =>
              Column.timestamp(
                Array.tabulate(rowCount)(i => fromUtcMicros(data(i), ctz.toTz)),
                nulls
              )
            case _ => Column.timestamp(Array.empty[Long])
          }

        case ta2: Expr.TimestampAdd[Row] =>
          evalColumn(ta2.ts, columns, ColumnType.TimestampType).flatMap { tsCol =>
            evalColumn(ta2.qty, columns, ColumnType.IntType).map {
              case Column.IntColumn(qd, qn) =>
                tsCol match {
                  case Column.TimestampColumn(td, tn) =>
                    val combined = tn | qn
                    Column.timestamp(
                      Array.tabulate(rowCount) { i =>
                        addTimestampUnit(ta2.unit, qd(i), td(i))
                      },
                      combined
                    )
                  case _ => Column.timestamp(Array.empty[Long])
                }
              case other => other
            }
          }

        case tdf: Expr.TimestampDiff[Row] =>
          for {
            startCol <- evalColumn(tdf.start, columns, ColumnType.TimestampType)
            endCol <- evalColumn(tdf.end, columns, ColumnType.TimestampType)
          } yield {
            (startCol, endCol) match {
              case (Column.TimestampColumn(sd2, sn), Column.TimestampColumn(ed, en)) =>
                val combined = sn | en
                Column.long(
                  Array.tabulate(rowCount)(i => diffTimestampUnits(tdf.unit, sd2(i), ed(i))),
                  combined
                )
              case _ => Column.long(Array.empty[Long])
            }
          }

        case wk: Expr.Weekday[Row] =>
          evalColumn(wk.expr, columns, ColumnType.DateType).map {
            case Column.DateColumn(data, nulls) =>
              Column.int(
                Array.tabulate(rowCount) { i =>
                  if (nulls.contains(i)) 0
                  else java.time.LocalDate.ofEpochDay(data(i).toLong).getDayOfWeek.getValue - 1
                },
                nulls
              )
            case _ => Column.int(Array.empty[Int])
          }

        case ap: Expr.ArrayAppend[Row, _] =>
          for {
            arrCol <- evalColumn(ap.arr, columns, ColumnType.AnyType)
            elemCol <- evalColumn(ap.elem, columns, inferExprColumnType(ap.elem, columns))
          } yield Column.any(
            Array.tabulate[Any](rowCount) { i =>
              arrCol.getValue(i) match {
                case s: Seq[?] => s.toVector :+ elemCol.getValue(i)
                case _ => Vector(elemCol.getValue(i))
              }
            }
          )

        case ap2: Expr.ArrayPrepend[Row, _] =>
          for {
            arrCol <- evalColumn(ap2.arr, columns, ColumnType.AnyType)
            elemCol <- evalColumn(ap2.elem, columns, inferExprColumnType(ap2.elem, columns))
          } yield Column.any(
            Array.tabulate[Any](rowCount) { i =>
              arrCol.getValue(i) match {
                case s: Seq[?] => elemCol.getValue(i) +: s.toVector
                case _ => Vector(elemCol.getValue(i))
              }
            }
          )

        case ai2: Expr.ArrayInsert[Row, _] =>
          for {
            arrCol <- evalColumn(ai2.arr, columns, ColumnType.AnyType)
            posCol <- evalColumn(ai2.pos, columns, ColumnType.IntType)
            elemCol <- evalColumn(ai2.elem, columns, inferExprColumnType(ai2.elem, columns))
          } yield Column.any(
            Array.tabulate[Any](rowCount) { i =>
              val seq = arrCol.getValue(i) match {
                case s: Seq[?] => s.toVector
                case _ => Vector.empty
              }
              val pos = posCol.getValue(i) match { case p: Int => p; case _ => 0 }
              val elem = elemCol.getValue(i)
              val idx = if (pos > 0) pos - 1 else if (pos < 0) seq.size + pos + 1 else 0
              if (idx >= seq.size) seq ++ Vector.fill(idx - seq.size)(null) :+ elem // scalafix:ok DisableSyntax.null
              else if (idx <= 0) elem +: seq
              else seq.patch(idx, Vector(elem), 0)
            }
          )

        case ar: Expr.ArrayRemove[Row, _] =>
          for {
            arrCol <- evalColumn(ar.arr, columns, ColumnType.AnyType)
            elemCol <- evalColumn(ar.elem, columns, inferExprColumnType(ar.elem, columns))
          } yield Column.any(
            Array.tabulate[Any](rowCount) { i =>
              val elem = elemCol.getValue(i)
              arrCol.getValue(i) match {
                case s: Seq[?] => s.filterNot(e => java.util.Objects.equals(e, elem))
                case _ => Vector.empty
              }
            }
          )

        case arp: Expr.ArrayRepeat[Row, _] =>
          for {
            elemCol <- evalColumn(arp.elem, columns, inferExprColumnType(arp.elem, columns))
            countCol <- evalColumn(arp.count, columns, ColumnType.IntType)
          } yield Column.any(
            Array.tabulate[Any](rowCount) { i =>
              val n = countCol.getValue(i) match { case c: Int => Math.max(c, 0); case _ => 0 }
              Vector.fill(n)(elemCol.getValue(i))
            }
          )

        case aj: Expr.ArrayJoin[Row] =>
          evalColumn(aj.arr, columns, ColumnType.AnyType).map { col =>
            Column.string(
              Array.tabulate(rowCount) { i =>
                if (col.isNull(RowIndex(i))) null // scalafix:ok DisableSyntax.null
                else
                  col.getValue(i) match {
                    case s: Seq[?] =>
                      s.flatMap {
                        case v: String => Some(v)
                        case v if Option(v).isEmpty => aj.nullReplacement
                        case v => Some(String.valueOf(v))
                      }.mkString(aj.delimiter)
                    case _ => null // scalafix:ok DisableSyntax.null
                  }
              },
              col.nullSet
            )
          }

        case amx: Expr.ArrayMax[Row, _] =>
          arrayExtremum(amx.arr, amx.ordering, columns, rowCount, isMax = true)

        case amn: Expr.ArrayMin[Row, _] =>
          arrayExtremum(amn.arr, amn.ordering, columns, rowCount, isMax = false)

        case ac2: Expr.ArrayCompact[Row, _] =>
          evalColumn(ac2.arr, columns, ColumnType.AnyType).map { col =>
            Column.any(
              Array.tabulate[Any](rowCount) { i =>
                col.getValue(i) match {
                  case s: Seq[?] => s.filterNot(v => Option(v).isEmpty)
                  case _ => Vector.empty
                }
              }
            )
          }

        case apo: Expr.ArrayPosition[Row, _] =>
          for {
            arrCol <- evalColumn(apo.arr, columns, ColumnType.AnyType)
            elemCol <- evalColumn(apo.elem, columns, inferExprColumnType(apo.elem, columns))
          } yield Column.int(
            Array.tabulate(rowCount) { i =>
              val elem = elemCol.getValue(i)
              arrCol.getValue(i) match {
                case s: Seq[?] =>
                  s.indexWhere(e => java.util.Objects.equals(e, elem)) + 1
                case _ => 0
              }
            }
          )

        case az: Expr.ArraysZip[Row] =>
          val arrCols = az.arrays.map(e => evalColumn(e, columns, ColumnType.AnyType))
          val firstErr = arrCols.collectFirst { case Left(err) => err }
          firstErr match {
            case Some(err) => Left(err)
            case None =>
              val cols = arrCols.collect { case Right(c) => c }
              Right(
                Column.any(
                  Array.tabulate[Any](rowCount) { i =>
                    val seqs = cols.map(c =>
                      c.getValue(i) match {
                        case s: Seq[?] => s.toVector
                        case _ => Vector.empty
                      }
                    )
                    val maxLen = seqs.map(_.size).foldLeft(0)(Math.max)
                    Vector.tabulate(maxLen) { pos =>
                      (0 until seqs.size).foldLeft(Map.empty[String, Any]) { (m, k) =>
                        val v = if (pos < seqs(k).size) seqs(k)(pos) else null // scalafix:ok DisableSyntax.null
                        m + (k.toString -> v)
                      }
                    }
                  }
                )
              )
          }

        case ao: Expr.ArraysOverlap[Row, _] =>
          for {
            leftCol <- evalColumn(ao.left, columns, ColumnType.AnyType)
            rightCol <- evalColumn(ao.right, columns, ColumnType.AnyType)
          } yield Column.boolean(
            Array.tabulate(rowCount) { i =>
              val toSet = (s: Seq[?]) => s.filterNot(v => Option(v).isEmpty).toSet
              (leftCol.getValue(i), rightCol.getValue(i)) match {
                case (l: Seq[?], r: Seq[?]) => toSet(l).exists(toSet(r).contains)
                case _ => false
              }
            }
          )

        case mfe: Expr.MapFromEntries[Row, _, _] =>
          evalColumn(mfe.arr, columns, ColumnType.AnyType).map { col =>
            Column.any(
              Array.tabulate[Any](rowCount) { i =>
                col.getValue(i) match {
                  case s: Seq[?] => s.collect { case (k, v) => k -> v }.toMap
                  case _ => Map.empty
                }
              }
            )
          }

        case ga: Expr.GetArray[Row, _] =>
          for {
            arrCol <- evalColumn(ga.arr, columns, ColumnType.AnyType)
            idxCol <- evalColumn(ga.index, columns, ColumnType.IntType)
          } yield Column.any(
            Array.tabulate[Any | Null](rowCount) { i =>
              val idx = idxCol.getValue(i) match { case v: Int => v; case _ => 0 }
              arrCol.getValue(i) match {
                case s: Seq[?] => if (idx >= 0 && idx < s.size) s(idx) else null // scalafix:ok DisableSyntax.null
                case _ => null // scalafix:ok DisableSyntax.null
              }
            }
          )

        case _: Expr.Posexplode[Row] | _: Expr.ExplodeOuter[Row] | _: Expr.Inline[Row] =>
          Left(ExecutionError.UnsupportedOperation("Generator functions require Dataset-level handling"))

        case _: Expr.Sum[Row] | _: Expr.SumDouble[Row] | _: Expr.SumLong[Row] | _: Expr.Count[Row] |
            _: Expr.Max[Row, ?] | _: Expr.Min[Row, ?] | _: Expr.Avg[Row] | _: Expr.CountDistinct[Row, ?] |
            _: Expr.CountIf[Row] | _: Expr.StdDev[Row] | _: Expr.StdDevPop[Row] | _: Expr.First[Row, ?] |
            _: Expr.Collect[Row, ?] | _: Expr.PercentileApprox[Row] | _: Expr.MaxBy[Row, ?, ?] |
            _: Expr.MinBy[Row, ?, ?] | _: Expr.MaxN[Row, ?] | _: Expr.MinN[Row, ?] | _: Expr.MaxByN[Row, ?, ?] |
            _: Expr.MinByN[Row, ?, ?] | _: Expr.Variance[Row] | _: Expr.VariancePop[Row] |
            _: Expr.ApproxCountDistinct[Row, ?] | _: Expr.CollectSet[Row, ?] | _: Expr.ExprLast[Row, ?] |
            _: Expr.AnyValue[Row, ?] | _: Expr.BoolAnd[Row] | _: Expr.BoolOr[Row] | _: Expr.Corr[Row] |
            _: Expr.CovarSamp[Row] | _: Expr.CovarPop[Row] | _: Expr.Median[Row] | _: Expr.Mode[Row, ?] =>
          Left(ExecutionError.UnsupportedOperation("Aggregations not supported in columnar evalColumn"))

        case _: Expr.RowNumber[Row] | _: Expr.Rank[Row] | _: Expr.DenseRank[Row] | _: Expr.Lag[Row, ?] |
            _: Expr.Lead[Row, ?] | _: Expr.NTile[Row] | _: Expr.CumeDist[Row] | _: Expr.PercentRank[Row] |
            _: Expr.NthValue[Row, ?] | _: Expr.FirstValue[Row, ?] | _: Expr.LastValue[Row, ?] =>
          Left(ExecutionError.UnsupportedOperation("Window functions not supported in columnar evalColumn"))

        case _: Expr.CurrentPath[Row] | _: Expr.SketchEstimate[Row] | _: Expr.SketchSummary[Row] |
            _: Expr.SketchTheta[Row] | _: Expr.SketchBinaryOp[Row] | _: Expr.TupleSketchAgg[Row] |
            _: Expr.SketchSetAgg[Row] | _: Expr.KllSketchAgg[Row] | _: Expr.KllQuantile[Row] | _: Expr.KllRank[Row] =>
          Left(
            ExecutionError.UnsupportedOperation(
              "Datasketches and current_path are Spark-only; use SparkInterpreter"
            )
          )
        case _: Expr.AesEncrypt[Row] | _: Expr.AesDecrypt[Row] | _: Expr.TryAesDecrypt[Row] =>
          Left(
            ExecutionError.UnsupportedOperation(
              "AES functions use Spark's key-derivation and cipher format; use SparkInterpreter"
            )
          )
        case soj: Expr.SchemaOfJson[Row] =>
          evalColumn(soj.expr, columns, ColumnType.StringType).map { col =>
            val out = new Array[Any | Null](rowCount)
            val outNulls = scala.collection.mutable.BitSet.empty
            (0 until rowCount).foreach { i =>
              if (col.isNull(RowIndex(i))) outNulls += i
              else
                col.getValue(i) match {
                  case s: String =>
                    parseJson(s) match {
                      case parser.core.Result.Success(v, _) =>
                        jsonValueToSqlType(v) match {
                          case Some(t) => out(i) = t
                          case None => outNulls += i
                        }
                      case _ => outNulls += i
                    }
                  case _ => outNulls += i
                }
            }
            Column.any(out, col.nullSet | (BitSet.empty ++ outNulls))
          }

        case jal: Expr.JsonArrayLength[Row] =>
          evalColumn(jal.expr, columns, ColumnType.StringType).map { col =>
            jsonPathAggregate(col, rowCount, jal.path) {
              case JsonValue.Array(items) => Some(items.size.toLong)
              case _ => None
            }
          }

        case jok: Expr.JsonObjectKeys[Row] =>
          evalColumn(jok.expr, columns, ColumnType.StringType).map { col =>
            jsonPathAggregate(col, rowCount, jok.path) {
              case JsonValue.Object(fields) => Some(fields.keys.toSeq)
              case _ => None
            }
          }

        case fj: Expr.FromJson[Row, _] =>
          evalColumn(fj.expr, columns, ColumnType.StringType).map { col =>
            val out = new Array[Any | Null](rowCount)
            val outNulls = scala.collection.mutable.BitSet.empty
            (0 until rowCount).foreach { i =>
              if (col.isNull(RowIndex(i))) outNulls += i
              else
                col.getValue(i) match {
                  case s: String =>
                    parseJson(s) match {
                      case parser.core.Result.Success(v: JsonValue, _) =>
                        fj.decoder.decode(v).toEither match {
                          case Right(value) => out(i) = value
                          case Left(_) => outNulls += i
                        }
                      case _ => outNulls += i
                    }
                  case _ => outNulls += i
                }
            }
            Column.any(out, col.nullSet | (BitSet.empty ++ outNulls))
          }

        case tj: Expr.ToJson[Row, t] =>
          evalColumn(tj.expr, columns, inferExprColumnType(tj.expr, columns)).map { col =>
            mapAny(col) { v =>
              // erasure boundary: the stored value is T by construction of the Expr
              val value = v.asInstanceOf[t] // scalafix:ok DisableSyntax.asInstanceOf
              formatJson(tj.encoder.encode(value))
            }
          }

      }
    }
  }

  private def rowWiseExtreme[Row, A](
    exprs: Vector[Expr[Row, A]],
    ordering: Ordering[A],
    columns: Vector[Column[?]],
    rowCount: Int,
    isMax: Boolean
  ): Either[ExecutionError, Column[?]] = {
    val colResults = exprs.map(e => evalColumn(e, columns, inferExprColumnType(e, columns)))
    val firstErr = colResults.collectFirst { case Left(err) => err }
    firstErr match {
      case Some(err) => Left(err)
      case None =>
        val cols = colResults.collect { case Right(c) => c }
        val out = Array.tabulate[Option[Any]](rowCount) { i =>
          val candidates = cols.filterNot(_.isNull(RowIndex(i))).map(_.getValue(i))
          if (candidates.isEmpty) None
          else Some(pickExtreme(candidates, ordering, isMax))
        }
        val outNulls = BitSet.fromSpecific((0 until rowCount).filter(i => out(i).isEmpty))
        val values = out.toVector.map(_.getOrElse(null)) // scalafix:ok DisableSyntax.null
        val result: Column[?] = Column.fromValues(values, inferExprColumnType(exprs.head, columns)) match {
          case Right(col) => col
          case Left(_) => Column.any(values.toArray, outNulls)
        }
        Right(result)
    }
  }

  private def pickExtreme[A](
    candidates: Vector[Any],
    ordering: Ordering[A],
    isMax: Boolean
  ): Any =
    candidates.foldLeft(candidates.head) { (best, v) =>
      val cmp = ordering.compare(v.asInstanceOf[A], best.asInstanceOf[A]) // scalafix:ok DisableSyntax.asInstanceOf
      if (isMax && cmp > 0 || !isMax && cmp < 0) v else best
    }

  private def arrayExtremum[Row, A](
    arrExpr: Expr[Row, Seq[A]],
    ordering: Ordering[A],
    columns: Vector[Column[?]],
    rowCount: Int,
    isMax: Boolean
  ): Either[ExecutionError, Column[?]] =
    evalColumn(arrExpr, columns, ColumnType.AnyType).map { col =>
      Column.any(
        Array.tabulate[Any | Null](rowCount) { i =>
          col.getValue(i) match {
            case s: Seq[?] =>
              val nonNull = s.filterNot(v => Option(v).isEmpty)
              if (nonNull.isEmpty) null // scalafix:ok DisableSyntax.null
              else {
                val start = nonNull.head.asInstanceOf[A] // scalafix:ok DisableSyntax.asInstanceOf
                nonNull.foldLeft(start) { (best, v) =>
                  val a = v.asInstanceOf[A] // scalafix:ok DisableSyntax.asInstanceOf
                  val cmp = ordering.compare(a, best)
                  if (isMax && cmp > 0 || !isMax && cmp < 0) a else best
                }
              }
            case _ => null // scalafix:ok DisableSyntax.null
          }
        },
        col.nullSet
      )
    }

  private def tryLongOp[Row](
    left: Expr[Row, Long],
    right: Expr[Row, Long],
    columns: Vector[Column[?]],
    rowCount: Int
  )(op: (Long, Long) => Option[Long]): Either[ExecutionError, Column[?]] =
    for {
      leftCol <- evalColumn(left, columns, ColumnType.LongType)
      rightCol <- evalColumn(right, columns, ColumnType.LongType)
    } yield (leftCol, rightCol) match {
      case (Column.LongColumn(ld, ln), Column.LongColumn(rd, rn)) =>
        val out = new Array[Long](rowCount)
        val outNulls = scala.collection.mutable.BitSet.empty
        (0 until rowCount).foreach { i =>
          op(ld(i), rd(i)) match {
            case Some(v) => out(i) = v
            case None => outNulls += i
          }
        }
        Column.long(out, ln | rn | (BitSet.empty ++ outNulls))
      case _ => Column.long(Array.empty[Long])
    }

  private def strIntBin[Row](
    strExpr: Expr[Row, String],
    intExpr: Expr[Row, Int],
    columns: Vector[Column[?]],
    rowCount: Int
  )(op: (String, Int) => String): Either[ExecutionError, Column[?]] =
    for {
      strCol <- evalColumn(strExpr, columns, ColumnType.StringType)
      intCol <- evalColumn(intExpr, columns, ColumnType.IntType)
    } yield (strCol, intCol) match {
      case (Column.StringColumn(sd, sn), Column.IntColumn(nd, nn)) =>
        val combined = sn | nn
        Column.string(
          Array.tabulate(rowCount) { i =>
            if (combined.contains(i)) null // scalafix:ok DisableSyntax.null
            else op(sd(i).nn, nd(i))
          },
          combined
        )
      case _ => Column.string(Array.empty[String | Null])
    }

  private def strIntPad[Row](
    strExpr: Expr[Row, String],
    lenExpr: Expr[Row, Int],
    pad: String,
    columns: Vector[Column[?]],
    rowCount: Int,
    isLpad: Boolean
  ): Either[ExecutionError, Column[?]] =
    for {
      strCol <- evalColumn(strExpr, columns, ColumnType.StringType)
      lenCol <- evalColumn(lenExpr, columns, ColumnType.IntType)
    } yield (strCol, lenCol) match {
      case (Column.StringColumn(sd, sn), Column.IntColumn(nd, nn)) =>
        val combined = sn | nn
        Column.string(
          Array.tabulate(rowCount) { i =>
            if (combined.contains(i)) null // scalafix:ok DisableSyntax.null
            else padString(sd(i).nn, nd(i), pad, isLpad)
          },
          combined
        )
      case _ => Column.string(Array.empty[String | Null])
    }

  private def padString(str: String, len: Int, pad: String, isLpad: Boolean): String = {
    if (len <= 0) ""
    else if (str.length >= len) str.substring(0, len)
    else {
      val filler = if (pad.isEmpty) " " else pad
      val missing = len - str.length
      val padding = LazyList.continually(filler).flatten.take(missing).mkString
      if (isLpad) padding + str else str + padding
    }
  }

  private def initcapWord(s: String): String =
    s.split(" ", -1)
      .toIndexedSeq
      .map(w => if (w.isEmpty) w else w.charAt(0).toUpper.toString + w.substring(1).toLowerCase)
      .mkString(" ")

  private def substringIndex(str: String, delim: String, count: Int): String =
    if (delim.isEmpty || count == 0) ""
    else if (count > 0) {
      val parts = str.split(java.util.regex.Pattern.quote(delim), -1)
      parts.take(count).mkString(delim)
    } else {
      val parts = str.split(java.util.regex.Pattern.quote(delim), -1)
      parts.takeRight(-count).mkString(delim)
    }

  private def levenshtein(a: String, b: String): Int = {
    val prev = Array.tabulate(b.length + 1)(identity)
    val curr = new Array[Int](b.length + 1)
    (0 until a.length).foreach { i =>
      curr(0) = i + 1
      (0 until b.length).foreach { j =>
        val cost = if (a.charAt(i) == b.charAt(j)) 0 else 1
        curr(j + 1) = Math.min(Math.min(curr(j) + 1, prev(j + 1) + 1), prev(j) + cost)
      }
      System.arraycopy(curr, 0, prev, 0, b.length + 1)
    }
    prev(b.length)
  }

  private def urlPart(url: String, part: String): String | Null = {
    try {
      val uri = java.net.URI.create(url)
      part.toUpperCase.nn match {
        case "HOST" => uri.getHost
        case "PATH" => uri.getPath
        case "QUERY" => uri.getQuery
        case "PROTOCOL" | "SCHEME" => uri.getScheme
        case "REF" | "FRAGMENT" => uri.getFragment
        case "AUTHORITY" => uri.getAuthority
        case "FILE" =>
          Option(uri.getPath)
            .map(p => p + Option(uri.getQuery).map("?" + _).getOrElse(""))
            .orNull
        case "USERINFO" => uri.getUserInfo
        case _ => Option.empty[String].orNull
      }
    } catch {
      case _: IllegalArgumentException => Option.empty[String].orNull
    }
  }

  private def unhexString(hex: String): Option[Array[Byte]] = {
    val normalized = if (hex.length % 2 == 1) "0" + hex else hex
    if (!normalized.forall(c => c.isDigit || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) None
    else
      Some(
        normalized.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
      )
  }

  private def epochMicrosToUtcDateTime(micros: Long): java.time.LocalDateTime =
    java.time.LocalDateTime.ofInstant(
      java.time.Instant.ofEpochSecond(micros / 1000000L, (micros % 1000000L) * 1000L),
      java.time.ZoneOffset.UTC
    )

  private def parseIsoTimestamp(text: String): Option[Long] =
    try {
      val normalized = if (text.length > 10) text.replace(' ', 'T') else text
      val ldt =
        if (text.length > 10) java.time.LocalDateTime.parse(normalized)
        else java.time.LocalDate.parse(normalized).atStartOfDay()
      Some(ldt.toEpochSecond(java.time.ZoneOffset.UTC) * 1000000L + ldt.getNano.toLong / 1000L)
    } catch {
      case _: java.time.format.DateTimeParseException => None
    }

  private def microsToInstant(micros: Long): java.time.Instant =
    java.time.Instant.ofEpochSecond(micros / 1000000L, (micros % 1000000L) * 1000L)

  /** Wall-preserving zone conversion: the wall clock in fromZone re-encoded in toZone. */
  private def shiftWallClock(micros: Long, fromZone: java.time.ZoneId, toZoneId: String): Long = {
    val wallInFrom = microsToInstant(micros).atZone(fromZone).toLocalDateTime
    wallInFrom.atZone(java.time.ZoneId.of(toZoneId)).toInstant.toEpochMilli * 1000L
  }

  /** from_utc semantics: render the instant's wall clock in tz, encode it back as UTC. */
  private def fromUtcMicros(micros: Long, tz: String): Long = {
    val wallInTz = microsToInstant(micros).atZone(java.time.ZoneId.of(tz)).toLocalDateTime
    wallInTz.toEpochSecond(java.time.ZoneOffset.UTC) * 1000000L + wallInTz.getNano.toLong / 1000L
  }

  private def addTimestampUnit(unit: String, qty: Int, micros: Long): Long = {
    val utc = epochMicrosToUtcDateTime(micros)
    val added = unit.toUpperCase.nn match {
      case "SECOND" | "SECONDS" => utc.plusSeconds(qty.toLong)
      case "MINUTE" | "MINUTES" => utc.plusMinutes(qty.toLong)
      case "HOUR" | "HOURS" => utc.plusHours(qty.toLong)
      case "DAY" | "DAYS" => utc.plusDays(qty.toLong)
      case "WEEK" | "WEEKS" => utc.plusWeeks(qty.toLong)
      case "MONTH" | "MONTHS" => utc.plusMonths(qty.toLong)
      case "YEAR" | "YEARS" => utc.plusYears(qty.toLong)
      case other => sys.error(s"Unsupported timestamp unit: $other")
    }
    added.toEpochSecond(java.time.ZoneOffset.UTC) * 1000000L + added.getNano.toLong / 1000L
  }

  private def diffTimestampUnits(unit: String, startMicros: Long, endMicros: Long): Long = {
    val microsDiff = endMicros - startMicros
    unit.toUpperCase.nn match {
      case "MICROSECOND" | "MICROSECONDS" => microsDiff
      case "MILLISECOND" | "MILLISECONDS" => microsDiff / 1000L
      case "SECOND" | "SECONDS" => microsDiff / 1000000L
      case "MINUTE" | "MINUTES" => microsDiff / 60000000L
      case "HOUR" | "HOURS" => microsDiff / 3600000000L
      case "DAY" | "DAYS" => microsDiff / 86400000000L
      case "WEEK" | "WEEKS" => microsDiff / 604800000000L
      case other => sys.error(s"Unsupported timestamp unit: $other (use MICROSECOND..WEEK)")
    }
  }

  private def flattenStructColumn(col: Column[?], ct: ColumnType): Vector[Column[?]] = (col, ct) match {
    case (Column.StructColumn(inner, _, _), ColumnType.StructType(_)) => inner
    case _ => Vector(col)
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
          Column.int(Array.tabulate(rowCount)(i => op(ld(i), rd(i))), ln | rn)
        case _ =>
          Column.int(Array.empty[Int])
      }
    }
  }

  private def vectorizedSafeBinOp[T: scala.reflect.ClassTag](
    left: Array[T],
    right: Array[T],
    rowCount: Int,
    isZero: T => Boolean,
    op: (T, T) => T,
    wrap: (Array[T], BitSet) => Column[?]
  ): Either[ExecutionError, Column[?]] = {
    val zeroIdx = (0 until rowCount).find(i => isZero(right(i)))
    zeroIdx match {
      case Some(i) => Left(ExecutionError.DivisionByZero(i))
      case None => Right(wrap(Array.tabulate(rowCount)(i => op(left(i), right(i))), BitSet.empty))
    }
  }

  private def vectorizedDiv(left: Array[Int], right: Array[Int], rowCount: Int): Either[ExecutionError, Column[?]] =
    vectorizedSafeBinOp(left, right, rowCount, _ == 0, _ / _, Column.int)

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
          Column.long(Array.tabulate(rowCount)(i => op(ld(i), rd(i))), ln | rn)
        case _ =>
          Column.long(Array.empty[Long])
      }
    }
  }

  private def vectorizedLongDiv(
    left: Array[Long],
    right: Array[Long],
    rowCount: Int
  ): Either[ExecutionError, Column[?]] =
    vectorizedSafeBinOp(left, right, rowCount, _ == 0L, _ / _, Column.long)

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
          Column.double(Array.tabulate(rowCount)(i => op(ld(i), rd(i))), ln | rn)
        case _ =>
          Column.double(Array.empty[Double])
      }
    }
  }

  private def vectorizedDoubleDiv(
    left: Array[Double],
    right: Array[Double],
    rowCount: Int
  ): Either[ExecutionError, Column[?]] =
    vectorizedSafeBinOp(left, right, rowCount, _ == 0.0, _ / _, Column.double)

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
        def cmpCol[T](ld: Array[T], ln: BitSet, rd: Array[T], rn: BitSet)(
          cmp: (T, T) => Boolean
        ): Either[ExecutionError, Column[?]] =
          Right(Column.boolean(Array.tabulate(rowCount)(i => cmp(ld(i), rd(i))), ln | rn))

        val matched: Either[ExecutionError, Column[?]] = (leftCol, rightCol) match {
          case (Column.IntColumn(ld, ln), Column.IntColumn(rd, rn)) => cmpCol(ld, ln, rd, rn)(intCmp)
          case (Column.LongColumn(ld, ln), Column.LongColumn(rd, rn)) => cmpCol(ld, ln, rd, rn)(longCmp)
          case (Column.DoubleColumn(ld, ln), Column.DoubleColumn(rd, rn)) => cmpCol(ld, ln, rd, rn)(doubleCmp)
          case (Column.StringColumn(ld, ln), Column.StringColumn(rd, rn)) =>
            Right(
              Column.boolean(
                Array.tabulate(rowCount)(i =>
                  if (ln.contains(i) || rn.contains(i)) false else stringCmp(ld(i).nn, rd(i).nn)
                ),
                ln | rn
              )
            )
          case (Column.DateColumn(ld, ln), Column.DateColumn(rd, rn)) => cmpCol(ld, ln, rd, rn)(intCmp)
          case _ =>
            Left(ExecutionError.UnsupportedOperation("Comparison not supported for untyped columns"))
        }
        matched
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
      Column.boolean(Array.tabulate(rowCount)(i => cmp(leftCol.getValue(i), rightCol.getValue(i))))
    }
  }

  private def vectorizedIntMod(left: Array[Int], right: Array[Int], rowCount: Int): Either[ExecutionError, Column[?]] =
    vectorizedSafeBinOp(left, right, rowCount, _ == 0, _ % _, Column.int)

  private def vectorizedLongMod(
    left: Array[Long],
    right: Array[Long],
    rowCount: Int
  ): Either[ExecutionError, Column[?]] =
    vectorizedSafeBinOp(left, right, rowCount, _ == 0L, _ % _, Column.long)

  private def vectorizedDoubleUnaryOp[Row](
    expr: Expr[Row, Double],
    columns: Vector[Column[?]],
    rowCount: Int
  )(op: Double => Double): Either[ExecutionError, Column[?]] = {
    evalColumn(expr, columns, ColumnType.DoubleType).map {
      case Column.DoubleColumn(data, nulls) =>
        Column.double(Array.tabulate(rowCount)(i => op(data(i))), nulls)
      case _ =>
        Column.double(Array.empty[Double])
    }
  }

  private def vectorizedStringHash[Row](
    expr: Expr[Row, String],
    columns: Vector[Column[?]]
  )(algorithm: String): Either[ExecutionError, Column[?]] = {
    val digest = java.security.MessageDigest.getInstance(algorithm).nn
    evalColumn(expr, columns, ColumnType.StringType).map {
      case sc: Column.StringColumn =>
        mapString(sc) { s =>
          digest.reset()
          hexEncode(digest.digest(s.getBytes("UTF-8")).nn)
        }
      case _ =>
        Column.string(Array.empty[String | Null])
    }
  }

  /** Evaluate aggregation expression over the entire dataset.
    *
    * Aggregations operate on all rows to produce a single value. Returns Any because the result is
    * consumed by DatasetInterpreter via Column.fromValues, which takes Vector[Any]. The GADT type
    * parameter A is used internally for typed column dispatch but erased at the return boundary.
    * Fixed-type aggregations (Sum, Avg, StdDev) operate on typed columns via evalColumn. Generic
    * aggregations (Max, Collect) use Column GADT pattern matching for typed access.
    */
  def evalAggregation[Row, A](
    expr: Expr[Row, A],
    columns: Vector[Column[?]]
  ): Either[ExecutionError, Any] = {
    if (columns.isEmpty || columns.head.length == 0) {
      (expr: @unchecked) match {
        case _: Expr.Count[Row] => Right(0L)
        case _: Expr.Sum[Row] => Right(0L)
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
        case _: Expr.Collect[Row, ?] => Right(Seq.empty)
        case _: Expr.PercentileApprox[Row] => Right(0.0)
        case _: Expr.MaxBy[Row, ?, ?] => Right(None)
        case _: Expr.MinBy[Row, ?, ?] => Right(None)
        case _: Expr.MaxN[Row, ?] => Right(Seq.empty)
        case _: Expr.MinN[Row, ?] => Right(Seq.empty)
        case _: Expr.MaxByN[Row, ?, ?] => Right(Seq.empty)
        case _: Expr.MinByN[Row, ?, ?] => Right(Seq.empty)
        case _: Expr.Variance[Row] => Right(0.0)
        case _: Expr.VariancePop[Row] => Right(0.0)
        case _: Expr.ApproxCountDistinct[Row, ?] => Right(0L)
        case _: Expr.CollectSet[Row, ?] => Right(Seq.empty)
        case _: Expr.ExprLast[Row, ?] => Right(None)
        case _: Expr.AnyValue[Row, ?] => Right(None)
        case _: Expr.BoolAnd[Row] => Right(true)
        case _: Expr.BoolOr[Row] => Right(false)
        case _: Expr.Corr[Row] => Right(0.0)
        case _: Expr.CovarSamp[Row] => Right(0.0)
        case _: Expr.CovarPop[Row] => Right(0.0)
        case _: Expr.Median[Row] => Right(0.0)
        case _: Expr.Mode[Row, ?] => Right(None)
        case _: Expr.BitAndAgg[Row] => Right(null) // scalafix:ok DisableSyntax.null
        case _: Expr.BitOrAgg[Row] => Right(0)
        case _: Expr.BitXorAgg[Row] => Right(0)
        case _: Expr.RegrAvgx[Row] | _: Expr.RegrAvgy[Row] | _: Expr.RegrCount[Row] | _: Expr.RegrIntercept[Row] |
            _: Expr.RegrR2[Row] | _: Expr.RegrSlope[Row] | _: Expr.RegrSxx[Row] | _: Expr.RegrSxy[Row] |
            _: Expr.RegrSyy[Row] | _: Expr.Kurtosis[Row] | _: Expr.Skewness[Row] | _: Expr.Percentile[Row] |
            _: Expr.SumDistinct[Row] =>
          Right(None)
        case _: Expr.HistogramNumeric[Row] =>
          Left(ExecutionError.UnsupportedOperation("HistogramNumeric is Spark-only"))
        case _: Expr.Grouping[Row] =>
          Left(ExecutionError.UnsupportedOperation("Grouping requires a grouping-sets context (Spark-only)"))
        case _: Expr.GroupingId[Row] =>
          Left(ExecutionError.UnsupportedOperation("GroupingId requires a grouping-sets context (Spark-only)"))
      }
    } else {
      val rowCount = columns.head.length
      (expr: @unchecked) match {
        case Expr.Count() =>
          Right(rowCount.toLong)

        case sum: Expr.Sum[Row] =>
          evalColumn(sum.expr, columns, ColumnType.IntType).map {
            case Column.IntColumn(data, nulls) => nullSafeFold(data, nulls, 0L)(_ + _)
            case _ => 0L
          }

        case sumD: Expr.SumDouble[Row] =>
          evalColumn(sumD.expr, columns, ColumnType.DoubleType).map {
            case Column.DoubleColumn(data, nulls) => nullSafeFold(data, nulls, 0.0)(_ + _)
            case _ => 0.0
          }

        case sumL: Expr.SumLong[Row] =>
          evalColumn(sumL.expr, columns, ColumnType.LongType).map {
            case Column.LongColumn(data, nulls) => nullSafeFold(data, nulls, 0L)(_ + _)
            case _ => 0L
          }

        case avg: Expr.Avg[Row] =>
          evalColumn(avg.expr, columns, ColumnType.DoubleType).map {
            case Column.DoubleColumn(data, nulls) =>
              val (total, count) = sumAndCount(data, nulls)
              if (count == 0) 0.0 else total / count
            case _ => 0.0
          }

        case countIf: Expr.CountIf[Row] =>
          evalColumn(countIf.predicate, columns, ColumnType.BooleanType).map {
            case Column.BooleanColumn(data, nulls) =>
              data.indices.foldLeft(0L)((acc, i) => if (!nulls.contains(i) && data(i)) acc + 1L else acc)
            case _ => 0L
          }

        case sd: Expr.StdDev[Row] =>
          aggregateDoubleExpr(sd.expr, columns) { (data, nulls) =>
            val (variance, count) = computeVariance(data, nulls)
            if (count <= 1) 0.0 else math.sqrt(variance / (count - 1))
          }

        case sdp: Expr.StdDevPop[Row] =>
          aggregateDoubleExpr(sdp.expr, columns) { (data, nulls) =>
            val (variance, count) = computeVariance(data, nulls)
            if (count == 0) 0.0 else math.sqrt(variance / count)
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
          evalColumn(ba.expr, columns, ColumnType.BooleanType).map {
            case Column.BooleanColumn(data, nulls) =>
              !data.indices.exists(i => !nulls.contains(i) && !data(i))
            case _ => true
          }

        case bo: Expr.BoolOr[Row] =>
          evalColumn(bo.expr, columns, ColumnType.BooleanType).map {
            case Column.BooleanColumn(data, nulls) =>
              data.indices.exists(i => !nulls.contains(i) && data(i))
            case _ => false
          }

        case v: Expr.Variance[Row] =>
          aggregateDoubleExpr(v.expr, columns) { (data, nulls) =>
            val (variance, count) = computeVariance(data, nulls)
            if (count <= 1) 0.0 else variance / (count - 1)
          }

        case vp: Expr.VariancePop[Row] =>
          aggregateDoubleExpr(vp.expr, columns) { (data, nulls) =>
            val (variance, count) = computeVariance(data, nulls)
            if (count == 0) 0.0 else variance / count
          }

        case corr: Expr.Corr[Row] =>
          bivariateStat(corr.left, corr.right, columns, minCount = 2) {
            (xData, yData, combinedNulls, xMean, yMean, _) =>
              val (cov, xVar, yVar) =
                xData.indices.filterNot(combinedNulls.contains).foldLeft((0.0, 0.0, 0.0)) { case ((c, xv, yv), i) =>
                  val dx = xData(i) - xMean; val dy = yData(i) - yMean; (c + dx * dy, xv + dx * dx, yv + dy * dy)
                }
              val denom = math.sqrt(xVar * yVar)
              if (denom == 0.0) 0.0 else cov / denom
          }

        case cs: Expr.CovarSamp[Row] =>
          bivariateStat(cs.left, cs.right, columns, minCount = 2) { (xData, yData, combinedNulls, xMean, yMean, n) =>
            computeCovariance(xData, yData, combinedNulls, xMean, yMean) / (n - 1)
          }

        case cp: Expr.CovarPop[Row] =>
          bivariateStat(cp.left, cp.right, columns, minCount = 1) { (xData, yData, combinedNulls, xMean, yMean, n) =>
            computeCovariance(xData, yData, combinedNulls, xMean, yMean) / n
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

        case countDist: Expr.CountDistinct[Row, ?] =>
          countDistinctAgg(countDist.expr, columns, rowCount)

        case acd: Expr.ApproxCountDistinct[Row, ?] =>
          countDistinctAgg(acd.expr, columns, rowCount)

        case max: Expr.Max[Row, ?] =>
          val colType = inferExprColumnType(max.expr, columns)
          evalColumn(max.expr, columns, colType).map { col =>
            findExtremum(col, rowCount, isMax = true)
          }

        case min: Expr.Min[Row, ?] =>
          val colType = inferExprColumnType(min.expr, columns)
          evalColumn(min.expr, columns, colType).map { col =>
            findExtremum(col, rowCount, isMax = false)
          }

        case first: Expr.First[Row, ?] =>
          firstNonNull(first.expr, columns, rowCount)

        case last: Expr.ExprLast[Row, ?] =>
          val colType = inferExprColumnType(last.expr, columns)
          evalColumn(last.expr, columns, colType).map { col =>
            (0 until rowCount).filter(i => !col.isNull(RowIndex(i))).lastOption.map(col.getValue)
          }

        case anyVal: Expr.AnyValue[Row, ?] =>
          firstNonNull(anyVal.expr, columns, rowCount)

        case mode: Expr.Mode[Row, ?] =>
          val colType = inferExprColumnType(mode.expr, columns)
          evalColumn(mode.expr, columns, colType).map { col =>
            val counts = (0 until rowCount)
              .filter(i => !col.isNull(RowIndex(i)))
              .foldLeft(scala.collection.mutable.LinkedHashMap.empty[Any, Int]) { (acc, i) =>
                val v = col.getValue(i)
                acc(v) = acc.getOrElse(v, 0) + 1
                acc
              }
            if (counts.isEmpty) None else Some(counts.maxBy(_._2)._1)
          }

        case collect: Expr.Collect[Row, ?] =>
          val colType = inferExprColumnType(collect.expr, columns)
          evalColumn(collect.expr, columns, colType).map { col =>
            (0 until rowCount).filter(i => !col.isNull(RowIndex(i))).map(col.getValue)
          }

        case cs: Expr.CollectSet[Row, ?] =>
          val colType = inferExprColumnType(cs.expr, columns)
          evalColumn(cs.expr, columns, colType).map { col =>
            val set = scala.collection.mutable.LinkedHashSet.empty[Any]
            (0 until rowCount).foreach(i => if (!col.isNull(RowIndex(i))) set += col.getValue(i))
            set.toSeq
          }

        case mb: Expr.MaxBy[Row, ?, ?] =>
          val valColType = inferExprColumnType(mb.valueExpr, columns)
          val keyColType = inferExprColumnType(mb.orderExpr, columns)
          for {
            valCol <- evalColumn(mb.valueExpr, columns, valColType)
            keyCol <- evalColumn(mb.orderExpr, columns, keyColType)
          } yield {
            findExtremumByKey(valCol, keyCol, rowCount, isMax = true)
          }

        case mb: Expr.MinBy[Row, ?, ?] =>
          val valColType = inferExprColumnType(mb.valueExpr, columns)
          val keyColType = inferExprColumnType(mb.orderExpr, columns)
          for {
            valCol <- evalColumn(mb.valueExpr, columns, valColType)
            keyCol <- evalColumn(mb.orderExpr, columns, keyColType)
          } yield {
            findExtremumByKey(valCol, keyCol, rowCount, isMax = false)
          }

        case mn: Expr.MaxN[Row, ?] =>
          val colType = inferExprColumnType(mn.expr, columns)
          evalColumn(mn.expr, columns, colType).map { col =>
            topNValues(col, rowCount, mn.n, isMax = true)
          }

        case mn: Expr.MinN[Row, ?] =>
          val colType = inferExprColumnType(mn.expr, columns)
          evalColumn(mn.expr, columns, colType).map { col =>
            topNValues(col, rowCount, mn.n, isMax = false)
          }

        case mbn: Expr.MaxByN[Row, ?, ?] =>
          val valColType = inferExprColumnType(mbn.valueExpr, columns)
          val keyColType = inferExprColumnType(mbn.orderExpr, columns)
          for {
            valCol <- evalColumn(mbn.valueExpr, columns, valColType)
            keyCol <- evalColumn(mbn.orderExpr, columns, keyColType)
          } yield {
            topNByKey(valCol, keyCol, rowCount, mbn.n, isMax = true)
          }

        case mbn: Expr.MinByN[Row, ?, ?] =>
          val valColType = inferExprColumnType(mbn.valueExpr, columns)
          val keyColType = inferExprColumnType(mbn.orderExpr, columns)
          for {
            valCol <- evalColumn(mbn.valueExpr, columns, valColType)
            keyCol <- evalColumn(mbn.orderExpr, columns, keyColType)
          } yield {
            topNByKey(valCol, keyCol, rowCount, mbn.n, isMax = false)
          }

        case ba: Expr.BitAndAgg[Row] =>
          evalColumn(ba.expr, columns, ColumnType.IntType).map {
            case Column.IntColumn(data, nulls) =>
              data.indices.filterNot(nulls.contains).foldLeft(-1)((acc, i) => acc & data(i))
            case _ => -1
          }

        case bo: Expr.BitOrAgg[Row] =>
          evalColumn(bo.expr, columns, ColumnType.IntType).map {
            case Column.IntColumn(data, nulls) =>
              data.indices.filterNot(nulls.contains).foldLeft(0)((acc, i) => acc | data(i))
            case _ => 0
          }

        case bx: Expr.BitXorAgg[Row] =>
          evalColumn(bx.expr, columns, ColumnType.IntType).map {
            case Column.IntColumn(data, nulls) =>
              data.indices.filterNot(nulls.contains).foldLeft(0)((acc, i) => acc ^ data(i))
            case _ => 0
          }

        case rv: Expr.RegrAvgx[Row] => regrMoments(rv.y, rv.x, columns, rowCount).map(_.map(s => s.avgx))
        case rv2: Expr.RegrAvgy[Row] => regrMoments(rv2.y, rv2.x, columns, rowCount).map(_.map(s => s.avgy))
        case rc: Expr.RegrCount[Row] => regrMoments(rc.y, rc.x, columns, rowCount).map(_.map(s => s.count))
        case ri: Expr.RegrIntercept[Row] =>
          regrMoments(ri.y, ri.x, columns, rowCount).map(_.flatMap(_.intercept))
        case rr: Expr.RegrR2[Row] => regrMoments(rr.y, rr.x, columns, rowCount).map(_.flatMap(_.r2))
        case rs: Expr.RegrSlope[Row] => regrMoments(rs.y, rs.x, columns, rowCount).map(_.flatMap(_.slope))
        case rsx: Expr.RegrSxx[Row] => regrMoments(rsx.y, rsx.x, columns, rowCount).map(_.map(s => s.sxx))
        case rsy: Expr.RegrSxy[Row] => regrMoments(rsy.y, rsy.x, columns, rowCount).map(_.map(s => s.sxy))
        case rsy2: Expr.RegrSyy[Row] => regrMoments(rsy2.y, rsy2.x, columns, rowCount).map(_.map(s => s.syy))

        case kurt: Expr.Kurtosis[Row] =>
          evalColumn(kurt.expr, columns, ColumnType.DoubleType).map { col =>
            centralMomentStats(col, rowCount).flatMap { case (n, m2, _, m4) =>
              if (n < 2 || m2 == 0.0) None else Some(m4 / (m2 * m2) - 3.0)
            }
          }

        case skw: Expr.Skewness[Row] =>
          evalColumn(skw.expr, columns, ColumnType.DoubleType).map { col =>
            centralMomentStats(col, rowCount).flatMap { case (n, m2, m3, _) =>
              if (n < 2 || m2 == 0.0) None else Some(m3 / math.pow(m2, 1.5))
            }
          }

        case sdist: Expr.SumDistinct[Row] =>
          evalColumn(sdist.expr, columns, ColumnType.LongType).map { col =>
            val seen = scala.collection.mutable.LinkedHashSet.empty[Long]
            (0 until rowCount).foreach { i =>
              if (!col.isNull(RowIndex(i))) {
                col.getValue(i) match {
                  case l: Long => seen += l
                  case _ => ()
                }
              }
            }
            if (seen.isEmpty) None else Some(seen.foldLeft(0L)(_ + _))
          }

        case pct: Expr.Percentile[Row] =>
          for {
            valueCol <- evalColumn(pct.expr, columns, ColumnType.DoubleType)
            pctCol <- evalColumn(pct.percentage, columns, ColumnType.DoubleType)
          } yield {
            val values = (0 until rowCount)
              .filterNot(i => valueCol.isNull(RowIndex(i)))
              .map(i => asDouble(valueCol.getValue(i)))
            val percentage = (0 until rowCount).collectFirst {
              case i if !pctCol.isNull(RowIndex(i)) => asDouble(pctCol.getValue(i))
            }
            for {
              p <- percentage
              sorted = values.sorted
              if sorted.nonEmpty
              pos = p * (sorted.size - 1)
              lo = math.floor(pos).toInt
              hi = math.ceil(pos).toInt
            } yield if (lo == hi) sorted(lo) else sorted(lo) + (sorted(hi) - sorted(lo)) * (pos - lo)
          }

        case _: Expr.HistogramNumeric[Row] =>
          Left(ExecutionError.UnsupportedOperation("HistogramNumeric is Spark-only"))
        case _: Expr.Grouping[Row] =>
          Left(ExecutionError.UnsupportedOperation("Grouping requires a grouping-sets context (Spark-only)"))
        case _: Expr.GroupingId[Row] =>
          Left(ExecutionError.UnsupportedOperation("GroupingId requires a grouping-sets context (Spark-only)"))

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
    evalColumn(subExpr, columns, ColumnType.DoubleType).map {
      case Column.DoubleColumn(data, nulls) => f(data, nulls)
      case _ => 0.0
    }
  }

  /** Biased central moments (m2, m3, m4) and count of the non-null doubles. */
  private def centralMomentStats(
    col: Column[?],
    rowCount: Int
  ): Option[(Long, Double, Double, Double)] = {
    val values = (0 until rowCount)
      .filterNot(i => col.isNull(RowIndex(i)))
      .map(i => asDouble(col.getValue(i)))
    val n = values.size.toLong
    if (n == 0L) None
    else {
      val mean = values.sum / n
      val (m2, m3, m4) = values.foldLeft((0.0, 0.0, 0.0)) { case ((m2, m3, m4), v) =>
        val d = v - mean
        val d2 = d * d
        (m2 + d2, m3 + d2 * d, m4 + d2 * d2)
      }
      Some((n, m2 / n, m3 / n, m4 / n))
    }
  }

  /** Evaluate `f` on the JSON value at `path` for each row; missing paths yield null. */
  private def jsonPathAggregate[A](
    col: Column[?],
    rowCount: Int,
    path: String
  )(f: JsonValue => Option[A]): Column[?] = {
    val segments = parseVariantPath(path)
    val outNulls = scala.collection.mutable.BitSet.empty
    val out = new Array[Any | Null](rowCount)
    (0 until rowCount).foreach { i =>
      if (col.isNull(RowIndex(i))) outNulls += i
      else
        col.getValue(i) match {
          case s: String =>
            parseJson(s) match {
              case parser.core.Result.Success(v, _) =>
                val atPath = segments.flatMap(walkVariantPath(v, _))
                atPath.flatMap(f) match {
                  case Some(a) => out(i) = a
                  case None => outNulls += i
                }
              case _ => outNulls += i
            }
          case _ => outNulls += i
        }
    }
    Column.any(out, col.nullSet | (BitSet.empty ++ outNulls))
  }

  /** Sufficient statistics of the (y, x) pairs where both are non-null. */
  private final case class RegrStats(
    count: Long,
    avgx: Double,
    avgy: Double,
    sxx: Double,
    syy: Double,
    sxy: Double
  ) {
    def slope: Option[Double] = if (sxx == 0.0) None else Some(sxy / sxx)
    def intercept: Option[Double] = slope.map(sl => avgy - sl * avgx)
    def r2: Option[Double] = if (sxx == 0.0 || syy == 0.0) None else Some((sxy * sxy) / (sxx * syy))
  }

  private def regrMoments[Row](
    yExpr: Expr[Row, Double],
    xExpr: Expr[Row, Double],
    columns: Vector[Column[?]],
    rowCount: Int
  ): Either[ExecutionError, Option[RegrStats]] =
    for {
      yCol <- evalColumn(yExpr, columns, ColumnType.DoubleType)
      xCol <- evalColumn(xExpr, columns, ColumnType.DoubleType)
    } yield {
      val pairs = (0 until rowCount).flatMap { i =>
        if (yCol.isNull(RowIndex(i)) || xCol.isNull(RowIndex(i))) Vector.empty[(Double, Double)]
        else
          Vector(
            (
              asDouble(yCol.getValue(i)),
              asDouble(xCol.getValue(i))
            )
          )
      }
      if (pairs.isEmpty) None
      else {
        val count = pairs.size.toLong
        val avgx = pairs.map(_._2).sum / count
        val avgy = pairs.map(_._1).sum / count
        val sxx = pairs.map { case (_, x) => (x - avgx) * (x - avgx) }.sum
        val syy = pairs.map { case (y, _) => (y - avgy) * (y - avgy) }.sum
        val sxy = pairs.map { case (y, x) => (x - avgx) * (y - avgy) }.sum
        Some(RegrStats(count, avgx, avgy, sxx, syy, sxy))
      }
    }

  private def asDouble(v: Any): Double = v match {
    case d: Double => d
    case n: java.lang.Number => n.doubleValue()
    case other => String.valueOf(other).nn.toDouble
  }

  /** Sum non-null values and count them in a single pass. */
  private def nullSafeFold[T, R](data: Array[T], nulls: BitSet, zero: R)(op: (R, T) => R): R =
    data.indices.foldLeft(zero)((acc, i) => if (nulls.contains(i)) acc else op(acc, data(i)))

  /** Map `f` over a StringColumn, preserving nulls. The single place where a null placeholder is
    * produced for reference-typed columns (the BitSet is authoritative).
    */
  private def mapString(col: Column[String])(f: String => String): Column[String] =
    col match {
      case Column.StringColumn(data, nulls) =>
        Column.string(
          Array.tabulate(data.length)(i =>
            if (nulls.contains(i)) null else f(data(i).nn)
          ), // scalafix:ok DisableSyntax.null
          nulls
        )
      case _ => Column.string(Array.empty[String | Null])
    }

  /** Map `f` over a StringColumn to a LongColumn, using 0L as the (never-read) null placeholder. */
  private def mapStringToLong(col: Column[String])(f: String => Long): Column[Long] =
    col match {
      case Column.StringColumn(data, nulls) =>
        Column.long(Array.tabulate(data.length)(i => if (nulls.contains(i)) 0L else f(data(i).nn)), nulls)
      case _ => Column.long(Array.empty[Long])
    }

  private def crc32Value(s: String): Long = {
    val crc = new java.util.zip.CRC32
    crc.update(s.getBytes("UTF-8"))
    crc.getValue
  }

  private def xxHash64Value(s: String): Long = xxHash64(s.getBytes("UTF-8"), 0, s.length, 42L)

  private def murmur3Hash(data: Array[Byte]): Int = murmur3_x86_32(data, 0, data.length, 42)

  private def murmur3_x86_32(data: Array[Byte], offset: Int, length: Int, seed: Int): Int = {
    val c1 = -862048943 // 0xcc9e2d51
    val c2 = 461845907 // 0x1b873593
    val aligned = length - (length % 4)

    @scala.annotation.tailrec
    def loopBlocks(i: Int, h: Int): Int =
      if (i >= aligned) h
      else {
        val k1 = (data(offset + i) & 0xff) | ((data(offset + i + 1) & 0xff) << 8) |
          ((data(offset + i + 2) & 0xff) << 16) | (data(offset + i + 3) << 24)
        loopBlocks(i + 4, mixH1(h, mixK1(k1, c1, c2)))
      }

    @scala.annotation.tailrec
    def loopTail(i: Int, h: Int): Int =
      if (i >= length) h
      else loopTail(i + 1, mixH1(h, mixK1(data(offset + i).toInt, c1, c2)))

    fmix32(loopTail(aligned, loopBlocks(0, seed)), length)
  }

  private def mixK1(k1: Int, c1: Int, c2: Int): Int =
    Integer.rotateLeft(k1 * c1, 15) * c2

  private def mixH1(h: Int, k: Int): Int =
    Integer.rotateLeft(h ^ k, 13) * 5 - 430675100 // 0xe6546b64

  private def fmix32(h: Int, length: Int): Int = {
    val h1 = h ^ length
    val h2 = h1 ^ (h1 >>> 16)
    val h3 = h2 * -2048144789 // 0x85ebca6b
    val h4 = h3 ^ (h3 >>> 13)
    val h5 = h4 * -1028477387 // 0xc2b2ae35
    h5 ^ (h5 >>> 16)
  }

  private val prime64_1 = 0x9e3779b185ebca87L
  private val prime64_2 = 0xc2b2ae3d27d4eb4fL
  private val prime64_3 = 0x165667b19e3779f9L
  private val prime64_4 = 0x85ebca77c2b2ae63L
  private val prime64_5 = 0x27d4eb2f165667c5L

  private def xxHash64(data: Array[Byte], offset: Int, length: Int, seed: Long): Long = {
    val end = offset + length

    @scala.annotation.tailrec
    def blockLoop(i: Int, v1: Long, v2: Long, v3: Long, v4: Long, limit: Int): (Int, Long, Long, Long, Long) =
      if (i > limit) (i, v1, v2, v3, v4)
      else
        blockLoop(
          i + 32,
          round64(v1, readLong(data, i)),
          round64(v2, readLong(data, i + 8)),
          round64(v3, readLong(data, i + 16)),
          round64(v4, readLong(data, i + 24)),
          limit
        )

    @scala.annotation.tailrec
    def wordLoop(i: Int, h: Long): (Int, Long) =
      if (i + 8 > end) (i, h)
      else wordLoop(i + 8, rotateLeft64(h ^ round64(0L, readLong(data, i)), 27) * prime64_1 + prime64_4)

    @scala.annotation.tailrec
    def byteLoop(i: Int, h: Long): Long =
      if (i >= end) h
      else byteLoop(i + 1, rotateLeft64(h ^ ((data(i) & 0xffL) * prime64_5), 11) * prime64_1)

    val (i0, h0) =
      if (length >= 32) {
        val limit = end - 32
        val (i, v1, v2, v3, v4) =
          blockLoop(offset, seed + prime64_1 + prime64_2, seed + prime64_2, seed, seed - prime64_1, limit)
        val merged = List(v1, v2, v3, v4).foldLeft(
          rotateLeft64(v1, 1) + rotateLeft64(v2, 7) + rotateLeft64(v3, 12) + rotateLeft64(v4, 18)
        )(mergeRound64)
        (i, merged)
      } else (offset, seed + prime64_5)

    val h1 = h0 + length.toLong
    val (i8, h8) = wordLoop(i0, h1)
    val (i4, h4) =
      if (i8 + 4 <= end) {
        (i8 + 4, rotateLeft64(h8 ^ ((readInt(data, i8) & 0xffffffffL) * prime64_1), 23) * prime64_2 + prime64_3)
      } else (i8, h8)
    val hb = byteLoop(i4, h4)

    val a = hb ^ (hb >>> 33)
    val b = a * prime64_2
    val c = b ^ (b >>> 29)
    val d = c * prime64_3
    d ^ (d >>> 32)
  }

  private def readLong(data: Array[Byte], i: Int): Long =
    (data(i) & 0xffL) | ((data(i + 1) & 0xffL) << 8) | ((data(i + 2) & 0xffL) << 16) |
      ((data(i + 3) & 0xffL) << 24) | ((data(i + 4) & 0xffL) << 32) | ((data(i + 5) & 0xffL) << 40) |
      ((data(i + 6) & 0xffL) << 48) | ((data(i + 7) & 0xffL) << 56)

  private def readInt(data: Array[Byte], i: Int): Int =
    (data(i) & 0xff) | ((data(i + 1) & 0xff) << 8) | ((data(i + 2) & 0xff) << 16) | (data(i + 3) << 24)

  private def round64(acc: Long, input: Long): Long = {
    val v = acc + input * prime64_2
    rotateLeft64(v, 31) * prime64_1
  }

  private def mergeRound64(acc: Long, v: Long): Long = {
    val mixed = (acc ^ round64(0L, v)) * prime64_1 + prime64_4
    mixed
  }

  private def rotateLeft64(value: Long, bits: Int): Long = (value << bits) | (value >>> (64 - bits))

  /** Map `f` over the values of any column, preserving nulls. */
  private def mapAny[A](col: Column[?])(f: Any => A): Column[?] =
    if (col.length == 0) Column.any(Array.empty[Any | Null])
    else
      Column.any(
        Array.tabulate[Any | Null](col.length)(i =>
          if (col.isNull(RowIndex(i))) null else f(col.getValue(i)) // scalafix:ok DisableSyntax.null
        ),
        col.nullSet
      )

  /** Map `f` over Seq-valued rows, preserving nulls. */
  private def mapSeq(col: Column[?])(f: Seq[?] => Seq[?]): Column[?] =
    mapAny(col) {
      case s: Seq[?] => f(s)
      case other => other
    }

  private def firstNonNull[Row](
    expr: Expr[Row, ?],
    columns: Vector[Column[?]],
    rowCount: Int
  ): Either[ExecutionError, Option[Any | Null]] = {
    val colType = inferExprColumnType(expr, columns)
    evalColumn(expr, columns, colType).map { col =>
      (0 until rowCount).find(i => !col.isNull(RowIndex(i))).map(col.getValue)
    }
  }

  private def computeCovariance(
    xData: Array[Double],
    yData: Array[Double],
    nulls: BitSet,
    xMean: Double,
    yMean: Double
  ): Double =
    xData.indices.filterNot(nulls.contains).foldLeft(0.0) { (acc, i) =>
      acc + (xData(i) - xMean) * (yData(i) - yMean)
    }

  private def bivariateStat[Row](
    leftExpr: Expr[Row, Double],
    rightExpr: Expr[Row, Double],
    columns: Vector[Column[?]],
    minCount: Int
  )(f: (Array[Double], Array[Double], BitSet, Double, Double, Int) => Double): Either[ExecutionError, Double] =
    for {
      leftCol <- evalColumn(leftExpr, columns, ColumnType.DoubleType)
      rightCol <- evalColumn(rightExpr, columns, ColumnType.DoubleType)
    } yield (leftCol, rightCol) match {
      case (Column.DoubleColumn(xData, xNulls), Column.DoubleColumn(yData, yNulls)) =>
        val combinedNulls = xNulls | yNulls
        val (xSum, n) = sumAndCount(xData, combinedNulls)
        val (ySum, _) = sumAndCount(yData, combinedNulls)
        if (n < minCount) 0.0
        else f(xData, yData, combinedNulls, xSum / n, ySum / n, n)
      case _ => 0.0
    }

  private def computeVariance(data: Array[Double], nulls: BitSet): (Double, Int) =
    sumAndCount(data, nulls) match {
      case (_, 0) => (0.0, 0)
      case (sum, count) =>
        val mean = sum / count
        val variance = data.indices.filterNot(nulls.contains).foldLeft(0.0) { (acc, i) =>
          val d = data(i) - mean; acc + d * d
        }
        (variance, count)
    }

  private def countDistinctAgg[Row](
    expr: Expr[Row, ?],
    columns: Vector[Column[?]],
    rowCount: Int
  ): Either[ExecutionError, Long] = {
    val colType = inferExprColumnType(expr, columns)
    evalColumn(expr, columns, colType).map { col =>
      (0 until rowCount).filter(i => !col.isNull(RowIndex(i))).map(col.getValue).toSet.size.toLong
    }
  }

  private def sumAndCount(data: Array[Double], nulls: BitSet): (Double, Int) = {
    data.indices.foldLeft((0.0, 0)) { case ((sum, count), i) =>
      if (nulls.contains(i)) (sum, count) else (sum + data(i), count + 1)
    }
  }

  /** Collect non-null doubles into a new array for sorting. */
  private def collectNonNullDoubles(data: Array[Double], nulls: BitSet): Array[Double] = {
    data.indices.filter(i => !nulls.contains(i)).map(data(_)).toArray
  }

  private def findExtremum(col: Column[?], rowCount: Int, isMax: Boolean): Option[Any] = {
    col match {
      case Column.AnyColumn(_, _) => None
      case _ =>
        val bestIdx = (0 until rowCount).foldLeft(-1) { (best, i) =>
          if (col.nullSet.contains(i)) best
          else if (best < 0) i
          else {
            val cmp = Column.compareAt(col, i, best)
            if (isMax && cmp > 0) i else if (!isMax && cmp < 0) i else best
          }
        }
        if (bestIdx < 0) None else Some(col.getValue(bestIdx))
    }
  }

  private def findExtremumByKey(
    valCol: Column[?],
    keyCol: Column[?],
    rowCount: Int,
    isMax: Boolean
  ): Option[Any] = {
    (0 until rowCount)
      .foldLeft((Option.empty[Any], -1)) { case ((bestValue, bestKeyIdx), i) =>
        if (valCol.isNull(RowIndex(i)) || keyCol.isNull(RowIndex(i))) (bestValue, bestKeyIdx)
        else if (bestKeyIdx < 0 || compareColumnValues(keyCol, i, bestKeyIdx, isMax))
          (Some(valCol.getValue(i)), i)
        else (bestValue, bestKeyIdx)
      }
      ._1
  }

  private def compareColumnValues(col: Column[?], i: Int, j: Int, wantGreater: Boolean): Boolean = {
    val cmp = Column.compareAt(col, i, j)
    if (wantGreater) cmp > 0 else cmp < 0
  }

  private def topNValues(col: Column[?], rowCount: Int, n: Int, isMax: Boolean): Seq[Any] = {
    val nonNullIndices = (0 until rowCount).filter(i => !col.nullSet.contains(i))
    val sorted = nonNullIndices.sortWith { (a, b) =>
      val cmp = Column.compareAt(col, a, b)
      if (isMax) cmp > 0 else cmp < 0
    }
    sorted.take(n).map(col.getValue)
  }

  private def topNByKey(
    valCol: Column[?],
    keyCol: Column[?],
    rowCount: Int,
    n: Int,
    isMax: Boolean
  ): Seq[Any] = {
    val indices = (0 until rowCount).filter(i => !valCol.isNull(RowIndex(i)) && !keyCol.isNull(RowIndex(i)))
    val sortedIndices = indices.sortWith { (a, b) =>
      if (isMax) compareColumnValues(keyCol, a, b, wantGreater = true)
      else compareColumnValues(keyCol, a, b, wantGreater = false)
    }
    sortedIndices.take(n).map(i => valCol.getValue(i))
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

  private def extractJsonPath(jsonStr: String, path: String): Either[ExecutionError, String | Null] = {
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

  private def jsonTupleScalar(pathResult: Option[JsonValue]): Any | Null = pathResult match {
    case Some(JsonValue.Str(s)) => s
    case Some(JsonValue.Bool(b)) => b.toString
    case Some(JsonValue.Null) => null // scalafix:ok DisableSyntax.null
    case Some(JsonValue.Number(n)) =>
      if (n == n.toLong.toDouble) n.toLong.toString else n.toString
    case Some(compound) => formatJson(compound)
    case None => null // scalafix:ok DisableSyntax.null
  }

  private def jsonTupleNulls(n: Int): Vector[Any | Null] =
    Vector.fill(n)(null) // scalafix:ok DisableSyntax.null

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

  private def variantGet[Row](
    expr: Expr[Row, ?],
    path: String,
    targetType: String,
    columns: Vector[Column[?]],
    rowCount: Int,
    tryMode: Boolean
  ): Either[ExecutionError, Column[?]] = {
    val innerType = inferExprColumnType(expr, columns)
    evalColumn(expr, columns, innerType).flatMap { col =>
      parseVariantPath(path) match {
        case None =>
          if (tryMode)
            Right(Column.any(Array.fill[Any | Null](rowCount)(null), BitSet.empty)) // scalafix:ok DisableSyntax.null
          else Left(ExecutionError.InvalidValue(s"Invalid variant path: $path"))
        case Some(segments) =>
          val outNulls = scala.collection.mutable.BitSet.empty
          val out = new Array[Any | Null](rowCount)
          val error = (0 until rowCount).foldLeft(Option.empty[ExecutionError]) { (err, i) =>
            err match {
              case some @ Some(_) => some
              case None =>
                if (col.isNull(RowIndex(i))) { outNulls += i; None }
                else
                  col.getValue(i) match {
                    case v: JsonValue =>
                      walkVariantPath(v, segments) match {
                        case None => out(i) = null; None // scalafix:ok DisableSyntax.null
                        case Some(found) =>
                          castJsonValue(found, targetType) match {
                            case Some(converted) => out(i) = converted; None
                            case None =>
                              if (tryMode) { outNulls += i; None }
                              else
                                Some(
                                  ExecutionError.InvalidValue(
                                    s"Cannot cast value at $path to $targetType at row $i"
                                  )
                                )
                          }
                      }
                    case other =>
                      Some(
                        ExecutionError.TypeMismatch("JsonValue", other.getClass.getSimpleName, "VariantGet")
                      )
                  }
            }
          }
          error match {
            case Some(e) => Left(e)
            case None => Right(Column.any(out, col.nullSet | (BitSet.empty ++ outNulls)))
          }
      }
    }
  }

  private def parseVariantPath(path: String): Option[List[Either[String, Int]]] = {
    def scanIndex(s: String, acc: List[Either[String, Int]]): Option[(List[Either[String, Int]], String)] =
      if (s.startsWith("[")) {
        val end = s.indexOf(']')
        val digits = if (end > 1) s.substring(1, end) else ""
        if (digits.nonEmpty && digits.forall(_.isDigit))
          scanIndex(s.substring(end + 1), Right(digits.toInt) :: acc)
        else None
      } else Some((acc, s))

    def scan(s: String, acc: List[Either[String, Int]]): Option[List[Either[String, Int]]] =
      if (s.isEmpty) Some(acc.reverse)
      else if (s.startsWith(".")) {
        val rest = s.drop(1)
        val nameEnd = rest.indexWhere(c => c == '.' || c == '[')
        val (name, remaining) = if (nameEnd < 0) (rest, "") else (rest.take(nameEnd), rest.drop(nameEnd))
        if (name.isEmpty) None
        else
          scanIndex(remaining, Left(name) :: acc) match {
            case Some((acc2, rest2)) => scan(rest2, acc2)
            case None => None
          }
      } else if (s.startsWith("[")) {
        scanIndex(s, acc) match {
          case Some((acc2, rest2)) => scan(rest2, acc2)
          case None => None
        }
      } else None

    val stripped = if (path.startsWith("$")) path.drop(1) else path
    scan(stripped, Nil)
  }

  private def walkVariantPath(value: JsonValue, segments: List[Either[String, Int]]): Option[JsonValue] =
    segments match {
      case Nil => Some(value)
      case Left(name) :: rest =>
        value match {
          case JsonValue.Object(fields) => fields.get(name).flatMap(walkVariantPath(_, rest))
          case _ => None
        }
      case Right(idx) :: rest =>
        value match {
          case JsonValue.Array(items) =>
            if (idx >= 0 && idx < items.length) walkVariantPath(items(idx), rest) else None
          case _ => None
        }
    }

  private def castJsonValue(value: JsonValue, targetType: String): Option[Any] =
    targetType.toUpperCase.nn match {
      case "VARIANT" => Some(value)
      case "STRING" =>
        value match {
          case JsonValue.Str(s) => Some(s)
          case JsonValue.Number(n) => Some(if (n == n.toLong.toDouble) n.toLong.toString else n.toString)
          case JsonValue.Bool(b) => Some(b.toString)
          case _ => None
        }
      case "BIGINT" | "INTEGER" | "INT" | "LONG" =>
        value match {
          case JsonValue.Number(n) if n.isValidInt => Some(n.toLong)
          case _ => None
        }
      case "DOUBLE" =>
        value match {
          case JsonValue.Number(n) => Some(n)
          case _ => None
        }
      case "BOOLEAN" =>
        value match {
          case JsonValue.Bool(b) => Some(b)
          case JsonValue.Number(n) => Some(n != 0.0)
          case _ => None
        }
      case _ => None
    }

  private def jsonValueToSqlType(value: JsonValue): Option[String] = value match {
    case JsonValue.Null => Some("VOID")
    case JsonValue.Bool(_) => Some("BOOLEAN")
    case JsonValue.Number(n) => Some(if (n.isValidInt) "BIGINT" else "DOUBLE")
    case JsonValue.Str(_) => Some("STRING")
    case JsonValue.Array(items) =>
      val elemTypes = items.map(jsonValueToSqlType).collect { case Some(t) => t }.distinct
      Some(s"ARRAY<${if (elemTypes.isEmpty) "VOID" else elemTypes.mkString(", ")}>")
    case JsonValue.Object(fields) =>
      val entries = fields.toVector.sortBy(_._1).flatMap { case (k, v) =>
        jsonValueToSqlType(v).map(t => s"$k: $t")
      }
      Some(s"OBJECT<${entries.mkString(", ")}>")
  }

  /** Convert a SQL LIKE pattern to a regex. `%` -> `.*`, `_` -> `.`, others escaped. */
  private def likeToRegex(pattern: String): scala.util.matching.Regex = {
    val sb = pattern.foldLeft(new StringBuilder("(?s)")) { (acc, c) =>
      c match {
        case '%' => acc.append(".*")
        case '_' => acc.append('.')
        case ch =>
          if ("\\[]{}()^$.|*+?".indexOf(ch) >= 0) acc.append('\\')
          acc.append(ch)
      }
    }
    sb.toString.r
  }
}
