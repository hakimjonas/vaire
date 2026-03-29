package net.ghoula.strongbow.interpreter

import net.ghoula.sarati.ast.json.JsonValue
import parsers.json.{formatJson, parseJson}

import scala.collection.immutable.BitSet

import net.ghoula.strongbow.column.{Column, ColumnType}
import net.ghoula.strongbow.errors.ExecutionError
import net.ghoula.strongbow.expr.Expr
import net.ghoula.strongbow.types.{Date, RowIndex}

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
      case _: Expr.SumDouble[_] | _: Expr.Avg[_] | _: Expr.StdDev[_] | _: Expr.StdDevPop[_] => ColumnType.DoubleType
      case _: Expr.Sum[_] | _: Expr.SumLong[_] | _: Expr.Count[_] | _: Expr.CountDistinct[_, _] | _: Expr.CountIf[_] =>
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
    * uses while-loops on typed arrays.
    */
  def evalColumn[Row, A](
    expr: Expr[Row, A],
    columns: Vector[Column[?]],
    columnType: ColumnType
  ): Either[ExecutionError, Column[?]] = {
    if (columns.isEmpty || columns.head.length == 0) {
      Right(Column.empty(columnType))
    } else {

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
                Column.string(Array.tabulate(rowCount)(i => ld(i).nn + rd(i)), ln | rn)
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
            case Column.StringColumn(data, nulls) =>
              Column.string(
                Array.tabulate(rowCount)(i =>
                  if (nulls.contains(i)) null else data(i).nn.toLowerCase // scalafix:ok DisableSyntax.null
                ),
                nulls
              )
            case _ => Column.string(Array.empty[String | Null])
          }

        case up: Expr.Upper[Row] =>
          evalColumn(up.expr, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              Column.string(
                Array.tabulate(rowCount)(i =>
                  if (nulls.contains(i)) null else data(i).nn.toUpperCase // scalafix:ok DisableSyntax.null
                ),
                nulls
              )
            case _ => Column.string(Array.empty[String | Null])
          }

        case tr: Expr.Trim[Row] =>
          evalColumn(tr.expr, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              Column.string(
                Array.tabulate(rowCount)(i =>
                  if (nulls.contains(i)) null else data(i).nn.trim // scalafix:ok DisableSyntax.null
                ),
                nulls
              )
            case _ => Column.string(Array.empty[String | Null])
          }

        case lt: Expr.LTrim[Row] =>
          evalColumn(lt.expr, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              Column.string(
                Array.tabulate(rowCount)(i =>
                  if (nulls.contains(i)) null else data(i).nn.stripLeading.nn // scalafix:ok DisableSyntax.null
                ),
                nulls
              )
            case _ => Column.string(Array.empty[String | Null])
          }

        case rt: Expr.RTrim[Row] =>
          evalColumn(rt.expr, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              Column.string(
                Array.tabulate(rowCount)(i =>
                  if (nulls.contains(i)) null else data(i).nn.stripTrailing.nn // scalafix:ok DisableSyntax.null
                ),
                nulls
              )
            case _ => Column.string(Array.empty[String | Null])
          }

        case ss: Expr.Substring[Row] =>
          evalColumn(ss.expr, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              Column.string(
                Array.tabulate(rowCount) { i =>
                  if (nulls.contains(i)) null // scalafix:ok DisableSyntax.null
                  else {
                    val s = data(i).nn
                    val start = Math.max(ss.pos - 1, 0)
                    val end = Math.min(start + ss.len, s.length)
                    if (start >= s.length) "" else s.substring(start, end)
                  }
                },
                nulls
              )
            case _ => Column.string(Array.empty[String | Null])
          }

        case sr: Expr.StringReplace[Row] =>
          evalColumn(sr.expr, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              Column.string(
                Array.tabulate(rowCount)(i =>
                  if (nulls.contains(i)) null // scalafix:ok DisableSyntax.null
                  else data(i).nn.replace(sr.search, sr.replacement)
                ),
                nulls
              )
            case _ => Column.string(Array.empty[String | Null])
          }

        case rr: Expr.RegexpReplace[Row] =>
          evalColumn(rr.expr, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              val compiled = java.util.regex.Pattern.compile(rr.pattern)
              Column.string(
                Array.tabulate(rowCount)(i =>
                  if (nulls.contains(i)) null // scalafix:ok DisableSyntax.null
                  else compiled.matcher(data(i)).replaceAll(rr.replacement).nn
                ),
                nulls
              )
            case _ => Column.string(Array.empty[String | Null])
          }

        case re: Expr.RegexpExtract[Row] =>
          evalColumn(re.expr, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              val compiled = java.util.regex.Pattern.compile(re.pattern)
              Column.string(
                Array.tabulate(rowCount) { i =>
                  if (nulls.contains(i)) null // scalafix:ok DisableSyntax.null
                  else {
                    val m = compiled.matcher(data(i))
                    if (m.find()) m.group(re.groupIdx) else ""
                  }
                },
                nulls
              )
            case _ => Column.string(Array.empty[String | Null])
          }

        case sp: Expr.StringSplit[Row] =>
          evalColumn(sp.expr, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              Column.any(
                Array.tabulate[Any | Null](rowCount)(i =>
                  if (nulls.contains(i)) null // scalafix:ok DisableSyntax.null
                  else data(i).nn.split(sp.delimiter, -1).toSeq
                ),
                nulls
              )
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
            val nulls = col match {
              case Column.IntColumn(_, n) => n
              case Column.LongColumn(_, n) => n
              case Column.DoubleColumn(_, n) => n
              case Column.StringColumn(_, n) => n
              case Column.BooleanColumn(_, n) => n
              case Column.DateColumn(_, n) => n
              case Column.AnyColumn(_, n) => n
            }
            Column.boolean(Array.tabulate(rowCount)(i => nulls.contains(i)))
          }

        case inn: Expr.IsNotNull[Row, _] =>
          val innerType = inferExprColumnType(inn.expr, columns)
          evalColumn(inn.expr, columns, innerType).map { col =>
            val nulls = col match {
              case Column.IntColumn(_, n) => n
              case Column.LongColumn(_, n) => n
              case Column.DoubleColumn(_, n) => n
              case Column.StringColumn(_, n) => n
              case Column.BooleanColumn(_, n) => n
              case Column.DateColumn(_, n) => n
              case Column.AnyColumn(_, n) => n
            }
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
              (exprCol, lowerCol, upperCol) match {
                case (Column.IntColumn(vd, vn), Column.IntColumn(ld, ln), Column.IntColumn(ud, un)) =>
                  val combined = vn | ln | un
                  Right(
                    Column.boolean(
                      Array.tabulate(rowCount)(i =>
                        if (combined.contains(i)) false else vd(i) >= ld(i) && vd(i) <= ud(i)
                      ),
                      combined
                    )
                  )
                case (Column.LongColumn(vd, vn), Column.LongColumn(ld, ln), Column.LongColumn(ud, un)) =>
                  val combined = vn | ln | un
                  Right(
                    Column.boolean(
                      Array.tabulate(rowCount)(i =>
                        if (combined.contains(i)) false else vd(i) >= ld(i) && vd(i) <= ud(i)
                      ),
                      combined
                    )
                  )
                case (Column.DoubleColumn(vd, vn), Column.DoubleColumn(ld, ln), Column.DoubleColumn(ud, un)) =>
                  val combined = vn | ln | un
                  Right(
                    Column.boolean(
                      Array.tabulate(rowCount)(i =>
                        if (combined.contains(i)) false else vd(i) >= ld(i) && vd(i) <= ud(i)
                      ),
                      combined
                    )
                  )
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
            val nulls = col match {
              case Column.IntColumn(_, n) => n
              case Column.LongColumn(_, n) => n
              case Column.DoubleColumn(_, n) => n
              case Column.StringColumn(_, n) => n
              case Column.BooleanColumn(_, n) => n
              case Column.DateColumn(_, n) => n
              case Column.AnyColumn(_, n) => n
            }
            Column.string(
              Array.tabulate(rowCount)(i =>
                if (nulls.contains(i)) null else String.valueOf(col.getValue(i)).nn // scalafix:ok DisableSyntax.null
              ),
              nulls
            )
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
                    val d = java.time.LocalDate.ofEpochDay(data(i).toLong);
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
          evalColumn(df.date, columns, ColumnType.DateType).map {
            case Column.DateColumn(data, nulls) =>
              val formatter = java.time.format.DateTimeFormatter.ofPattern(df.format)
              Column.string(
                Array.tabulate(rowCount)(i =>
                  if (nulls.contains(i)) null // scalafix:ok DisableSyntax.null
                  else java.time.LocalDate.ofEpochDay(data(i).toLong).format(formatter)
                ),
                nulls
              )
            case _ => Column.string(Array.empty[String | Null])
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
            val nulls = col match { case Column.AnyColumn(_, n) => n; case _ => BitSet.empty }
            Column.any(
              Array.tabulate[Any](rowCount)(i =>
                if (nulls.contains(i)) null // scalafix:ok DisableSyntax.null
                else col.getValue(i) match { case s: Seq[?] => s.distinct; case other => other }
              ),
              nulls
            )
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
            Column.any(Array.tabulate[Any](rowCount) { i =>
              col.getValue(i) match {
                case s: Seq[Seq[?] @unchecked] => s.flatten
                case other => other
              }
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
          vectorizedStringHash(md.expr, columns, rowCount)("MD5")

        case sh: Expr.Sha1[Row] =>
          vectorizedStringHash(sh.expr, columns, rowCount)("SHA-1")

        case sh2: Expr.Sha2[Row] =>
          vectorizedStringHash(sh2.expr, columns, rowCount)(sha2Algorithm(sh2.bitLength))

        case ue: Expr.UrlEncode[Row] =>
          evalColumn(ue.expr, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              Column.string(
                Array.tabulate(rowCount)(i =>
                  if (nulls.contains(i)) null
                  else java.net.URLEncoder.encode(data(i).nn, "UTF-8").nn // scalafix:ok DisableSyntax.null
                ),
                nulls
              )
            case _ => Column.string(Array.empty[String | Null])
          }

        case ud: Expr.UrlDecode[Row] =>
          evalColumn(ud.expr, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              Column.string(
                Array.tabulate(rowCount)(i =>
                  if (nulls.contains(i)) null
                  else java.net.URLDecoder.decode(data(i).nn, "UTF-8").nn // scalafix:ok DisableSyntax.null
                ),
                nulls
              )
            case _ => Column.string(Array.empty[String | Null])
          }

        case b64e: Expr.Base64Encode[Row] =>
          evalColumn(b64e.expr, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              val encoder = java.util.Base64.getEncoder.nn
              Column.string(
                Array.tabulate(rowCount)(i =>
                  if (nulls.contains(i)) null
                  else encoder.encodeToString(data(i).nn.getBytes("UTF-8")).nn // scalafix:ok DisableSyntax.null
                ),
                nulls
              )
            case _ => Column.string(Array.empty[String | Null])
          }

        case b64d: Expr.Base64Decode[Row] =>
          evalColumn(b64d.expr, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              val decoder = java.util.Base64.getDecoder.nn
              Column.string(
                Array.tabulate(rowCount)(i =>
                  if (nulls.contains(i)) null
                  else new String(decoder.decode(data(i).nn), "UTF-8") // scalafix:ok DisableSyntax.null
                ),
                nulls
              )
            case _ => Column.string(Array.empty[String | Null])
          }

        case hx: Expr.Hex[Row] =>
          evalColumn(hx.expr, columns, ColumnType.StringType).map {
            case Column.StringColumn(data, nulls) =>
              Column.string(
                Array.tabulate(rowCount)(i =>
                  if (nulls.contains(i)) null
                  else hexEncode(data(i).nn.getBytes("UTF-8")) // scalafix:ok DisableSyntax.null
                ),
                nulls
              )
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
      }
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
          Column.int(Array.tabulate(rowCount)(i => op(ld(i), rd(i))), ln | rn)
        case _ =>
          Column.int(Array.empty[Int])
      }
    }
  }

  private def vectorizedDiv(left: Array[Int], right: Array[Int], rowCount: Int): Either[ExecutionError, Column[?]] = {
    val zeroIdx = (0 until rowCount).find(i => right(i) == 0)
    zeroIdx match {
      case Some(i) => Left(ExecutionError.DivisionByZero(i))
      case None => Right(Column.int(Array.tabulate(rowCount)(i => left(i) / right(i))))
    }
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
  ): Either[ExecutionError, Column[?]] = {
    val zeroIdx = (0 until rowCount).find(i => right(i) == 0L)
    zeroIdx match {
      case Some(i) => Left(ExecutionError.DivisionByZero(i))
      case None => Right(Column.long(Array.tabulate(rowCount)(i => left(i) / right(i))))
    }
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
  ): Either[ExecutionError, Column[?]] = {
    val zeroIdx = (0 until rowCount).find(i => right(i) == 0.0)
    zeroIdx match {
      case Some(i) => Left(ExecutionError.DivisionByZero(i))
      case None => Right(Column.double(Array.tabulate(rowCount)(i => left(i) / right(i))))
    }
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
        val matched: Either[ExecutionError, Column[?]] = (leftCol, rightCol) match {
          case (Column.IntColumn(ld, ln), Column.IntColumn(rd, rn)) =>
            Right(Column.boolean(Array.tabulate(rowCount)(i => intCmp(ld(i), rd(i))), ln | rn))
          case (Column.LongColumn(ld, ln), Column.LongColumn(rd, rn)) =>
            Right(Column.boolean(Array.tabulate(rowCount)(i => longCmp(ld(i), rd(i))), ln | rn))
          case (Column.DoubleColumn(ld, ln), Column.DoubleColumn(rd, rn)) =>
            Right(Column.boolean(Array.tabulate(rowCount)(i => doubleCmp(ld(i), rd(i))), ln | rn))
          case (Column.StringColumn(ld, ln), Column.StringColumn(rd, rn)) =>
            Right(
              Column.boolean(
                Array.tabulate(rowCount)(i =>
                  if (ln.contains(i) || rn.contains(i)) false else stringCmp(ld(i).nn, rd(i).nn)
                ),
                ln | rn
              )
            )
          case (Column.DateColumn(ld, ln), Column.DateColumn(rd, rn)) =>
            Right(Column.boolean(Array.tabulate(rowCount)(i => intCmp(ld(i), rd(i))), ln | rn))
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

  private def vectorizedIntMod(
    left: Array[Int],
    right: Array[Int],
    rowCount: Int
  ): Either[ExecutionError, Column[?]] = {
    val zeroIdx = (0 until rowCount).find(i => right(i) == 0)
    zeroIdx match {
      case Some(i) => Left(ExecutionError.DivisionByZero(i))
      case None => Right(Column.int(Array.tabulate(rowCount)(i => left(i) % right(i))))
    }
  }

  private def vectorizedLongMod(
    left: Array[Long],
    right: Array[Long],
    rowCount: Int
  ): Either[ExecutionError, Column[?]] = {
    val zeroIdx = (0 until rowCount).find(i => right(i) == 0L)
    zeroIdx match {
      case Some(i) => Left(ExecutionError.DivisionByZero(i))
      case None => Right(Column.long(Array.tabulate(rowCount)(i => left(i) % right(i))))
    }
  }

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
    columns: Vector[Column[?]],
    rowCount: Int
  )(algorithm: String): Either[ExecutionError, Column[?]] = {
    evalColumn(expr, columns, ColumnType.StringType).map {
      case Column.StringColumn(data, nulls) =>
        val digest = java.security.MessageDigest.getInstance(algorithm).nn
        Column.string(
          Array.tabulate(rowCount) { i =>
            if (nulls.contains(i)) null // scalafix:ok DisableSyntax.null
            else { digest.reset(); hexEncode(digest.digest(data(i).nn.getBytes("UTF-8")).nn) }
          },
          nulls
        )
      case _ =>
        Column.string(Array.empty[String | Null])
    }
  }

  /** Evaluate aggregation expression over entire dataset.
    *
    * Aggregations operate on all rows to produce a single value. Returns Any because the result is
    * consumed by DatasetInterpreter via Column.fromValues which takes Vector[Any]. The GADT type
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
      }
    } else {
      val rowCount = columns.head.length
      (expr: @unchecked) match {
        case Expr.Count() =>
          Right(rowCount.toLong)

        case sum: Expr.Sum[Row] =>
          evalColumn(sum.expr, columns, ColumnType.IntType).map {
            case Column.IntColumn(data, nulls) =>
              (0 until data.length).foldLeft(0L)((acc, i) => if (nulls.contains(i)) acc else acc + data(i))
            case _ => 0L
          }

        case sumD: Expr.SumDouble[Row] =>
          evalColumn(sumD.expr, columns, ColumnType.DoubleType).map {
            case Column.DoubleColumn(data, nulls) =>
              (0 until data.length).foldLeft(0.0)((acc, i) => if (nulls.contains(i)) acc else acc + data(i))
            case _ => 0.0
          }

        case sumL: Expr.SumLong[Row] =>
          evalColumn(sumL.expr, columns, ColumnType.LongType).map {
            case Column.LongColumn(data, nulls) =>
              (0 until data.length).foldLeft(0L)((acc, i) => if (nulls.contains(i)) acc else acc + data(i))
            case _ => 0L
          }

        case avg: Expr.Avg[Row] =>
          evalColumn(avg.expr, columns, ColumnType.DoubleType).map {
            case Column.DoubleColumn(data, nulls) =>
              val (total, count) = (0 until data.length).foldLeft((0.0, 0)) { case ((sum, cnt), i) =>
                if (nulls.contains(i)) (sum, cnt) else (sum + data(i), cnt + 1)
              }
              if (count == 0) 0.0 else total / count
            case _ => 0.0
          }

        case countIf: Expr.CountIf[Row] =>
          evalColumn(countIf.predicate, columns, ColumnType.BooleanType).map {
            case Column.BooleanColumn(data, nulls) =>
              (0 until data.length).foldLeft(0L)((acc, i) => if (!nulls.contains(i) && data(i)) acc + 1L else acc)
            case _ => 0L
          }

        case sd: Expr.StdDev[Row] =>
          aggregateDoubleExpr(sd.expr, columns) { (data, nulls) =>
            val (sum, count) = sumAndCount(data, nulls)
            if (count <= 1) 0.0
            else {
              val mean = sum / count
              val variance = (0 until data.length).foldLeft(0.0) { (acc, i) =>
                if (nulls.contains(i)) acc else { val d = data(i) - mean; acc + d * d }
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
              val variance = (0 until data.length).foldLeft(0.0) { (acc, i) =>
                if (nulls.contains(i)) acc else { val d = data(i) - mean; acc + d * d }
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
          evalColumn(ba.expr, columns, ColumnType.BooleanType).map {
            case Column.BooleanColumn(data, nulls) =>
              !(0 until data.length).exists(i => !nulls.contains(i) && !data(i))
            case _ => true
          }

        case bo: Expr.BoolOr[Row] =>
          evalColumn(bo.expr, columns, ColumnType.BooleanType).map {
            case Column.BooleanColumn(data, nulls) =>
              (0 until data.length).exists(i => !nulls.contains(i) && data(i))
            case _ => false
          }

        case v: Expr.Variance[Row] =>
          aggregateDoubleExpr(v.expr, columns) { (data, nulls) =>
            val (sum, count) = sumAndCount(data, nulls)
            if (count <= 1) 0.0
            else {
              val mean = sum / count
              val variance = (0 until data.length).foldLeft(0.0) { (acc, i) =>
                if (nulls.contains(i)) acc else { val d = data(i) - mean; acc + d * d }
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
              val variance = (0 until data.length).foldLeft(0.0) { (acc, i) =>
                if (nulls.contains(i)) acc else { val d = data(i) - mean; acc + d * d }
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
                  val (cov, xVar, yVar) = (0 until xData.length).foldLeft((0.0, 0.0, 0.0)) { case ((c, xv, yv), i) =>
                    if (combinedNulls.contains(i)) (c, xv, yv)
                    else {
                      val dx = xData(i) - xMean; val dy = yData(i) - yMean; (c + dx * dy, xv + dx * dx, yv + dy * dy)
                    }
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
                  val cov = (0 until xData.length).foldLeft(0.0) { (acc, i) =>
                    if (combinedNulls.contains(i)) acc else acc + (xData(i) - xMean) * (yData(i) - yMean)
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
                  val cov = (0 until xData.length).foldLeft(0.0) { (acc, i) =>
                    if (combinedNulls.contains(i)) acc else acc + (xData(i) - xMean) * (yData(i) - yMean)
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

        case countDist: Expr.CountDistinct[Row, ?] =>
          val colType = inferExprColumnType(countDist.expr, columns)
          evalColumn(countDist.expr, columns, colType).map { col =>
            (0 until rowCount).filter(i => !col.isNull(RowIndex(i))).map(col.getValue).toSet.size.toLong
          }

        case acd: Expr.ApproxCountDistinct[Row, ?] =>
          val colType = inferExprColumnType(acd.expr, columns)
          evalColumn(acd.expr, columns, colType).map { col =>
            (0 until rowCount).filter(i => !col.isNull(RowIndex(i))).map(col.getValue).toSet.size.toLong
          }

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
          val colType = inferExprColumnType(first.expr, columns)
          evalColumn(first.expr, columns, colType).map { col =>
            (0 until rowCount).find(i => !col.isNull(RowIndex(i))).map(col.getValue)
          }

        case last: Expr.ExprLast[Row, ?] =>
          val colType = inferExprColumnType(last.expr, columns)
          evalColumn(last.expr, columns, colType).map { col =>
            (0 until rowCount).filter(i => !col.isNull(RowIndex(i))).lastOption.map(col.getValue)
          }

        case anyVal: Expr.AnyValue[Row, ?] =>
          val colType = inferExprColumnType(anyVal.expr, columns)
          evalColumn(anyVal.expr, columns, colType).map { col =>
            (0 until rowCount).find(i => !col.isNull(RowIndex(i))).map(col.getValue)
          }

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
            (0 until rowCount).filter(i => !col.isNull(RowIndex(i))).map(col.getValue).toSeq
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
    (0 until data.length).foldLeft((0.0, 0)) { case ((sum, count), i) =>
      if (nulls.contains(i)) (sum, count) else (sum + data(i), count + 1)
    }
  }

  /** Collect non-null doubles into a new array for sorting. */
  private def collectNonNullDoubles(data: Array[Double], nulls: BitSet): Array[Double] = {
    data.indices.filter(i => !nulls.contains(i)).map(data(_)).toArray
  }

  private def findExtremum(col: Column[?], rowCount: Int, isMax: Boolean): Option[Any] = {
    def foldExtremum[T](data: Array[T], nulls: BitSet, better: (T, T) => Boolean): Option[T] =
      (0 until rowCount).foldLeft(Option.empty[T]) { (best, i) =>
        if (nulls.contains(i)) best
        else
          best match {
            case None => Some(data(i))
            case Some(b) => Some(if (better(data(i), b)) data(i) else b)
          }
      }

    col match {
      case Column.IntColumn(data, nulls) =>
        foldExtremum(data, nulls, (a: Int, b: Int) => if (isMax) a > b else a < b)
      case Column.LongColumn(data, nulls) =>
        foldExtremum(data, nulls, (a: Long, b: Long) => if (isMax) a > b else a < b)
      case Column.DoubleColumn(data, nulls) =>
        foldExtremum(data, nulls, (a: Double, b: Double) => if (isMax) a > b else a < b)
      case Column.StringColumn(data, nulls) =>
        foldExtremum(
          data,
          nulls,
          (a: String | Null, b: String | Null) => { val c = a.nn.compareTo(b); if (isMax) c > 0 else c < 0 }
        )
      case Column.DateColumn(data, nulls) =>
        (0 until rowCount).foldLeft(Option.empty[Date]) { (best, i) =>
          if (nulls.contains(i)) best
          else {
            val d = Date.ofEpochDay(data(i).toLong)
            best match {
              case None => Some(d)
              case Some(b) =>
                val cmp = data(i).compareTo(b.toEpochDay.toInt)
                Some(if (isMax && cmp > 0) d else if (!isMax && cmp < 0) d else b)
            }
          }
        }
      case Column.BooleanColumn(data, nulls) =>
        foldExtremum(data, nulls, (a: Boolean, b: Boolean) => if (isMax) a && !b else !a && b)
      case Column.AnyColumn(_, _) => None
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
    col match {
      case Column.IntColumn(data, _) =>
        if (wantGreater) data(i) > data(j) else data(i) < data(j)
      case Column.LongColumn(data, _) =>
        if (wantGreater) data(i) > data(j) else data(i) < data(j)
      case Column.DoubleColumn(data, _) =>
        if (wantGreater) data(i) > data(j) else data(i) < data(j)
      case Column.StringColumn(data, _) =>
        val cmp = data(i).nn.compareTo(data(j))
        if (wantGreater) cmp > 0 else cmp < 0
      case Column.DateColumn(data, _) =>
        if (wantGreater) data(i) > data(j) else data(i) < data(j)
      case Column.BooleanColumn(data, _) =>
        if (wantGreater) data(i) && !data(j) else !data(i) && data(j)
      case Column.AnyColumn(_, _) => false
    }
  }

  private def topNValues(col: Column[?], rowCount: Int, n: Int, isMax: Boolean): Seq[Any] = {
    col match {
      case Column.IntColumn(data, nulls) =>
        val vals = collectNonNullTyped(data, nulls, rowCount)
        val sorted = if (isMax) vals.sorted(using Ordering[Int].reverse) else vals.sorted
        sorted.take(n).toSeq
      case Column.LongColumn(data, nulls) =>
        val vals = collectNonNullTyped(data, nulls, rowCount)
        val sorted = if (isMax) vals.sorted(using Ordering[Long].reverse) else vals.sorted
        sorted.take(n).toSeq
      case Column.DoubleColumn(data, nulls) =>
        val vals = collectNonNullTyped(data, nulls, rowCount)
        val sorted = if (isMax) vals.sorted(using Ordering[Double].reverse) else vals.sorted
        sorted.take(n).toSeq
      case Column.StringColumn(data, nulls) =>
        val vals = collectNonNullTyped(data, nulls, rowCount).map(_.nn)
        val sorted = if (isMax) vals.sorted(using Ordering[String].reverse) else vals.sorted
        sorted.take(n).toSeq
      case Column.DateColumn(data, nulls) =>
        val vals = (0 until rowCount).filter(!nulls.contains(_)).map(i => Date.ofEpochDay(data(i).toLong)).toVector
        val sorted = if (isMax) vals.sorted(using Ordering[Date].reverse) else vals.sorted
        sorted.take(n).toSeq
      case Column.BooleanColumn(data, nulls) =>
        val vals = collectNonNullTyped(data, nulls, rowCount)
        val sorted = if (isMax) vals.sorted(using Ordering[Boolean].reverse) else vals.sorted
        sorted.take(n).toSeq
      case Column.AnyColumn(data, nulls) =>
        val vals = collectNonNullTyped(data, nulls, rowCount)
        vals.take(n).toSeq
    }
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
    sortedIndices.take(n).map(i => valCol.getValue(i)).toSeq
  }

  private def collectNonNullTyped[T](data: Array[T], nulls: BitSet, rowCount: Int): Vector[T] = {
    (0 until rowCount).filter(i => !nulls.contains(i)).map(data(_)).toVector
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
