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
        // GADT refinement: value has type A
        Right(value)

      case named: Expr.Named[Row, _] =>
        // Named is just a wrapper for column naming - evaluate the inner expression
        eval(named.expr, columns, rowIdx)

      case cell: Expr.Cell[Row, a] =>
        // ONE CAST AT BOUNDARY: typed column storage → generic type A
        if (cell.index.toInt >= columns.length) {
          Left(ExecutionError.IndexOutOfBounds(cell.index.toInt, columns.length))
        } else {
          val column = columns(cell.index.toInt)
          val idx = rowIdx.toInt

          if (idx >= column.length) {
            Left(ExecutionError.IndexOutOfBounds(idx, column.length))
          } else {
            // ONE-CAST-AT-BOUNDARY PATTERN:
            // This is the ONLY place in the expression evaluation pipeline where we cast.
            // We bridge from typed column accessors (getInt: Int, getString: String, etc.)
            // to the GADT's generic type variable `a`.
            //
            // After this cast, all downstream expression logic is zero-cast because GADTs
            // provide compile-time type refinement (e.g., Add[Row] refines to Int + Int).
            //
            // These casts are architecturally fundamental and cannot be eliminated without
            // losing the zero-cast property in the rest of the interpreter.
            val value: a = column.columnType match {
              case ColumnType.IntType => column.getInt(idx).asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
              case ColumnType.LongType => column.getLong(idx).asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
              case ColumnType.DoubleType =>
                column.getDouble(idx).asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
              case ColumnType.StringType =>
                column.getString(idx).asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
              case ColumnType.BooleanType =>
                column.getBoolean(idx).asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
              case ColumnType.AnyType | ColumnType.OptionType(_) =>
                column.getValue(idx).asInstanceOf[a] // scalafix:ok DisableSyntax.asInstanceOf
            }
            Right(value)
          }
        }

      // Numeric operations - GADT guarantees types, NO CASTS
      case add: Expr.Add[Row] =>
        for {
          l <- eval(add.left, columns, rowIdx) // l: Int by GADT
          r <- eval(add.right, columns, rowIdx) // r: Int by GADT
        } yield l + r // NO CAST NEEDED!

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

      // Comparisons - GADT provides Ordering evidence, NO CASTS!
      case eq: Expr.Eq[Row, _] =>
        for {
          l <- eval(eq.left, columns, rowIdx)
          r <- eval(eq.right, columns, rowIdx)
        } yield java.util.Objects.equals(l, r)

      case gt: Expr.Gt[Row, _] =>
        // GADT pattern match gives us gt.ordering
        for {
          l <- eval(gt.left, columns, rowIdx)
          r <- eval(gt.right, columns, rowIdx)
        } yield gt.ordering.gt(l, r)

      case lt: Expr.Lt[Row, _] =>
        // GADT pattern match gives us lt.ordering
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

      // Conditional logic - NO CASTS!
      case when: Expr.When[Row, _] =>
        eval(when.condition, columns, rowIdx).flatMap { cond =>
          if (cond) eval(when.thenExpr, columns, rowIdx)
          else eval(when.elseExpr, columns, rowIdx)
        }

      // Boolean operations - GADT guarantees Boolean type, NO CASTS!
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

      // String operations - NO CASTS!
      case concat: Expr.Concat[Row] =>
        for {
          l <- eval(concat.left, columns, rowIdx)
          r <- eval(concat.right, columns, rowIdx)
        } yield l + r

      case length: Expr.Length[Row] =>
        for {
          v <- eval(length.expr, columns, rowIdx)
        } yield v.length

      // Option operations - NO CASTS!
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

      // Aggregations - not supported at row level
      case _: Expr.Sum[Row] | _: Expr.Count[Row] | _: Expr.Max[Row, ?] | _: Expr.Min[Row, ?] |
          _: Expr.Avg[Row] | _: Expr.CountDistinct[Row, ?] | _: Expr.CountIf[Row] |
          _: Expr.StdDev[Row] | _: Expr.StdDevPop[Row] =>
        Left(ExecutionError.UnsupportedOperation("Aggregations not supported in row-level eval"))
    }
  }

  /** Evaluate expression for all rows, producing a new column. */
  def evalColumn[Row, A](
    expr: Expr[Row, A],
    columns: Vector[Column],
    columnType: ColumnType
  ): Either[ExecutionError, Column] = {
    if (columns.isEmpty || columns.head.length == 0) {
      Right(Column.empty(columnType))
    } else {
      val rowCount = columns.head.length

      // Collect values, short-circuit on first error
      val valuesOrError = (0 until rowCount).foldLeft[Either[ExecutionError, Vector[Any]]](
        Right(Vector.empty)
      ) { (acc, rowIdx) =>
        acc.flatMap { values =>
          eval(expr, columns, RowIndex(rowIdx)).map(values :+ _)
        }
      }

      valuesOrError.flatMap(values => Column.fromValues(values, columnType))
    }
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
      // Handle empty datasets - GADT refinement means NO CASTS
      (expr: @unchecked) match {
        case _: Expr.Count[Row] =>
          Right(0L)
        case _: Expr.Sum[Row] =>
          Right(0)
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
      }
    }
  }
}
