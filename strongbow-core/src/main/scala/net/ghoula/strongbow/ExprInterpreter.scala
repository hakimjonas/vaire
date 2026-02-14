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
      case _: Expr.Sum[Row] | _: Expr.Count[Row] | _: Expr.Max[Row, ?] | _: Expr.Min[Row, ?] | _: Expr.Avg[Row] |
          _: Expr.CountDistinct[Row, ?] | _: Expr.CountIf[Row] | _: Expr.StdDev[Row] | _: Expr.StdDevPop[Row] =>
        Left(ExecutionError.UnsupportedOperation("Aggregations not supported in row-level eval"))
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

      // Short-circuit And/Or — current eval evaluates both sides unconditionally
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
    }
  }

  /** Unboxed evaluation returning Any — internal helper for evalBoolean's sub-expressions.
    *
    * Avoids Either wrapping. Throws on errors (programming bugs, not data quality).
    */
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

      // Boolean expressions delegate to evalBoolean
      case boolExpr: (Expr.Gt[Row, _] | Expr.Lt[Row, _] | Expr.Gte[Row, _] | Expr.Lte[Row, _] | Expr.Eq[Row, _] |
            Expr.Neq[Row, _] | Expr.And[Row] | Expr.Or[Row] | Expr.Not[Row] | Expr.IsDefined[Row, _]) =>
        evalBoolean(
          boolExpr.asInstanceOf[Expr[Row, Boolean]],
          columns,
          rowIdx
        ) // scalafix:ok DisableSyntax.asInstanceOf
    }
  }

  /** Infer the ColumnType of an expression's operands by inspecting the expression tree and
    * columns.
    *
    * Used by vectorized evaluation to determine array types for sub-expressions.
    */
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
      case _: Expr.Add[_] | _: Expr.Sub[_] | _: Expr.Mul[_] | _: Expr.Div[_] | _: Expr.Length[_] =>
        ColumnType.IntType
      case _: Expr.Concat[_] => ColumnType.StringType
      case _: Expr.Gt[_, _] | _: Expr.Lt[_, _] | _: Expr.Gte[_, _] | _: Expr.Lte[_, _] | _: Expr.Eq[_, _] |
          _: Expr.Neq[_, _] | _: Expr.And[_] | _: Expr.Or[_] | _: Expr.Not[_] | _: Expr.IsDefined[_, _] =>
        ColumnType.BooleanType
      case w: Expr.When[_, _] => inferExprColumnType(w.thenExpr, columns)
      case g: Expr.GetOrElse[_, _] => inferExprColumnType(g.expr, columns)
      case _: Expr.Sum[_] => ColumnType.IntType
      case _: Expr.Count[_] | _: Expr.CountDistinct[_, _] | _: Expr.CountIf[_] => ColumnType.LongType
      case _: Expr.Avg[_] | _: Expr.StdDev[_] | _: Expr.StdDevPop[_] => ColumnType.DoubleType
      case _: Expr.Max[_, _] | _: Expr.Min[_, _] => ColumnType.AnyType
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
      // Cell reference — return column directly, zero work
      case cell: Expr.Cell[_, _] =>
        Right(columns(cell.index.toInt))

      // Named — unwrap and recurse
      case named: Expr.Named[_, _] =>
        evalColumn(named.expr, columns, columnType)

      // Constant — fill typed array
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
          case _ =>
            Right(Column.any(Array.fill(rowCount)(c.value.asInstanceOf[Any]))) // scalafix:ok DisableSyntax.asInstanceOf
        }

      // Arithmetic — vectorized int array operations
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

      // Comparisons — vectorized, dispatch on operand column type
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

      // Boolean ops — vectorized on boolean arrays
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

      // String ops — vectorized on string arrays
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

      // When — evaluate condition, then pick from then/else columns
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

      // Fallback — row-by-row for anything not yet vectorized
      case _ =>
        evalColumnRowByRow(expr, columns, columnType, rowCount)
    }
  }

  /** Vectorized int binary operation helper. */
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

  /** Vectorized division with zero-check. */
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

  /** Vectorized comparison helper — evaluates operands to columns, compares element-wise. */
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

  /** Row-by-row fallback for expressions not yet vectorized. */
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
