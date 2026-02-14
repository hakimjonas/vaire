package net.ghoula.strongbow

import scala.deriving.Mirror
import scala.quoted.{Expr as QExpr, *}

import net.ghoula.strongbow.Expr as SExpr
import net.ghoula.strongbow.types.ColumnIndex

object ExprMacro {

  /** Compile a field-access lambda to (Expr.Cell, ColumnType) at compile time. */
  inline def column[T, A](inline f: T => A)(using m: Mirror.ProductOf[T]): (SExpr[T, A], ColumnType) =
    ${ columnImpl[T, A, m.MirroredElemLabels, m.MirroredElemTypes]('f) }

  /** Compile a boolean lambda to an Expr predicate at compile time. */
  inline def predicate[T](inline f: T => Boolean)(using m: Mirror.ProductOf[T]): SExpr[T, Boolean] =
    ${ predicateImpl[T, m.MirroredElemLabels, m.MirroredElemTypes]('f) }

  // ---------------------------------------------------------------------------
  // Label extraction (reuses Schema pattern)
  // ---------------------------------------------------------------------------

  private def getLabels[Labels <: Tuple: Type](using q: Quotes): List[String] = {
    import q.reflect.*

    def extract[L <: Tuple: Type]: List[String] = Type.of[L] match {
      case '[EmptyTuple] => Nil
      case '[label *: rest] =>
        Type.of[label] match {
          case '[l] =>
            TypeRepr.of[l] match {
              case ConstantType(StringConstant(s)) => s :: extract[rest]
              case other =>
                report.errorAndAbort(s"Expected string literal label, found ${other.show}")
            }
        }
    }

    extract[Labels]
  }

  // ---------------------------------------------------------------------------
  // ColumnType inference at compile time
  // ---------------------------------------------------------------------------

  private def inferColumnType[A: Type](using q: Quotes): QExpr[ColumnType] = {
    import q.reflect.*
    Type.of[A] match {
      case '[Int] => '{ ColumnType.IntType }
      case '[Long] => '{ ColumnType.LongType }
      case '[Double] => '{ ColumnType.DoubleType }
      case '[String] => '{ ColumnType.StringType }
      case '[Boolean] => '{ ColumnType.BooleanType }
      case _ =>
        report.errorAndAbort(
          s"Unsupported field type ${Type.show[A]}. Only Int, Long, Double, String, Boolean are supported."
        )
    }
  }

  // ---------------------------------------------------------------------------
  // column macro implementation
  // ---------------------------------------------------------------------------

  private def columnImpl[T: Type, A: Type, Labels <: Tuple: Type, Elems <: Tuple: Type](
    f: QExpr[T => A]
  )(using q: Quotes): QExpr[(SExpr[T, A], ColumnType)] = {
    import q.reflect.*

    val labels = getLabels[Labels]
    val (fieldName, fieldIndex) = extractFieldAccess(f.asTerm, labels)
    val colType = inferColumnType[A]
    val nameExpr = QExpr(fieldName)
    val indexExpr = QExpr(fieldIndex)

    '{ (SExpr.Cell[T, A]($nameExpr, ColumnIndex($indexExpr)), $colType) }
  }

  // ---------------------------------------------------------------------------
  // predicate macro implementation
  // ---------------------------------------------------------------------------

  private def predicateImpl[T: Type, Labels <: Tuple: Type, Elems <: Tuple: Type](
    f: QExpr[T => Boolean]
  )(using q: Quotes): QExpr[SExpr[T, Boolean]] = {
    import q.reflect.*

    val labels = getLabels[Labels]
    val term = f.asTerm

    // Extract the lambda body
    val (paramName, body) = extractLambdaBody(term)
    compileBooleanExpr[T](body, paramName, labels)
  }

  // ---------------------------------------------------------------------------
  // Lambda AST helpers
  // ---------------------------------------------------------------------------

  private def extractFieldAccess(using
    q: Quotes
  )(
    term: q.reflect.Term,
    labels: List[String]
  ): (String, Int) = {
    import q.reflect.*

    // Drill through Inlined wrappers and Block wrappers
    val unwrapped = unwrapTerm(term)

    unwrapped match {
      // Lambda(params, Select(Ident(paramName), fieldName))
      case Lambda(_, body) =>
        val innerBody = unwrapTerm(body)
        innerBody match {
          case Select(Ident(_), fieldName) =>
            val idx = labels.indexOf(fieldName)
            if (idx < 0)
              report.errorAndAbort(
                s"Field '$fieldName' not found. Available fields: ${labels.mkString(", ")}"
              )
            (fieldName, idx)
          case other =>
            report.errorAndAbort(
              s"Expected simple field access (e.g. _.age), got: ${other.show}"
            )
        }
      case other =>
        report.errorAndAbort(
          s"Expected lambda expression, got: ${other.show}"
        )
    }
  }

  private def extractLambdaBody(using
    q: Quotes
  )(
    term: q.reflect.Term
  ): (String, q.reflect.Term) = {
    import q.reflect.*

    val unwrapped = unwrapTerm(term)
    unwrapped match {
      case Lambda(params, body) =>
        val paramName = params.head.name
        (paramName, body)
      case other =>
        report.errorAndAbort(s"Expected lambda expression, got: ${other.show}")
    }
  }

  private def unwrapTerm(using q: Quotes)(term: q.reflect.Term): q.reflect.Term = {
    import q.reflect.*
    term match {
      case Inlined(_, _, inner) => unwrapTerm(inner)
      case Block(Nil, inner) => unwrapTerm(inner)
      case other => other
    }
  }

  // ---------------------------------------------------------------------------
  // Boolean expression compiler
  // ---------------------------------------------------------------------------

  private def compileBooleanExpr[T: Type](using
    q: Quotes
  )(
    body: q.reflect.Term,
    paramName: String,
    labels: List[String]
  ): QExpr[SExpr[T, Boolean]] = {
    import q.reflect.*

    val unwrapped = unwrapTerm(body)

    unwrapped match {
      // &&
      case Apply(Select(left, "&&"), List(right)) =>
        val l = compileBooleanExpr[T](left, paramName, labels)
        val r = compileBooleanExpr[T](right, paramName, labels)
        '{ SExpr.And($l, $r) }

      // ||
      case Apply(Select(left, "||"), List(right)) =>
        val l = compileBooleanExpr[T](left, paramName, labels)
        val r = compileBooleanExpr[T](right, paramName, labels)
        '{ SExpr.Or($l, $r) }

      // unary_! (prefix not)
      case Apply(Select(inner, "unary_!"), Nil) =>
        val i = compileBooleanExpr[T](inner, paramName, labels)
        '{ SExpr.Not($i) }
      case Select(inner, "unary_!") =>
        val i = compileBooleanExpr[T](inner, paramName, labels)
        '{ SExpr.Not($i) }

      // Comparisons: >, >=, <, <=, ==, !=
      case Apply(Select(left, op), List(right)) if Set(">", ">=", "<", "<=", "==", "!=").contains(op) =>
        compileComparison[T](left, op, right, paramName, labels)

      // Boolean field access: _.isActive
      case sel @ Select(Ident(name), fieldName) if name == paramName =>
        val idx = labels.indexOf(fieldName)
        if (idx < 0)
          report.errorAndAbort(
            s"Field '$fieldName' not found. Available fields: ${labels.mkString(", ")}"
          )
        // Verify field type is Boolean using AST type info
        val fieldType = sel.tpe.widen
        if (!(fieldType =:= TypeRepr.of[Boolean]))
          report.errorAndAbort(
            s"Field '$fieldName' is ${fieldType.show}, not Boolean"
          )
        val nameExpr = QExpr(fieldName)
        val indexExpr = QExpr(idx)
        '{ SExpr.Cell[T, Boolean]($nameExpr, ColumnIndex($indexExpr)) }

      case other =>
        report.errorAndAbort(
          s"Unsupported boolean expression: ${other.show}"
        )
    }
  }

  // ---------------------------------------------------------------------------
  // Comparison compiler — dispatches to type-specific compilers
  // ---------------------------------------------------------------------------

  private def compileComparison[T: Type](using
    q: Quotes
  )(
    left: q.reflect.Term,
    op: String,
    right: q.reflect.Term,
    paramName: String,
    labels: List[String]
  ): QExpr[SExpr[T, Boolean]] = {
    import q.reflect.*

    // Determine operand type from left side
    val leftType = left.tpe.widen

    if (leftType =:= TypeRepr.of[Int]) {
      val l = compileIntExpr[T](left, paramName, labels)
      val r = compileIntExpr[T](right, paramName, labels)
      buildComparison[T, Int](l, op, r)
    } else if (leftType =:= TypeRepr.of[Long]) {
      val l = compileLongExpr[T](left, paramName, labels)
      val r = compileLongExpr[T](right, paramName, labels)
      buildComparison[T, Long](l, op, r)
    } else if (leftType =:= TypeRepr.of[Double]) {
      val l = compileDoubleExpr[T](left, paramName, labels)
      val r = compileDoubleExpr[T](right, paramName, labels)
      buildComparison[T, Double](l, op, r)
    } else if (leftType =:= TypeRepr.of[String]) {
      val l = compileStringExpr[T](left, paramName, labels)
      val r = compileStringExpr[T](right, paramName, labels)
      buildComparison[T, String](l, op, r)
    } else if (leftType =:= TypeRepr.of[Boolean]) {
      val l = compileBooleanValueExpr[T](left, paramName, labels)
      val r = compileBooleanValueExpr[T](right, paramName, labels)
      buildEqComparison[T, Boolean](l, op, r)
    } else {
      report.errorAndAbort(
        s"Unsupported comparison operand type: ${leftType.show}. Only Int, Long, Double, String, Boolean are supported."
      )
    }
  }

  private def buildComparison[T: Type, A: Type](using
    q: Quotes
  )(
    left: QExpr[SExpr[T, A]],
    op: String,
    right: QExpr[SExpr[T, A]]
  ): QExpr[SExpr[T, Boolean]] = {
    import q.reflect.*

    val ord = QExpr
      .summon[Ordering[A]]
      .getOrElse(
        report.errorAndAbort(s"No Ordering instance found for ${Type.show[A]}")
      )

    op match {
      case ">" => '{ SExpr.Gt($left, $right, $ord) }
      case ">=" => '{ SExpr.Gte($left, $right, $ord) }
      case "<" => '{ SExpr.Lt($left, $right, $ord) }
      case "<=" => '{ SExpr.Lte($left, $right, $ord) }
      case "==" => '{ SExpr.Eq($left, $right) }
      case "!=" => '{ SExpr.Neq($left, $right) }
      case _ => report.errorAndAbort(s"Unknown comparison operator: $op")
    }
  }

  private def buildEqComparison[T: Type, A: Type](using
    q: Quotes
  )(
    left: QExpr[SExpr[T, A]],
    op: String,
    right: QExpr[SExpr[T, A]]
  ): QExpr[SExpr[T, Boolean]] = {
    import q.reflect.*
    op match {
      case "==" => '{ SExpr.Eq($left, $right) }
      case "!=" => '{ SExpr.Neq($left, $right) }
      case _ =>
        report.errorAndAbort(s"Operator '$op' not supported for Boolean operands. Only == and != are allowed.")
    }
  }

  // ---------------------------------------------------------------------------
  // Type-specific expression compilers
  // ---------------------------------------------------------------------------

  private def compileIntExpr[T: Type](using
    q: Quotes
  )(
    term: q.reflect.Term,
    paramName: String,
    labels: List[String]
  ): QExpr[SExpr[T, Int]] = {
    import q.reflect.*

    val unwrapped = unwrapTerm(term)

    unwrapped match {
      // Field access: _.age
      case Select(Ident(name), fieldName) if name == paramName =>
        val idx = labels.indexOf(fieldName)
        if (idx < 0) report.errorAndAbort(s"Field '$fieldName' not found.")
        val nameExpr = QExpr(fieldName)
        val indexExpr = QExpr(idx)
        '{ SExpr.Cell[T, Int]($nameExpr, ColumnIndex($indexExpr)) }

      // Int literal
      case Literal(IntConstant(v)) =>
        val vExpr = QExpr(v)
        '{ SExpr.Const[T, Int]($vExpr) }

      // Arithmetic: +, -, *, /
      case Apply(Select(left, "+"), List(right)) =>
        val l = compileIntExpr[T](left, paramName, labels)
        val r = compileIntExpr[T](right, paramName, labels)
        '{ SExpr.Add($l, $r) }

      case Apply(Select(left, "-"), List(right)) =>
        val l = compileIntExpr[T](left, paramName, labels)
        val r = compileIntExpr[T](right, paramName, labels)
        '{ SExpr.Sub($l, $r) }

      case Apply(Select(left, "*"), List(right)) =>
        val l = compileIntExpr[T](left, paramName, labels)
        val r = compileIntExpr[T](right, paramName, labels)
        '{ SExpr.Mul($l, $r) }

      case Apply(Select(left, "/"), List(right)) =>
        val l = compileIntExpr[T](left, paramName, labels)
        val r = compileIntExpr[T](right, paramName, labels)
        '{ SExpr.Div($l, $r) }

      case other =>
        report.errorAndAbort(s"Unsupported Int expression: ${other.show}")
    }
  }

  private def compileLongExpr[T: Type](using
    q: Quotes
  )(
    term: q.reflect.Term,
    paramName: String,
    labels: List[String]
  ): QExpr[SExpr[T, Long]] = {
    import q.reflect.*

    val unwrapped = unwrapTerm(term)

    unwrapped match {
      case Select(Ident(name), fieldName) if name == paramName =>
        val idx = labels.indexOf(fieldName)
        if (idx < 0) report.errorAndAbort(s"Field '$fieldName' not found.")
        val nameExpr = QExpr(fieldName)
        val indexExpr = QExpr(idx)
        '{ SExpr.Cell[T, Long]($nameExpr, ColumnIndex($indexExpr)) }

      case Literal(LongConstant(v)) =>
        val vExpr = QExpr(v)
        '{ SExpr.Const[T, Long]($vExpr) }

      case other =>
        report.errorAndAbort(s"Unsupported Long expression: ${other.show}")
    }
  }

  private def compileDoubleExpr[T: Type](using
    q: Quotes
  )(
    term: q.reflect.Term,
    paramName: String,
    labels: List[String]
  ): QExpr[SExpr[T, Double]] = {
    import q.reflect.*

    val unwrapped = unwrapTerm(term)

    unwrapped match {
      case Select(Ident(name), fieldName) if name == paramName =>
        val idx = labels.indexOf(fieldName)
        if (idx < 0) report.errorAndAbort(s"Field '$fieldName' not found.")
        val nameExpr = QExpr(fieldName)
        val indexExpr = QExpr(idx)
        '{ SExpr.Cell[T, Double]($nameExpr, ColumnIndex($indexExpr)) }

      case Literal(DoubleConstant(v)) =>
        val vExpr = QExpr(v)
        '{ SExpr.Const[T, Double]($vExpr) }

      case other =>
        report.errorAndAbort(s"Unsupported Double expression: ${other.show}")
    }
  }

  private def compileStringExpr[T: Type](using
    q: Quotes
  )(
    term: q.reflect.Term,
    paramName: String,
    labels: List[String]
  ): QExpr[SExpr[T, String]] = {
    import q.reflect.*

    val unwrapped = unwrapTerm(term)

    unwrapped match {
      case Select(Ident(name), fieldName) if name == paramName =>
        val idx = labels.indexOf(fieldName)
        if (idx < 0) report.errorAndAbort(s"Field '$fieldName' not found.")
        val nameExpr = QExpr(fieldName)
        val indexExpr = QExpr(idx)
        '{ SExpr.Cell[T, String]($nameExpr, ColumnIndex($indexExpr)) }

      case Literal(StringConstant(v)) =>
        val vExpr = QExpr(v)
        '{ SExpr.Const[T, String]($vExpr) }

      case other =>
        report.errorAndAbort(s"Unsupported String expression: ${other.show}")
    }
  }

  private def compileBooleanValueExpr[T: Type](using
    q: Quotes
  )(
    term: q.reflect.Term,
    paramName: String,
    labels: List[String]
  ): QExpr[SExpr[T, Boolean]] = {
    import q.reflect.*

    val unwrapped = unwrapTerm(term)

    unwrapped match {
      case Select(Ident(name), fieldName) if name == paramName =>
        val idx = labels.indexOf(fieldName)
        if (idx < 0) report.errorAndAbort(s"Field '$fieldName' not found.")
        val nameExpr = QExpr(fieldName)
        val indexExpr = QExpr(idx)
        '{ SExpr.Cell[T, Boolean]($nameExpr, ColumnIndex($indexExpr)) }

      case Literal(BooleanConstant(v)) =>
        val vExpr = QExpr(v)
        '{ SExpr.Const[T, Boolean]($vExpr) }

      case other =>
        report.errorAndAbort(s"Unsupported Boolean expression: ${other.show}")
    }
  }
}

// ---------------------------------------------------------------------------
// Dataset convenience extensions using ExprMacro
// ---------------------------------------------------------------------------

extension [T](ds: Dataset[T]) {

  /** Filter using lambda compiled to Expr at compile time. */
  inline def where(inline f: T => Boolean)(using m: Mirror.ProductOf[T]): Dataset[T] =
    Dataset.Filter(ds, ExprMacro.predicate(f))

  /** Sort by field, compiled to SortByExpr. */
  inline def sortByColumn[K](inline f: T => K)(using m: Mirror.ProductOf[T], ord: Ordering[K]): Dataset[T] = {
    val (expr, colType) = ExprMacro.column(f)
    Dataset.SortByExpr(ds, expr, colType, ord)
  }

  /** Group by field, compiled to GroupByExpr. */
  inline def groupByColumn[K](inline f: T => K)(using m: Mirror.ProductOf[T]): Grouped[K, T] = {
    val (expr, colType) = ExprMacro.column(f)
    Grouped.GroupByExpr(ds, expr, colType)
  }
}
