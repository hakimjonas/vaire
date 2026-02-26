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

  /** Compile an arbitrary T => A expression, dispatching on the return type. */
  inline def compileExpr[T, A](inline f: T => A)(using
    m: Mirror.ProductOf[T]
  ): (SExpr[T, A], ColumnType) =
    ${ compileExprImpl[T, A, m.MirroredElemLabels, m.MirroredElemTypes]('f) }

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

  private def inferColumnType[A: Type](using q: Quotes): QExpr[ColumnType] = {
    import q.reflect.*
    Type.of[A] match {
      case '[Int] => '{ ColumnType.IntType }
      case '[Long] => '{ ColumnType.LongType }
      case '[Double] => '{ ColumnType.DoubleType }
      case '[String] => '{ ColumnType.StringType }
      case '[Boolean] => '{ ColumnType.BooleanType }
      case '[java.time.LocalDate] => '{ ColumnType.DateType }
      case _ =>
        report.errorAndAbort(
          s"Unsupported field type ${Type.show[A]}. Only Int, Long, Double, String, Boolean, LocalDate are supported."
        )
    }
  }

  private def columnImpl[T: Type, A: Type, Labels <: Tuple: Type, Elems <: Tuple: Type](
    f: QExpr[T => A]
  )(using q: Quotes): QExpr[(SExpr[T, A], ColumnType)] = {
    import q.reflect.*

    val labels = getLabels[Labels]
    val (fieldName, fieldIndex) = extractFieldAccess[T](f.asTerm, labels)
    val colType = inferColumnType[A]
    val nameExpr = QExpr(fieldName)
    val indexExpr = QExpr(fieldIndex)

    '{ (SExpr.Cell[T, A]($nameExpr, ColumnIndex($indexExpr)), $colType) }
  }

  private def predicateImpl[T: Type, Labels <: Tuple: Type, Elems <: Tuple: Type](
    f: QExpr[T => Boolean]
  )(using q: Quotes): QExpr[SExpr[T, Boolean]] = {
    import q.reflect.*

    val labels = getLabels[Labels]
    val term = f.asTerm

    val (paramName, body) = extractLambdaBody(term)
    compileBooleanExpr[T](body, paramName, labels)
  }

  private def compileExprImpl[T: Type, A: Type, Labels <: Tuple: Type, Elems <: Tuple: Type](
    f: QExpr[T => A]
  )(using q: Quotes): QExpr[(SExpr[T, A], ColumnType)] = {
    import q.reflect.*

    val labels = getLabels[Labels]
    val term = f.asTerm
    val (paramName, body) = extractLambdaBody(term)
    val bodyType = body.tpe.widen

    if (bodyType =:= TypeRepr.of[Int]) {
      val expr = compileIntExpr[T](body, paramName, labels)
      '{ ($expr.asInstanceOf[SExpr[T, A]], ColumnType.IntType) } // scalafix:ok DisableSyntax.asInstanceOf
    } else if (bodyType =:= TypeRepr.of[Long]) {
      val expr = compileLongExpr[T](body, paramName, labels)
      '{ ($expr.asInstanceOf[SExpr[T, A]], ColumnType.LongType) } // scalafix:ok DisableSyntax.asInstanceOf
    } else if (bodyType =:= TypeRepr.of[Double]) {
      val expr = compileDoubleExpr[T](body, paramName, labels)
      '{ ($expr.asInstanceOf[SExpr[T, A]], ColumnType.DoubleType) } // scalafix:ok DisableSyntax.asInstanceOf
    } else if (bodyType =:= TypeRepr.of[String]) {
      val expr = compileStringExpr[T](body, paramName, labels)
      '{ ($expr.asInstanceOf[SExpr[T, A]], ColumnType.StringType) } // scalafix:ok DisableSyntax.asInstanceOf
    } else if (bodyType =:= TypeRepr.of[Boolean]) {
      val expr = compileBooleanExpr[T](body, paramName, labels)
      '{ ($expr.asInstanceOf[SExpr[T, A]], ColumnType.BooleanType) } // scalafix:ok DisableSyntax.asInstanceOf
    } else {
      report.errorAndAbort(
        s"Unsupported expression return type: ${bodyType.show}. Only Int, Long, Double, String, Boolean are supported."
      )
    }
  }

  private def extractFieldAccess[T: Type](using
    q: Quotes
  )(
    term: q.reflect.Term,
    labels: List[String]
  ): (String, Int) = {
    import q.reflect.*

    val unwrapped = unwrapTerm(term)

    unwrapped match {
      case Lambda(_, body) =>
        val innerBody = unwrapTerm(body)
        innerBody match {
          case Select(Select(Ident(_), outerField), innerField) =>
            val outerIdx = labels.indexOf(outerField)
            if (outerIdx < 0)
              report.errorAndAbort(
                s"Field '$outerField' not found. Available fields: ${labels.mkString(", ")}"
              )
            val outerFieldSym = TypeRepr.of[T].typeSymbol.caseFields(outerIdx)
            val outerFieldType = TypeRepr.of[T].memberType(outerFieldSym)
            val innerFields = outerFieldType.typeSymbol.caseFields
            val innerIdx = innerFields.indexWhere(_.name == innerField)
            if (innerIdx < 0)
              report.errorAndAbort(
                s"Field '$innerField' not found in ${outerFieldType.show}."
              )
            val outerOffset = flatColumnOffset[T](outerIdx)
            val innerOffset =
              innerFields.take(innerIdx).map(f => columnCountOfTypeRepr(outerFieldType.memberType(f))).sum
            (s"$outerField.$innerField", outerOffset + innerOffset)

          case Select(Ident(_), fieldName) =>
            val idx = labels.indexOf(fieldName)
            if (idx < 0)
              report.errorAndAbort(
                s"Field '$fieldName' not found. Available fields: ${labels.mkString(", ")}"
              )
            (fieldName, flatColumnOffset[T](idx))

          case other =>
            report.errorAndAbort(
              s"Expected field access (e.g. _.age or _.address.city), got: ${other.show}"
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

  private def columnCountOfTypeRepr(using q: Quotes)(tpe: q.reflect.TypeRepr): Int = {
    import q.reflect.*
    val widened = tpe.widen
    if (
      widened =:= TypeRepr.of[Int] || widened =:= TypeRepr.of[Long] ||
      widened =:= TypeRepr.of[Double] || widened =:= TypeRepr.of[String] ||
      widened =:= TypeRepr.of[Boolean]
    ) {
      1
    } else {
      val sym = widened.typeSymbol
      val fields = sym.caseFields
      if (fields.isEmpty) {
        report.errorAndAbort(s"Type ${widened.show} is not a supported primitive or case class")
      }
      fields.map(f => columnCountOfTypeRepr(widened.memberType(f))).sum
    }
  }

  private def flatColumnOffset[T: Type](using q: Quotes)(fieldIdx: Int): Int = {
    import q.reflect.*
    val tRepr = TypeRepr.of[T]
    val fields = tRepr.typeSymbol.caseFields
    fields.take(fieldIdx).map(f => columnCountOfTypeRepr(tRepr.memberType(f))).sum
  }

  private def compileFieldAccess[T: Type, A: Type](using
    q: Quotes
  )(
    term: q.reflect.Term,
    paramName: String,
    labels: List[String]
  ): Option[QExpr[SExpr[T, A]]] = {
    import q.reflect.*

    term match {
      case Select(Select(Ident(name), outerField), innerField) if name == paramName =>
        val outerIdx = labels.indexOf(outerField)
        if (outerIdx < 0) None
        else {
          val outerFieldSym = TypeRepr.of[T].typeSymbol.caseFields(outerIdx)
          val outerFieldType = TypeRepr.of[T].memberType(outerFieldSym)
          val innerFields = outerFieldType.typeSymbol.caseFields
          val innerIdx = innerFields.indexWhere(_.name == innerField)
          if (innerIdx < 0) None
          else {
            val innerFieldType = outerFieldType.memberType(innerFields(innerIdx))
            if (!(innerFieldType.widen =:= TypeRepr.of[A])) None
            else {
              val outerOffset = flatColumnOffset[T](outerIdx)
              val innerOffset =
                innerFields.take(innerIdx).map(f => columnCountOfTypeRepr(outerFieldType.memberType(f))).sum
              val totalIdx = outerOffset + innerOffset
              val nameStr = s"$outerField.$innerField"
              val nameExpr = QExpr(nameStr)
              val indexExpr = QExpr(totalIdx)
              Some('{ SExpr.Cell[T, A]($nameExpr, ColumnIndex($indexExpr)) })
            }
          }
        }

      case Select(Ident(name), fieldName) if name == paramName =>
        val fieldIdx = labels.indexOf(fieldName)
        if (fieldIdx < 0) None
        else {
          val idx = flatColumnOffset[T](fieldIdx)
          val nameExpr = QExpr(fieldName)
          val indexExpr = QExpr(idx)
          Some('{ SExpr.Cell[T, A]($nameExpr, ColumnIndex($indexExpr)) })
        }

      case _ => None
    }
  }

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
      case Apply(Select(left, "&&"), List(right)) =>
        val l = compileBooleanExpr[T](left, paramName, labels)
        val r = compileBooleanExpr[T](right, paramName, labels)
        '{ SExpr.And($l, $r) }

      case Apply(Select(left, "||"), List(right)) =>
        val l = compileBooleanExpr[T](left, paramName, labels)
        val r = compileBooleanExpr[T](right, paramName, labels)
        '{ SExpr.Or($l, $r) }

      case Apply(Select(inner, "unary_!"), Nil) =>
        val i = compileBooleanExpr[T](inner, paramName, labels)
        '{ SExpr.Not($i) }
      case Select(inner, "unary_!") =>
        val i = compileBooleanExpr[T](inner, paramName, labels)
        '{ SExpr.Not($i) }

      case Apply(Select(left, op), List(right)) if Set(">", ">=", "<", "<=", "==", "!=").contains(op) =>
        compileComparison[T](left, op, right, paramName, labels)

      case other =>
        compileFieldAccess[T, Boolean](other, paramName, labels).getOrElse {
          report.errorAndAbort(
            s"Unsupported boolean expression: ${other.show}"
          )
        }
    }
  }

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

  private def compileIntExpr[T: Type](using
    q: Quotes
  )(
    term: q.reflect.Term,
    paramName: String,
    labels: List[String]
  ): QExpr[SExpr[T, Int]] = {
    import q.reflect.*

    val unwrapped = unwrapTerm(term)

    compileFieldAccess[T, Int](unwrapped, paramName, labels).getOrElse {
      unwrapped match {
        case Literal(IntConstant(v)) =>
          val vExpr = QExpr(v)
          '{ SExpr.Const[T, Int]($vExpr) }

        case Apply(Select(inner, "length"), Nil) if inner.tpe.widen =:= TypeRepr.of[String] =>
          val s = compileStringExpr[T](inner, paramName, labels)
          '{ SExpr.Length($s) }

        case Select(inner, "length") if inner.tpe.widen =:= TypeRepr.of[String] =>
          val s = compileStringExpr[T](inner, paramName, labels)
          '{ SExpr.Length($s) }

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

    compileFieldAccess[T, Long](unwrapped, paramName, labels).getOrElse {
      unwrapped match {
        case Literal(LongConstant(v)) =>
          val vExpr = QExpr(v)
          '{ SExpr.Const[T, Long]($vExpr) }

        case Apply(Select(left, "+"), List(right)) =>
          val l = compileLongExpr[T](left, paramName, labels)
          val r = compileLongExpr[T](right, paramName, labels)
          '{ SExpr.AddLong($l, $r) }

        case Apply(Select(left, "-"), List(right)) =>
          val l = compileLongExpr[T](left, paramName, labels)
          val r = compileLongExpr[T](right, paramName, labels)
          '{ SExpr.SubLong($l, $r) }

        case Apply(Select(left, "*"), List(right)) =>
          val l = compileLongExpr[T](left, paramName, labels)
          val r = compileLongExpr[T](right, paramName, labels)
          '{ SExpr.MulLong($l, $r) }

        case Apply(Select(left, "/"), List(right)) =>
          val l = compileLongExpr[T](left, paramName, labels)
          val r = compileLongExpr[T](right, paramName, labels)
          '{ SExpr.DivLong($l, $r) }

        case other =>
          report.errorAndAbort(s"Unsupported Long expression: ${other.show}")
      }
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

    compileFieldAccess[T, Double](unwrapped, paramName, labels).getOrElse {
      unwrapped match {
        case Literal(DoubleConstant(v)) =>
          val vExpr = QExpr(v)
          '{ SExpr.Const[T, Double]($vExpr) }

        case Apply(Select(left, "+"), List(right)) =>
          val l = compileDoubleExpr[T](left, paramName, labels)
          val r = compileDoubleExpr[T](right, paramName, labels)
          '{ SExpr.AddDouble($l, $r) }

        case Apply(Select(left, "-"), List(right)) =>
          val l = compileDoubleExpr[T](left, paramName, labels)
          val r = compileDoubleExpr[T](right, paramName, labels)
          '{ SExpr.SubDouble($l, $r) }

        case Apply(Select(left, "*"), List(right)) =>
          val l = compileDoubleExpr[T](left, paramName, labels)
          val r = compileDoubleExpr[T](right, paramName, labels)
          '{ SExpr.MulDouble($l, $r) }

        case Apply(Select(left, "/"), List(right)) =>
          val l = compileDoubleExpr[T](left, paramName, labels)
          val r = compileDoubleExpr[T](right, paramName, labels)
          '{ SExpr.DivDouble($l, $r) }

        case other =>
          report.errorAndAbort(s"Unsupported Double expression: ${other.show}")
      }
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

    compileFieldAccess[T, String](unwrapped, paramName, labels).getOrElse {
      unwrapped match {
        case Literal(StringConstant(v)) =>
          val vExpr = QExpr(v)
          '{ SExpr.Const[T, String]($vExpr) }

        case Apply(Select(left, "+"), List(right)) =>
          val l = compileStringExpr[T](left, paramName, labels)
          val r = compileStringExpr[T](right, paramName, labels)
          '{ SExpr.Concat($l, $r) }

        case other =>
          report.errorAndAbort(s"Unsupported String expression: ${other.show}")
      }
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

    compileFieldAccess[T, Boolean](unwrapped, paramName, labels).getOrElse {
      unwrapped match {
        case Literal(BooleanConstant(v)) =>
          val vExpr = QExpr(v)
          '{ SExpr.Const[T, Boolean]($vExpr) }

        case other =>
          report.errorAndAbort(s"Unsupported Boolean expression: ${other.show}")
      }
    }
  }
}

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
  inline def groupByColumn[K](inline f: T => K)(using
    m: Mirror.ProductOf[T],
    sk: Schema[K],
    st: Schema[T]
  ): Grouped[K, T] = {
    val (expr, colType) = ExprMacro.column(f)
    ds.groupByExpr(expr, colType)
  }
}
