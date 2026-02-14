package net.ghoula.strongbow

import scala.annotation.targetName

import net.ghoula.strongbow.types.ColumnIndex

/** Type-safe expression language for dataset operations.
  *
  * Expr[Row, A] is a GADT that carries type evidence through pattern matching. No casts
  * needed—types are refined correctly in each case.
  *
  * @tparam Row
  *   The row type this expression operates on
  * @tparam A
  *   The result type of evaluating this expression
  */
enum Expr[Row, +A] {
  case Cell[Row, A](name: String, index: ColumnIndex) extends Expr[Row, A]
  case Const[Row, A](value: A) extends Expr[Row, A]
  case Named[Row, A](expr: Expr[Row, A], name: String) extends Expr[Row, A]

  case Add[Row](left: Expr[Row, Int], right: Expr[Row, Int]) extends Expr[Row, Int]
  case Sub[Row](left: Expr[Row, Int], right: Expr[Row, Int]) extends Expr[Row, Int]
  case Mul[Row](left: Expr[Row, Int], right: Expr[Row, Int]) extends Expr[Row, Int]
  case Div[Row](left: Expr[Row, Int], right: Expr[Row, Int]) extends Expr[Row, Int]

  case AddLong[Row](left: Expr[Row, Long], right: Expr[Row, Long]) extends Expr[Row, Long]
  case SubLong[Row](left: Expr[Row, Long], right: Expr[Row, Long]) extends Expr[Row, Long]
  case MulLong[Row](left: Expr[Row, Long], right: Expr[Row, Long]) extends Expr[Row, Long]
  case DivLong[Row](left: Expr[Row, Long], right: Expr[Row, Long]) extends Expr[Row, Long]

  case AddDouble[Row](left: Expr[Row, Double], right: Expr[Row, Double]) extends Expr[Row, Double]
  case SubDouble[Row](left: Expr[Row, Double], right: Expr[Row, Double]) extends Expr[Row, Double]
  case MulDouble[Row](left: Expr[Row, Double], right: Expr[Row, Double]) extends Expr[Row, Double]
  case DivDouble[Row](left: Expr[Row, Double], right: Expr[Row, Double]) extends Expr[Row, Double]

  case Gt[Row, A](left: Expr[Row, A], right: Expr[Row, A], ordering: Ordering[A]) extends Expr[Row, Boolean]
  case Gte[Row, A](left: Expr[Row, A], right: Expr[Row, A], ordering: Ordering[A]) extends Expr[Row, Boolean]
  case Lt[Row, A](left: Expr[Row, A], right: Expr[Row, A], ordering: Ordering[A]) extends Expr[Row, Boolean]
  case Lte[Row, A](left: Expr[Row, A], right: Expr[Row, A], ordering: Ordering[A]) extends Expr[Row, Boolean]
  case Eq[Row, A](left: Expr[Row, A], right: Expr[Row, A]) extends Expr[Row, Boolean]
  case Neq[Row, A](left: Expr[Row, A], right: Expr[Row, A]) extends Expr[Row, Boolean]

  case And[Row](left: Expr[Row, Boolean], right: Expr[Row, Boolean]) extends Expr[Row, Boolean]
  case Or[Row](left: Expr[Row, Boolean], right: Expr[Row, Boolean]) extends Expr[Row, Boolean]
  case Not[Row](expr: Expr[Row, Boolean]) extends Expr[Row, Boolean]

  case When[Row, A](condition: Expr[Row, Boolean], thenExpr: Expr[Row, A], elseExpr: Expr[Row, A]) extends Expr[Row, A]

  case Concat[Row](left: Expr[Row, String], right: Expr[Row, String]) extends Expr[Row, String]
  case Length[Row](expr: Expr[Row, String]) extends Expr[Row, Int]

  case IsDefined[Row, A](expr: Expr[Row, Option[A]]) extends Expr[Row, Boolean]
  case GetOrElse[Row, A](expr: Expr[Row, Option[A]], default: A) extends Expr[Row, A]

  case Sum[Row](expr: Expr[Row, Int]) extends Expr[Row, Int]
  case Count[Row]() extends Expr[Row, Long]
  case Max[Row, A](expr: Expr[Row, A], ordering: Ordering[A]) extends Expr[Row, Option[A]]
  case Min[Row, A](expr: Expr[Row, A], ordering: Ordering[A]) extends Expr[Row, Option[A]]
  case Avg[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case CountDistinct[Row, A](expr: Expr[Row, A]) extends Expr[Row, Long]
  case CountIf[Row](predicate: Expr[Row, Boolean]) extends Expr[Row, Long]
  case StdDev[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case StdDevPop[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
}

object Expr {
  def cell[Row, A](name: String, index: ColumnIndex): Expr[Row, A] = {
    Cell(name, index)
  }

  def const[Row, A](value: A): Expr[Row, A] = {
    Const(value)
  }

  /** Create a literal expression from a value. */
  def lit[Row, A](value: A): Expr[Row, A] = {
    Const(value)
  }

  /** Create a conditional (when/then/else) expression. */
  def when[Row, A](
    condition: Expr[Row, Boolean],
    thenExpr: Expr[Row, A],
    elseExpr: Expr[Row, A]
  ): Expr[Row, A] = {
    When(condition, thenExpr, elseExpr)
  }

  /** Count distinct values. */
  def countDistinct[Row, A](expr: Expr[Row, A]): Expr[Row, Long] = {
    CountDistinct(expr)
  }

  /** Count rows where predicate is true. */
  def countIf[Row](predicate: Expr[Row, Boolean]): Expr[Row, Long] = {
    CountIf(predicate)
  }

  /** Sample standard deviation. */
  def stddev[Row](expr: Expr[Row, Double]): Expr[Row, Double] = {
    StdDev(expr)
  }

  /** Population standard deviation. */
  def stddevPop[Row](expr: Expr[Row, Double]): Expr[Row, Double] = {
    StdDevPop(expr)
  }

  extension [Row, A](left: Expr[Row, A]) {

    /** Rename this expression for output. */
    def as(name: String): Expr[Row, A] = {
      Named(left, name)
    }

    def ===(right: Expr[Row, A]): Expr[Row, Boolean] = {
      Eq(left, right)
    }

    def !==(right: Expr[Row, A]): Expr[Row, Boolean] = {
      Neq(left, right)
    }
  }

  extension [Row, A: Ordering](left: Expr[Row, A]) {
    def >(right: Expr[Row, A]): Expr[Row, Boolean] = {
      Gt(left, right, summon[Ordering[A]])
    }

    def >=(right: Expr[Row, A]): Expr[Row, Boolean] = {
      Gte(left, right, summon[Ordering[A]])
    }

    def <(right: Expr[Row, A]): Expr[Row, Boolean] = {
      Lt(left, right, summon[Ordering[A]])
    }

    def <=(right: Expr[Row, A]): Expr[Row, Boolean] = {
      Lte(left, right, summon[Ordering[A]])
    }
  }

  extension [Row](left: Expr[Row, Int]) {
    inline def +(right: Expr[Row, Int]): Expr[Row, Int] = Add(left, right)
    inline def -(right: Expr[Row, Int]): Expr[Row, Int] = Sub(left, right)
    inline def *(right: Expr[Row, Int]): Expr[Row, Int] = Mul(left, right)
    inline def /(right: Expr[Row, Int]): Expr[Row, Int] = Div(left, right)
  }

  extension [Row](left: Expr[Row, Long]) {
    @targetName("addLong")
    inline def +(right: Expr[Row, Long]): Expr[Row, Long] = AddLong(left, right)
    @targetName("subLong")
    inline def -(right: Expr[Row, Long]): Expr[Row, Long] = SubLong(left, right)
    @targetName("mulLong")
    inline def *(right: Expr[Row, Long]): Expr[Row, Long] = MulLong(left, right)
    @targetName("divLong")
    inline def /(right: Expr[Row, Long]): Expr[Row, Long] = DivLong(left, right)
  }

  extension [Row](left: Expr[Row, Double]) {
    @targetName("addDouble")
    inline def +(right: Expr[Row, Double]): Expr[Row, Double] = AddDouble(left, right)
    @targetName("subDouble")
    inline def -(right: Expr[Row, Double]): Expr[Row, Double] = SubDouble(left, right)
    @targetName("mulDouble")
    inline def *(right: Expr[Row, Double]): Expr[Row, Double] = MulDouble(left, right)
    @targetName("divDouble")
    inline def /(right: Expr[Row, Double]): Expr[Row, Double] = DivDouble(left, right)
  }

  extension [Row](left: Expr[Row, Boolean]) {
    inline def &&(right: Expr[Row, Boolean]): Expr[Row, Boolean] = And(left, right)
    inline def ||(right: Expr[Row, Boolean]): Expr[Row, Boolean] = Or(left, right)
    inline def unary_! : Expr[Row, Boolean] = Not(left)
  }

  extension [Row](left: Expr[Row, String]) {
    inline def ++(right: Expr[Row, String]): Expr[Row, String] = Concat(left, right)
    inline def length: Expr[Row, Int] = Length(left)
  }

  extension [Row, A](e: Expr[Row, Option[A]]) {
    inline def isDefined: Expr[Row, Boolean] = IsDefined(e)
    inline def getOrElse(default: A): Expr[Row, A] = GetOrElse(e, default)
  }

  /** Infer the output ColumnType of an expression, if statically known. */
  extension [Row, A](expr: Expr[Row, A]) {
    def outputType: Option[ColumnType] = (expr: @unchecked) match {
      case _: Expr.Cell[_, _] => None
      case _: Expr.Const[_, _] => None
      case n: Expr.Named[_, _] => n.expr.outputType
      case _: Expr.Add[_] | _: Expr.Sub[_] | _: Expr.Mul[_] | _: Expr.Div[_] | _: Expr.Sum[_] =>
        Some(ColumnType.IntType)
      case _: Expr.AddLong[_] | _: Expr.SubLong[_] | _: Expr.MulLong[_] | _: Expr.DivLong[_] =>
        Some(ColumnType.LongType)
      case _: Expr.AddDouble[_] | _: Expr.SubDouble[_] | _: Expr.MulDouble[_] | _: Expr.DivDouble[_] =>
        Some(ColumnType.DoubleType)
      case _: Expr.Gt[_, _] | _: Expr.Gte[_, _] | _: Expr.Lt[_, _] | _: Expr.Lte[_, _] | _: Expr.Eq[_, _] |
          _: Expr.Neq[_, _] | _: Expr.And[_] | _: Expr.Or[_] | _: Expr.Not[_] | _: Expr.IsDefined[_, _] =>
        Some(ColumnType.BooleanType)
      case w: Expr.When[_, _] => w.thenExpr.outputType
      case _: Expr.Concat[_] => Some(ColumnType.StringType)
      case _: Expr.Length[_] => Some(ColumnType.IntType)
      case g: Expr.GetOrElse[_, _] => g.expr.outputType
      case _: Expr.Count[_] | _: Expr.CountDistinct[_, _] | _: Expr.CountIf[_] =>
        Some(ColumnType.LongType)
      case _: Expr.Avg[_] | _: Expr.StdDev[_] | _: Expr.StdDevPop[_] =>
        Some(ColumnType.DoubleType)
      case _: Expr.Max[_, _] | _: Expr.Min[_, _] => None
    }
  }
}
