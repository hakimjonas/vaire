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
  case Like[Row](expr: Expr[Row, String], pattern: String) extends Expr[Row, Boolean]

  case Lower[Row](expr: Expr[Row, String]) extends Expr[Row, String]
  case Upper[Row](expr: Expr[Row, String]) extends Expr[Row, String]
  case Trim[Row](expr: Expr[Row, String]) extends Expr[Row, String]
  case LTrim[Row](expr: Expr[Row, String]) extends Expr[Row, String]
  case RTrim[Row](expr: Expr[Row, String]) extends Expr[Row, String]
  case Substring[Row](expr: Expr[Row, String], pos: Int, len: Int) extends Expr[Row, String]
  case StringReplace[Row](expr: Expr[Row, String], search: String, replacement: String) extends Expr[Row, String]
  case RegexpReplace[Row](expr: Expr[Row, String], pattern: String, replacement: String) extends Expr[Row, String]
  case RegexpExtract[Row](expr: Expr[Row, String], pattern: String, groupIdx: Int) extends Expr[Row, String]
  case StringSplit[Row](expr: Expr[Row, String], delimiter: String) extends Expr[Row, Seq[String]]
  case StartsWith[Row](expr: Expr[Row, String], prefix: Expr[Row, String]) extends Expr[Row, Boolean]
  case EndsWith[Row](expr: Expr[Row, String], suffix: Expr[Row, String]) extends Expr[Row, Boolean]
  case StringContains[Row](expr: Expr[Row, String], substr: Expr[Row, String]) extends Expr[Row, Boolean]
  case ConcatWs[Row](separator: String, exprs: Vector[Expr[Row, String]]) extends Expr[Row, String]

  case Coalesce[Row, A](exprs: Vector[Expr[Row, A]]) extends Expr[Row, A]
  case IsNull[Row, A](expr: Expr[Row, A]) extends Expr[Row, Boolean]
  case IsNotNull[Row, A](expr: Expr[Row, A]) extends Expr[Row, Boolean]
  case In[Row, A](expr: Expr[Row, A], values: Vector[A]) extends Expr[Row, Boolean]
  case Between[Row, A](expr: Expr[Row, A], lower: Expr[Row, A], upper: Expr[Row, A], ordering: Ordering[A])
      extends Expr[Row, Boolean]

  case Mod[Row](left: Expr[Row, Int], right: Expr[Row, Int]) extends Expr[Row, Int]
  case ModLong[Row](left: Expr[Row, Long], right: Expr[Row, Long]) extends Expr[Row, Long]
  case Abs[Row](expr: Expr[Row, Int]) extends Expr[Row, Int]
  case AbsLong[Row](expr: Expr[Row, Long]) extends Expr[Row, Long]
  case AbsDouble[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Negate[Row](expr: Expr[Row, Int]) extends Expr[Row, Int]
  case NegateLong[Row](expr: Expr[Row, Long]) extends Expr[Row, Long]
  case NegateDouble[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Round[Row](expr: Expr[Row, Double], scale: Int) extends Expr[Row, Double]
  case Floor[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Ceil[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  case CastToLong[Row](expr: Expr[Row, Int]) extends Expr[Row, Long]
  case CastToDouble[Row](expr: Expr[Row, Int]) extends Expr[Row, Double]
  case CastLongToDouble[Row](expr: Expr[Row, Long]) extends Expr[Row, Double]
  case CastToString[Row, A](expr: Expr[Row, A]) extends Expr[Row, String]

  case IsDefined[Row, A](expr: Expr[Row, Option[A]]) extends Expr[Row, Boolean]
  case GetOrElse[Row, A](expr: Expr[Row, Option[A]], default: A) extends Expr[Row, A]

  case Sum[Row](expr: Expr[Row, Int]) extends Expr[Row, Int]
  case SumDouble[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case SumLong[Row](expr: Expr[Row, Long]) extends Expr[Row, Long]
  case Count[Row]() extends Expr[Row, Long]
  case Max[Row, A](expr: Expr[Row, A], ordering: Ordering[A]) extends Expr[Row, Option[A]]
  case Min[Row, A](expr: Expr[Row, A], ordering: Ordering[A]) extends Expr[Row, Option[A]]
  case Avg[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case CountDistinct[Row, A](expr: Expr[Row, A]) extends Expr[Row, Long]
  case CountIf[Row](predicate: Expr[Row, Boolean]) extends Expr[Row, Long]
  case StdDev[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case StdDevPop[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  case First[Row, A](expr: Expr[Row, A]) extends Expr[Row, Option[A]]
  case Collect[Row, A](expr: Expr[Row, A]) extends Expr[Row, Seq[A]]
  case Option2Iterable[Row, A](expr: Expr[Row, Option[A]]) extends Expr[Row, Iterable[A]]

  // Phase 3: Advanced aggregations
  case PercentileApprox[Row](expr: Expr[Row, Double], percentile: Double, accuracy: Int) extends Expr[Row, Double]
  case MaxBy[Row, A, K](valueExpr: Expr[Row, A], orderExpr: Expr[Row, K], ordering: Ordering[K])
      extends Expr[Row, Option[A]]
  case MinBy[Row, A, K](valueExpr: Expr[Row, A], orderExpr: Expr[Row, K], ordering: Ordering[K])
      extends Expr[Row, Option[A]]
  case MaxN[Row, A](expr: Expr[Row, A], n: Int, ordering: Ordering[A]) extends Expr[Row, Seq[A]]
  case MinN[Row, A](expr: Expr[Row, A], n: Int, ordering: Ordering[A]) extends Expr[Row, Seq[A]]
  case MaxByN[Row, A, K](valueExpr: Expr[Row, A], orderExpr: Expr[Row, K], n: Int, ordering: Ordering[K])
      extends Expr[Row, Seq[A]]
  case MinByN[Row, A, K](valueExpr: Expr[Row, A], orderExpr: Expr[Row, K], n: Int, ordering: Ordering[K])
      extends Expr[Row, Seq[A]]

  // Phase 3: Date expressions
  case DateAddDays[Row](date: Expr[Row, java.time.LocalDate], days: Expr[Row, Int])
      extends Expr[Row, java.time.LocalDate]
  case DateSubDays[Row](date: Expr[Row, java.time.LocalDate], days: Expr[Row, Int])
      extends Expr[Row, java.time.LocalDate]
  case DateAddMonths[Row](date: Expr[Row, java.time.LocalDate], months: Expr[Row, Int])
      extends Expr[Row, java.time.LocalDate]
  case DateDiff[Row](left: Expr[Row, java.time.LocalDate], right: Expr[Row, java.time.LocalDate]) extends Expr[Row, Int]
  case ExtractYear[Row](date: Expr[Row, java.time.LocalDate]) extends Expr[Row, Int]
  case ExtractMonth[Row](date: Expr[Row, java.time.LocalDate]) extends Expr[Row, Int]
  case ExtractDay[Row](date: Expr[Row, java.time.LocalDate]) extends Expr[Row, Int]

  // Phase 5: Window function expressions
  case RowNumber[Row]() extends Expr[Row, Int]
  case Rank[Row]() extends Expr[Row, Int]
  case DenseRank[Row]() extends Expr[Row, Int]
  case Lag[Row, A](expr: Expr[Row, A], offset: Int, default: Option[A]) extends Expr[Row, A]
  case Lead[Row, A](expr: Expr[Row, A], offset: Int, default: Option[A]) extends Expr[Row, A]

  case Sqrt[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Pow[Row](base: Expr[Row, Double], exponent: Expr[Row, Double]) extends Expr[Row, Double]
  case Log[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Log10[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Log2[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Exp[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Sin[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Cos[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Tan[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Asin[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Acos[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Atan[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Atan2[Row](y: Expr[Row, Double], x: Expr[Row, Double]) extends Expr[Row, Double]
  case Signum[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Rand[Row](seed: Long) extends Expr[Row, Double]

  case DayOfWeek[Row](date: Expr[Row, java.time.LocalDate]) extends Expr[Row, Int]
  case DayOfYear[Row](date: Expr[Row, java.time.LocalDate]) extends Expr[Row, Int]
  case WeekOfYear[Row](date: Expr[Row, java.time.LocalDate]) extends Expr[Row, Int]
  case Quarter[Row](date: Expr[Row, java.time.LocalDate]) extends Expr[Row, Int]
  case LastDay[Row](date: Expr[Row, java.time.LocalDate]) extends Expr[Row, java.time.LocalDate]
  case NextDay[Row](date: Expr[Row, java.time.LocalDate], dayOfWeek: String) extends Expr[Row, java.time.LocalDate]
  case MonthsBetween[Row](end: Expr[Row, java.time.LocalDate], start: Expr[Row, java.time.LocalDate])
      extends Expr[Row, Double]
  case DateTrunc[Row](unit: String, date: Expr[Row, java.time.LocalDate]) extends Expr[Row, java.time.LocalDate]
  case DateFormat[Row](date: Expr[Row, java.time.LocalDate], format: String) extends Expr[Row, String]
  case MakeDate[Row](year: Expr[Row, Int], month: Expr[Row, Int], day: Expr[Row, Int])
      extends Expr[Row, java.time.LocalDate]

  case Variance[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case VariancePop[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case ApproxCountDistinct[Row, A](expr: Expr[Row, A]) extends Expr[Row, Long]
  case CollectSet[Row, A](expr: Expr[Row, A]) extends Expr[Row, Seq[A]]
  case ExprLast[Row, A](expr: Expr[Row, A]) extends Expr[Row, Option[A]]
  case AnyValue[Row, A](expr: Expr[Row, A]) extends Expr[Row, Option[A]]
  case BoolAnd[Row](expr: Expr[Row, Boolean]) extends Expr[Row, Boolean]
  case BoolOr[Row](expr: Expr[Row, Boolean]) extends Expr[Row, Boolean]
  case Corr[Row](left: Expr[Row, Double], right: Expr[Row, Double]) extends Expr[Row, Double]
  case CovarSamp[Row](left: Expr[Row, Double], right: Expr[Row, Double]) extends Expr[Row, Double]
  case CovarPop[Row](left: Expr[Row, Double], right: Expr[Row, Double]) extends Expr[Row, Double]
  case Median[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Mode[Row, A](expr: Expr[Row, A]) extends Expr[Row, Option[A]]

  case NTile[Row](n: Int) extends Expr[Row, Int]
  case CumeDist[Row]() extends Expr[Row, Double]
  case PercentRank[Row]() extends Expr[Row, Double]
  case NthValue[Row, A](expr: Expr[Row, A], n: Int) extends Expr[Row, A]
  case FirstValue[Row, A](expr: Expr[Row, A]) extends Expr[Row, A]
  case LastValue[Row, A](expr: Expr[Row, A]) extends Expr[Row, A]
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

  /** Sum of Double-valued expressions. */
  def sumDouble[Row](expr: Expr[Row, Double]): Expr[Row, Double] = {
    SumDouble(expr)
  }

  /** Sum of Long-valued expressions. */
  def sumLong[Row](expr: Expr[Row, Long]): Expr[Row, Long] = {
    SumLong(expr)
  }

  /** Approximate percentile of Double-valued expressions. */
  def percentileApprox[Row](expr: Expr[Row, Double], percentile: Double, accuracy: Int = 10000): Expr[Row, Double] = {
    PercentileApprox(expr, percentile, accuracy)
  }

  /** Approximate median (percentile = 0.5). */
  def medianApprox[Row](expr: Expr[Row, Double], accuracy: Int = 10000): Expr[Row, Double] = {
    PercentileApprox(expr, 0.5, accuracy)
  }

  /** Value of valueExpr at the row where orderExpr is maximum. */
  def maxBy[Row, A, K: Ordering](valueExpr: Expr[Row, A], orderExpr: Expr[Row, K]): Expr[Row, Option[A]] = {
    MaxBy(valueExpr, orderExpr, summon[Ordering[K]])
  }

  /** Value of valueExpr at the row where orderExpr is minimum. */
  def minBy[Row, A, K: Ordering](valueExpr: Expr[Row, A], orderExpr: Expr[Row, K]): Expr[Row, Option[A]] = {
    MinBy(valueExpr, orderExpr, summon[Ordering[K]])
  }

  /** Top N values by ordering. */
  def maxN[Row, A: Ordering](expr: Expr[Row, A], n: Int): Expr[Row, Seq[A]] = {
    MaxN(expr, n, summon[Ordering[A]])
  }

  /** Bottom N values by ordering. */
  def minN[Row, A: Ordering](expr: Expr[Row, A], n: Int): Expr[Row, Seq[A]] = {
    MinN(expr, n, summon[Ordering[A]])
  }

  /** Top N values of valueExpr ordered by orderExpr. */
  def maxByN[Row, A, K: Ordering](valueExpr: Expr[Row, A], orderExpr: Expr[Row, K], n: Int): Expr[Row, Seq[A]] = {
    MaxByN(valueExpr, orderExpr, n, summon[Ordering[K]])
  }

  /** Bottom N values of valueExpr ordered by orderExpr. */
  def minByN[Row, A, K: Ordering](valueExpr: Expr[Row, A], orderExpr: Expr[Row, K], n: Int): Expr[Row, Seq[A]] = {
    MinByN(valueExpr, orderExpr, n, summon[Ordering[K]])
  }

  /** Random double with a fixed seed for reproducibility. */
  def rand[Row](seed: Long): Expr[Row, Double] = {
    Rand(seed)
  }

  /** Construct a date from year, month, day integer expressions. */
  def makeDate[Row](
    year: Expr[Row, Int],
    month: Expr[Row, Int],
    day: Expr[Row, Int]
  ): Expr[Row, java.time.LocalDate] = {
    MakeDate(year, month, day)
  }

  /** Sample variance. */
  def variance[Row](expr: Expr[Row, Double]): Expr[Row, Double] = {
    Variance(expr)
  }

  /** Population variance. */
  def variancePop[Row](expr: Expr[Row, Double]): Expr[Row, Double] = {
    VariancePop(expr)
  }

  /** Approximate count of distinct values. */
  def approxCountDistinct[Row, A](expr: Expr[Row, A]): Expr[Row, Long] = {
    ApproxCountDistinct(expr)
  }

  /** Collect distinct values into a set. */
  def collectSet[Row, A](expr: Expr[Row, A]): Expr[Row, Seq[A]] = {
    CollectSet(expr)
  }

  /** Last value in a group. */
  def last[Row, A](expr: Expr[Row, A]): Expr[Row, Option[A]] = {
    ExprLast(expr)
  }

  /** Any arbitrary value from a group. */
  def anyValue[Row, A](expr: Expr[Row, A]): Expr[Row, Option[A]] = {
    AnyValue(expr)
  }

  /** True if all values are true. */
  def boolAnd[Row](expr: Expr[Row, Boolean]): Expr[Row, Boolean] = {
    BoolAnd(expr)
  }

  /** True if any value is true. */
  def boolOr[Row](expr: Expr[Row, Boolean]): Expr[Row, Boolean] = {
    BoolOr(expr)
  }

  /** Pearson correlation coefficient. */
  def corr[Row](left: Expr[Row, Double], right: Expr[Row, Double]): Expr[Row, Double] = {
    Corr(left, right)
  }

  /** Sample covariance. */
  def covarSamp[Row](left: Expr[Row, Double], right: Expr[Row, Double]): Expr[Row, Double] = {
    CovarSamp(left, right)
  }

  /** Population covariance. */
  def covarPop[Row](left: Expr[Row, Double], right: Expr[Row, Double]): Expr[Row, Double] = {
    CovarPop(left, right)
  }

  /** Median value. */
  def median[Row](expr: Expr[Row, Double]): Expr[Row, Double] = {
    Median(expr)
  }

  /** Most frequent value. */
  def mode[Row, A](expr: Expr[Row, A]): Expr[Row, Option[A]] = {
    Mode(expr)
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
    @targetName("modInt")
    inline def %(right: Expr[Row, Int]): Expr[Row, Int] = Mod(left, right)
    inline def abs: Expr[Row, Int] = Abs(left)
    inline def negate: Expr[Row, Int] = Negate(left)
    inline def castToLong: Expr[Row, Long] = CastToLong(left)
    @targetName("castIntToDouble")
    inline def castToDouble: Expr[Row, Double] = CastToDouble(left)
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
    @targetName("modLong")
    inline def %(right: Expr[Row, Long]): Expr[Row, Long] = ModLong(left, right)
    @targetName("absLong")
    inline def abs: Expr[Row, Long] = AbsLong(left)
    @targetName("negateLong")
    inline def negate: Expr[Row, Long] = NegateLong(left)
    inline def castToDouble: Expr[Row, Double] = CastLongToDouble(left)
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
    @targetName("absDouble")
    inline def abs: Expr[Row, Double] = AbsDouble(left)
    @targetName("negateDouble")
    inline def negate: Expr[Row, Double] = NegateDouble(left)
    inline def round(scale: Int): Expr[Row, Double] = Round(left, scale)
    inline def floor: Expr[Row, Double] = Floor(left)
    inline def ceil: Expr[Row, Double] = Ceil(left)
    inline def sqrt: Expr[Row, Double] = Sqrt(left)
    inline def pow(exponent: Expr[Row, Double]): Expr[Row, Double] = Pow(left, exponent)
    inline def log: Expr[Row, Double] = Log(left)
    inline def log10: Expr[Row, Double] = Log10(left)
    inline def log2: Expr[Row, Double] = Log2(left)
    inline def exp: Expr[Row, Double] = Exp(left)
    inline def sin: Expr[Row, Double] = Sin(left)
    inline def cos: Expr[Row, Double] = Cos(left)
    inline def tan: Expr[Row, Double] = Tan(left)
    inline def asin: Expr[Row, Double] = Asin(left)
    inline def acos: Expr[Row, Double] = Acos(left)
    inline def atan: Expr[Row, Double] = Atan(left)
    inline def atan2(x: Expr[Row, Double]): Expr[Row, Double] = Atan2(left, x)
    inline def signum: Expr[Row, Double] = Signum(left)
  }

  extension [Row](left: Expr[Row, Boolean]) {
    inline def &&(right: Expr[Row, Boolean]): Expr[Row, Boolean] = And(left, right)
    inline def ||(right: Expr[Row, Boolean]): Expr[Row, Boolean] = Or(left, right)
    inline def unary_! : Expr[Row, Boolean] = Not(left)
  }

  extension [Row](left: Expr[Row, String]) {
    inline def ++(right: Expr[Row, String]): Expr[Row, String] = Concat(left, right)
    inline def length: Expr[Row, Int] = Length(left)
    inline def like(pattern: String): Expr[Row, Boolean] = Like(left, pattern)

    inline def lower: Expr[Row, String] = Lower(left)
    inline def upper: Expr[Row, String] = Upper(left)
    inline def trim: Expr[Row, String] = Trim(left)
    inline def ltrim: Expr[Row, String] = LTrim(left)
    inline def rtrim: Expr[Row, String] = RTrim(left)
    inline def substring(pos: Int, len: Int): Expr[Row, String] = Substring(left, pos, len)
    inline def replace(search: String, replacement: String): Expr[Row, String] =
      StringReplace(left, search, replacement)
    inline def regexpReplace(pattern: String, replacement: String): Expr[Row, String] =
      RegexpReplace(left, pattern, replacement)
    inline def regexpExtract(pattern: String, groupIdx: Int): Expr[Row, String] = RegexpExtract(left, pattern, groupIdx)
    inline def split(delimiter: String): Expr[Row, Seq[String]] = StringSplit(left, delimiter)
    inline def startsWith(prefix: Expr[Row, String]): Expr[Row, Boolean] = StartsWith(left, prefix)
    inline def endsWith(suffix: Expr[Row, String]): Expr[Row, Boolean] = EndsWith(left, suffix)
    inline def contains(substr: Expr[Row, String]): Expr[Row, Boolean] = StringContains(left, substr)
  }

  extension [Row, A](e: Expr[Row, Option[A]]) {
    inline def isDefined: Expr[Row, Boolean] = IsDefined(e)
    inline def getOrElse(default: A): Expr[Row, A] = GetOrElse(e, default)
  }

  /** Return the first non-null value from a list of expressions. */
  def coalesce[Row, A](exprs: Expr[Row, A]*): Expr[Row, A] = {
    Coalesce(exprs.toVector)
  }

  /** Concatenate strings with a separator, skipping nulls. */
  def concatWs[Row](separator: String, exprs: Expr[Row, String]*): Expr[Row, String] = {
    ConcatWs(separator, exprs.toVector)
  }

  extension [Row, A](left: Expr[Row, A]) {
    inline def isNull: Expr[Row, Boolean] = IsNull(left)
    inline def isNotNull: Expr[Row, Boolean] = IsNotNull(left)
    inline def in(values: Vector[A]): Expr[Row, Boolean] = In(left, values)
    inline def castToString: Expr[Row, String] = CastToString(left)
  }

  extension [Row, A: Ordering](left: Expr[Row, A]) {
    @targetName("betweenOrdered")
    inline def between(lower: Expr[Row, A], upper: Expr[Row, A]): Expr[Row, Boolean] =
      Between(left, lower, upper, summon[Ordering[A]])
  }

  extension [Row](d: Expr[Row, java.time.LocalDate]) {
    inline def addDays(days: Expr[Row, Int]): Expr[Row, java.time.LocalDate] = DateAddDays(d, days)
    inline def subDays(days: Expr[Row, Int]): Expr[Row, java.time.LocalDate] = DateSubDays(d, days)
    inline def addMonths(months: Expr[Row, Int]): Expr[Row, java.time.LocalDate] = DateAddMonths(d, months)
    inline def dateDiff(other: Expr[Row, java.time.LocalDate]): Expr[Row, Int] = DateDiff(d, other)
    inline def year: Expr[Row, Int] = ExtractYear(d)
    inline def month: Expr[Row, Int] = ExtractMonth(d)
    inline def day: Expr[Row, Int] = ExtractDay(d)
    inline def dayOfWeek: Expr[Row, Int] = DayOfWeek(d)
    inline def dayOfYear: Expr[Row, Int] = DayOfYear(d)
    inline def weekOfYear: Expr[Row, Int] = WeekOfYear(d)
    inline def quarter: Expr[Row, Int] = Quarter(d)
    inline def lastDay: Expr[Row, java.time.LocalDate] = LastDay(d)
    inline def nextDay(dayOfWeek: String): Expr[Row, java.time.LocalDate] = NextDay(d, dayOfWeek)
    inline def monthsBetween(other: Expr[Row, java.time.LocalDate]): Expr[Row, Double] = MonthsBetween(d, other)
    inline def dateTrunc(unit: String): Expr[Row, java.time.LocalDate] = DateTrunc(unit, d)
    inline def dateFormat(format: String): Expr[Row, String] = DateFormat(d, format)
  }

  /** Infer the output ColumnType of an expression, if statically known. */
  extension [Row, A](expr: Expr[Row, A]) {
    def outputType: Option[ColumnType] = (expr: @unchecked) match {
      case _: Expr.Cell[_, _] => None
      case _: Expr.Const[_, _] => None
      case n: Expr.Named[_, _] => n.expr.outputType
      case _: Expr.Add[_] | _: Expr.Sub[_] | _: Expr.Mul[_] | _: Expr.Div[_] | _: Expr.Sum[_] | _: Expr.Mod[_] |
          _: Expr.Abs[_] | _: Expr.Negate[_] =>
        Some(ColumnType.IntType)
      case _: Expr.AddLong[_] | _: Expr.SubLong[_] | _: Expr.MulLong[_] | _: Expr.DivLong[_] | _: Expr.SumLong[_] |
          _: Expr.ModLong[_] | _: Expr.AbsLong[_] | _: Expr.NegateLong[_] | _: Expr.CastToLong[_] =>
        Some(ColumnType.LongType)
      case _: Expr.AddDouble[_] | _: Expr.SubDouble[_] | _: Expr.MulDouble[_] | _: Expr.DivDouble[_] |
          _: Expr.SumDouble[_] | _: Expr.AbsDouble[_] | _: Expr.NegateDouble[_] | _: Expr.Round[_] | _: Expr.Floor[_] |
          _: Expr.Ceil[_] | _: Expr.CastToDouble[_] | _: Expr.CastLongToDouble[_] =>
        Some(ColumnType.DoubleType)
      case _: Expr.Gt[_, _] | _: Expr.Gte[_, _] | _: Expr.Lt[_, _] | _: Expr.Lte[_, _] | _: Expr.Eq[_, _] |
          _: Expr.Neq[_, _] | _: Expr.And[_] | _: Expr.Or[_] | _: Expr.Not[_] | _: Expr.IsDefined[_, _] |
          _: Expr.Like[_] | _: Expr.StartsWith[_] | _: Expr.EndsWith[_] | _: Expr.StringContains[_] |
          _: Expr.IsNull[_, _] | _: Expr.IsNotNull[_, _] | _: Expr.In[_, _] | _: Expr.Between[_, _] =>
        Some(ColumnType.BooleanType)
      case w: Expr.When[_, _] => w.thenExpr.outputType
      case _: Expr.Concat[_] | _: Expr.Lower[_] | _: Expr.Upper[_] | _: Expr.Trim[_] | _: Expr.LTrim[_] |
          _: Expr.RTrim[_] | _: Expr.Substring[_] | _: Expr.StringReplace[_] | _: Expr.RegexpReplace[_] |
          _: Expr.RegexpExtract[_] | _: Expr.ConcatWs[_] | _: Expr.CastToString[_, _] =>
        Some(ColumnType.StringType)
      case _: Expr.StringSplit[_] => Some(ColumnType.AnyType)
      case _: Expr.Length[_] => Some(ColumnType.IntType)
      case c: Expr.Coalesce[_, _] => c.exprs.headOption.flatMap(_.outputType)
      case g: Expr.GetOrElse[_, _] => g.expr.outputType
      case _: Expr.Count[_] | _: Expr.CountDistinct[_, _] | _: Expr.CountIf[_] =>
        Some(ColumnType.LongType)
      case _: Expr.Avg[_] | _: Expr.StdDev[_] | _: Expr.StdDevPop[_] =>
        Some(ColumnType.DoubleType)
      case _: Expr.Max[_, _] | _: Expr.Min[_, _] | _: Expr.First[_, _] => None
      case _: Expr.Collect[_, _] => Some(ColumnType.AnyType)
      case _: Expr.Option2Iterable[_, _] => Some(ColumnType.AnyType)
      case _: Expr.PercentileApprox[_] => Some(ColumnType.DoubleType)
      case _: Expr.MaxBy[_, _, _] | _: Expr.MinBy[_, _, _] => None
      case _: Expr.MaxN[_, _] | _: Expr.MinN[_, _] | _: Expr.MaxByN[_, _, _] | _: Expr.MinByN[_, _, _] =>
        Some(ColumnType.AnyType)
      case _: Expr.DateAddDays[_] | _: Expr.DateSubDays[_] | _: Expr.DateAddMonths[_] =>
        Some(ColumnType.DateType)
      case _: Expr.DateDiff[_] | _: Expr.ExtractYear[_] | _: Expr.ExtractMonth[_] | _: Expr.ExtractDay[_] =>
        Some(ColumnType.IntType)
      case _: Expr.RowNumber[_] | _: Expr.Rank[_] | _: Expr.DenseRank[_] =>
        Some(ColumnType.IntType)
      case _: Expr.Lag[_, _] | _: Expr.Lead[_, _] => None
      case _: Expr.Sqrt[_] | _: Expr.Pow[_] | _: Expr.Log[_] | _: Expr.Log10[_] | _: Expr.Log2[_] | _: Expr.Exp[_] |
          _: Expr.Sin[_] | _: Expr.Cos[_] | _: Expr.Tan[_] | _: Expr.Asin[_] | _: Expr.Acos[_] | _: Expr.Atan[_] |
          _: Expr.Atan2[_] | _: Expr.Signum[_] | _: Expr.Rand[_] =>
        Some(ColumnType.DoubleType)
      case _: Expr.DayOfWeek[_] | _: Expr.DayOfYear[_] | _: Expr.WeekOfYear[_] | _: Expr.Quarter[_] =>
        Some(ColumnType.IntType)
      case _: Expr.LastDay[_] | _: Expr.NextDay[_] | _: Expr.DateTrunc[_] | _: Expr.MakeDate[_] =>
        Some(ColumnType.DateType)
      case _: Expr.MonthsBetween[_] => Some(ColumnType.DoubleType)
      case _: Expr.DateFormat[_] => Some(ColumnType.StringType)
      case _: Expr.Variance[_] | _: Expr.VariancePop[_] | _: Expr.Corr[_] | _: Expr.CovarSamp[_] | _: Expr.CovarPop[_] |
          _: Expr.Median[_] =>
        Some(ColumnType.DoubleType)
      case _: Expr.ApproxCountDistinct[_, _] => Some(ColumnType.LongType)
      case _: Expr.CollectSet[_, _] => Some(ColumnType.AnyType)
      case _: Expr.ExprLast[_, _] | _: Expr.AnyValue[_, _] | _: Expr.Mode[_, _] => None
      case _: Expr.BoolAnd[_] | _: Expr.BoolOr[_] => Some(ColumnType.BooleanType)
      case _: Expr.NTile[_] => Some(ColumnType.IntType)
      case _: Expr.CumeDist[_] | _: Expr.PercentRank[_] => Some(ColumnType.DoubleType)
      case _: Expr.NthValue[_, _] | _: Expr.FirstValue[_, _] | _: Expr.LastValue[_, _] => None
    }
  }
}
