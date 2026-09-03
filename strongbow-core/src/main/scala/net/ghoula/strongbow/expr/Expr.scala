package net.ghoula.strongbow.expr

import net.ghoula.sarati.ast.json.JsonValue
import net.ghoula.sarati.codec.{Decoder, Encoder}

import scala.annotation.targetName

import net.ghoula.strongbow.Schema
import net.ghoula.strongbow.column.ColumnType
import net.ghoula.strongbow.types.{
  Binary,
  ColumnIndex,
  Date,
  DayTimeInterval,
  Decimal,
  Time,
  Timestamp,
  YearMonthInterval
}

/** A lambda variable binder for higher-order expressions.
  *
  * The phantom type on [[Binder]] gives `LambdaVar(binder: Binder[A])` type `Expr[Row, A]` by
  * construction. Binders are created and handed to lambda bodies by the higher-order combinators
  * ([[Expr.transform]], [[Expr.aggregate]], ...); a `LambdaVar` is evaluated against the binding
  * established by the nearest enclosing higher-order expression, so unbound use fails evaluation
  * rather than compiling away.
  *
  * Identity semantics (reference equality) are load-bearing: nested and sibling lambdas must not
  * collide in the binding scope, so Binder is a plain class, not a case class.
  */
final class Binder[A] private[expr] ()

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

  /** A column reference, resolved by position and column type at evaluation time. */
  case Cell[Row, A](name: String, index: ColumnIndex) extends Expr[Row, A]

  /** A constant literal value shared by every row. */
  case Const[Row, A](value: A) extends Expr[Row, A]

  /** An expression whose output column carries the given name (SQL `AS alias`). */
  case Named[Row, A](expr: Expr[Row, A], name: String) extends Expr[Row, A]

  /** A lambda variable bound by the nearest enclosing higher-order expression. */
  case LambdaVar[Row, A](binder: Binder[A]) extends Expr[Row, A]

  /** SQL `+` — integer addition. */
  case Add[Row](left: Expr[Row, Int], right: Expr[Row, Int]) extends Expr[Row, Int]

  /** SQL `-` — integer subtraction. */
  case Sub[Row](left: Expr[Row, Int], right: Expr[Row, Int]) extends Expr[Row, Int]

  /** SQL `*` — integer multiplication. */
  case Mul[Row](left: Expr[Row, Int], right: Expr[Row, Int]) extends Expr[Row, Int]

  /** SQL `/` — integer division; dividing by zero fails the row (see the error-policy design). */
  case Div[Row](left: Expr[Row, Int], right: Expr[Row, Int]) extends Expr[Row, Int]

  /** SQL `+` on Long — addition. */
  case AddLong[Row](left: Expr[Row, Long], right: Expr[Row, Long]) extends Expr[Row, Long]

  /** SQL `-` on Long — subtraction. */
  case SubLong[Row](left: Expr[Row, Long], right: Expr[Row, Long]) extends Expr[Row, Long]

  /** SQL `*` on Long — multiplication. */
  case MulLong[Row](left: Expr[Row, Long], right: Expr[Row, Long]) extends Expr[Row, Long]

  /** SQL `/` on Long — division; dividing by zero fails the row. */
  case DivLong[Row](left: Expr[Row, Long], right: Expr[Row, Long]) extends Expr[Row, Long]

  /** SQL `+` on Double — addition. */
  case AddDouble[Row](left: Expr[Row, Double], right: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `-` on Double — subtraction. */
  case SubDouble[Row](left: Expr[Row, Double], right: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `*` on Double — multiplication. */
  case MulDouble[Row](left: Expr[Row, Double], right: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `/` on Double — division. */
  case DivDouble[Row](left: Expr[Row, Double], right: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `>` — strict greater-than under the given ordering. */
  case Gt[Row, A](left: Expr[Row, A], right: Expr[Row, A], ordering: Ordering[A]) extends Expr[Row, Boolean]

  /** SQL `>=` — greater-than-or-equal under the given ordering. */
  case Gte[Row, A](left: Expr[Row, A], right: Expr[Row, A], ordering: Ordering[A]) extends Expr[Row, Boolean]

  /** SQL `<` — strict less-than under the given ordering. */
  case Lt[Row, A](left: Expr[Row, A], right: Expr[Row, A], ordering: Ordering[A]) extends Expr[Row, Boolean]

  /** SQL `<=` — less-than-or-equal under the given ordering. */
  case Lte[Row, A](left: Expr[Row, A], right: Expr[Row, A], ordering: Ordering[A]) extends Expr[Row, Boolean]

  /** SQL `=` — equality. */
  case Eq[Row, A](left: Expr[Row, A], right: Expr[Row, A]) extends Expr[Row, Boolean]

  /** SQL `<>` — inequality. */
  case Neq[Row, A](left: Expr[Row, A], right: Expr[Row, A]) extends Expr[Row, Boolean]

  /** SQL `AND` — logical conjunction. */
  case And[Row](left: Expr[Row, Boolean], right: Expr[Row, Boolean]) extends Expr[Row, Boolean]

  /** SQL `OR` — logical disjunction. */
  case Or[Row](left: Expr[Row, Boolean], right: Expr[Row, Boolean]) extends Expr[Row, Boolean]

  /** SQL `NOT` — logical negation. */
  case Not[Row](expr: Expr[Row, Boolean]) extends Expr[Row, Boolean]

  /** SQL `CASE WHEN condition THEN thenExpr ELSE elseExpr END` — conditional selection. */
  case When[Row, A](condition: Expr[Row, Boolean], thenExpr: Expr[Row, A], elseExpr: Expr[Row, A]) extends Expr[Row, A]

  /** SQL `concat` — string concatenation. */
  case Concat[Row](left: Expr[Row, String], right: Expr[Row, String]) extends Expr[Row, String]

  /** SQL `length` — character length of the string. */
  case Length[Row](expr: Expr[Row, String]) extends Expr[Row, Int]

  /** SQL `LIKE` — SQL-pattern match with `%` and `_` wildcards. */
  case Like[Row](expr: Expr[Row, String], pattern: String) extends Expr[Row, Boolean]

  /** SQL `lower` — lower-cases the string. */
  case Lower[Row](expr: Expr[Row, String]) extends Expr[Row, String]

  /** SQL `upper` — upper-cases the string. */
  case Upper[Row](expr: Expr[Row, String]) extends Expr[Row, String]

  /** SQL `trim` — strips whitespace from both ends. */
  case Trim[Row](expr: Expr[Row, String]) extends Expr[Row, String]

  /** SQL `ltrim` — strips leading whitespace. */
  case LTrim[Row](expr: Expr[Row, String]) extends Expr[Row, String]

  /** SQL `rtrim` — strips trailing whitespace. */
  case RTrim[Row](expr: Expr[Row, String]) extends Expr[Row, String]

  /** SQL `substring(expr, pos, len)` — 1-based substring of `len` characters. */
  case Substring[Row](expr: Expr[Row, String], pos: Int, len: Int) extends Expr[Row, String]

  /** SQL `replace` — replaces every occurrence of `search` with `replacement`. */
  case StringReplace[Row](expr: Expr[Row, String], search: String, replacement: String) extends Expr[Row, String]

  /** SQL `regexp_replace` — replaces matches of the Java regex pattern. */
  case RegexpReplace[Row](expr: Expr[Row, String], pattern: String, replacement: String) extends Expr[Row, String]

  /** SQL `regexp_extract` — the first match of the pattern, `groupIdx`-th group (empty when no
    * match).
    */
  case RegexpExtract[Row](expr: Expr[Row, String], pattern: String, groupIdx: Int) extends Expr[Row, String]

  /** SQL `split` — splits on the delimiter (a Java regex) into an array. */
  case StringSplit[Row](expr: Expr[Row, String], delimiter: String) extends Expr[Row, Seq[String]]

  /** SQL `startswith` — whether the string starts with the prefix. */
  case StartsWith[Row](expr: Expr[Row, String], prefix: Expr[Row, String]) extends Expr[Row, Boolean]

  /** SQL `endswith` — whether the string ends with the suffix. */
  case EndsWith[Row](expr: Expr[Row, String], suffix: Expr[Row, String]) extends Expr[Row, Boolean]

  /** SQL `contains` — whether the string contains the substring. */
  case StringContains[Row](expr: Expr[Row, String], substr: Expr[Row, String]) extends Expr[Row, Boolean]

  /** SQL `concat_ws` — joins the expressions with the separator, skipping nulls. */
  case ConcatWs[Row](separator: String, exprs: Vector[Expr[Row, String]]) extends Expr[Row, String]

  /** SQL `coalesce` — the first non-null value, null when all are null. */
  case Coalesce[Row, A](exprs: Vector[Expr[Row, A]]) extends Expr[Row, A]

  /** SQL `IS NULL`. */
  case IsNull[Row, A](expr: Expr[Row, A]) extends Expr[Row, Boolean]

  /** SQL `IS NOT NULL`. */
  case IsNotNull[Row, A](expr: Expr[Row, A]) extends Expr[Row, Boolean]

  /** SQL `IN` — membership in the given value list. */
  case In[Row, A](expr: Expr[Row, A], values: Vector[A]) extends Expr[Row, Boolean]

  /** SQL `BETWEEN lower AND upper` — inclusive range test under the given ordering. */
  case Between[Row, A](expr: Expr[Row, A], lower: Expr[Row, A], upper: Expr[Row, A], ordering: Ordering[A])
      extends Expr[Row, Boolean]

  /** SQL `%` / `mod` — integer remainder; a zero divisor fails the row. */
  case Mod[Row](left: Expr[Row, Int], right: Expr[Row, Int]) extends Expr[Row, Int]

  /** SQL `%` / `mod` on Long — remainder; a zero divisor fails the row. */
  case ModLong[Row](left: Expr[Row, Long], right: Expr[Row, Long]) extends Expr[Row, Long]

  /** SQL `abs` — absolute value. */
  case Abs[Row](expr: Expr[Row, Int]) extends Expr[Row, Int]

  /** SQL `abs` on Long — absolute value. */
  case AbsLong[Row](expr: Expr[Row, Long]) extends Expr[Row, Long]

  /** SQL `abs` on Double — absolute value. */
  case AbsDouble[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL unary minus. */
  case Negate[Row](expr: Expr[Row, Int]) extends Expr[Row, Int]

  /** SQL unary minus on Long. */
  case NegateLong[Row](expr: Expr[Row, Long]) extends Expr[Row, Long]

  /** SQL unary minus on Double. */
  case NegateDouble[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `round(expr, scale)` — half-up rounding to `scale` decimal places. */
  case Round[Row](expr: Expr[Row, Double], scale: Int) extends Expr[Row, Double]

  /** SQL `floor` — largest integral value not greater than the input. */
  case Floor[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `ceil` — smallest integral value not less than the input. */
  case Ceil[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `CAST(expr AS BIGINT)` — Int to Long. */
  case CastToLong[Row](expr: Expr[Row, Int]) extends Expr[Row, Long]

  /** SQL `CAST(expr AS DOUBLE)` — Int to Double. */
  case CastToDouble[Row](expr: Expr[Row, Int]) extends Expr[Row, Double]

  /** SQL `CAST(expr AS DOUBLE)` — Long to Double. */
  case CastLongToDouble[Row](expr: Expr[Row, Long]) extends Expr[Row, Double]

  /** SQL `CAST(expr AS STRING)` — renders the value as text. */
  case CastToString[Row, A](expr: Expr[Row, A]) extends Expr[Row, String]

  /** Scala `isDefined` on an Option column — true where the value is present. */
  case IsDefined[Row, A](expr: Expr[Row, Option[A]]) extends Expr[Row, Boolean]

  /** Scala `getOrElse` on an Option column — the contained value, or `default` where absent. */
  case GetOrElse[Row, A](expr: Expr[Row, Option[A]], default: A) extends Expr[Row, A]

  /** SQL `SUM` over Int — Long result. */
  case Sum[Row](expr: Expr[Row, Int]) extends Expr[Row, Long]

  /** SQL `SUM` over Double. */
  case SumDouble[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `SUM` over Long. */
  case SumLong[Row](expr: Expr[Row, Long]) extends Expr[Row, Long]

  /** SQL `COUNT(*)` — number of rows in the group. */
  case Count[Row]() extends Expr[Row, Long]

  /** SQL `MAX` — null (None) for an empty or all-null group. */
  case Max[Row, A](expr: Expr[Row, A], ordering: Ordering[A]) extends Expr[Row, Option[A]]

  /** SQL `MIN` — null (None) for an empty or all-null group. */
  case Min[Row, A](expr: Expr[Row, A], ordering: Ordering[A]) extends Expr[Row, Option[A]]

  /** SQL `AVG`. */
  case Avg[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `COUNT(DISTINCT expr)`. */
  case CountDistinct[Row, A](expr: Expr[Row, A]) extends Expr[Row, Long]

  /** SQL `count_if` — number of rows in the group where the predicate holds. */
  case CountIf[Row](predicate: Expr[Row, Boolean]) extends Expr[Row, Long]

  /** SQL `stddev_samp` — sample standard deviation. */
  case StdDev[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `stddev_pop` — population standard deviation. */
  case StdDevPop[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `first` — the first value of the group (None for an empty group). */
  case First[Row, A](expr: Expr[Row, A]) extends Expr[Row, Option[A]]

  /** SQL `collect_list` — the group's values in encounter order. */
  case Collect[Row, A](expr: Expr[Row, A]) extends Expr[Row, Seq[A]]

  /** Converts an Option column to an iterable column — empty where the option is None. */
  case Option2Iterable[Row, A](expr: Expr[Row, Option[A]]) extends Expr[Row, Iterable[A]]

  /** SQL `percentile_approx` — approximate percentile with the given compression accuracy. */
  case PercentileApprox[Row](expr: Expr[Row, Double], percentile: Double, accuracy: Int) extends Expr[Row, Double]

  /** SQL `max_by` — the value expression's result from the row with the greatest order key. */
  case MaxBy[Row, A, K](valueExpr: Expr[Row, A], orderExpr: Expr[Row, K], ordering: Ordering[K])
      extends Expr[Row, Option[A]]

  /** SQL `min_by` — the value expression's result from the row with the least order key. */
  case MinBy[Row, A, K](valueExpr: Expr[Row, A], orderExpr: Expr[Row, K], ordering: Ordering[K])
      extends Expr[Row, Option[A]]

  /** The `n` largest values of the group, ordered descending. */
  case MaxN[Row, A](expr: Expr[Row, A], n: Int, ordering: Ordering[A]) extends Expr[Row, Seq[A]]

  /** The `n` smallest values of the group, ordered ascending. */
  case MinN[Row, A](expr: Expr[Row, A], n: Int, ordering: Ordering[A]) extends Expr[Row, Seq[A]]

  /** The `n` value-expression results from the rows with the greatest order keys. */
  case MaxByN[Row, A, K](valueExpr: Expr[Row, A], orderExpr: Expr[Row, K], n: Int, ordering: Ordering[K])
      extends Expr[Row, Seq[A]]

  /** The `n` value-expression results from the rows with the least order keys. */
  case MinByN[Row, A, K](valueExpr: Expr[Row, A], orderExpr: Expr[Row, K], n: Int, ordering: Ordering[K])
      extends Expr[Row, Seq[A]]

  /** SQL `date_add` — the date shifted forward by `days` days. */
  case DateAddDays[Row](date: Expr[Row, Date], days: Expr[Row, Int]) extends Expr[Row, Date]

  /** SQL `date_sub` — the date shifted back by `days` days. */
  case DateSubDays[Row](date: Expr[Row, Date], days: Expr[Row, Int]) extends Expr[Row, Date]

  /** SQL `add_months` — the date shifted forward by `months` months, clamped to the month end. */
  case DateAddMonths[Row](date: Expr[Row, Date], months: Expr[Row, Int]) extends Expr[Row, Date]

  /** SQL `datediff` — whole days from `right` to `left`. */
  case DateDiff[Row](left: Expr[Row, Date], right: Expr[Row, Date]) extends Expr[Row, Int]

  /** SQL `year` — the calendar year. */
  case ExtractYear[Row](date: Expr[Row, Date]) extends Expr[Row, Int]

  /** SQL `month` — the calendar month, 1-12. */
  case ExtractMonth[Row](date: Expr[Row, Date]) extends Expr[Row, Int]

  /** SQL `day` — the day of month, 1-31. */
  case ExtractDay[Row](date: Expr[Row, Date]) extends Expr[Row, Int]

  /** Window `row_number()` — 1-based position within the partition ordering. */
  case RowNumber[Row]() extends Expr[Row, Int]

  /** Window `rank()` — rank with gaps for ties. */
  case Rank[Row]() extends Expr[Row, Int]

  /** Window `dense_rank()` — rank without gaps for ties. */
  case DenseRank[Row]() extends Expr[Row, Int]

  /** Window `lag(expr, offset, default)` — the value `offset` rows earlier. */
  case Lag[Row, A](expr: Expr[Row, A], offset: Int, default: Option[A]) extends Expr[Row, A]

  /** Window `lead(expr, offset, default)` — the value `offset` rows later. */
  case Lead[Row, A](expr: Expr[Row, A], offset: Int, default: Option[A]) extends Expr[Row, A]

  /** SQL `sqrt`. */
  case Sqrt[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `pow` — base raised to the exponent. */
  case Pow[Row](base: Expr[Row, Double], exponent: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `ln` — natural logarithm. */
  case Log[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `log10`. */
  case Log10[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `log2`. */
  case Log2[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `exp` — e raised to the input. */
  case Exp[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `sin`. */
  case Sin[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `cos`. */
  case Cos[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `tan`. */
  case Tan[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `asin`. */
  case Asin[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `acos`. */
  case Acos[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `atan`. */
  case Atan[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `atan2`. */
  case Atan2[Row](y: Expr[Row, Double], x: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `signum` — -1, 0 or 1 by the input's sign. */
  case Signum[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `rand(seed)` — deterministic pseudo-random double in [0, 1). */
  case Rand[Row](seed: Long) extends Expr[Row, Double]

  /** SQL `dayofweek` — 1 (Sunday) through 7 (Saturday). */
  case DayOfWeek[Row](date: Expr[Row, Date]) extends Expr[Row, Int]

  /** SQL `dayofyear` — 1-based day within the year. */
  case DayOfYear[Row](date: Expr[Row, Date]) extends Expr[Row, Int]

  /** SQL `weekofyear` — ISO week number. */
  case WeekOfYear[Row](date: Expr[Row, Date]) extends Expr[Row, Int]

  /** SQL `quarter` — 1 through 4. */
  case Quarter[Row](date: Expr[Row, Date]) extends Expr[Row, Int]

  /** SQL `last_day` — the last day of the date's month. */
  case LastDay[Row](date: Expr[Row, Date]) extends Expr[Row, Date]

  /** SQL `next_day` — the first date after the input falling on the named weekday. */
  case NextDay[Row](date: Expr[Row, Date], dayOfWeek: String) extends Expr[Row, Date]

  /** SQL `months_between` — whole-and-fractional months from `start` to `end`. */
  case MonthsBetween[Row](end: Expr[Row, Date], start: Expr[Row, Date]) extends Expr[Row, Double]

  /** SQL `date_trunc` — the date truncated to the named unit (year, month, day, ...). */
  case DateTrunc[Row](unit: String, date: Expr[Row, Date]) extends Expr[Row, Date]

  /** SQL `date_format` — the date rendered with the given pattern. */
  case DateFormat[Row](date: Expr[Row, Date], format: String) extends Expr[Row, String]

  /** SQL `make_date` — a date from calendar parts. */
  case MakeDate[Row](year: Expr[Row, Int], month: Expr[Row, Int], day: Expr[Row, Int]) extends Expr[Row, Date]

  /** SQL `var_samp` — sample variance. */
  case Variance[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `var_pop` — population variance. */
  case VariancePop[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `approx_count_distinct`. */
  case ApproxCountDistinct[Row, A](expr: Expr[Row, A]) extends Expr[Row, Long]

  /** SQL `collect_set` — the group's distinct values. */
  case CollectSet[Row, A](expr: Expr[Row, A]) extends Expr[Row, Seq[A]]

  /** SQL `last` — the last value of the group (None for an empty group). */
  case ExprLast[Row, A](expr: Expr[Row, A]) extends Expr[Row, Option[A]]

  /** SQL `any_value` — an unspecified non-null value of the group when one exists. */
  case AnyValue[Row, A](expr: Expr[Row, A]) extends Expr[Row, Option[A]]

  /** SQL `bool_and` — true when no value in the group is false. */
  case BoolAnd[Row](expr: Expr[Row, Boolean]) extends Expr[Row, Boolean]

  /** SQL `bool_or` — true when any value in the group is true. */
  case BoolOr[Row](expr: Expr[Row, Boolean]) extends Expr[Row, Boolean]

  /** SQL `corr` — Pearson correlation of the pair. */
  case Corr[Row](left: Expr[Row, Double], right: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `covar_samp` — sample covariance of the pair. */
  case CovarSamp[Row](left: Expr[Row, Double], right: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `covar_pop` — population covariance of the pair. */
  case CovarPop[Row](left: Expr[Row, Double], right: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `median`. */
  case Median[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `mode` — the most frequent value of the group (None for an empty group). */
  case Mode[Row, A](expr: Expr[Row, A]) extends Expr[Row, Option[A]]

  /** Window `ntile(n)` — roughly-equal bucket number 1..n within the partition ordering. */
  case NTile[Row](n: Int) extends Expr[Row, Int]

  /** Window `cume_dist` — the fraction of partition rows at or before the current row. */
  case CumeDist[Row]() extends Expr[Row, Double]

  /** Window `percent_rank` — the relative rank within the partition, 0 to 1. */
  case PercentRank[Row]() extends Expr[Row, Double]

  /** Window `nth_value(expr, n)` — the n-th value within the partition ordering. */
  case NthValue[Row, A](expr: Expr[Row, A], n: Int) extends Expr[Row, A]

  /** Window `first_value`. */
  case FirstValue[Row, A](expr: Expr[Row, A]) extends Expr[Row, A]

  /** Window `last_value`. */
  case LastValue[Row, A](expr: Expr[Row, A]) extends Expr[Row, A]

  /** SQL `size` — number of elements in the array. */
  case ArraySize[Row, A](expr: Expr[Row, Seq[A]]) extends Expr[Row, Int]

  /** SQL `array_contains`. */
  case ArrayContains[Row, A](expr: Expr[Row, Seq[A]], value: Expr[Row, A]) extends Expr[Row, Boolean]

  /** SQL `explode` — one row per element; the columnar result carries the element column. */
  case Explode[Row, A](expr: Expr[Row, Seq[A]]) extends Expr[Row, A]

  /** SQL `array_sort` — ascending under the given ordering. */
  case ArraySort[Row, A](expr: Expr[Row, Seq[A]], ordering: Ordering[A]) extends Expr[Row, Seq[A]]

  /** SQL `array_distinct` — removes duplicates, preserving first occurrences. */
  case ArrayDistinct[Row, A](expr: Expr[Row, Seq[A]]) extends Expr[Row, Seq[A]]

  /** SQL `array_union` — distinct elements from either array. */
  case ArrayUnion[Row, A](left: Expr[Row, Seq[A]], right: Expr[Row, Seq[A]]) extends Expr[Row, Seq[A]]

  /** SQL `array_intersect` — distinct elements present in both arrays. */
  case ArrayIntersect[Row, A](left: Expr[Row, Seq[A]], right: Expr[Row, Seq[A]]) extends Expr[Row, Seq[A]]

  /** SQL `array_except` — distinct elements of the left array absent from the right. */
  case ArrayExcept[Row, A](left: Expr[Row, Seq[A]], right: Expr[Row, Seq[A]]) extends Expr[Row, Seq[A]]

  /** SQL `flatten` — concatenates an array of arrays one level deep. */
  case Flatten[Row, A](expr: Expr[Row, Seq[Seq[A]]]) extends Expr[Row, Seq[A]]

  /** SQL `element_at` — 1-based element access; out-of-range fails the row. */
  case ElementAt[Row, A](expr: Expr[Row, Seq[A]], index: Expr[Row, Int]) extends Expr[Row, A]

  /** SQL `slice` — `length` elements starting at the 1-based `start`. */
  case ArraySlice[Row, A](expr: Expr[Row, Seq[A]], start: Int, length: Int) extends Expr[Row, Seq[A]]

  /** SQL `map_keys`. */
  case MapKeys[Row, K, V](expr: Expr[Row, Map[K, V]]) extends Expr[Row, Seq[K]]

  /** SQL `map_values`. */
  case MapValues[Row, K, V](expr: Expr[Row, Map[K, V]]) extends Expr[Row, Seq[V]]

  /** SQL `map_contains_key`. */
  case MapContainsKey[Row, K, V](expr: Expr[Row, Map[K, V]], key: Expr[Row, K]) extends Expr[Row, Boolean]

  /** SQL `map_entries` — the map as an array of key-value pairs. */
  case MapEntries[Row, K, V](expr: Expr[Row, Map[K, V]]) extends Expr[Row, Seq[(K, V)]]

  /** SQL `map_from_arrays` — a map from the parallel key and value arrays. */
  case MapFromArrays[Row, K, V](keys: Expr[Row, Seq[K]], values: Expr[Row, Seq[V]]) extends Expr[Row, Map[K, V]]

  /** SQL `map_concat` — the union of both maps, right side winning on key conflicts. */
  case MapConcat[Row, K, V](left: Expr[Row, Map[K, V]], right: Expr[Row, Map[K, V]]) extends Expr[Row, Map[K, V]]

  /** Higher-order: apply a lambda to every element (and optionally its index) of an array. */
  case Transform[Row, A, B](
    array: Expr[Row, Seq[A]],
    binder: Binder[A],
    indexBinder: Option[Binder[Int]],
    body: Expr[Row, B]
  ) extends Expr[Row, Seq[B]]

  /** Higher-order: keep the elements (and optionally the index) whose lambda result is true. */
  case Filter[Row, A](
    array: Expr[Row, Seq[A]],
    binder: Binder[A],
    indexBinder: Option[Binder[Int]],
    body: Expr[Row, Boolean]
  ) extends Expr[Row, Seq[A]]

  /** Higher-order: whether the lambda holds for at least one element (three-valued). */
  case Exists[Row, A](array: Expr[Row, Seq[A]], binder: Binder[A], body: Expr[Row, Boolean]) extends Expr[Row, Boolean]

  /** Higher-order: whether the lambda holds for every element (three-valued). */
  case ForAll[Row, A](array: Expr[Row, Seq[A]], binder: Binder[A], body: Expr[Row, Boolean]) extends Expr[Row, Boolean]

  /** Higher-order: fold the array left-to-right from `zero`, then optionally project the
    * accumulator with `finish`. Spark's `aggregate`/`reduce`.
    */
  case Aggregate[Row, S, A](
    array: Expr[Row, Seq[A]],
    zero: Expr[Row, S],
    accBinder: Binder[S],
    elemBinder: Binder[A],
    merge: Expr[Row, S],
    finish: Option[(Binder[S], Expr[Row, S])]
  ) extends Expr[Row, S]

  /** Higher-order: merge two arrays element-wise; the shorter is null-padded, so the lambda binds
    * `Option` elements.
    */
  case ZipWith[Row, A, B, C](
    left: Expr[Row, Seq[A]],
    right: Expr[Row, Seq[B]],
    leftBinder: Binder[Option[A]],
    rightBinder: Binder[Option[B]],
    body: Expr[Row, C]
  ) extends Expr[Row, Seq[C]]

  /** Higher-order: keep the map entries whose lambda result is true. */
  case MapFilter[Row, K, V](
    map: Expr[Row, Map[K, V]],
    keyBinder: Binder[K],
    valueBinder: Binder[V],
    body: Expr[Row, Boolean]
  ) extends Expr[Row, Map[K, V]]

  /** Higher-order: merge two maps by key; missing values bind as `None`. */
  case MapZipWith[Row, K, V1, V2, C](
    left: Expr[Row, Map[K, V1]],
    right: Expr[Row, Map[K, V2]],
    keyBinder: Binder[K],
    leftBinder: Binder[Option[V1]],
    rightBinder: Binder[Option[V2]],
    body: Expr[Row, C]
  ) extends Expr[Row, Map[K, C]]

  /** Higher-order: transform map keys (the result must be distinct per Spark; duplicate detection
    * uses boxed equality in-memory, so it diverges from Spark's Catalyst equality only for
    * array/struct keys).
    */
  case TransformKeys[Row, K, V, K2](
    map: Expr[Row, Map[K, V]],
    keyBinder: Binder[K],
    valueBinder: Binder[V],
    body: Expr[Row, K2]
  ) extends Expr[Row, Map[K2, V]]

  /** Higher-order: transform map values. */
  case TransformValues[Row, K, V, V2](
    map: Expr[Row, Map[K, V]],
    keyBinder: Binder[K],
    valueBinder: Binder[V],
    body: Expr[Row, V2]
  ) extends Expr[Row, Map[K, V2]]

  /** Higher-order: sort by a comparator lambda (negative/zero/positive, like Spark's
    * `array_sort(e, comparator)`); a null comparator result fails evaluation.
    */
  case ArraySortComparator[Row, A](
    array: Expr[Row, Seq[A]],
    leftBinder: Binder[A],
    rightBinder: Binder[A],
    body: Expr[Row, Int]
  ) extends Expr[Row, Seq[A]]

  /** SQL `md5` — hex digest of the string. */
  case Md5[Row](expr: Expr[Row, String]) extends Expr[Row, String]

  /** SQL `sha1` — hex digest of the string. */
  case Sha1[Row](expr: Expr[Row, String]) extends Expr[Row, String]

  /** SQL `sha2` — hex digest with the given bit length (224, 256, 384 or 512). */
  case Sha2[Row](expr: Expr[Row, String], bitLength: Int) extends Expr[Row, String]

  /** SQL `url_encode` — application/x-www-form-urlencoded form of the string. */
  case UrlEncode[Row](expr: Expr[Row, String]) extends Expr[Row, String]

  /** SQL `url_decode` — inverse of url_encode. */
  case UrlDecode[Row](expr: Expr[Row, String]) extends Expr[Row, String]

  /** SQL `base64` — base64 form of the string. */
  case Base64Encode[Row](expr: Expr[Row, String]) extends Expr[Row, String]

  /** SQL `unbase64` — decodes base64 into a string. */
  case Base64Decode[Row](expr: Expr[Row, String]) extends Expr[Row, String]

  /** SQL `hex` — hexadecimal form of the string. */
  case Hex[Row](expr: Expr[Row, String]) extends Expr[Row, String]

  /** SQL `crc32`. */
  case Crc32[Row](expr: Expr[Row, String]) extends Expr[Row, Long]

  /** SQL `xxhash64` — 64-bit hash of the string. */
  case XxHash64[Row](expr: Expr[Row, String]) extends Expr[Row, Long]

  /** SQL `hash` — 32-bit Murmur3 hash of the string. */
  case Hash[Row](expr: Expr[Row, String]) extends Expr[Row, Int]

  /** SQL `aes_encrypt` — AES-GCM encryption of the string under the key. */
  case AesEncrypt[Row](expr: Expr[Row, String], key: Expr[Row, String]) extends Expr[Row, Binary]

  /** SQL `aes_decrypt` — decrypts an aes_encrypt result; wrong keys fail the row. */
  case AesDecrypt[Row](expr: Expr[Row, String], key: Expr[Row, String]) extends Expr[Row, String]

  /** In-memory lenient aes_decrypt — null instead of failure on undecryptable input. */
  case TryAesDecrypt[Row](expr: Expr[Row, String], key: Expr[Row, String]) extends Expr[Row, String]

  /** SQL `get_json_object` — the JSON value at the path, rendered as text. */
  case GetJsonObject[Row](expr: Expr[Row, String], path: String) extends Expr[Row, String]

  /** XPath 1.0 over an XML document (Spark's `xpath*` family). `path` is a constant, parsed once
    * per evaluation; per-row XML is parsed with [[net.ghoula.sarati.ast.xml.xpathXmlConfig]].
    * Invalid XML or a failing evaluation errors the whole column (mirroring Spark's query failure)
    * with the row index in the message; null or empty inputs yield null rows. Documents with a DTD
    * are not parseable by the in-memory XML parser (Spark expands internal DTD entities) — a
    * documented divergence.
    */
  case Xpath[Row](expr: Expr[Row, String], path: String) extends Expr[Row, Seq[String | Null]]

  /** XPath 1.0 string projection: string-value of the first node in document order. */
  case XpathString[Row](expr: Expr[Row, String], path: String) extends Expr[Row, String]

  /** XPath 1.0 boolean projection. */
  case XpathBoolean[Row](expr: Expr[Row, String], path: String) extends Expr[Row, Boolean]

  /** XPath 1.0 numeric projections: `number()` coercion of the result, truncated for the integral
    * kinds (NaN truncates to 0, matching Spark); float/double keep NaN.
    */
  case XpathShort[Row](expr: Expr[Row, String], path: String) extends Expr[Row, Short]

  /** SQL `xpath_int` — the XPath 1.0 result coerced to Int. */
  case XpathInt[Row](expr: Expr[Row, String], path: String) extends Expr[Row, Int]

  /** SQL `xpath_long` — the XPath 1.0 result coerced to Long. */
  case XpathLong[Row](expr: Expr[Row, String], path: String) extends Expr[Row, Long]

  /** SQL `xpath_float` — the XPath 1.0 result coerced to Float. */
  case XpathFloat[Row](expr: Expr[Row, String], path: String) extends Expr[Row, Float]

  /** SQL `xpath_double` — the XPath 1.0 result coerced to Double (NaN when not numeric). */
  case XpathDouble[Row](expr: Expr[Row, String], path: String) extends Expr[Row, Double]

  /** In-memory-only try variants of the [[Xpath]]* family: per-row evaluation failures (malformed
    * XML) yield null rows instead of failing the column. Invalid paths still fail (a path error is
    * a programming error, not data). On Spark these report `UnsupportedOperation` — Spark's Column
    * model has no way to catch per-row evaluation failures, so the try semantics cannot be
    * expressed there.
    */
  case TryXpath[Row](expr: Expr[Row, String], path: String) extends Expr[Row, Seq[String | Null]]

  /** In-memory lenient xpath_string — null on malformed input or failed evaluation. */
  case TryXpathString[Row](expr: Expr[Row, String], path: String) extends Expr[Row, String]

  /** In-memory lenient xpath_boolean — null on malformed input or failed evaluation. */
  case TryXpathBoolean[Row](expr: Expr[Row, String], path: String) extends Expr[Row, Boolean]

  /** In-memory lenient xpath_short — null on malformed input or failed evaluation. */
  case TryXpathShort[Row](expr: Expr[Row, String], path: String) extends Expr[Row, Short]

  /** In-memory lenient xpath_int — null on malformed input or failed evaluation. */
  case TryXpathInt[Row](expr: Expr[Row, String], path: String) extends Expr[Row, Int]

  /** In-memory lenient xpath_long — null on malformed input or failed evaluation. */
  case TryXpathLong[Row](expr: Expr[Row, String], path: String) extends Expr[Row, Long]

  /** In-memory lenient xpath_float — null on malformed input or failed evaluation. */
  case TryXpathFloat[Row](expr: Expr[Row, String], path: String) extends Expr[Row, Float]

  /** In-memory lenient xpath_double — null on malformed input or failed evaluation. */
  case TryXpathDouble[Row](expr: Expr[Row, String], path: String) extends Expr[Row, Double]

  /** SQL `json_tuple` — the named keys' values from the JSON object, in key order (null where
    * absent).
    */
  case JsonTuple[Row](expr: Expr[Row, String], keys: Vector[String]) extends Expr[Row, Seq[String | Null]]

  /** Time of day as fractional seconds (SQL DECIMAL). */
  case TimeToSeconds[Row](expr: Expr[Row, Time]) extends Expr[Row, Decimal]

  /** Time of day in whole milliseconds since midnight. */
  case TimeToMillis[Row](expr: Expr[Row, Time]) extends Expr[Row, Long]

  /** Time of day in whole microseconds since midnight. */
  case TimeToMicros[Row](expr: Expr[Row, Time]) extends Expr[Row, Long]

  /** A Time from fractional seconds since midnight. */
  case TimeFromSeconds[Row](expr: Expr[Row, Double]) extends Expr[Row, Time]

  /** A Time from whole milliseconds since midnight. */
  case TimeFromMillis[Row](expr: Expr[Row, Long]) extends Expr[Row, Time]

  /** A Time from whole microseconds since midnight. */
  case TimeFromMicros[Row](expr: Expr[Row, Long]) extends Expr[Row, Time]

  /** SQL `timestamp_bucket` — the timestamp floored into buckets of the given interval, anchored at
    * `origin`.
    */
  case TimeBucket[Row](
    bucketSize: Expr[Row, DayTimeInterval],
    ts: Expr[Row, Timestamp],
    origin: Expr[Row, Timestamp]
  ) extends Expr[Row, Timestamp]

  /** The path of the input file being processed (Spark `current_path`). */
  case CurrentPath[Row]() extends Expr[Row, String]

  /** SQL `initcap` — first letter of each word upper-cased. */
  case Initcap[Row](expr: Expr[Row, String]) extends Expr[Row, String]

  /** SQL `instr` — 1-based position of the first occurrence of the substring (0 when absent). */
  case Instr[Row](str: Expr[Row, String], substr: Expr[Row, String]) extends Expr[Row, Int]

  /** SQL `substring_index` — the substring before (count > 0) or after (count < 0) the count-th
    * delimiter.
    */
  case SubstringIndex[Row](str: Expr[Row, String], delim: String, count: Int) extends Expr[Row, String]

  /** SQL `left` — the leftmost `n` characters. */
  case LeftStr[Row](str: Expr[Row, String], n: Expr[Row, Int]) extends Expr[Row, String]

  /** SQL `right` — the rightmost `n` characters. */
  case RightStr[Row](str: Expr[Row, String], n: Expr[Row, Int]) extends Expr[Row, String]

  /** SQL `repeat` — the string repeated `n` times. */
  case Repeat[Row](str: Expr[Row, String], n: Expr[Row, Int]) extends Expr[Row, String]

  /** SQL `reverse`. */
  case Reverse[Row](str: Expr[Row, String]) extends Expr[Row, String]

  /** SQL `lpad` — left-padded to `len` characters with `pad` (truncated when longer). */
  case Lpad[Row](str: Expr[Row, String], len: Expr[Row, Int], pad: String) extends Expr[Row, String]

  /** SQL `rpad` — right-padded to `len` characters with `pad` (truncated when longer). */
  case Rpad[Row](str: Expr[Row, String], len: Expr[Row, Int], pad: String) extends Expr[Row, String]

  /** SQL `translate` — character-wise substitution of `matching` by `replace`. */
  case Translate[Row](str: Expr[Row, String], matching: String, replace: String) extends Expr[Row, String]

  /** SQL `format_string` — printf-style formatting of the argument expressions. */
  case FormatString[Row](format: String, args: Vector[Expr[Row, Any]]) extends Expr[Row, String]

  /** SQL `ascii` — the first character's code point (0 for empty). */
  case Ascii[Row](str: Expr[Row, String]) extends Expr[Row, Int]

  /** SQL `char` — the single character for the code point. */
  case Chr[Row](expr: Expr[Row, Int]) extends Expr[Row, String]

  /** SQL `levenshtein` — edit distance between the two strings. */
  case Levenshtein[Row](left: Expr[Row, String], right: Expr[Row, String]) extends Expr[Row, Int]

  /** SQL `rlike` / `regexp` — full Java-regex match against the string. */
  case Rlike[Row](str: Expr[Row, String], pattern: String) extends Expr[Row, Boolean]

  /** SQL `regexp_extract_all` — all matches of the pattern, `groupIdx`-th group each. */
  case RegexpExtractAll[Row](str: Expr[Row, String], pattern: String, groupIdx: Int) extends Expr[Row, Seq[String]]

  /** SQL `split_part` — the 1-based part after splitting on the delimiter. */
  case SplitPart[Row](str: Expr[Row, String], delim: String, part: Expr[Row, Int]) extends Expr[Row, String]

  /** SQL `parse_url` — the named part (HOST, PATH, QUERY, ...) of the URL. */
  case ParseUrl[Row](url: Expr[Row, String], part: String) extends Expr[Row, String]

  /** SQL `to_char` — the number rendered with the given decimal format pattern. */
  case NumberToChar[Row](expr: Expr[Row, Double], format: String) extends Expr[Row, String]

  /** SQL `cbrt` — cube root. */
  case Cbrt[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `hypot` — sqrt(left^2 + right^2). */
  case Hypot[Row](left: Expr[Row, Double], right: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `bin` — the Long in binary (no leading zeros). */
  case Bin[Row](expr: Expr[Row, Long]) extends Expr[Row, String]

  /** SQL `unhex` — hexadecimal text decoded into binary. */
  case Unhex[Row](expr: Expr[Row, String]) extends Expr[Row, Binary]

  /** SQL `bround` — half-even (banker's) rounding to `scale` decimal places. */
  case Bround[Row](expr: Expr[Row, Double], scale: Int) extends Expr[Row, Double]

  /** SQL `conv` — the numeral string converted between bases. */
  case Conv[Row](num: Expr[Row, String], fromBase: Int, toBase: Int) extends Expr[Row, String]

  /** SQL `factorial`. */
  case Factorial[Row](expr: Expr[Row, Int]) extends Expr[Row, Long]

  /** SQL `sinh`. */
  case Sinh[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `cosh`. */
  case Cosh[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `tanh`. */
  case Tanh[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `asinh`. */
  case Asinh[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `acosh`. */
  case Acosh[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `atanh`. */
  case Atanh[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `degrees` — radians to degrees. */
  case Degrees[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `radians` — degrees to radians. */
  case Radians[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `pi()` — the constant. */
  case Pi[Row]() extends Expr[Row, Double]

  /** Euler's number as a constant expression. */
  case Euler[Row]() extends Expr[Row, Double]

  /** SQL `width_bucket` — the 1-based bucket index of the value across `buckets` equal-width
    * buckets spanning min..max.
    */
  case WidthBucket[Row](
    value: Expr[Row, Double],
    min: Expr[Row, Double],
    max: Expr[Row, Double],
    buckets: Expr[Row, Int]
  ) extends Expr[Row, Int]

  /** SQL `randn(seed)` — deterministic standard-normal sample. */
  case Randn[Row](seed: Long) extends Expr[Row, Double]

  /** SQL `pmod` — positive remainder (result has the divisor's sign). */
  case PmodInt[Row](left: Expr[Row, Int], right: Expr[Row, Int]) extends Expr[Row, Int]

  /** SQL `pmod` on Long — positive remainder. */
  case PmodLong[Row](left: Expr[Row, Long], right: Expr[Row, Long]) extends Expr[Row, Long]

  /** SQL `log1p` — ln(1 + x). */
  case Log1p[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `expm1` — e^x - 1. */
  case Expm1[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]

  /** Lenient Long addition — null instead of failure on Long overflow. */
  case TryAddLong[Row](left: Expr[Row, Long], right: Expr[Row, Long]) extends Expr[Row, Long]

  /** Lenient Long subtraction — null instead of failure on Long overflow. */
  case TrySubtractLong[Row](left: Expr[Row, Long], right: Expr[Row, Long]) extends Expr[Row, Long]

  /** Lenient Long multiplication — null instead of failure on Long overflow. */
  case TryMultiplyLong[Row](left: Expr[Row, Long], right: Expr[Row, Long]) extends Expr[Row, Long]

  /** Lenient Long division — null on a zero divisor (Spark `try_divide`). */
  case TryDivideLong[Row](left: Expr[Row, Long], right: Expr[Row, Long]) extends Expr[Row, Double]

  /** Lenient Double division — null on a zero divisor (Spark `try_divide`). */
  case TryDivideDouble[Row](left: Expr[Row, Double], right: Expr[Row, Double]) extends Expr[Row, Double]

  /** Lenient Int addition — null instead of failure on Int overflow. */
  case TryAddInt[Row](left: Expr[Row, Int], right: Expr[Row, Int]) extends Expr[Row, Int]

  /** SQL `unix_timestamp` — seconds since epoch. */
  case UnixTimestamp[Row](expr: Expr[Row, Timestamp]) extends Expr[Row, Long]

  /** SQL `from_unixtime` — the epoch seconds rendered as a timestamp string. */
  case FromUnixtime[Row](expr: Expr[Row, Long]) extends Expr[Row, String]

  /** SQL `to_timestamp` — the string parsed as a timestamp (fails the row when unparseable). */
  case ToTimestamp[Row](expr: Expr[Row, String]) extends Expr[Row, Timestamp]

  /** SQL `to_date` — the string parsed as a date (fails the row when unparseable). */
  case ToDate[Row](expr: Expr[Row, String]) extends Expr[Row, Date]

  /** SQL `current_date` — the evaluation-time date. */
  case CurrentDate[Row]() extends Expr[Row, Date]

  /** SQL `now` — the evaluation-time timestamp. */
  case Now[Row]() extends Expr[Row, Timestamp]

  /** SQL `timestamp_seconds` — a timestamp from fractional seconds. */
  case TimestampSeconds[Row](expr: Expr[Row, Double]) extends Expr[Row, Timestamp]

  /** SQL `timestamp_millis` — a timestamp from milliseconds since epoch. */
  case TimestampMillis[Row](expr: Expr[Row, Long]) extends Expr[Row, Timestamp]

  /** SQL `timestamp_micros` — a timestamp from microseconds since epoch. */
  case TimestampMicros[Row](expr: Expr[Row, Long]) extends Expr[Row, Timestamp]

  /** SQL `make_timestamp` — a timestamp from calendar and time-of-day parts. */
  case MakeTimestamp[Row](
    year: Expr[Row, Int],
    month: Expr[Row, Int],
    day: Expr[Row, Int],
    hour: Expr[Row, Int],
    minute: Expr[Row, Int],
    sec: Expr[Row, Double]
  ) extends Expr[Row, Timestamp]

  /** SQL `make_dt_interval` — a day-time interval from parts. */
  case MakeDtInterval[Row](
    days: Expr[Row, Long],
    hours: Expr[Row, Int],
    minutes: Expr[Row, Int],
    seconds: Expr[Row, Double]
  ) extends Expr[Row, DayTimeInterval]

  /** SQL `make_ym_interval` — a year-month interval from years and months. */
  case MakeYmInterval[Row](years: Expr[Row, Int], months: Expr[Row, Int]) extends Expr[Row, YearMonthInterval]

  /** SQL `hour`. */
  case HourOf[Row](expr: Expr[Row, Timestamp]) extends Expr[Row, Int]

  /** SQL `minute`. */
  case MinuteOf[Row](expr: Expr[Row, Timestamp]) extends Expr[Row, Int]

  /** SQL `second`. */
  case SecondOf[Row](expr: Expr[Row, Timestamp]) extends Expr[Row, Int]

  /** SQL `from_utc_timestamp` — the instant interpreted in UTC shifted to the named zone. */
  case FromUtcTimestamp[Row](expr: Expr[Row, Timestamp], tz: String) extends Expr[Row, Timestamp]

  /** SQL `to_utc_timestamp` — the local wall-clock time in the named zone re-interpreted as UTC. */
  case ToUtcTimestamp[Row](expr: Expr[Row, Timestamp], tz: String) extends Expr[Row, Timestamp]

  /** SQL `timestamp_add` — the timestamp shifted by `qty` of the named unit. */
  case TimestampAdd[Row](unit: String, qty: Expr[Row, Int], ts: Expr[Row, Timestamp]) extends Expr[Row, Timestamp]

  /** SQL `timestampdiff` — whole units from `start` to `end`. */
  case TimestampDiff[Row](unit: String, start: Expr[Row, Timestamp], end: Expr[Row, Timestamp]) extends Expr[Row, Long]

  /** SQL `convert_timezone` — the wall-clock time converted between the named zones. */
  case ConvertTimezone[Row](expr: Expr[Row, Timestamp], fromTz: String, toTz: String) extends Expr[Row, Timestamp]

  /** SQL `weekday` — 0 (Monday) through 6 (Sunday). */
  case Weekday[Row](expr: Expr[Row, Date]) extends Expr[Row, Int]

  /** SQL `array_append` — the element added at the end. */
  case ArrayAppend[Row, A](arr: Expr[Row, Seq[A]], elem: Expr[Row, A]) extends Expr[Row, Seq[A]]

  /** SQL `array_prepend` — the element added at the front. */
  case ArrayPrepend[Row, A](arr: Expr[Row, Seq[A]], elem: Expr[Row, A]) extends Expr[Row, Seq[A]]

  /** SQL `array_insert` — the element inserted at the 1-based position (appending past the end). */
  case ArrayInsert[Row, A](arr: Expr[Row, Seq[A]], pos: Expr[Row, Int], elem: Expr[Row, A]) extends Expr[Row, Seq[A]]

  /** SQL `array_remove` — all occurrences of the element removed. */
  case ArrayRemove[Row, A](arr: Expr[Row, Seq[A]], elem: Expr[Row, A]) extends Expr[Row, Seq[A]]

  /** SQL `array_repeat` — the element repeated `count` times. */
  case ArrayRepeat[Row, A](elem: Expr[Row, A], count: Expr[Row, Int]) extends Expr[Row, Seq[A]]

  /** SQL `array_join` — the elements joined with the delimiter (nulls replaced by nullReplacement
    * when given).
    */
  case ArrayJoin[Row](arr: Expr[Row, Seq[String]], delimiter: String, nullReplacement: Option[String])
      extends Expr[Row, String]

  /** SQL `array_max` — the largest element (null for an empty array). */
  case ArrayMax[Row, A](arr: Expr[Row, Seq[A]], ordering: Ordering[A]) extends Expr[Row, A]

  /** SQL `array_min` — the smallest element (null for an empty array). */
  case ArrayMin[Row, A](arr: Expr[Row, Seq[A]], ordering: Ordering[A]) extends Expr[Row, A]

  /** SQL `array_compact` — nulls removed. */
  case ArrayCompact[Row, A](arr: Expr[Row, Seq[A]]) extends Expr[Row, Seq[A]]

  /** SQL `array_position` — 1-based index of the first occurrence (0 when absent). */
  case ArrayPosition[Row, A](arr: Expr[Row, Seq[A]], elem: Expr[Row, A]) extends Expr[Row, Int]

  /** SQL `arrays_zip` — element-wise zip rendered as an array of objects keyed by position. */
  case ArraysZip[Row](arrays: Vector[Expr[Row, Seq[?]]]) extends Expr[Row, Seq[Map[String, Any]]]

  /** SQL `arrays_overlap` — whether the arrays share any element. */
  case ArraysOverlap[Row, A](left: Expr[Row, Seq[A]], right: Expr[Row, Seq[A]]) extends Expr[Row, Boolean]

  /** SQL `map_from_entries` — a map from an array of key-value pairs. */
  case MapFromEntries[Row, K, V](arr: Expr[Row, Seq[(K, V)]]) extends Expr[Row, Map[K, V]]

  /** SQL `get` — 0-based array element access (Spark `get`); out-of-range fails the row. */
  case GetArray[Row, A](arr: Expr[Row, Seq[A]], index: Expr[Row, Int]) extends Expr[Row, A]

  /** SQL `posexplode` — element with its 0-based position. */
  case Posexplode[Row](expr: Expr[Row, Seq[?]]) extends Expr[Row, Any]

  /** SQL `explode_outer` — one row per element, null row for empty arrays. */
  case ExplodeOuter[Row](expr: Expr[Row, Seq[?]]) extends Expr[Row, Any]

  /** SQL `inline` — explodes an array of structs into their fields. */
  case Inline[Row](expr: Expr[Row, Seq[?]]) extends Expr[Row, Any]

  /** SQL `regr_avgx` — mean of the independent values over non-null pairs. */
  case RegrAvgx[Row](y: Expr[Row, Double], x: Expr[Row, Double]) extends Expr[Row, Option[Double]]

  /** SQL `regr_avgy` — mean of the dependent values over non-null pairs. */
  case RegrAvgy[Row](y: Expr[Row, Double], x: Expr[Row, Double]) extends Expr[Row, Option[Double]]

  /** SQL `regr_count` — number of non-null pairs. */
  case RegrCount[Row](y: Expr[Row, Double], x: Expr[Row, Double]) extends Expr[Row, Option[Long]]

  /** SQL `regr_intercept` — the fitted line's intercept. */
  case RegrIntercept[Row](y: Expr[Row, Double], x: Expr[Row, Double]) extends Expr[Row, Option[Double]]

  /** SQL `regr_r2` — the coefficient of determination. */
  case RegrR2[Row](y: Expr[Row, Double], x: Expr[Row, Double]) extends Expr[Row, Option[Double]]

  /** SQL `regr_slope` — the fitted line's slope. */
  case RegrSlope[Row](y: Expr[Row, Double], x: Expr[Row, Double]) extends Expr[Row, Option[Double]]

  /** SQL `regr_sxx` — sum of squares of the independent values. */
  case RegrSxx[Row](y: Expr[Row, Double], x: Expr[Row, Double]) extends Expr[Row, Option[Double]]

  /** SQL `regr_sxy` — summed covariance terms. */
  case RegrSxy[Row](y: Expr[Row, Double], x: Expr[Row, Double]) extends Expr[Row, Option[Double]]

  /** SQL `regr_syy` — sum of squares of the dependent values. */
  case RegrSyy[Row](y: Expr[Row, Double], x: Expr[Row, Double]) extends Expr[Row, Option[Double]]

  /** SQL `kurtosis` — excess kurtosis of the group. */
  case Kurtosis[Row](expr: Expr[Row, Double]) extends Expr[Row, Option[Double]]

  /** SQL `skewness` — skewness of the group. */
  case Skewness[Row](expr: Expr[Row, Double]) extends Expr[Row, Option[Double]]

  /** SQL `percentile` — exact (interpolated) percentile; null for an empty group. */
  case Percentile[Row](expr: Expr[Row, Double], percentage: Expr[Row, Double]) extends Expr[Row, Option[Double]]

  /** SQL `sum(DISTINCT expr)` — null for an empty group. */
  case SumDistinct[Row](expr: Expr[Row, Long]) extends Expr[Row, Option[Long]]

  /** Spark-only: numeric histogram with at most nBins bins, as an array of (x, y) structs.
    *
    * Deliberate deviation from the parity plan: Spark's histogram algorithm is not reimplemented
    * in-memory; the in-memory interpreter returns Left(Unsupported).
    */
  case HistogramNumeric[Row](expr: Expr[Row, Double], nBins: Expr[Row, Int]) extends Expr[Row, Any]

  /** Spark-only: grouping indicator for GROUPING SETS / ROLLUP / CUBE.
    *
    * Deliberate deviation: Strongbow's model has no grouping-sets context, so the in-memory
    * interpreter returns Left(Unsupported).
    */
  case Grouping[Row](expr: Expr[Row, Any]) extends Expr[Row, Int]

  /** Spark-only: bit vector of grouping indicators over the given columns.
    *
    * Deliberate deviation: requires a grouping-sets context absent from Strongbow's model;
    * in-memory returns Left(Unsupported).
    */
  case GroupingId[Row](exprs: Vector[Expr[Row, Any]]) extends Expr[Row, Long]

  /** DDL schema of the JSON document, e.g. "STRUCT<a: BIGINT, b: STRING>". */
  case SchemaOfJson[Row](expr: Expr[Row, String]) extends Expr[Row, String]

  /** Number of elements in the JSON array at `path` (defaults to the root, "$"). */
  case JsonArrayLength[Row](expr: Expr[Row, String], path: String) extends Expr[Row, Long]

  /** Keys of the JSON object at `path` (defaults to the root, "$"), as an array of strings. */
  case JsonObjectKeys[Row](expr: Expr[Row, String], path: String) extends Expr[Row, Seq[String]]

  /** Parse a JSON string into T via a Sarati decoder.
    *
    * @param schema
    *   Spark DDL schema string used by the Spark backend (e.g. "STRUCT<id: BIGINT>").
    */
  case FromJson[Row, T](expr: Expr[Row, String], schema: String, decoder: Decoder[JsonValue, T]) extends Expr[Row, T]

  /** Serialize T to a JSON string via a Sarati encoder. */
  case ToJson[Row, T](expr: Expr[Row, T], encoder: Encoder[T, JsonValue]) extends Expr[Row, String]

  /** SQL `greatest` — the largest of the argument expressions. */
  case Greatest[Row, A](exprs: Vector[Expr[Row, A]], ordering: Ordering[A]) extends Expr[Row, A]

  /** SQL `least` — the smallest of the argument expressions. */
  case Least[Row, A](exprs: Vector[Expr[Row, A]], ordering: Ordering[A]) extends Expr[Row, A]

  /** SQL `nullif` — null when the operands are equal, otherwise the left operand. */
  case NullIf[Row, A](left: Expr[Row, A], right: Expr[Row, A]) extends Expr[Row, A]

  /** SQL `nvl2` — `value` when `test` is non-null, `alt` otherwise. */
  case Nvl2[Row, T, A](test: Expr[Row, T], value: Expr[Row, A], alt: Expr[Row, A]) extends Expr[Row, A]

  /** SQL `nanvl` — the left operand unless it is NaN, then the right operand. */
  case Nanvl[Row](left: Expr[Row, Double], right: Expr[Row, Double]) extends Expr[Row, Double]

  /** SQL `bit_count` — number of set bits. */
  case BitCount[Row](expr: Expr[Row, Int]) extends Expr[Row, Int]

  /** SQL `bit_get` — the bit at the 0-based position (0 or 1). */
  case BitGet[Row](expr: Expr[Row, Int], pos: Expr[Row, Int]) extends Expr[Row, Int]

  /** SQL `shiftleft`. */
  case ShiftLeft[Row](expr: Expr[Row, Int], n: Expr[Row, Int]) extends Expr[Row, Int]

  /** SQL `shiftright` — signed right shift. */
  case ShiftRight[Row](expr: Expr[Row, Int], n: Expr[Row, Int]) extends Expr[Row, Int]

  /** SQL `shiftrightunsigned` — logical right shift. */
  case ShiftRightUnsigned[Row](expr: Expr[Row, Int], n: Expr[Row, Int]) extends Expr[Row, Int]

  /** SQL `~` — bitwise complement. */
  case BitwiseNot[Row](expr: Expr[Row, Int]) extends Expr[Row, Int]

  /** SQL `bit_and` — aggregate bitwise AND. */
  case BitAndAgg[Row](expr: Expr[Row, Int]) extends Expr[Row, Int]

  /** SQL `bit_or` — aggregate bitwise OR. */
  case BitOrAgg[Row](expr: Expr[Row, Int]) extends Expr[Row, Int]

  /** SQL `bit_xor` — aggregate bitwise XOR. */
  case BitXorAgg[Row](expr: Expr[Row, Int]) extends Expr[Row, Int]

  /** Parses the JSON text into a variant value; malformed input fails the row. */
  case ParseJson[Row](expr: Expr[Row, String]) extends Expr[Row, Any]

  /** SQL `variant_get` — the variant value at the given path, asserted to the target type. */
  case VariantGet[Row](expr: Expr[Row, Any], path: String, targetType: String) extends Expr[Row, Any]

  /** Lenient variant_get — null on absent paths or type mismatches. */
  case TryVariantGet[Row](expr: Expr[Row, Any], path: String, targetType: String) extends Expr[Row, Any]

  /** SQL `is_variant_null` — whether the variant holds a JSON null. */
  case IsVariantNull[Row](expr: Expr[Row, Any]) extends Expr[Row, Boolean]

  /** SQL `schema_of_variant` — the variant's inferred schema in SQL DDL form. */
  case SchemaOfVariant[Row](expr: Expr[Row, Any]) extends Expr[Row, String]

  /** SQL `is_valid_variant`? — whether the string parses as a well-formed variant. */
  case IsValidVariant[Row](expr: Expr[Row, String]) extends Expr[Row, Boolean]

  /** SQL `variant_explode` — one row per top-level variant entry. */
  case VariantExplode[Row](expr: Expr[Row, Any]) extends Expr[Row, Any]

  /** Estimate of the distinct count from the named sketch family's binary (Spark-only,
    * DataSketches-backed).
    */
  case SketchEstimate[Row](fn: String, sketch: Expr[Row, Binary]) extends Expr[Row, Long]

  /** Summary statistic (min/max/mean/stddev by mode) from the named sketch family's binary
    * (Spark-only).
    */
  case SketchSummary[Row](fn: String, sketch: Expr[Row, Binary], mode: Option[String]) extends Expr[Row, Double]

  /** Theta estimate from the named sketch family's binary (Spark-only). */
  case SketchTheta[Row](fn: String, sketch: Expr[Row, Binary]) extends Expr[Row, Double]

  /** Combines two sketches of the named family — union or intersection per `fn` (Spark-only). */
  case SketchBinaryOp[Row](
    fn: String,
    left: Expr[Row, Binary],
    right: Expr[Row, Binary],
    lgNomEntries: Option[Int],
    mode: Option[String]
  ) extends Expr[Row, Binary]

  /** Aggregate building a tuple sketch over (key, summary) pairs under the named `fn` policy
    * (Spark-only).
    */
  case TupleSketchAgg[Row](
    fn: String,
    key: Expr[Row, ?],
    summary: Expr[Row, ?],
    lgNomEntries: Option[Int],
    mode: Option[String]
  ) extends Expr[Row, Binary]

  /** Aggregate merging the group's sketches under the named `fn` union/intersection policy
    * (Spark-only).
    */
  case SketchSetAgg[Row](
    fn: String,
    sketch: Expr[Row, Binary],
    lgNomEntries: Option[Int],
    mode: Option[String]
  ) extends Expr[Row, Binary]

  /** Aggregate building a KLL quantiles sketch over the group's values (Spark-only). */
  case KllSketchAgg[Row](fn: String, value: Expr[Row, ?], k: Option[Int]) extends Expr[Row, Binary]

  /** The value at the given rank from a KLL sketch (Spark-only). */
  case KllQuantile[Row](fn: String, sketch: Expr[Row, Binary], rank: Expr[Row, Double]) extends Expr[Row, Any]

  /** The rank of the given value in a KLL sketch (Spark-only). */
  case KllRank[Row](fn: String, sketch: Expr[Row, Binary], quantile: Expr[Row, ?]) extends Expr[Row, Double]

  /** Constructs a struct row value from named field expressions. */
  case Struct[Row, T](
    fields: Vector[(String, Expr[Row, ?], ColumnType)],
    schema: Schema[T]
  ) extends Expr[Row, T]

  /** Struct field access — the field selected by index, named for readability. */
  case GetField[Row, T, F](struct: Expr[Row, T], fieldIndex: ColumnIndex, fieldName: String) extends Expr[Row, F]
}

/** Smart constructors and the operator DSL over expressions. */
object Expr {

  /** A column reference by schema field name and position. */
  def cell[Row, A](name: String, index: ColumnIndex): Expr[Row, A] = {
    Cell(name, index)
  }

  /** A constant literal expression. */
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
  ): Expr[Row, Date] = {
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

    /** SQL `=` — equality. */
    def ===(right: Expr[Row, A]): Expr[Row, Boolean] = {
      Eq(left, right)
    }

    /** SQL `<>` — inequality. */
    def !==(right: Expr[Row, A]): Expr[Row, Boolean] = {
      Neq(left, right)
    }
  }

  extension [Row, A: Ordering](left: Expr[Row, A]) {

    /** SQL `>` — greater-than under the value's Ordering. */
    def >(right: Expr[Row, A]): Expr[Row, Boolean] = {
      Gt(left, right, summon[Ordering[A]])
    }

    /** SQL `>=` — greater-than-or-equal under the value's Ordering. */
    def >=(right: Expr[Row, A]): Expr[Row, Boolean] = {
      Gte(left, right, summon[Ordering[A]])
    }

    /** SQL `<` — less-than under the value's Ordering. */
    def <(right: Expr[Row, A]): Expr[Row, Boolean] = {
      Lt(left, right, summon[Ordering[A]])
    }

    /** SQL `<=` — less-than-or-equal under the value's Ordering. */
    def <=(right: Expr[Row, A]): Expr[Row, Boolean] = {
      Lte(left, right, summon[Ordering[A]])
    }
  }

  extension [Row](left: Expr[Row, Int]) {

    /** SQL `+` — integer addition. */
    inline def +(right: Expr[Row, Int]): Expr[Row, Int] = Add(left, right)

    /** SQL `-` — integer subtraction. */
    inline def -(right: Expr[Row, Int]): Expr[Row, Int] = Sub(left, right)

    /** SQL `*` — integer multiplication. */
    inline def *(right: Expr[Row, Int]): Expr[Row, Int] = Mul(left, right)

    /** SQL `/` — integer division; a zero divisor fails the row. */
    inline def /(right: Expr[Row, Int]): Expr[Row, Int] = Div(left, right)

    /** SQL `%` — integer remainder; a zero divisor fails the row. */
    @targetName("modInt")
    inline def %(right: Expr[Row, Int]): Expr[Row, Int] = Mod(left, right)

    /** SQL `abs`. */
    inline def abs: Expr[Row, Int] = Abs(left)

    /** SQL unary minus. */
    inline def negate: Expr[Row, Int] = Negate(left)

    /** SQL `CAST(expr AS BIGINT)` — Int to Long. */
    inline def castToLong: Expr[Row, Long] = CastToLong(left)

    /** SQL `CAST(expr AS DOUBLE)` — Int to Double. */
    @targetName("castIntToDouble")
    inline def castToDouble: Expr[Row, Double] = CastToDouble(left)
  }

  extension [Row](left: Expr[Row, Long]) {

    /** SQL `+` on Long — addition. */
    @targetName("addLong")
    inline def +(right: Expr[Row, Long]): Expr[Row, Long] = AddLong(left, right)

    /** SQL `-` on Long — subtraction. */
    @targetName("subLong")
    inline def -(right: Expr[Row, Long]): Expr[Row, Long] = SubLong(left, right)

    /** SQL `*` on Long — multiplication. */
    @targetName("mulLong")
    inline def *(right: Expr[Row, Long]): Expr[Row, Long] = MulLong(left, right)

    /** SQL `/` on Long — division; a zero divisor fails the row. */
    @targetName("divLong")
    inline def /(right: Expr[Row, Long]): Expr[Row, Long] = DivLong(left, right)

    /** SQL `%` on Long — remainder; a zero divisor fails the row. */
    @targetName("modLong")
    inline def %(right: Expr[Row, Long]): Expr[Row, Long] = ModLong(left, right)

    /** SQL `abs`. */
    @targetName("absLong")
    inline def abs: Expr[Row, Long] = AbsLong(left)

    /** SQL unary minus. */
    @targetName("negateLong")
    inline def negate: Expr[Row, Long] = NegateLong(left)

    /** SQL `CAST(expr AS DOUBLE)` — Long to Double. */
    inline def castToDouble: Expr[Row, Double] = CastLongToDouble(left)
  }

  extension [Row](left: Expr[Row, Double]) {

    /** SQL `+` on Double — addition. */
    @targetName("addDouble")
    inline def +(right: Expr[Row, Double]): Expr[Row, Double] = AddDouble(left, right)

    /** SQL `-` on Double — subtraction. */
    @targetName("subDouble")
    inline def -(right: Expr[Row, Double]): Expr[Row, Double] = SubDouble(left, right)

    /** SQL `*` on Double — multiplication. */
    @targetName("mulDouble")
    inline def *(right: Expr[Row, Double]): Expr[Row, Double] = MulDouble(left, right)

    /** SQL `/` on Double — division. */
    @targetName("divDouble")
    inline def /(right: Expr[Row, Double]): Expr[Row, Double] = DivDouble(left, right)

    /** SQL `abs`. */
    @targetName("absDouble")
    inline def abs: Expr[Row, Double] = AbsDouble(left)

    /** SQL unary minus. */
    @targetName("negateDouble")
    inline def negate: Expr[Row, Double] = NegateDouble(left)

    /** SQL `round(expr, scale)` — half-up rounding. */
    inline def round(scale: Int): Expr[Row, Double] = Round(left, scale)

    /** SQL `floor`. */
    inline def floor: Expr[Row, Double] = Floor(left)

    /** SQL `ceil`. */
    inline def ceil: Expr[Row, Double] = Ceil(left)

    /** SQL `sqrt`. */
    inline def sqrt: Expr[Row, Double] = Sqrt(left)

    /** SQL `pow`. */
    inline def pow(exponent: Expr[Row, Double]): Expr[Row, Double] = Pow(left, exponent)

    /** SQL `ln` — natural logarithm. */
    inline def log: Expr[Row, Double] = Log(left)

    /** SQL `log10`. */
    inline def log10: Expr[Row, Double] = Log10(left)

    /** SQL `log2`. */
    inline def log2: Expr[Row, Double] = Log2(left)

    /** SQL `exp`. */
    inline def exp: Expr[Row, Double] = Exp(left)

    /** SQL `sin`. */
    inline def sin: Expr[Row, Double] = Sin(left)

    /** SQL `cos`. */
    inline def cos: Expr[Row, Double] = Cos(left)

    /** SQL `tan`. */
    inline def tan: Expr[Row, Double] = Tan(left)

    /** SQL `asin`. */
    inline def asin: Expr[Row, Double] = Asin(left)

    /** SQL `acos`. */
    inline def acos: Expr[Row, Double] = Acos(left)

    /** SQL `atan`. */
    inline def atan: Expr[Row, Double] = Atan(left)

    /** SQL `atan2` — angle of the point (x, left). */
    inline def atan2(x: Expr[Row, Double]): Expr[Row, Double] = Atan2(left, x)

    /** SQL `signum`. */
    inline def signum: Expr[Row, Double] = Signum(left)
  }

  extension [Row](left: Expr[Row, Boolean]) {

    /** SQL `AND`. */
    inline def &&(right: Expr[Row, Boolean]): Expr[Row, Boolean] = And(left, right)

    /** SQL `OR`. */
    inline def ||(right: Expr[Row, Boolean]): Expr[Row, Boolean] = Or(left, right)

    /** SQL `NOT` — negates the predicate. */
    inline def unary_! : Expr[Row, Boolean] = Not(left)
  }

  extension [Row](left: Expr[Row, String]) {

    /** SQL `concat` — string concatenation. */
    inline def ++(right: Expr[Row, String]): Expr[Row, String] = Concat(left, right)

    /** SQL `length`. */
    inline def length: Expr[Row, Int] = Length(left)

    /** SQL `LIKE` with `%` and `_` wildcards. */
    inline def like(pattern: String): Expr[Row, Boolean] = Like(left, pattern)

    /** SQL `lower`. */
    inline def lower: Expr[Row, String] = Lower(left)

    /** SQL `upper`. */
    inline def upper: Expr[Row, String] = Upper(left)

    /** SQL `trim`. */
    inline def trim: Expr[Row, String] = Trim(left)

    /** SQL `ltrim`. */
    inline def ltrim: Expr[Row, String] = LTrim(left)

    /** SQL `rtrim`. */
    inline def rtrim: Expr[Row, String] = RTrim(left)

    /** SQL `substring(expr, pos, len)` — 1-based. */
    inline def substring(pos: Int, len: Int): Expr[Row, String] = Substring(left, pos, len)

    /** SQL `replace`. */
    inline def replace(search: String, replacement: String): Expr[Row, String] =
      StringReplace(left, search, replacement)

    /** SQL `regexp_replace`. */
    inline def regexpReplace(pattern: String, replacement: String): Expr[Row, String] =
      RegexpReplace(left, pattern, replacement)

    /** SQL `regexp_extract` — the `groupIdx`-th group of the first match. */
    inline def regexpExtract(pattern: String, groupIdx: Int): Expr[Row, String] = RegexpExtract(left, pattern, groupIdx)

    /** SQL `split` on the delimiter (a Java regex). */
    inline def split(delimiter: String): Expr[Row, Seq[String]] = StringSplit(left, delimiter)

    /** SQL `startswith`. */
    inline def startsWith(prefix: Expr[Row, String]): Expr[Row, Boolean] = StartsWith(left, prefix)

    /** SQL `endswith`. */
    inline def endsWith(suffix: Expr[Row, String]): Expr[Row, Boolean] = EndsWith(left, suffix)

    /** SQL `contains`. */
    inline def contains(substr: Expr[Row, String]): Expr[Row, Boolean] = StringContains(left, substr)

    /** SQL `md5`. */
    inline def md5: Expr[Row, String] = Md5(left)

    /** SQL `sha1`. */
    inline def sha1: Expr[Row, String] = Sha1(left)

    /** SQL `sha2` with the given bit length. */
    inline def sha2(bitLength: Int): Expr[Row, String] = Sha2(left, bitLength)

    /** SQL `url_encode`. */
    inline def urlEncode: Expr[Row, String] = UrlEncode(left)

    /** SQL `url_decode`. */
    inline def urlDecode: Expr[Row, String] = UrlDecode(left)

    /** SQL `base64`. */
    inline def base64Encode: Expr[Row, String] = Base64Encode(left)

    /** SQL `unbase64`. */
    inline def base64Decode: Expr[Row, String] = Base64Decode(left)

    /** SQL `hex`. */
    inline def hex: Expr[Row, String] = Hex(left)

    /** SQL `crc32`. */
    inline def crc32: Expr[Row, Long] = Crc32(left)

    /** SQL `xxhash64`. */
    inline def xxhash64: Expr[Row, Long] = XxHash64(left)

    /** SQL `hash`. */
    inline def hash: Expr[Row, Int] = Hash(left)

    /** SQL `aes_encrypt` under the key. */
    inline def aesEncrypt(key: Expr[Row, String]): Expr[Row, Binary] = AesEncrypt(left, key)

    /** SQL `aes_decrypt` under the key; wrong keys fail the row. */
    inline def aesDecrypt(key: Expr[Row, String]): Expr[Row, String] = AesDecrypt(left, key)

    /** Lenient aes_decrypt — null instead of failure on undecryptable input. */
    inline def tryAesDecrypt(key: Expr[Row, String]): Expr[Row, String] = TryAesDecrypt(left, key)

    /** SQL `get_json_object`. */
    inline def getJsonObject(path: String): Expr[Row, String] = GetJsonObject(left, path)

    /** SQL `xpath` — the node-set as an array of string values. */
    inline def xpath(path: String): Expr[Row, Seq[String | Null]] = Xpath(left, path)

    /** SQL `xpath_string` — the first matching node's string value. */
    inline def xpathString(path: String): Expr[Row, String] = XpathString(left, path)

    /** SQL `xpath_boolean`. */
    inline def xpathBoolean(path: String): Expr[Row, Boolean] = XpathBoolean(left, path)

    /** SQL `xpath_short`. */
    inline def xpathShort(path: String): Expr[Row, Short] = XpathShort(left, path)

    /** SQL `xpath_int`. */
    inline def xpathInt(path: String): Expr[Row, Int] = XpathInt(left, path)

    /** SQL `xpath_long`. */
    inline def xpathLong(path: String): Expr[Row, Long] = XpathLong(left, path)

    /** SQL `xpath_float`. */
    inline def xpathFloat(path: String): Expr[Row, Float] = XpathFloat(left, path)

    /** SQL `xpath_double` — NaN when the result is not numeric. */
    inline def xpathDouble(path: String): Expr[Row, Double] = XpathDouble(left, path)

    /** SQL `xpath_double` — numeric result as Double. */
    inline def xpathNumber(path: String): Expr[Row, Double] = XpathDouble(left, path)

    /** In-memory lenient xpath — null on malformed input or failed evaluation. */
    inline def tryXpath(path: String): Expr[Row, Seq[String | Null]] = TryXpath(left, path)

    /** In-memory lenient xpath_string. */
    inline def tryXpathString(path: String): Expr[Row, String] = TryXpathString(left, path)

    /** In-memory lenient xpath_boolean. */
    inline def tryXpathBoolean(path: String): Expr[Row, Boolean] = TryXpathBoolean(left, path)

    /** In-memory lenient xpath_short. */
    inline def tryXpathShort(path: String): Expr[Row, Short] = TryXpathShort(left, path)

    /** In-memory lenient xpath_int. */
    inline def tryXpathInt(path: String): Expr[Row, Int] = TryXpathInt(left, path)

    /** In-memory lenient xpath_long. */
    inline def tryXpathLong(path: String): Expr[Row, Long] = TryXpathLong(left, path)

    /** In-memory lenient xpath_float. */
    inline def tryXpathFloat(path: String): Expr[Row, Float] = TryXpathFloat(left, path)

    /** In-memory lenient xpath_double. */
    inline def tryXpathDouble(path: String): Expr[Row, Double] = TryXpathDouble(left, path)

    /** In-memory lenient xpath_double — numeric result as Double. */
    inline def tryXpathNumber(path: String): Expr[Row, Double] = TryXpathDouble(left, path)
  }

  extension [Row, A](e: Expr[Row, Option[A]]) {

    /** Scala `isDefined` on the Option column. */
    inline def isDefined: Expr[Row, Boolean] = IsDefined(e)

    /** Scala `getOrElse` on the Option column. */
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

    /** SQL `IS NULL`. */
    inline def isNull: Expr[Row, Boolean] = IsNull(left)

    /** SQL `IS NOT NULL`. */
    inline def isNotNull: Expr[Row, Boolean] = IsNotNull(left)

    /** SQL `IN` — membership in the value list. */
    inline def in(values: Vector[A]): Expr[Row, Boolean] = In(left, values)

    /** SQL `CAST(expr AS STRING)`. */
    inline def castToString: Expr[Row, String] = CastToString(left)
  }

  extension [Row, A: Ordering](left: Expr[Row, A]) {

    /** SQL `BETWEEN lower AND upper` — inclusive range test. */
    @targetName("betweenOrdered")
    inline def between(lower: Expr[Row, A], upper: Expr[Row, A]): Expr[Row, Boolean] =
      Between(left, lower, upper, summon[Ordering[A]])
  }

  extension [Row](t: Expr[Row, Time]) {

    /** Time of day as fractional seconds (SQL DECIMAL). */
    inline def timeToSeconds: Expr[Row, Decimal] = TimeToSeconds(t)

    /** Time of day in whole milliseconds since midnight. */
    inline def timeToMillis: Expr[Row, Long] = TimeToMillis(t)

    /** Time of day in whole microseconds since midnight. */
    inline def timeToMicros: Expr[Row, Long] = TimeToMicros(t)
  }

  /** Create a TIME from seconds since midnight (fractional seconds allowed). */
  def timeFromSeconds[Row](seconds: Expr[Row, Double]): Expr[Row, Time] = TimeFromSeconds(seconds)

  /** Create a TIME from milliseconds since midnight. */
  def timeFromMillis[Row](millis: Expr[Row, Long]): Expr[Row, Time] = TimeFromMillis(millis)

  /** Create a TIME from microseconds since midnight. */
  def timeFromMicros[Row](micros: Expr[Row, Long]): Expr[Row, Time] = TimeFromMicros(micros)

  /** Bucket a timestamp into fixed-size day-time interval buckets aligned to `origin`. */
  def timeBucket[Row](
    bucketSize: Expr[Row, DayTimeInterval],
    ts: Expr[Row, Timestamp],
    origin: Expr[Row, Timestamp]
  ): Expr[Row, Timestamp] = TimeBucket(bucketSize, ts, origin)

  extension [Row](d: Expr[Row, Date]) {

    /** SQL `date_add`. */
    inline def addDays(days: Expr[Row, Int]): Expr[Row, Date] = DateAddDays(d, days)

    /** SQL `date_sub`. */
    inline def subDays(days: Expr[Row, Int]): Expr[Row, Date] = DateSubDays(d, days)

    /** SQL `add_months`. */
    inline def addMonths(months: Expr[Row, Int]): Expr[Row, Date] = DateAddMonths(d, months)

    /** SQL `datediff` — whole days to `other`. */
    inline def dateDiff(other: Expr[Row, Date]): Expr[Row, Int] = DateDiff(d, other)

    /** SQL `year`. */
    inline def year: Expr[Row, Int] = ExtractYear(d)

    /** SQL `month`. */
    inline def month: Expr[Row, Int] = ExtractMonth(d)

    /** SQL `day`. */
    inline def day: Expr[Row, Int] = ExtractDay(d)

    /** SQL `dayofweek` — 1 (Sunday) through 7 (Saturday). */
    inline def dayOfWeek: Expr[Row, Int] = DayOfWeek(d)

    /** SQL `dayofyear`. */
    inline def dayOfYear: Expr[Row, Int] = DayOfYear(d)

    /** SQL `weekofyear` — ISO week number. */
    inline def weekOfYear: Expr[Row, Int] = WeekOfYear(d)

    /** SQL `quarter`. */
    inline def quarter: Expr[Row, Int] = Quarter(d)

    /** SQL `last_day`. */
    inline def lastDay: Expr[Row, Date] = LastDay(d)

    /** SQL `next_day` — the next date on the named weekday. */
    inline def nextDay(dayOfWeek: String): Expr[Row, Date] = NextDay(d, dayOfWeek)

    /** SQL `months_between`. */
    inline def monthsBetween(other: Expr[Row, Date]): Expr[Row, Double] = MonthsBetween(d, other)

    /** SQL `date_trunc` to the named unit. */
    inline def dateTrunc(unit: String): Expr[Row, Date] = DateTrunc(unit, d)

    /** SQL `date_format` with the given pattern. */
    inline def dateFormat(format: String): Expr[Row, String] = DateFormat(d, format)
  }

  extension [Row, A](e: Expr[Row, Seq[A]]) {

    /** SQL `size`. */
    inline def arraySize: Expr[Row, Int] = ArraySize(e)

    /** SQL `array_contains`. */
    inline def arrayContains(value: Expr[Row, A]): Expr[Row, Boolean] = ArrayContains(e, value)

    /** SQL `explode`. */
    inline def explode: Expr[Row, A] = Explode(e)

    /** SQL `array_sort` — ascending under the element ordering. */
    inline def arraySort(using ordering: Ordering[A]): Expr[Row, Seq[A]] = ArraySort(e, ordering)

    /** SQL `array_distinct`. */
    inline def arrayDistinct: Expr[Row, Seq[A]] = ArrayDistinct(e)

    /** SQL `array_union`. */
    inline def arrayUnion(other: Expr[Row, Seq[A]]): Expr[Row, Seq[A]] = ArrayUnion(e, other)

    /** SQL `array_intersect`. */
    inline def arrayIntersect(other: Expr[Row, Seq[A]]): Expr[Row, Seq[A]] = ArrayIntersect(e, other)

    /** SQL `array_except`. */
    inline def arrayExcept(other: Expr[Row, Seq[A]]): Expr[Row, Seq[A]] = ArrayExcept(e, other)

    /** SQL `element_at` — 1-based; out-of-range fails the row. */
    inline def elementAt(index: Expr[Row, Int]): Expr[Row, A] = ElementAt(e, index)

    /** SQL `slice` — `length` elements from the 1-based `start`. */
    inline def arraySlice(start: Int, length: Int): Expr[Row, Seq[A]] = ArraySlice(e, start, length)

    /** SQL `transform` — element-wise mapping; the body's element variable is bound by position. */
    def transform[B](body: Expr[Row, A] => Expr[Row, B]): Expr[Row, Seq[B]] = {
      val binder = Binder[A]()
      Transform(e, binder, None, body(LambdaVar(binder)))
    }

    /** SQL `transform` — element-wise mapping with the 0-based element index. */
    def transform[B](body: (Expr[Row, A], Expr[Row, Int]) => Expr[Row, B]): Expr[Row, Seq[B]] = {
      val binder = Binder[A]()
      val indexBinder = Binder[Int]()
      Transform(e, binder, Some(indexBinder), body(LambdaVar(binder), LambdaVar(indexBinder)))
    }

    /** SQL `filter` — keeps elements where the body predicate holds. */
    def filter(body: Expr[Row, A] => Expr[Row, Boolean]): Expr[Row, Seq[A]] = {
      val binder = Binder[A]()
      Filter(e, binder, None, body(LambdaVar(binder)))
    }

    /** SQL `filter` — keeps elements where the body predicate holds, with the 0-based index. */
    def filter(body: (Expr[Row, A], Expr[Row, Int]) => Expr[Row, Boolean]): Expr[Row, Seq[A]] = {
      val binder = Binder[A]()
      val indexBinder = Binder[Int]()
      Filter(e, binder, Some(indexBinder), body(LambdaVar(binder), LambdaVar(indexBinder)))
    }

    /** SQL `exists` — true when any element satisfies the body predicate. */
    def exists(body: Expr[Row, A] => Expr[Row, Boolean]): Expr[Row, Boolean] = {
      val binder = Binder[A]()
      Exists(e, binder, body(LambdaVar(binder)))
    }

    /** SQL `forall` — true when every element satisfies the body predicate. */
    def forall(body: Expr[Row, A] => Expr[Row, Boolean]): Expr[Row, Boolean] = {
      val binder = Binder[A]()
      ForAll(e, binder, body(LambdaVar(binder)))
    }

    /** SQL `aggregate` — left fold over the elements, threading the accumulator. */
    def aggregate[S](zero: Expr[Row, S])(merge: (Expr[Row, S], Expr[Row, A]) => Expr[Row, S]): Expr[Row, S] = {
      val accBinder = Binder[S]()
      val elemBinder = Binder[A]()
      Aggregate(e, zero, accBinder, elemBinder, merge(LambdaVar(accBinder), LambdaVar(elemBinder)), None)
    }

    /** SQL `aggregate` — fold with an index-aware merge function. */
    def aggregate[S](
      zero: Expr[Row, S]
    )(
      merge: (Expr[Row, S], Expr[Row, A]) => Expr[Row, S],
      finish: Expr[Row, S] => Expr[Row, S]
    ): Expr[Row, S] = {
      val accBinder = Binder[S]()
      val elemBinder = Binder[A]()
      val finishBinder = Binder[S]()
      Aggregate(
        e,
        zero,
        accBinder,
        elemBinder,
        merge(LambdaVar(accBinder), LambdaVar(elemBinder)),
        Some((finishBinder, finish(LambdaVar(finishBinder))))
      )
    }

    /** SQL `zip_with` — element-wise combination of two arrays with the merge function. */
    def zipWith[B, C](
      other: Expr[Row, Seq[B]]
    )(body: (Expr[Row, Option[A]], Expr[Row, Option[B]]) => Expr[Row, C]): Expr[Row, Seq[C]] = {
      val leftBinder = Binder[Option[A]]()
      val rightBinder = Binder[Option[B]]()
      ZipWith(e, other, leftBinder, rightBinder, body(LambdaVar(leftBinder), LambdaVar(rightBinder)))
    }

    /** SQL `array_sort(comparator)` — orders by the comparator's negative/zero/positive result. */
    def arraySortBy(body: (Expr[Row, A], Expr[Row, A]) => Expr[Row, Int]): Expr[Row, Seq[A]] = {
      val leftBinder = Binder[A]()
      val rightBinder = Binder[A]()
      ArraySortComparator(e, leftBinder, rightBinder, body(LambdaVar(leftBinder), LambdaVar(rightBinder)))
    }
  }

  extension [Row, A](e: Expr[Row, Seq[Seq[A]]]) {

    /** SQL `flatten`. */
    inline def flatten: Expr[Row, Seq[A]] = Flatten(e)
  }

  extension [Row, K, V](e: Expr[Row, Map[K, V]]) {

    /** SQL `map_keys`. */
    inline def mapKeys: Expr[Row, Seq[K]] = MapKeys(e)

    /** SQL `map_values`. */
    inline def mapValues: Expr[Row, Seq[V]] = MapValues(e)

    /** SQL `map_contains_key`. */
    inline def mapContainsKey(key: Expr[Row, K]): Expr[Row, Boolean] = MapContainsKey(e, key)

    /** SQL `map_entries`. */
    inline def mapEntries: Expr[Row, Seq[(K, V)]] = MapEntries(e)

    /** SQL `map_concat` — right side winning on key conflicts. */
    inline def mapConcat(other: Expr[Row, Map[K, V]]): Expr[Row, Map[K, V]] = MapConcat(e, other)

    /** SQL `map_filter` — keeps entries where the body predicate holds. */
    def mapFilter(body: (Expr[Row, K], Expr[Row, V]) => Expr[Row, Boolean]): Expr[Row, Map[K, V]] = {
      val keyBinder = Binder[K]()
      val valueBinder = Binder[V]()
      MapFilter(e, keyBinder, valueBinder, body(LambdaVar(keyBinder), LambdaVar(valueBinder)))
    }

    /** SQL `transform_keys`. */
    def transformKeys[K2](body: (Expr[Row, K], Expr[Row, V]) => Expr[Row, K2]): Expr[Row, Map[K2, V]] = {
      val keyBinder = Binder[K]()
      val valueBinder = Binder[V]()
      TransformKeys(e, keyBinder, valueBinder, body(LambdaVar(keyBinder), LambdaVar(valueBinder)))
    }

    /** SQL `transform_values`. */
    def transformValues[V2](body: (Expr[Row, K], Expr[Row, V]) => Expr[Row, V2]): Expr[Row, Map[K, V2]] = {
      val keyBinder = Binder[K]()
      val valueBinder = Binder[V]()
      TransformValues(e, keyBinder, valueBinder, body(LambdaVar(keyBinder), LambdaVar(valueBinder)))
    }

    /** SQL `map_zip_with` — merges two maps with the merge function over (key, value, otherValue).
      */
    def mapZipWith[V2, C](other: Expr[Row, Map[K, V2]])(
      body: (Expr[Row, K], Expr[Row, Option[V]], Expr[Row, Option[V2]]) => Expr[Row, C]
    ): Expr[Row, Map[K, C]] = {
      val keyBinder = Binder[K]()
      val leftBinder = Binder[Option[V]]()
      val rightBinder = Binder[Option[V2]]()
      MapZipWith(
        e,
        other,
        keyBinder,
        leftBinder,
        rightBinder,
        body(LambdaVar(keyBinder), LambdaVar(leftBinder), LambdaVar(rightBinder))
      )
    }
  }

  /** The path of the input file being processed (Spark `current_path`). */
  def currentPath[Row](): Expr[Row, String] = CurrentPath()

  /** Tuple sketch aggregate over double keys and summaries (Spark-only). */
  def tupleSketchAggDouble[Row](
    key: Expr[Row, ?],
    summary: Expr[Row, ?],
    lgNomEntries: Option[Int] = None,
    mode: Option[String] = None
  ): Expr[Row, Binary] = TupleSketchAgg("tuple_sketch_agg_double", key, summary, lgNomEntries, mode)

  /** Tuple sketch aggregate over integer keys and summaries (Spark-only). */
  def tupleSketchAggInteger[Row](
    key: Expr[Row, ?],
    summary: Expr[Row, ?],
    lgNomEntries: Option[Int] = None,
    mode: Option[String] = None
  ): Expr[Row, Binary] = TupleSketchAgg("tuple_sketch_agg_integer", key, summary, lgNomEntries, mode)

  /** Distinct-count estimate from a tuple sketch over doubles (Spark-only). */
  def tupleSketchEstimateDouble[Row](sketch: Expr[Row, Binary]): Expr[Row, Long] =
    SketchEstimate("tuple_sketch_estimate_double", sketch)

  /** Distinct-count estimate from a tuple sketch over integers (Spark-only). */
  def tupleSketchEstimateInteger[Row](sketch: Expr[Row, Binary]): Expr[Row, Long] =
    SketchEstimate("tuple_sketch_estimate_integer", sketch)

  /** Summary statistic from a tuple sketch over doubles (Spark-only). */
  def tupleSketchSummaryDouble[Row](sketch: Expr[Row, Binary], mode: Option[String] = None): Expr[Row, Double] =
    SketchSummary("tuple_sketch_summary_double", sketch, mode)

  /** Summary statistic from a tuple sketch over integers (Spark-only). */
  def tupleSketchSummaryInteger[Row](sketch: Expr[Row, Binary], mode: Option[String] = None): Expr[Row, Double] =
    SketchSummary("tuple_sketch_summary_integer", sketch, mode)

  /** Theta estimate from a tuple sketch over doubles (Spark-only). */
  def tupleSketchThetaDouble[Row](sketch: Expr[Row, Binary]): Expr[Row, Double] =
    SketchTheta("tuple_sketch_theta_double", sketch)

  /** Theta estimate from a tuple sketch over integers (Spark-only). */
  def tupleSketchThetaInteger[Row](sketch: Expr[Row, Binary]): Expr[Row, Double] =
    SketchTheta("tuple_sketch_theta_integer", sketch)

  /** Aggregate unioning tuple sketches over doubles (Spark-only). */
  def tupleUnionAggDouble[Row](
    sketch: Expr[Row, Binary],
    lgNomEntries: Option[Int] = None,
    mode: Option[String] = None
  ): Expr[Row, Binary] = SketchSetAgg("tuple_union_agg_double", sketch, lgNomEntries, mode)

  /** Aggregate unioning tuple sketches over integers (Spark-only). */
  def tupleUnionAggInteger[Row](
    sketch: Expr[Row, Binary],
    lgNomEntries: Option[Int] = None,
    mode: Option[String] = None
  ): Expr[Row, Binary] = SketchSetAgg("tuple_union_agg_integer", sketch, lgNomEntries, mode)

  /** Aggregate intersecting tuple sketches over doubles (Spark-only). */
  def tupleIntersectionAggDouble[Row](sketch: Expr[Row, Binary], mode: Option[String] = None): Expr[Row, Binary] =
    SketchSetAgg("tuple_intersection_agg_double", sketch, None, mode)

  /** Aggregate intersecting tuple sketches over integers (Spark-only). */
  def tupleIntersectionAggInteger[Row](sketch: Expr[Row, Binary], mode: Option[String] = None): Expr[Row, Binary] =
    SketchSetAgg("tuple_intersection_agg_integer", sketch, None, mode)

  /** Row-wise union of two tuple sketches over doubles (Spark-only). */
  def tupleUnionDouble[Row](
    left: Expr[Row, Binary],
    right: Expr[Row, Binary],
    lgNomEntries: Option[Int] = None,
    mode: Option[String] = None
  ): Expr[Row, Binary] = SketchBinaryOp("tuple_union_double", left, right, lgNomEntries, mode)

  /** Row-wise union of two tuple sketches over integers (Spark-only). */
  def tupleUnionInteger[Row](
    left: Expr[Row, Binary],
    right: Expr[Row, Binary],
    lgNomEntries: Option[Int] = None,
    mode: Option[String] = None
  ): Expr[Row, Binary] = SketchBinaryOp("tuple_union_integer", left, right, lgNomEntries, mode)

  /** Row-wise union of two theta sketches over doubles (Spark-only). */
  def tupleUnionThetaDouble[Row](
    left: Expr[Row, Binary],
    right: Expr[Row, Binary],
    lgNomEntries: Option[Int] = None,
    mode: Option[String] = None
  ): Expr[Row, Binary] = SketchBinaryOp("tuple_union_theta_double", left, right, lgNomEntries, mode)

  /** Row-wise union of two theta sketches over integers (Spark-only). */
  def tupleUnionThetaInteger[Row](
    left: Expr[Row, Binary],
    right: Expr[Row, Binary],
    lgNomEntries: Option[Int] = None,
    mode: Option[String] = None
  ): Expr[Row, Binary] = SketchBinaryOp("tuple_union_theta_integer", left, right, lgNomEntries, mode)

  /** Row-wise intersection of two tuple sketches over doubles (Spark-only). */
  def tupleIntersectionDouble[Row](
    left: Expr[Row, Binary],
    right: Expr[Row, Binary],
    mode: Option[String] = None
  ): Expr[Row, Binary] = SketchBinaryOp("tuple_intersection_double", left, right, None, mode)

  /** Row-wise intersection of two tuple sketches over integers (Spark-only). */
  def tupleIntersectionInteger[Row](
    left: Expr[Row, Binary],
    right: Expr[Row, Binary],
    mode: Option[String] = None
  ): Expr[Row, Binary] = SketchBinaryOp("tuple_intersection_integer", left, right, None, mode)

  /** Row-wise intersection of two theta sketches over doubles (Spark-only). */
  def tupleIntersectionThetaDouble[Row](
    left: Expr[Row, Binary],
    right: Expr[Row, Binary],
    mode: Option[String] = None
  ): Expr[Row, Binary] = SketchBinaryOp("tuple_intersection_theta_double", left, right, None, mode)

  /** Row-wise intersection of two theta sketches over integers (Spark-only). */
  def tupleIntersectionThetaInteger[Row](
    left: Expr[Row, Binary],
    right: Expr[Row, Binary],
    mode: Option[String] = None
  ): Expr[Row, Binary] = SketchBinaryOp("tuple_intersection_theta_integer", left, right, None, mode)

  /** Row-wise difference (left minus right) of two tuple sketches over doubles (Spark-only). */
  def tupleDifferenceDouble[Row](left: Expr[Row, Binary], right: Expr[Row, Binary]): Expr[Row, Binary] =
    SketchBinaryOp("tuple_difference_double", left, right, None, None)

  /** Row-wise difference (left minus right) of two tuple sketches over integers (Spark-only). */
  def tupleDifferenceInteger[Row](left: Expr[Row, Binary], right: Expr[Row, Binary]): Expr[Row, Binary] =
    SketchBinaryOp("tuple_difference_integer", left, right, None, None)

  /** Row-wise theta difference of two tuple sketches over doubles (Spark-only). */
  def tupleDifferenceThetaDouble[Row](left: Expr[Row, Binary], right: Expr[Row, Binary]): Expr[Row, Binary] =
    SketchBinaryOp("tuple_difference_theta_double", left, right, None, None)

  /** Row-wise theta difference of two tuple sketches over integers (Spark-only). */
  def tupleDifferenceThetaInteger[Row](left: Expr[Row, Binary], right: Expr[Row, Binary]): Expr[Row, Binary] =
    SketchBinaryOp("tuple_difference_theta_integer", left, right, None, None)

  /** KLL quantiles sketch aggregate over bigints (Spark-only). */
  def kllSketchAggBigint[Row](value: Expr[Row, ?], k: Option[Int] = None): Expr[Row, Binary] =
    KllSketchAgg("kll_sketch_agg_bigint", value, k)

  /** KLL quantiles sketch aggregate over floats (Spark-only). */
  def kllSketchAggFloat[Row](value: Expr[Row, ?], k: Option[Int] = None): Expr[Row, Binary] =
    KllSketchAgg("kll_sketch_agg_float", value, k)

  /** KLL quantiles sketch aggregate over doubles (Spark-only). */
  def kllSketchAggDouble[Row](value: Expr[Row, ?], k: Option[Int] = None): Expr[Row, Binary] =
    KllSketchAgg("kll_sketch_agg_double", value, k)

  /** Aggregate merging KLL sketches over bigints (Spark-only). */
  def kllMergeAggBigint[Row](sketch: Expr[Row, Binary], k: Option[Int] = None): Expr[Row, Binary] =
    SketchSetAgg("kll_merge_agg_bigint", sketch, k, None)

  /** Aggregate merging KLL sketches over floats (Spark-only). */
  def kllMergeAggFloat[Row](sketch: Expr[Row, Binary], k: Option[Int] = None): Expr[Row, Binary] =
    SketchSetAgg("kll_merge_agg_float", sketch, k, None)

  /** Aggregate merging KLL sketches over doubles (Spark-only). */
  def kllMergeAggDouble[Row](sketch: Expr[Row, Binary], k: Option[Int] = None): Expr[Row, Binary] =
    SketchSetAgg("kll_merge_agg_double", sketch, k, None)

  /** Number of values accumulated in a KLL bigint sketch (Spark-only). */
  def kllSketchGetNBigint[Row](sketch: Expr[Row, Binary]): Expr[Row, Long] =
    SketchEstimate("kll_sketch_get_n_bigint", sketch)

  /** Number of values accumulated in a KLL float sketch (Spark-only). */
  def kllSketchGetNFloat[Row](sketch: Expr[Row, Binary]): Expr[Row, Long] =
    SketchEstimate("kll_sketch_get_n_float", sketch)

  /** Number of values accumulated in a KLL double sketch (Spark-only). */
  def kllSketchGetNDouble[Row](sketch: Expr[Row, Binary]): Expr[Row, Long] =
    SketchEstimate("kll_sketch_get_n_double", sketch)

  /** The value at the given rank in a KLL bigint sketch (Spark-only). */
  def kllSketchGetQuantileBigint[Row](sketch: Expr[Row, Binary], rank: Expr[Row, Double]): Expr[Row, Any] =
    KllQuantile("kll_sketch_get_quantile_bigint", sketch, rank)

  /** The value at the given rank in a KLL float sketch (Spark-only). */
  def kllSketchGetQuantileFloat[Row](sketch: Expr[Row, Binary], rank: Expr[Row, Double]): Expr[Row, Any] =
    KllQuantile("kll_sketch_get_quantile_float", sketch, rank)

  /** The value at the given rank in a KLL double sketch (Spark-only). */
  def kllSketchGetQuantileDouble[Row](sketch: Expr[Row, Binary], rank: Expr[Row, Double]): Expr[Row, Any] =
    KllQuantile("kll_sketch_get_quantile_double", sketch, rank)

  /** The rank of the given value in a KLL bigint sketch (Spark-only). */
  def kllSketchGetRankBigint[Row](sketch: Expr[Row, Binary], quantile: Expr[Row, ?]): Expr[Row, Double] =
    KllRank("kll_sketch_get_rank_bigint", sketch, quantile)

  /** The rank of the given value in a KLL float sketch (Spark-only). */
  def kllSketchGetRankFloat[Row](sketch: Expr[Row, Binary], quantile: Expr[Row, ?]): Expr[Row, Double] =
    KllRank("kll_sketch_get_rank_float", sketch, quantile)

  /** The rank of the given value in a KLL double sketch (Spark-only). */
  def kllSketchGetRankDouble[Row](sketch: Expr[Row, Binary], quantile: Expr[Row, ?]): Expr[Row, Double] =
    KllRank("kll_sketch_get_rank_double", sketch, quantile)

  /** Row-wise merge of two KLL bigint sketches (Spark-only). */
  def kllSketchMergeBigint[Row](left: Expr[Row, Binary], right: Expr[Row, Binary]): Expr[Row, Binary] =
    SketchBinaryOp("kll_sketch_merge_bigint", left, right, None, None)

  /** Row-wise merge of two KLL float sketches (Spark-only). */
  def kllSketchMergeFloat[Row](left: Expr[Row, Binary], right: Expr[Row, Binary]): Expr[Row, Binary] =
    SketchBinaryOp("kll_sketch_merge_float", left, right, None, None)

  /** Row-wise merge of two KLL double sketches (Spark-only). */
  def kllSketchMergeDouble[Row](left: Expr[Row, Binary], right: Expr[Row, Binary]): Expr[Row, Binary] =
    SketchBinaryOp("kll_sketch_merge_double", left, right, None, None)

  /** Parse a JSON string into a variant value. */
  def parseJson[Row](expr: Expr[Row, String]): Expr[Row, Any] = ParseJson(expr)

  /** Extract a value at a variant path (e.g. "$.a.b[0]"), cast to targetType (e.g. "BIGINT"). */
  def variantGet[Row](expr: Expr[Row, Any], path: String, targetType: String): Expr[Row, Any] =
    VariantGet(expr, path, targetType)

  /** Like variantGet but returns null when the path or cast fails. */
  def tryVariantGet[Row](expr: Expr[Row, Any], path: String, targetType: String): Expr[Row, Any] =
    TryVariantGet(expr, path, targetType)

  /** True when the variant value is a variant-typed null. */
  def isVariantNull[Row](expr: Expr[Row, Any]): Expr[Row, Boolean] = IsVariantNull(expr)

  /** SQL type string of a variant value. */
  def schemaOfVariant[Row](expr: Expr[Row, Any]): Expr[Row, String] = SchemaOfVariant(expr)

  /** True when the JSON string parses as a valid variant. */
  def isValidVariant[Row](expr: Expr[Row, String]): Expr[Row, Boolean] = IsValidVariant(expr)

  /** SQL `variant_explode` — one row per top-level variant entry. */
  def variantExplode[Row](expr: Expr[Row, Any]): Expr[Row, Any] = VariantExplode(expr)

  /** Row-wise greatest of the given expressions, skipping nulls. */
  def greatest[Row, A: Ordering](exprs: Expr[Row, A]*): Expr[Row, A] =
    Greatest(exprs.toVector, summon[Ordering[A]])

  /** Row-wise least of the given expressions, skipping nulls. */
  def least[Row, A: Ordering](exprs: Expr[Row, A]*): Expr[Row, A] =
    Least(exprs.toVector, summon[Ordering[A]])

  /** Returns null if left equals right, otherwise left. */
  def nullif[Row, A](left: Expr[Row, A], right: Expr[Row, A]): Expr[Row, A] = NullIf(left, right)

  /** Returns alt when test is null, otherwise value (SQL NVL2). */
  def nvl2[Row, T, A](test: Expr[Row, T], value: Expr[Row, A], alt: Expr[Row, A]): Expr[Row, A] =
    Nvl2(test, value, alt)

  /** Returns the first expression unless it is NaN, in which case the second. */
  def nanvl[Row](left: Expr[Row, Double], right: Expr[Row, Double]): Expr[Row, Double] = Nanvl(left, right)

  /** Returns alt when expr is null, otherwise expr. */
  def ifnull[Row, A](expr: Expr[Row, A], alt: Expr[Row, A]): Expr[Row, A] = Coalesce(Vector(expr, alt))

  extension [Row](left: Expr[Row, Int]) {

    /** SQL `bit_count`. */
    inline def bitCount: Expr[Row, Int] = BitCount(left)

    /** SQL `bit_get`. */
    inline def bitGet(pos: Expr[Row, Int]): Expr[Row, Int] = BitGet(left, pos)

    /** SQL `shiftleft`. */
    inline def shiftLeft(n: Expr[Row, Int]): Expr[Row, Int] = ShiftLeft(left, n)

    /** SQL `shiftright` — signed. */
    inline def shiftRight(n: Expr[Row, Int]): Expr[Row, Int] = ShiftRight(left, n)

    /** SQL `shiftrightunsigned` — logical. */
    inline def shiftRightUnsigned(n: Expr[Row, Int]): Expr[Row, Int] = ShiftRightUnsigned(left, n)

    /** SQL `~` — bitwise complement. */
    inline def bitwiseNot: Expr[Row, Int] = BitwiseNot(left)
  }

  /** Aggregate bitwise AND of all non-null values. */
  def bitAnd[Row](expr: Expr[Row, Int]): Expr[Row, Int] = BitAndAgg(expr)

  /** Aggregate bitwise OR of all non-null values. */
  def bitOr[Row](expr: Expr[Row, Int]): Expr[Row, Int] = BitOrAgg(expr)

  /** Aggregate bitwise XOR of all non-null values. */
  def bitXor[Row](expr: Expr[Row, Int]): Expr[Row, Int] = BitXorAgg(expr)

  extension [Row](left: Expr[Row, String]) {

    /** SQL `initcap`. */
    inline def initcap: Expr[Row, String] = Initcap(left)

    /** SQL `instr`. */
    inline def instr(substr: Expr[Row, String]): Expr[Row, Int] = Instr(left, substr)

    /** SQL `substring_index`. */
    inline def substringIndex(delim: String, count: Int): Expr[Row, String] = SubstringIndex(left, delim, count)

    /** SQL `left`. */
    inline def leftStr(n: Expr[Row, Int]): Expr[Row, String] = LeftStr(left, n)

    /** SQL `right`. */
    inline def rightStr(n: Expr[Row, Int]): Expr[Row, String] = RightStr(left, n)

    /** SQL `repeat`. */
    inline def repeat(n: Expr[Row, Int]): Expr[Row, String] = Repeat(left, n)

    /** SQL `reverse`. */
    inline def reverse: Expr[Row, String] = Reverse(left)

    /** SQL `lpad`. */
    inline def lpad(len: Expr[Row, Int], pad: String): Expr[Row, String] = Lpad(left, len, pad)

    /** SQL `rpad`. */
    inline def rpad(len: Expr[Row, Int], pad: String): Expr[Row, String] = Rpad(left, len, pad)

    /** SQL `translate`. */
    inline def translate(matching: String, replace: String): Expr[Row, String] = Translate(left, matching, replace)

    /** SQL `ascii`. */
    inline def ascii: Expr[Row, Int] = Ascii(left)

    /** SQL `levenshtein`. */
    inline def levenshtein(other: Expr[Row, String]): Expr[Row, Int] = Levenshtein(left, other)

    /** SQL `rlike` — full Java-regex match. */
    inline def rlike(pattern: String): Expr[Row, Boolean] = Rlike(left, pattern)

    /** SQL `regexp_extract_all`. */
    inline def regexpExtractAll(pattern: String, groupIdx: Int): Expr[Row, Seq[String]] =
      RegexpExtractAll(left, pattern, groupIdx)

    /** SQL `split_part`. */
    inline def splitPart(delim: String, part: Expr[Row, Int]): Expr[Row, String] = SplitPart(left, delim, part)

    /** SQL `parse_url`. */
    inline def parseUrl(part: String): Expr[Row, String] = ParseUrl(left, part)
  }

  /** Character with the given code point. */
  def chr[Row](expr: Expr[Row, Int]): Expr[Row, String] = Chr(expr)

  /** Render a numeric value with the given java.text.DecimalFormat-style pattern. */
  extension [Row](left: Expr[Row, Double]) {

    /** SQL `to_char` with the given decimal format pattern. */
    inline def toChar(format: String): Expr[Row, String] = NumberToChar(left, format)
  }

  /** Format the arguments with the given java.util.Formatter-style format string. */
  def formatString[Row](format: String, args: Expr[Row, Any]*): Expr[Row, String] =
    FormatString(format, args.toVector)

  extension [Row](left: Expr[Row, Double]) {

    /** SQL `cbrt`. */
    inline def cbrt: Expr[Row, Double] = Cbrt(left)

    /** SQL `hypot`. */
    inline def hypot(other: Expr[Row, Double]): Expr[Row, Double] = Hypot(left, other)

    /** SQL `bround` — half-even rounding. */
    inline def bround(scale: Int): Expr[Row, Double] = Bround(left, scale)

    /** SQL `sinh`. */
    inline def sinh: Expr[Row, Double] = Sinh(left)

    /** SQL `cosh`. */
    inline def cosh: Expr[Row, Double] = Cosh(left)

    /** SQL `tanh`. */
    inline def tanh: Expr[Row, Double] = Tanh(left)

    /** SQL `asinh`. */
    inline def asinh: Expr[Row, Double] = Asinh(left)

    /** SQL `acosh`. */
    inline def acosh: Expr[Row, Double] = Acosh(left)

    /** SQL `atanh`. */
    inline def atanh: Expr[Row, Double] = Atanh(left)

    /** SQL `degrees`. */
    inline def degrees: Expr[Row, Double] = Degrees(left)

    /** SQL `radians`. */
    inline def radians: Expr[Row, Double] = Radians(left)

    /** SQL `log1p`. */
    inline def log1p: Expr[Row, Double] = Log1p(left)

    /** SQL `expm1`. */
    inline def expm1: Expr[Row, Double] = Expm1(left)

    /** Lenient Double division — null on a zero divisor (Spark `try_divide`). */
    @targetName("tryDivideDouble")
    inline def tryDivide(other: Expr[Row, Double]): Expr[Row, Double] = TryDivideDouble(left, other)

    /** SQL `width_bucket`. */
    inline def widthBucket(min: Expr[Row, Double], max: Expr[Row, Double], buckets: Expr[Row, Int]): Expr[Row, Int] =
      WidthBucket(left, min, max, buckets)
  }

  /** Binary representation of a Long value. */
  def bin[Row](expr: Expr[Row, Long]): Expr[Row, String] = Bin(expr)

  /** Hexadecimal string decoded to binary; null for invalid input. */
  def unhex[Row](expr: Expr[Row, String]): Expr[Row, Binary] = Unhex(expr)

  /** Convert a number between string bases. */
  def conv[Row](num: Expr[Row, String], fromBase: Int, toBase: Int): Expr[Row, String] =
    Conv(num, fromBase, toBase)

  /** SQL `factorial`. */
  def factorial[Row](expr: Expr[Row, Int]): Expr[Row, Long] = Factorial(expr)

  /** SQL `pi()` — the constant. */
  def pi[Row]: Expr[Row, Double] = Pi[Row]()

  /** Euler's number as a constant expression. */
  def euler[Row]: Expr[Row, Double] = Euler[Row]()

  /** Standard-normal random values with a fixed seed for reproducibility. */
  def randn[Row](seed: Long): Expr[Row, Double] = Randn(seed)

  extension [Row](left: Expr[Row, Int]) {

    /** SQL `pmod` — positive remainder; a zero divisor fails the row. */
    @targetName("pmodInt")
    inline def pmod(right: Expr[Row, Int]): Expr[Row, Int] = PmodInt(left, right)

    /** Lenient Int addition — null instead of failure on overflow. */
    @targetName("tryAddInt")
    inline def tryAdd(right: Expr[Row, Int]): Expr[Row, Int] = TryAddInt(left, right)
  }

  extension [Row](left: Expr[Row, Long]) {

    /** SQL `pmod` on Long — positive remainder; a zero divisor fails the row. */
    @targetName("pmodLong")
    inline def pmod(right: Expr[Row, Long]): Expr[Row, Long] = PmodLong(left, right)

    /** Lenient Long addition — null instead of failure on overflow. */
    @targetName("tryAddLong")
    inline def tryAdd(right: Expr[Row, Long]): Expr[Row, Long] = TryAddLong(left, right)

    /** Lenient Long subtraction — null instead of failure on overflow. */
    @targetName("trySubtractLong")
    inline def trySubtract(right: Expr[Row, Long]): Expr[Row, Long] = TrySubtractLong(left, right)

    /** Lenient Long multiplication — null instead of failure on overflow. */
    @targetName("tryMultiplyLong")
    inline def tryMultiply(right: Expr[Row, Long]): Expr[Row, Long] = TryMultiplyLong(left, right)

    /** Lenient Long division — null on a zero divisor (Spark `try_divide`). */
    @targetName("tryDivideLong")
    inline def tryDivide(right: Expr[Row, Long]): Expr[Row, Double] = TryDivideLong(left, right)
  }

  extension [Row](t: Expr[Row, Timestamp]) {

    /** SQL `unix_timestamp`. */
    inline def unixTimestamp: Expr[Row, Long] = UnixTimestamp(t)

    /** SQL `hour`. */
    inline def hour: Expr[Row, Int] = HourOf(t)

    /** SQL `minute`. */
    inline def minute: Expr[Row, Int] = MinuteOf(t)

    /** SQL `second`. */
    inline def second: Expr[Row, Int] = SecondOf(t)

    /** SQL `from_utc_timestamp`. */
    inline def fromUtc(tz: String): Expr[Row, Timestamp] = FromUtcTimestamp(t, tz)

    /** SQL `to_utc_timestamp`. */
    inline def toUtc(tz: String): Expr[Row, Timestamp] = ToUtcTimestamp(t, tz)

    /** SQL `convert_timezone` — wall-clock conversion between the named zones. */
    inline def convertTimezone(fromTz: String, toTz: String): Expr[Row, Timestamp] =
      ConvertTimezone(t, fromTz, toTz)

    /** SQL `timestamp_add` — shifted by `qty` of the named unit. */
    inline def timestampAdd(unit: String, qty: Expr[Row, Int]): Expr[Row, Timestamp] =
      TimestampAdd(unit, qty, t)
  }

  /** Parse an ISO-8601 datetime string into a timestamp (UTC wall-clock interpretation). */
  def toTimestamp[Row](expr: Expr[Row, String]): Expr[Row, Timestamp] = ToTimestamp(expr)

  /** Parse an ISO-8601 date string into a date. */
  def toDate[Row](expr: Expr[Row, String]): Expr[Row, Date] = ToDate(expr)

  /** SQL `current_date` — the evaluation-time date. */
  def current_date[Row]: Expr[Row, Date] = CurrentDate[Row]()

  /** SQL `now` — the evaluation-time timestamp. */
  def now[Row]: Expr[Row, Timestamp] = Now[Row]()

  /** Build a timestamp from epoch seconds (fractional). */
  def timestampSeconds[Row](expr: Expr[Row, Double]): Expr[Row, Timestamp] = TimestampSeconds(expr)

  /** Build a timestamp from epoch milliseconds. */
  def timestampMillis[Row](expr: Expr[Row, Long]): Expr[Row, Timestamp] = TimestampMillis(expr)

  /** Build a timestamp from epoch microseconds. */
  def timestampMicros[Row](expr: Expr[Row, Long]): Expr[Row, Timestamp] = TimestampMicros(expr)

  /** Build a timestamp from components; interpreted in UTC. */
  def makeTimestamp[Row](
    year: Expr[Row, Int],
    month: Expr[Row, Int],
    day: Expr[Row, Int],
    hour: Expr[Row, Int],
    minute: Expr[Row, Int],
    sec: Expr[Row, Double]
  ): Expr[Row, Timestamp] = MakeTimestamp(year, month, day, hour, minute, sec)

  /** Build a day-time interval from days, hours, minutes and (fractional) seconds. */
  def makeDtInterval[Row](
    days: Expr[Row, Long],
    hours: Expr[Row, Int],
    minutes: Expr[Row, Int],
    seconds: Expr[Row, Double]
  ): Expr[Row, DayTimeInterval] = MakeDtInterval(days, hours, minutes, seconds)

  /** Build a year-month interval from years and months. */
  def makeYmInterval[Row](years: Expr[Row, Int], months: Expr[Row, Int]): Expr[Row, YearMonthInterval] =
    MakeYmInterval(years, months)

  /** Difference between end and start in the given unit (SECOND, MINUTE, HOUR, DAY, MONTH, YEAR).
    */
  def timestampDiff[Row](unit: String, start: Expr[Row, Timestamp], end: Expr[Row, Timestamp]): Expr[Row, Long] =
    TimestampDiff(unit, start, end)

  extension [Row](d: Expr[Row, Date]) {

    /** SQL `weekday` — 0 (Monday) through 6 (Sunday). */
    inline def weekday: Expr[Row, Int] = Weekday(d)
  }

  extension [Row, A](e: Expr[Row, Seq[A]]) {

    /** SQL `array_append`. */
    inline def arrayAppend(elem: Expr[Row, A]): Expr[Row, Seq[A]] = ArrayAppend(e, elem)

    /** SQL `array_prepend`. */
    inline def arrayPrepend(elem: Expr[Row, A]): Expr[Row, Seq[A]] = ArrayPrepend(e, elem)

    /** SQL `array_insert` at the 1-based position (appending past the end). */
    inline def arrayInsert(pos: Expr[Row, Int], elem: Expr[Row, A]): Expr[Row, Seq[A]] = ArrayInsert(e, pos, elem)

    /** SQL `array_remove`. */
    inline def arrayRemove(elem: Expr[Row, A]): Expr[Row, Seq[A]] = ArrayRemove(e, elem)

    /** SQL `array_compact` — nulls removed. */
    inline def arrayCompact: Expr[Row, Seq[A]] = ArrayCompact(e)

    /** SQL `array_position` — 1-based (0 when absent). */
    inline def arrayPosition(elem: Expr[Row, A]): Expr[Row, Int] = ArrayPosition(e, elem)

    /** SQL `arrays_overlap`. */
    inline def arraysOverlap(other: Expr[Row, Seq[A]]): Expr[Row, Boolean] = ArraysOverlap(e, other)

    /** SQL `get` — 0-based element access; out-of-range fails the row. */
    inline def getArray(index: Expr[Row, Int]): Expr[Row, A] = GetArray(e, index)

    /** SQL `array_max` — null for an empty array. */
    inline def arrayMax(using ordering: Ordering[A]): Expr[Row, A] = ArrayMax(e, ordering)

    /** SQL `array_min` — null for an empty array. */
    inline def arrayMin(using ordering: Ordering[A]): Expr[Row, A] = ArrayMin(e, ordering)
  }

  /** Repeat elem count times as an array. */
  def arrayRepeat[Row, A](elem: Expr[Row, A], count: Expr[Row, Int]): Expr[Row, Seq[A]] =
    ArrayRepeat(elem, count)

  /** Concatenate string array elements with the delimiter, skipping (or replacing) nulls. */
  def arrayJoin[Row](
    arr: Expr[Row, Seq[String]],
    delimiter: String,
    nullReplacement: Option[String] = None
  ): Expr[Row, String] =
    ArrayJoin(arr, delimiter, nullReplacement)

  /** Zip arrays position-wise into structs keyed by "0", "1", ... */
  def arraysZip[Row](arrays: Expr[Row, Seq[?]]*): Expr[Row, Seq[Map[String, Any]]] =
    ArraysZip(arrays.toVector)

  /** Build a map from an array of key-value pairs. */
  def mapFromEntries[Row, K, V](arr: Expr[Row, Seq[(K, V)]]): Expr[Row, Map[K, V]] = MapFromEntries(arr)

  extension [Row, A](e: Expr[Row, Seq[A]]) {

    /** SQL `posexplode` — element with its 0-based position. */
    inline def posexplode: Expr[Row, Any] = Posexplode(e)

    /** SQL `explode_outer` — null row for empty arrays. */
    inline def explodeOuter: Expr[Row, Any] = ExplodeOuter(e)

    /** SQL `inline` — explodes an array of structs into their fields. */
    inline def inlineArray: Expr[Row, Any] = Inline(e)
  }

  /** Average of x over (x, y) pairs where both are non-null. */
  def regrAvgx[Row](y: Expr[Row, Double], x: Expr[Row, Double]): Expr[Row, Option[Double]] = RegrAvgx(y, x)

  /** Average of y over (x, y) pairs where both are non-null. */
  def regrAvgy[Row](y: Expr[Row, Double], x: Expr[Row, Double]): Expr[Row, Option[Double]] = RegrAvgy(y, x)

  /** Count of (x, y) pairs where both are non-null. */
  def regrCount[Row](y: Expr[Row, Double], x: Expr[Row, Double]): Expr[Row, Option[Long]] = RegrCount(y, x)

  /** Linear-regression intercept of y over x. */
  def regrIntercept[Row](y: Expr[Row, Double], x: Expr[Row, Double]): Expr[Row, Option[Double]] =
    RegrIntercept(y, x)

  /** Coefficient of determination (R²) of y over x. */
  def regrR2[Row](y: Expr[Row, Double], x: Expr[Row, Double]): Expr[Row, Option[Double]] = RegrR2(y, x)

  /** Linear-regression slope of y over x. */
  def regrSlope[Row](y: Expr[Row, Double], x: Expr[Row, Double]): Expr[Row, Option[Double]] = RegrSlope(y, x)

  /** Sum of squared deviations of x. */
  def regrSxx[Row](y: Expr[Row, Double], x: Expr[Row, Double]): Expr[Row, Option[Double]] = RegrSxx(y, x)

  /** Sum of products of deviations of x and y. */
  def regrSxy[Row](y: Expr[Row, Double], x: Expr[Row, Double]): Expr[Row, Option[Double]] = RegrSxy(y, x)

  /** Sum of squared deviations of y. */
  def regrSyy[Row](y: Expr[Row, Double], x: Expr[Row, Double]): Expr[Row, Option[Double]] = RegrSyy(y, x)

  /** Excess kurtosis (biased moment estimator, matching Spark). */
  def kurtosis[Row](expr: Expr[Row, Double]): Expr[Row, Option[Double]] = Kurtosis(expr)

  /** Skewness (biased moment estimator, matching Spark). */
  def skewness[Row](expr: Expr[Row, Double]): Expr[Row, Option[Double]] = Skewness(expr)

  /** Exact percentile with linear interpolation (percentage in [0, 1]). */
  def percentile[Row](expr: Expr[Row, Double], percentage: Expr[Row, Double]): Expr[Row, Option[Double]] =
    Percentile(expr, percentage)

  /** Sum of the distinct non-null values; None for an empty or all-null group. */
  def sumDistinct[Row](expr: Expr[Row, Long]): Expr[Row, Option[Long]] = SumDistinct(expr)

  /** Spark-only: numeric histogram with at most nBins bins, as an array of (x, y) structs. */
  def histogramNumeric[Row](expr: Expr[Row, Double], nBins: Expr[Row, Int]): Expr[Row, Any] =
    HistogramNumeric(expr, nBins)

  /** Spark-only: grouping indicator for GROUPING SETS / ROLLUP / CUBE. */
  def grouping[Row](expr: Expr[Row, Any]): Expr[Row, Int] = Grouping(expr)

  /** Spark-only: bit vector of grouping indicators over the given columns. */
  def groupingId[Row](exprs: Expr[Row, Any]*): Expr[Row, Long] = GroupingId(exprs.toVector)

  /** DDL schema string of a JSON document. */
  def schemaOfJson[Row](expr: Expr[Row, String]): Expr[Row, String] = SchemaOfJson(expr)

  /** Number of elements in the JSON array at path (e.g. "$.a[0]"); root when omitted. */
  def jsonArrayLength[Row](expr: Expr[Row, String], path: String = "$"): Expr[Row, Long] =
    JsonArrayLength(expr, path)

  /** Keys of the JSON object at path; root when omitted. */
  def jsonObjectKeys[Row](expr: Expr[Row, String], path: String = "$"): Expr[Row, Seq[String]] =
    JsonObjectKeys(expr, path)

  /** Extract the values of the given top-level keys from a JSON object, one element per key.
    *
    * Scalar values become their string form, compound values their compact JSON text, and absent
    * keys or JSON nulls become null elements (typed `String | Null`), in the order of `keys`. Keys
    * are literal top-level names (no path syntax). The result column is boxed (AnyColumn).
    *
    * The Spark backend maps to `array(get_json_object(...))` over bracket-quoted key paths, which
    * matches Spark's native `json_tuple` (a generator that cannot be nested in expressions) for
    * strings, booleans, compound values, JSON nulls, absent keys and invalid documents. Two
    * documented divergences remain: number formatting differs on edge cases (Spark renders `1.0` as
    * `"1.0"` and `1e10` as `"1.0E10"`; in-memory yields `"1"` and `"10000000000"` — a pre-existing
    * `GetJsonObject` gap rooted in `JsonValue.Number` storing only the double; follow-up: preserve
    * the raw number token in Sarati), and a key containing both a single quote and a dot or bracket
    * is unaddressable by any Spark path syntax, which fails the Spark conversion while in-memory
    * still extracts it.
    */
  def jsonTuple[Row](expr: Expr[Row, String], keys: String*): Expr[Row, Seq[String | Null]] =
    JsonTuple(expr, keys.toVector)

  /** Parse a JSON string into T; `schema` is the Spark DDL string for the Spark backend. */
  def fromJson[Row, T](expr: Expr[Row, String], schema: String)(using
    decoder: Decoder[JsonValue, T]
  ): Expr[Row, T] = FromJson(expr, schema, decoder)

  /** Serialize a value to a JSON string via a Sarati encoder. */
  def toJson[Row, T](expr: Expr[Row, T])(using encoder: Encoder[T, JsonValue]): Expr[Row, String] =
    ToJson(expr, encoder)

  extension [Row, T](structExpr: Expr[Row, T]) {

    /** Extract a struct field by its position in the struct's flattened column layout.
      *
      * @param fieldIndex
      *   Position of the field's column in the struct's flattened schema.
      * @param fieldName
      *   Field name used by the Spark backend (`getField`).
      */
    def getField[F](fieldIndex: ColumnIndex, fieldName: String): Expr[Row, F] =
      GetField(structExpr, fieldIndex, fieldName)
  }

  /** SQL `map_from_arrays` — a map from the parallel key and value arrays. */
  def mapFromArrays[Row, K, V](keys: Expr[Row, Seq[K]], values: Expr[Row, Seq[V]]): Expr[Row, Map[K, V]] =
    MapFromArrays(keys, values)

  /** Construct a struct expression whose output type is the struct's flat schema layout.
    *
    * Each field is a (name, expression, column type) triple; the field columns are stored in the
    * order given by `schema` (the caller must keep them consistent).
    */
  def struct[Row, T](
    field: (String, Expr[Row, ?], ColumnType),
    fields: (String, Expr[Row, ?], ColumnType)*
  )(using schema: Schema[T]): Expr[Row, T] =
    Struct(field +: fields.toVector, schema)

  /** Infer the output ColumnType of an expression, if statically known. */
  extension [Row, A](expr: Expr[Row, A]) {

    /** The output column type of the expression when statically known. */
    def outputType: Option[ColumnType] = (expr: @unchecked) match {
      case _: Expr.Cell[_, _] => None
      case _: Expr.Const[_, _] => None
      case n: Expr.Named[_, _] => n.expr.outputType
      case _: Expr.Add[_] | _: Expr.Sub[_] | _: Expr.Mul[_] | _: Expr.Div[_] | _: Expr.Mod[_] | _: Expr.Abs[_] |
          _: Expr.Negate[_] =>
        Some(ColumnType.IntType)
      case _: Expr.AddLong[_] | _: Expr.SubLong[_] | _: Expr.MulLong[_] | _: Expr.DivLong[_] | _: Expr.SumLong[_] |
          _: Expr.Sum[_] | _: Expr.ModLong[_] | _: Expr.AbsLong[_] | _: Expr.NegateLong[_] | _: Expr.CastToLong[_] =>
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
      case _: Expr.StringSplit[_] | _: Expr.JsonTuple[_] | _: Expr.Xpath[_] | _: Expr.TryXpath[_] =>
        Some(ColumnType.AnyType)
      case _: Expr.XpathString[_] | _: Expr.TryXpathString[_] => Some(ColumnType.StringType)
      case _: Expr.XpathBoolean[_] | _: Expr.TryXpathBoolean[_] => Some(ColumnType.BooleanType)
      case _: Expr.XpathShort[_] | _: Expr.TryXpathShort[_] => Some(ColumnType.ShortType)
      case _: Expr.XpathInt[_] | _: Expr.TryXpathInt[_] => Some(ColumnType.IntType)
      case _: Expr.XpathLong[_] | _: Expr.TryXpathLong[_] => Some(ColumnType.LongType)
      case _: Expr.XpathFloat[_] | _: Expr.TryXpathFloat[_] => Some(ColumnType.FloatType)
      case _: Expr.XpathDouble[_] | _: Expr.TryXpathDouble[_] => Some(ColumnType.DoubleType)
      case _: Expr.LambdaVar[_, _] | _: Expr.Transform[_, _, _] | _: Expr.ZipWith[_, _, _, _] |
          _: Expr.Aggregate[_, _, _] | _: Expr.MapZipWith[_, _, _, _, _] | _: Expr.TransformKeys[_, _, _, _] |
          _: Expr.TransformValues[_, _, _, _] | _: Expr.ArraySortComparator[_, _] =>
        Some(ColumnType.AnyType)
      case _: Expr.Filter[_, _] | _: Expr.MapFilter[_, _, _] => Some(ColumnType.AnyType)
      case _: Expr.Exists[_, _] | _: Expr.ForAll[_, _] => Some(ColumnType.BooleanType)
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
      case _: Expr.ArraySize[_, _] => Some(ColumnType.IntType)
      case _: Expr.ArrayContains[_, _] | _: Expr.MapContainsKey[_, _, _] => Some(ColumnType.BooleanType)
      case _: Expr.Explode[_, _] | _: Expr.ElementAt[_, _] => None
      case _: Expr.ArraySort[_, _] | _: Expr.ArrayDistinct[_, _] | _: Expr.ArrayUnion[_, _] |
          _: Expr.ArrayIntersect[_, _] | _: Expr.ArrayExcept[_, _] | _: Expr.Flatten[_, _] | _: Expr.ArraySlice[_, _] |
          _: Expr.MapKeys[_, _, _] | _: Expr.MapValues[_, _, _] | _: Expr.MapEntries[_, _, _] =>
        Some(ColumnType.AnyType)
      case _: Expr.MapFromArrays[_, _, _] | _: Expr.MapConcat[_, _, _] => Some(ColumnType.AnyType)
      case _: Expr.Md5[_] | _: Expr.Sha1[_] | _: Expr.Sha2[_] | _: Expr.UrlEncode[_] | _: Expr.UrlDecode[_] |
          _: Expr.Base64Encode[_] | _: Expr.Base64Decode[_] | _: Expr.Hex[_] | _: Expr.GetJsonObject[_] |
          _: Expr.AesDecrypt[_] | _: Expr.TryAesDecrypt[_] =>
        Some(ColumnType.StringType)
      case _: Expr.Crc32[_] | _: Expr.XxHash64[_] => Some(ColumnType.LongType)
      case _: Expr.Hash[_] => Some(ColumnType.IntType)
      case _: Expr.AesEncrypt[_] => Some(ColumnType.BinaryType)
      case s: Expr.Struct[_, _] =>
        Some(ColumnType.StructType(s.schema.columnNames.zip(s.schema.columnTypes)))
      case _: Expr.GetField[_, _, _] => None
      case _: Expr.TimeToSeconds[_] => Some(ColumnType.DecimalType(14, 6))
      case _: Expr.TimeToMillis[_] | _: Expr.TimeToMicros[_] => Some(ColumnType.LongType)
      case _: Expr.TimeFromSeconds[_] | _: Expr.TimeFromMillis[_] | _: Expr.TimeFromMicros[_] =>
        Some(ColumnType.TimeType)
      case _: Expr.TimeBucket[_] => Some(ColumnType.TimestampType)
      case _: Expr.CurrentPath[_] => Some(ColumnType.StringType)
      case _: Expr.SketchEstimate[_] => Some(ColumnType.LongType)
      case _: Expr.SketchSummary[_] => Some(ColumnType.DoubleType)
      case _: Expr.SketchBinaryOp[_] | _: Expr.TupleSketchAgg[_] | _: Expr.SketchSetAgg[_] | _: Expr.KllSketchAgg[_] =>
        Some(ColumnType.BinaryType)
      case _: Expr.SketchTheta[_] => Some(ColumnType.DoubleType)
      case _: Expr.KllQuantile[_] => Some(ColumnType.AnyType)
      case _: Expr.KllRank[_] => Some(ColumnType.DoubleType)
      case _: Expr.ParseJson[_] | _: Expr.VariantGet[_] | _: Expr.TryVariantGet[_] | _: Expr.VariantExplode[_] =>
        Some(ColumnType.VariantType)
      case _: Expr.IsVariantNull[_] | _: Expr.IsValidVariant[_] => Some(ColumnType.BooleanType)
      case _: Expr.SchemaOfVariant[_] => Some(ColumnType.StringType)
      case _: Expr.FromJson[_, _] => Some(ColumnType.AnyType)
      case _: Expr.ToJson[_, _] => Some(ColumnType.StringType)
    }
  }
}
