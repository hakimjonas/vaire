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

  case Sum[Row](expr: Expr[Row, Int]) extends Expr[Row, Long]
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

  case DateAddDays[Row](date: Expr[Row, Date], days: Expr[Row, Int]) extends Expr[Row, Date]
  case DateSubDays[Row](date: Expr[Row, Date], days: Expr[Row, Int]) extends Expr[Row, Date]
  case DateAddMonths[Row](date: Expr[Row, Date], months: Expr[Row, Int]) extends Expr[Row, Date]
  case DateDiff[Row](left: Expr[Row, Date], right: Expr[Row, Date]) extends Expr[Row, Int]
  case ExtractYear[Row](date: Expr[Row, Date]) extends Expr[Row, Int]
  case ExtractMonth[Row](date: Expr[Row, Date]) extends Expr[Row, Int]
  case ExtractDay[Row](date: Expr[Row, Date]) extends Expr[Row, Int]

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

  case DayOfWeek[Row](date: Expr[Row, Date]) extends Expr[Row, Int]
  case DayOfYear[Row](date: Expr[Row, Date]) extends Expr[Row, Int]
  case WeekOfYear[Row](date: Expr[Row, Date]) extends Expr[Row, Int]
  case Quarter[Row](date: Expr[Row, Date]) extends Expr[Row, Int]
  case LastDay[Row](date: Expr[Row, Date]) extends Expr[Row, Date]
  case NextDay[Row](date: Expr[Row, Date], dayOfWeek: String) extends Expr[Row, Date]
  case MonthsBetween[Row](end: Expr[Row, Date], start: Expr[Row, Date]) extends Expr[Row, Double]
  case DateTrunc[Row](unit: String, date: Expr[Row, Date]) extends Expr[Row, Date]
  case DateFormat[Row](date: Expr[Row, Date], format: String) extends Expr[Row, String]
  case MakeDate[Row](year: Expr[Row, Int], month: Expr[Row, Int], day: Expr[Row, Int]) extends Expr[Row, Date]

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

  case ArraySize[Row, A](expr: Expr[Row, Seq[A]]) extends Expr[Row, Int]
  case ArrayContains[Row, A](expr: Expr[Row, Seq[A]], value: Expr[Row, A]) extends Expr[Row, Boolean]
  case Explode[Row, A](expr: Expr[Row, Seq[A]]) extends Expr[Row, A]
  case ArraySort[Row, A](expr: Expr[Row, Seq[A]], ordering: Ordering[A]) extends Expr[Row, Seq[A]]
  case ArrayDistinct[Row, A](expr: Expr[Row, Seq[A]]) extends Expr[Row, Seq[A]]
  case ArrayUnion[Row, A](left: Expr[Row, Seq[A]], right: Expr[Row, Seq[A]]) extends Expr[Row, Seq[A]]
  case ArrayIntersect[Row, A](left: Expr[Row, Seq[A]], right: Expr[Row, Seq[A]]) extends Expr[Row, Seq[A]]
  case ArrayExcept[Row, A](left: Expr[Row, Seq[A]], right: Expr[Row, Seq[A]]) extends Expr[Row, Seq[A]]
  case Flatten[Row, A](expr: Expr[Row, Seq[Seq[A]]]) extends Expr[Row, Seq[A]]
  case ElementAt[Row, A](expr: Expr[Row, Seq[A]], index: Expr[Row, Int]) extends Expr[Row, A]
  case ArraySlice[Row, A](expr: Expr[Row, Seq[A]], start: Int, length: Int) extends Expr[Row, Seq[A]]

  case MapKeys[Row, K, V](expr: Expr[Row, Map[K, V]]) extends Expr[Row, Seq[K]]
  case MapValues[Row, K, V](expr: Expr[Row, Map[K, V]]) extends Expr[Row, Seq[V]]
  case MapContainsKey[Row, K, V](expr: Expr[Row, Map[K, V]], key: Expr[Row, K]) extends Expr[Row, Boolean]
  case MapEntries[Row, K, V](expr: Expr[Row, Map[K, V]]) extends Expr[Row, Seq[(K, V)]]
  case MapFromArrays[Row, K, V](keys: Expr[Row, Seq[K]], values: Expr[Row, Seq[V]]) extends Expr[Row, Map[K, V]]
  case MapConcat[Row, K, V](left: Expr[Row, Map[K, V]], right: Expr[Row, Map[K, V]]) extends Expr[Row, Map[K, V]]

  case Md5[Row](expr: Expr[Row, String]) extends Expr[Row, String]
  case Sha1[Row](expr: Expr[Row, String]) extends Expr[Row, String]
  case Sha2[Row](expr: Expr[Row, String], bitLength: Int) extends Expr[Row, String]

  case UrlEncode[Row](expr: Expr[Row, String]) extends Expr[Row, String]
  case UrlDecode[Row](expr: Expr[Row, String]) extends Expr[Row, String]
  case Base64Encode[Row](expr: Expr[Row, String]) extends Expr[Row, String]
  case Base64Decode[Row](expr: Expr[Row, String]) extends Expr[Row, String]
  case Hex[Row](expr: Expr[Row, String]) extends Expr[Row, String]

  case Crc32[Row](expr: Expr[Row, String]) extends Expr[Row, Long]
  case XxHash64[Row](expr: Expr[Row, String]) extends Expr[Row, Long]
  case Hash[Row](expr: Expr[Row, String]) extends Expr[Row, Int]
  case AesEncrypt[Row](expr: Expr[Row, String], key: Expr[Row, String]) extends Expr[Row, Binary]
  case AesDecrypt[Row](expr: Expr[Row, String], key: Expr[Row, String]) extends Expr[Row, String]
  case TryAesDecrypt[Row](expr: Expr[Row, String], key: Expr[Row, String]) extends Expr[Row, String]

  case GetJsonObject[Row](expr: Expr[Row, String], path: String) extends Expr[Row, String]

  case TimeToSeconds[Row](expr: Expr[Row, Time]) extends Expr[Row, Decimal]
  case TimeToMillis[Row](expr: Expr[Row, Time]) extends Expr[Row, Long]
  case TimeToMicros[Row](expr: Expr[Row, Time]) extends Expr[Row, Long]
  case TimeFromSeconds[Row](expr: Expr[Row, Double]) extends Expr[Row, Time]
  case TimeFromMillis[Row](expr: Expr[Row, Long]) extends Expr[Row, Time]
  case TimeFromMicros[Row](expr: Expr[Row, Long]) extends Expr[Row, Time]
  case TimeBucket[Row](
    bucketSize: Expr[Row, DayTimeInterval],
    ts: Expr[Row, Timestamp],
    origin: Expr[Row, Timestamp]
  ) extends Expr[Row, Timestamp]

  case CurrentPath[Row]() extends Expr[Row, String]

  case Initcap[Row](expr: Expr[Row, String]) extends Expr[Row, String]
  case Instr[Row](str: Expr[Row, String], substr: Expr[Row, String]) extends Expr[Row, Int]
  case SubstringIndex[Row](str: Expr[Row, String], delim: String, count: Int) extends Expr[Row, String]
  case LeftStr[Row](str: Expr[Row, String], n: Expr[Row, Int]) extends Expr[Row, String]
  case RightStr[Row](str: Expr[Row, String], n: Expr[Row, Int]) extends Expr[Row, String]
  case Repeat[Row](str: Expr[Row, String], n: Expr[Row, Int]) extends Expr[Row, String]
  case Reverse[Row](str: Expr[Row, String]) extends Expr[Row, String]
  case Lpad[Row](str: Expr[Row, String], len: Expr[Row, Int], pad: String) extends Expr[Row, String]
  case Rpad[Row](str: Expr[Row, String], len: Expr[Row, Int], pad: String) extends Expr[Row, String]
  case Translate[Row](str: Expr[Row, String], matching: String, replace: String) extends Expr[Row, String]
  case FormatString[Row](format: String, args: Vector[Expr[Row, Any]]) extends Expr[Row, String]
  case Ascii[Row](str: Expr[Row, String]) extends Expr[Row, Int]
  case Chr[Row](expr: Expr[Row, Int]) extends Expr[Row, String]
  case Levenshtein[Row](left: Expr[Row, String], right: Expr[Row, String]) extends Expr[Row, Int]
  case Rlike[Row](str: Expr[Row, String], pattern: String) extends Expr[Row, Boolean]
  case RegexpExtractAll[Row](str: Expr[Row, String], pattern: String, groupIdx: Int) extends Expr[Row, Seq[String]]
  case SplitPart[Row](str: Expr[Row, String], delim: String, part: Expr[Row, Int]) extends Expr[Row, String]
  case ParseUrl[Row](url: Expr[Row, String], part: String) extends Expr[Row, String]
  case NumberToChar[Row](expr: Expr[Row, Double], format: String) extends Expr[Row, String]

  case Cbrt[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Hypot[Row](left: Expr[Row, Double], right: Expr[Row, Double]) extends Expr[Row, Double]
  case Bin[Row](expr: Expr[Row, Long]) extends Expr[Row, String]
  case Unhex[Row](expr: Expr[Row, String]) extends Expr[Row, Binary]
  case Bround[Row](expr: Expr[Row, Double], scale: Int) extends Expr[Row, Double]
  case Conv[Row](num: Expr[Row, String], fromBase: Int, toBase: Int) extends Expr[Row, String]
  case Factorial[Row](expr: Expr[Row, Int]) extends Expr[Row, Long]
  case Sinh[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Cosh[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Tanh[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Asinh[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Acosh[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Atanh[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Degrees[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Radians[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Pi[Row]() extends Expr[Row, Double]
  case Euler[Row]() extends Expr[Row, Double]
  case WidthBucket[Row](
    value: Expr[Row, Double],
    min: Expr[Row, Double],
    max: Expr[Row, Double],
    buckets: Expr[Row, Int]
  ) extends Expr[Row, Int]
  case Randn[Row](seed: Long) extends Expr[Row, Double]
  case PmodInt[Row](left: Expr[Row, Int], right: Expr[Row, Int]) extends Expr[Row, Int]
  case PmodLong[Row](left: Expr[Row, Long], right: Expr[Row, Long]) extends Expr[Row, Long]
  case Log1p[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case Expm1[Row](expr: Expr[Row, Double]) extends Expr[Row, Double]
  case TryAddLong[Row](left: Expr[Row, Long], right: Expr[Row, Long]) extends Expr[Row, Long]
  case TrySubtractLong[Row](left: Expr[Row, Long], right: Expr[Row, Long]) extends Expr[Row, Long]
  case TryMultiplyLong[Row](left: Expr[Row, Long], right: Expr[Row, Long]) extends Expr[Row, Long]
  case TryDivideLong[Row](left: Expr[Row, Long], right: Expr[Row, Long]) extends Expr[Row, Double]
  case TryDivideDouble[Row](left: Expr[Row, Double], right: Expr[Row, Double]) extends Expr[Row, Double]
  case TryAddInt[Row](left: Expr[Row, Int], right: Expr[Row, Int]) extends Expr[Row, Int]

  case UnixTimestamp[Row](expr: Expr[Row, Timestamp]) extends Expr[Row, Long]
  case FromUnixtime[Row](expr: Expr[Row, Long]) extends Expr[Row, String]
  case ToTimestamp[Row](expr: Expr[Row, String]) extends Expr[Row, Timestamp]
  case ToDate[Row](expr: Expr[Row, String]) extends Expr[Row, Date]
  case CurrentDate[Row]() extends Expr[Row, Date]
  case Now[Row]() extends Expr[Row, Timestamp]
  case TimestampSeconds[Row](expr: Expr[Row, Double]) extends Expr[Row, Timestamp]
  case TimestampMillis[Row](expr: Expr[Row, Long]) extends Expr[Row, Timestamp]
  case TimestampMicros[Row](expr: Expr[Row, Long]) extends Expr[Row, Timestamp]
  case MakeTimestamp[Row](
    year: Expr[Row, Int],
    month: Expr[Row, Int],
    day: Expr[Row, Int],
    hour: Expr[Row, Int],
    minute: Expr[Row, Int],
    sec: Expr[Row, Double]
  ) extends Expr[Row, Timestamp]
  case MakeDtInterval[Row](
    days: Expr[Row, Long],
    hours: Expr[Row, Int],
    minutes: Expr[Row, Int],
    seconds: Expr[Row, Double]
  ) extends Expr[Row, DayTimeInterval]
  case MakeYmInterval[Row](years: Expr[Row, Int], months: Expr[Row, Int]) extends Expr[Row, YearMonthInterval]
  case HourOf[Row](expr: Expr[Row, Timestamp]) extends Expr[Row, Int]
  case MinuteOf[Row](expr: Expr[Row, Timestamp]) extends Expr[Row, Int]
  case SecondOf[Row](expr: Expr[Row, Timestamp]) extends Expr[Row, Int]
  case FromUtcTimestamp[Row](expr: Expr[Row, Timestamp], tz: String) extends Expr[Row, Timestamp]
  case ToUtcTimestamp[Row](expr: Expr[Row, Timestamp], tz: String) extends Expr[Row, Timestamp]
  case TimestampAdd[Row](unit: String, qty: Expr[Row, Int], ts: Expr[Row, Timestamp]) extends Expr[Row, Timestamp]
  case TimestampDiff[Row](unit: String, start: Expr[Row, Timestamp], end: Expr[Row, Timestamp]) extends Expr[Row, Long]
  case ConvertTimezone[Row](expr: Expr[Row, Timestamp], fromTz: String, toTz: String) extends Expr[Row, Timestamp]
  case Weekday[Row](expr: Expr[Row, Date]) extends Expr[Row, Int]

  case ArrayAppend[Row, A](arr: Expr[Row, Seq[A]], elem: Expr[Row, A]) extends Expr[Row, Seq[A]]
  case ArrayPrepend[Row, A](arr: Expr[Row, Seq[A]], elem: Expr[Row, A]) extends Expr[Row, Seq[A]]
  case ArrayInsert[Row, A](arr: Expr[Row, Seq[A]], pos: Expr[Row, Int], elem: Expr[Row, A]) extends Expr[Row, Seq[A]]
  case ArrayRemove[Row, A](arr: Expr[Row, Seq[A]], elem: Expr[Row, A]) extends Expr[Row, Seq[A]]
  case ArrayRepeat[Row, A](elem: Expr[Row, A], count: Expr[Row, Int]) extends Expr[Row, Seq[A]]
  case ArrayJoin[Row](arr: Expr[Row, Seq[String]], delimiter: String, nullReplacement: Option[String])
      extends Expr[Row, String]
  case ArrayMax[Row, A](arr: Expr[Row, Seq[A]], ordering: Ordering[A]) extends Expr[Row, A]
  case ArrayMin[Row, A](arr: Expr[Row, Seq[A]], ordering: Ordering[A]) extends Expr[Row, A]
  case ArrayCompact[Row, A](arr: Expr[Row, Seq[A]]) extends Expr[Row, Seq[A]]
  case ArrayPosition[Row, A](arr: Expr[Row, Seq[A]], elem: Expr[Row, A]) extends Expr[Row, Int]
  case ArraysZip[Row](arrays: Vector[Expr[Row, Seq[?]]]) extends Expr[Row, Seq[Map[String, Any]]]
  case ArraysOverlap[Row, A](left: Expr[Row, Seq[A]], right: Expr[Row, Seq[A]]) extends Expr[Row, Boolean]
  case MapFromEntries[Row, K, V](arr: Expr[Row, Seq[(K, V)]]) extends Expr[Row, Map[K, V]]
  case GetArray[Row, A](arr: Expr[Row, Seq[A]], index: Expr[Row, Int]) extends Expr[Row, A]

  case Posexplode[Row](expr: Expr[Row, Seq[?]]) extends Expr[Row, Any]
  case ExplodeOuter[Row](expr: Expr[Row, Seq[?]]) extends Expr[Row, Any]
  case Inline[Row](expr: Expr[Row, Seq[?]]) extends Expr[Row, Any]

  case RegrAvgx[Row](y: Expr[Row, Double], x: Expr[Row, Double]) extends Expr[Row, Option[Double]]
  case RegrAvgy[Row](y: Expr[Row, Double], x: Expr[Row, Double]) extends Expr[Row, Option[Double]]
  case RegrCount[Row](y: Expr[Row, Double], x: Expr[Row, Double]) extends Expr[Row, Option[Long]]
  case RegrIntercept[Row](y: Expr[Row, Double], x: Expr[Row, Double]) extends Expr[Row, Option[Double]]
  case RegrR2[Row](y: Expr[Row, Double], x: Expr[Row, Double]) extends Expr[Row, Option[Double]]
  case RegrSlope[Row](y: Expr[Row, Double], x: Expr[Row, Double]) extends Expr[Row, Option[Double]]
  case RegrSxx[Row](y: Expr[Row, Double], x: Expr[Row, Double]) extends Expr[Row, Option[Double]]
  case RegrSxy[Row](y: Expr[Row, Double], x: Expr[Row, Double]) extends Expr[Row, Option[Double]]
  case RegrSyy[Row](y: Expr[Row, Double], x: Expr[Row, Double]) extends Expr[Row, Option[Double]]
  case Kurtosis[Row](expr: Expr[Row, Double]) extends Expr[Row, Option[Double]]
  case Skewness[Row](expr: Expr[Row, Double]) extends Expr[Row, Option[Double]]
  case Percentile[Row](expr: Expr[Row, Double], percentage: Expr[Row, Double]) extends Expr[Row, Option[Double]]
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

  case Greatest[Row, A](exprs: Vector[Expr[Row, A]], ordering: Ordering[A]) extends Expr[Row, A]
  case Least[Row, A](exprs: Vector[Expr[Row, A]], ordering: Ordering[A]) extends Expr[Row, A]
  case NullIf[Row, A](left: Expr[Row, A], right: Expr[Row, A]) extends Expr[Row, A]
  case Nvl2[Row, T, A](test: Expr[Row, T], value: Expr[Row, A], alt: Expr[Row, A]) extends Expr[Row, A]
  case Nanvl[Row](left: Expr[Row, Double], right: Expr[Row, Double]) extends Expr[Row, Double]

  case BitCount[Row](expr: Expr[Row, Int]) extends Expr[Row, Int]
  case BitGet[Row](expr: Expr[Row, Int], pos: Expr[Row, Int]) extends Expr[Row, Int]
  case ShiftLeft[Row](expr: Expr[Row, Int], n: Expr[Row, Int]) extends Expr[Row, Int]
  case ShiftRight[Row](expr: Expr[Row, Int], n: Expr[Row, Int]) extends Expr[Row, Int]
  case ShiftRightUnsigned[Row](expr: Expr[Row, Int], n: Expr[Row, Int]) extends Expr[Row, Int]
  case BitwiseNot[Row](expr: Expr[Row, Int]) extends Expr[Row, Int]
  case BitAndAgg[Row](expr: Expr[Row, Int]) extends Expr[Row, Int]
  case BitOrAgg[Row](expr: Expr[Row, Int]) extends Expr[Row, Int]
  case BitXorAgg[Row](expr: Expr[Row, Int]) extends Expr[Row, Int]

  case ParseJson[Row](expr: Expr[Row, String]) extends Expr[Row, Any]
  case VariantGet[Row](expr: Expr[Row, Any], path: String, targetType: String) extends Expr[Row, Any]
  case TryVariantGet[Row](expr: Expr[Row, Any], path: String, targetType: String) extends Expr[Row, Any]
  case IsVariantNull[Row](expr: Expr[Row, Any]) extends Expr[Row, Boolean]
  case SchemaOfVariant[Row](expr: Expr[Row, Any]) extends Expr[Row, String]
  case IsValidVariant[Row](expr: Expr[Row, String]) extends Expr[Row, Boolean]
  case VariantExplode[Row](expr: Expr[Row, Any]) extends Expr[Row, Any]

  case SketchEstimate[Row](fn: String, sketch: Expr[Row, Binary]) extends Expr[Row, Long]
  case SketchSummary[Row](fn: String, sketch: Expr[Row, Binary], mode: Option[String]) extends Expr[Row, Double]
  case SketchTheta[Row](fn: String, sketch: Expr[Row, Binary]) extends Expr[Row, Double]
  case SketchBinaryOp[Row](
    fn: String,
    left: Expr[Row, Binary],
    right: Expr[Row, Binary],
    lgNomEntries: Option[Int],
    mode: Option[String]
  ) extends Expr[Row, Binary]
  case TupleSketchAgg[Row](
    fn: String,
    key: Expr[Row, ?],
    summary: Expr[Row, ?],
    lgNomEntries: Option[Int],
    mode: Option[String]
  ) extends Expr[Row, Binary]
  case SketchSetAgg[Row](
    fn: String,
    sketch: Expr[Row, Binary],
    lgNomEntries: Option[Int],
    mode: Option[String]
  ) extends Expr[Row, Binary]
  case KllSketchAgg[Row](fn: String, value: Expr[Row, ?], k: Option[Int]) extends Expr[Row, Binary]
  case KllQuantile[Row](fn: String, sketch: Expr[Row, Binary], rank: Expr[Row, Double]) extends Expr[Row, Any]
  case KllRank[Row](fn: String, sketch: Expr[Row, Binary], quantile: Expr[Row, ?]) extends Expr[Row, Double]

  case Struct[Row, T](
    fields: Vector[(String, Expr[Row, ?], ColumnType)],
    schema: Schema[T]
  ) extends Expr[Row, T]
  case GetField[Row, T, F](struct: Expr[Row, T], fieldIndex: ColumnIndex, fieldName: String) extends Expr[Row, F]
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
    inline def md5: Expr[Row, String] = Md5(left)
    inline def sha1: Expr[Row, String] = Sha1(left)
    inline def sha2(bitLength: Int): Expr[Row, String] = Sha2(left, bitLength)
    inline def urlEncode: Expr[Row, String] = UrlEncode(left)
    inline def urlDecode: Expr[Row, String] = UrlDecode(left)
    inline def base64Encode: Expr[Row, String] = Base64Encode(left)
    inline def base64Decode: Expr[Row, String] = Base64Decode(left)
    inline def hex: Expr[Row, String] = Hex(left)
    inline def crc32: Expr[Row, Long] = Crc32(left)
    inline def xxhash64: Expr[Row, Long] = XxHash64(left)
    inline def hash: Expr[Row, Int] = Hash(left)
    inline def aesEncrypt(key: Expr[Row, String]): Expr[Row, Binary] = AesEncrypt(left, key)
    inline def aesDecrypt(key: Expr[Row, String]): Expr[Row, String] = AesDecrypt(left, key)
    inline def tryAesDecrypt(key: Expr[Row, String]): Expr[Row, String] = TryAesDecrypt(left, key)
    inline def getJsonObject(path: String): Expr[Row, String] = GetJsonObject(left, path)
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

  extension [Row](t: Expr[Row, Time]) {
    inline def timeToSeconds: Expr[Row, Decimal] = TimeToSeconds(t)
    inline def timeToMillis: Expr[Row, Long] = TimeToMillis(t)
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
    inline def addDays(days: Expr[Row, Int]): Expr[Row, Date] = DateAddDays(d, days)
    inline def subDays(days: Expr[Row, Int]): Expr[Row, Date] = DateSubDays(d, days)
    inline def addMonths(months: Expr[Row, Int]): Expr[Row, Date] = DateAddMonths(d, months)
    inline def dateDiff(other: Expr[Row, Date]): Expr[Row, Int] = DateDiff(d, other)
    inline def year: Expr[Row, Int] = ExtractYear(d)
    inline def month: Expr[Row, Int] = ExtractMonth(d)
    inline def day: Expr[Row, Int] = ExtractDay(d)
    inline def dayOfWeek: Expr[Row, Int] = DayOfWeek(d)
    inline def dayOfYear: Expr[Row, Int] = DayOfYear(d)
    inline def weekOfYear: Expr[Row, Int] = WeekOfYear(d)
    inline def quarter: Expr[Row, Int] = Quarter(d)
    inline def lastDay: Expr[Row, Date] = LastDay(d)
    inline def nextDay(dayOfWeek: String): Expr[Row, Date] = NextDay(d, dayOfWeek)
    inline def monthsBetween(other: Expr[Row, Date]): Expr[Row, Double] = MonthsBetween(d, other)
    inline def dateTrunc(unit: String): Expr[Row, Date] = DateTrunc(unit, d)
    inline def dateFormat(format: String): Expr[Row, String] = DateFormat(d, format)
  }

  extension [Row, A](e: Expr[Row, Seq[A]]) {
    inline def arraySize: Expr[Row, Int] = ArraySize(e)
    inline def arrayContains(value: Expr[Row, A]): Expr[Row, Boolean] = ArrayContains(e, value)
    inline def explode: Expr[Row, A] = Explode(e)
    inline def arraySort(using ordering: Ordering[A]): Expr[Row, Seq[A]] = ArraySort(e, ordering)
    inline def arrayDistinct: Expr[Row, Seq[A]] = ArrayDistinct(e)
    inline def arrayUnion(other: Expr[Row, Seq[A]]): Expr[Row, Seq[A]] = ArrayUnion(e, other)
    inline def arrayIntersect(other: Expr[Row, Seq[A]]): Expr[Row, Seq[A]] = ArrayIntersect(e, other)
    inline def arrayExcept(other: Expr[Row, Seq[A]]): Expr[Row, Seq[A]] = ArrayExcept(e, other)
    inline def elementAt(index: Expr[Row, Int]): Expr[Row, A] = ElementAt(e, index)
    inline def arraySlice(start: Int, length: Int): Expr[Row, Seq[A]] = ArraySlice(e, start, length)
  }

  extension [Row, A](e: Expr[Row, Seq[Seq[A]]]) {
    inline def flatten: Expr[Row, Seq[A]] = Flatten(e)
  }

  extension [Row, K, V](e: Expr[Row, Map[K, V]]) {
    inline def mapKeys: Expr[Row, Seq[K]] = MapKeys(e)
    inline def mapValues: Expr[Row, Seq[V]] = MapValues(e)
    inline def mapContainsKey(key: Expr[Row, K]): Expr[Row, Boolean] = MapContainsKey(e, key)
    inline def mapEntries: Expr[Row, Seq[(K, V)]] = MapEntries(e)
    inline def mapConcat(other: Expr[Row, Map[K, V]]): Expr[Row, Map[K, V]] = MapConcat(e, other)
  }

  def currentPath[Row](): Expr[Row, String] = CurrentPath()

  def tupleSketchAggDouble[Row](
    key: Expr[Row, ?],
    summary: Expr[Row, ?],
    lgNomEntries: Option[Int] = None,
    mode: Option[String] = None
  ): Expr[Row, Binary] = TupleSketchAgg("tuple_sketch_agg_double", key, summary, lgNomEntries, mode)

  def tupleSketchAggInteger[Row](
    key: Expr[Row, ?],
    summary: Expr[Row, ?],
    lgNomEntries: Option[Int] = None,
    mode: Option[String] = None
  ): Expr[Row, Binary] = TupleSketchAgg("tuple_sketch_agg_integer", key, summary, lgNomEntries, mode)

  def tupleSketchEstimateDouble[Row](sketch: Expr[Row, Binary]): Expr[Row, Long] =
    SketchEstimate("tuple_sketch_estimate_double", sketch)

  def tupleSketchEstimateInteger[Row](sketch: Expr[Row, Binary]): Expr[Row, Long] =
    SketchEstimate("tuple_sketch_estimate_integer", sketch)

  def tupleSketchSummaryDouble[Row](sketch: Expr[Row, Binary], mode: Option[String] = None): Expr[Row, Double] =
    SketchSummary("tuple_sketch_summary_double", sketch, mode)

  def tupleSketchSummaryInteger[Row](sketch: Expr[Row, Binary], mode: Option[String] = None): Expr[Row, Double] =
    SketchSummary("tuple_sketch_summary_integer", sketch, mode)

  def tupleSketchThetaDouble[Row](sketch: Expr[Row, Binary]): Expr[Row, Double] =
    SketchTheta("tuple_sketch_theta_double", sketch)

  def tupleSketchThetaInteger[Row](sketch: Expr[Row, Binary]): Expr[Row, Double] =
    SketchTheta("tuple_sketch_theta_integer", sketch)

  def tupleUnionAggDouble[Row](
    sketch: Expr[Row, Binary],
    lgNomEntries: Option[Int] = None,
    mode: Option[String] = None
  ): Expr[Row, Binary] = SketchSetAgg("tuple_union_agg_double", sketch, lgNomEntries, mode)

  def tupleUnionAggInteger[Row](
    sketch: Expr[Row, Binary],
    lgNomEntries: Option[Int] = None,
    mode: Option[String] = None
  ): Expr[Row, Binary] = SketchSetAgg("tuple_union_agg_integer", sketch, lgNomEntries, mode)

  def tupleIntersectionAggDouble[Row](sketch: Expr[Row, Binary], mode: Option[String] = None): Expr[Row, Binary] =
    SketchSetAgg("tuple_intersection_agg_double", sketch, None, mode)

  def tupleIntersectionAggInteger[Row](sketch: Expr[Row, Binary], mode: Option[String] = None): Expr[Row, Binary] =
    SketchSetAgg("tuple_intersection_agg_integer", sketch, None, mode)

  def tupleUnionDouble[Row](
    left: Expr[Row, Binary],
    right: Expr[Row, Binary],
    lgNomEntries: Option[Int] = None,
    mode: Option[String] = None
  ): Expr[Row, Binary] = SketchBinaryOp("tuple_union_double", left, right, lgNomEntries, mode)

  def tupleUnionInteger[Row](
    left: Expr[Row, Binary],
    right: Expr[Row, Binary],
    lgNomEntries: Option[Int] = None,
    mode: Option[String] = None
  ): Expr[Row, Binary] = SketchBinaryOp("tuple_union_integer", left, right, lgNomEntries, mode)

  def tupleUnionThetaDouble[Row](
    left: Expr[Row, Binary],
    right: Expr[Row, Binary],
    lgNomEntries: Option[Int] = None,
    mode: Option[String] = None
  ): Expr[Row, Binary] = SketchBinaryOp("tuple_union_theta_double", left, right, lgNomEntries, mode)

  def tupleUnionThetaInteger[Row](
    left: Expr[Row, Binary],
    right: Expr[Row, Binary],
    lgNomEntries: Option[Int] = None,
    mode: Option[String] = None
  ): Expr[Row, Binary] = SketchBinaryOp("tuple_union_theta_integer", left, right, lgNomEntries, mode)

  def tupleIntersectionDouble[Row](
    left: Expr[Row, Binary],
    right: Expr[Row, Binary],
    mode: Option[String] = None
  ): Expr[Row, Binary] = SketchBinaryOp("tuple_intersection_double", left, right, None, mode)

  def tupleIntersectionInteger[Row](
    left: Expr[Row, Binary],
    right: Expr[Row, Binary],
    mode: Option[String] = None
  ): Expr[Row, Binary] = SketchBinaryOp("tuple_intersection_integer", left, right, None, mode)

  def tupleIntersectionThetaDouble[Row](
    left: Expr[Row, Binary],
    right: Expr[Row, Binary],
    mode: Option[String] = None
  ): Expr[Row, Binary] = SketchBinaryOp("tuple_intersection_theta_double", left, right, None, mode)

  def tupleIntersectionThetaInteger[Row](
    left: Expr[Row, Binary],
    right: Expr[Row, Binary],
    mode: Option[String] = None
  ): Expr[Row, Binary] = SketchBinaryOp("tuple_intersection_theta_integer", left, right, None, mode)

  def tupleDifferenceDouble[Row](left: Expr[Row, Binary], right: Expr[Row, Binary]): Expr[Row, Binary] =
    SketchBinaryOp("tuple_difference_double", left, right, None, None)

  def tupleDifferenceInteger[Row](left: Expr[Row, Binary], right: Expr[Row, Binary]): Expr[Row, Binary] =
    SketchBinaryOp("tuple_difference_integer", left, right, None, None)

  def tupleDifferenceThetaDouble[Row](left: Expr[Row, Binary], right: Expr[Row, Binary]): Expr[Row, Binary] =
    SketchBinaryOp("tuple_difference_theta_double", left, right, None, None)

  def tupleDifferenceThetaInteger[Row](left: Expr[Row, Binary], right: Expr[Row, Binary]): Expr[Row, Binary] =
    SketchBinaryOp("tuple_difference_theta_integer", left, right, None, None)

  def kllSketchAggBigint[Row](value: Expr[Row, ?], k: Option[Int] = None): Expr[Row, Binary] =
    KllSketchAgg("kll_sketch_agg_bigint", value, k)

  def kllSketchAggFloat[Row](value: Expr[Row, ?], k: Option[Int] = None): Expr[Row, Binary] =
    KllSketchAgg("kll_sketch_agg_float", value, k)

  def kllSketchAggDouble[Row](value: Expr[Row, ?], k: Option[Int] = None): Expr[Row, Binary] =
    KllSketchAgg("kll_sketch_agg_double", value, k)

  def kllMergeAggBigint[Row](sketch: Expr[Row, Binary], k: Option[Int] = None): Expr[Row, Binary] =
    SketchSetAgg("kll_merge_agg_bigint", sketch, k, None)

  def kllMergeAggFloat[Row](sketch: Expr[Row, Binary], k: Option[Int] = None): Expr[Row, Binary] =
    SketchSetAgg("kll_merge_agg_float", sketch, k, None)

  def kllMergeAggDouble[Row](sketch: Expr[Row, Binary], k: Option[Int] = None): Expr[Row, Binary] =
    SketchSetAgg("kll_merge_agg_double", sketch, k, None)

  def kllSketchGetNBigint[Row](sketch: Expr[Row, Binary]): Expr[Row, Long] =
    SketchEstimate("kll_sketch_get_n_bigint", sketch)

  def kllSketchGetNFloat[Row](sketch: Expr[Row, Binary]): Expr[Row, Long] =
    SketchEstimate("kll_sketch_get_n_float", sketch)

  def kllSketchGetNDouble[Row](sketch: Expr[Row, Binary]): Expr[Row, Long] =
    SketchEstimate("kll_sketch_get_n_double", sketch)

  def kllSketchGetQuantileBigint[Row](sketch: Expr[Row, Binary], rank: Expr[Row, Double]): Expr[Row, Any] =
    KllQuantile("kll_sketch_get_quantile_bigint", sketch, rank)

  def kllSketchGetQuantileFloat[Row](sketch: Expr[Row, Binary], rank: Expr[Row, Double]): Expr[Row, Any] =
    KllQuantile("kll_sketch_get_quantile_float", sketch, rank)

  def kllSketchGetQuantileDouble[Row](sketch: Expr[Row, Binary], rank: Expr[Row, Double]): Expr[Row, Any] =
    KllQuantile("kll_sketch_get_quantile_double", sketch, rank)

  def kllSketchGetRankBigint[Row](sketch: Expr[Row, Binary], quantile: Expr[Row, ?]): Expr[Row, Double] =
    KllRank("kll_sketch_get_rank_bigint", sketch, quantile)

  def kllSketchGetRankFloat[Row](sketch: Expr[Row, Binary], quantile: Expr[Row, ?]): Expr[Row, Double] =
    KllRank("kll_sketch_get_rank_float", sketch, quantile)

  def kllSketchGetRankDouble[Row](sketch: Expr[Row, Binary], quantile: Expr[Row, ?]): Expr[Row, Double] =
    KllRank("kll_sketch_get_rank_double", sketch, quantile)

  def kllSketchMergeBigint[Row](left: Expr[Row, Binary], right: Expr[Row, Binary]): Expr[Row, Binary] =
    SketchBinaryOp("kll_sketch_merge_bigint", left, right, None, None)

  def kllSketchMergeFloat[Row](left: Expr[Row, Binary], right: Expr[Row, Binary]): Expr[Row, Binary] =
    SketchBinaryOp("kll_sketch_merge_float", left, right, None, None)

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
    inline def bitCount: Expr[Row, Int] = BitCount(left)
    inline def bitGet(pos: Expr[Row, Int]): Expr[Row, Int] = BitGet(left, pos)
    inline def shiftLeft(n: Expr[Row, Int]): Expr[Row, Int] = ShiftLeft(left, n)
    inline def shiftRight(n: Expr[Row, Int]): Expr[Row, Int] = ShiftRight(left, n)
    inline def shiftRightUnsigned(n: Expr[Row, Int]): Expr[Row, Int] = ShiftRightUnsigned(left, n)
    inline def bitwiseNot: Expr[Row, Int] = BitwiseNot(left)
  }

  /** Aggregate bitwise AND of all non-null values. */
  def bitAnd[Row](expr: Expr[Row, Int]): Expr[Row, Int] = BitAndAgg(expr)

  /** Aggregate bitwise OR of all non-null values. */
  def bitOr[Row](expr: Expr[Row, Int]): Expr[Row, Int] = BitOrAgg(expr)

  /** Aggregate bitwise XOR of all non-null values. */
  def bitXor[Row](expr: Expr[Row, Int]): Expr[Row, Int] = BitXorAgg(expr)

  extension [Row](left: Expr[Row, String]) {
    inline def initcap: Expr[Row, String] = Initcap(left)
    inline def instr(substr: Expr[Row, String]): Expr[Row, Int] = Instr(left, substr)
    inline def substringIndex(delim: String, count: Int): Expr[Row, String] = SubstringIndex(left, delim, count)
    inline def leftStr(n: Expr[Row, Int]): Expr[Row, String] = LeftStr(left, n)
    inline def rightStr(n: Expr[Row, Int]): Expr[Row, String] = RightStr(left, n)
    inline def repeat(n: Expr[Row, Int]): Expr[Row, String] = Repeat(left, n)
    inline def reverse: Expr[Row, String] = Reverse(left)
    inline def lpad(len: Expr[Row, Int], pad: String): Expr[Row, String] = Lpad(left, len, pad)
    inline def rpad(len: Expr[Row, Int], pad: String): Expr[Row, String] = Rpad(left, len, pad)
    inline def translate(matching: String, replace: String): Expr[Row, String] = Translate(left, matching, replace)
    inline def ascii: Expr[Row, Int] = Ascii(left)
    inline def levenshtein(other: Expr[Row, String]): Expr[Row, Int] = Levenshtein(left, other)
    inline def rlike(pattern: String): Expr[Row, Boolean] = Rlike(left, pattern)
    inline def regexpExtractAll(pattern: String, groupIdx: Int): Expr[Row, Seq[String]] =
      RegexpExtractAll(left, pattern, groupIdx)
    inline def splitPart(delim: String, part: Expr[Row, Int]): Expr[Row, String] = SplitPart(left, delim, part)
    inline def parseUrl(part: String): Expr[Row, String] = ParseUrl(left, part)
  }

  /** Character with the given code point. */
  def chr[Row](expr: Expr[Row, Int]): Expr[Row, String] = Chr(expr)

  /** Render a numeric value with the given java.text.DecimalFormat-style pattern. */
  extension [Row](left: Expr[Row, Double]) {
    inline def toChar(format: String): Expr[Row, String] = NumberToChar(left, format)
  }

  /** Format the arguments with the given java.util.Formatter-style format string. */
  def formatString[Row](format: String, args: Expr[Row, Any]*): Expr[Row, String] =
    FormatString(format, args.toVector)

  extension [Row](left: Expr[Row, Double]) {
    inline def cbrt: Expr[Row, Double] = Cbrt(left)
    inline def hypot(other: Expr[Row, Double]): Expr[Row, Double] = Hypot(left, other)
    inline def bround(scale: Int): Expr[Row, Double] = Bround(left, scale)
    inline def sinh: Expr[Row, Double] = Sinh(left)
    inline def cosh: Expr[Row, Double] = Cosh(left)
    inline def tanh: Expr[Row, Double] = Tanh(left)
    inline def asinh: Expr[Row, Double] = Asinh(left)
    inline def acosh: Expr[Row, Double] = Acosh(left)
    inline def atanh: Expr[Row, Double] = Atanh(left)
    inline def degrees: Expr[Row, Double] = Degrees(left)
    inline def radians: Expr[Row, Double] = Radians(left)
    inline def log1p: Expr[Row, Double] = Log1p(left)
    inline def expm1: Expr[Row, Double] = Expm1(left)
    @targetName("tryDivideDouble")
    inline def tryDivide(other: Expr[Row, Double]): Expr[Row, Double] = TryDivideDouble(left, other)
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

  def factorial[Row](expr: Expr[Row, Int]): Expr[Row, Long] = Factorial(expr)

  def pi[Row]: Expr[Row, Double] = Pi[Row]()

  def euler[Row]: Expr[Row, Double] = Euler[Row]()

  /** Standard-normal random values with a fixed seed for reproducibility. */
  def randn[Row](seed: Long): Expr[Row, Double] = Randn(seed)

  extension [Row](left: Expr[Row, Int]) {
    @targetName("pmodInt")
    inline def pmod(right: Expr[Row, Int]): Expr[Row, Int] = PmodInt(left, right)
    @targetName("tryAddInt")
    inline def tryAdd(right: Expr[Row, Int]): Expr[Row, Int] = TryAddInt(left, right)
  }

  extension [Row](left: Expr[Row, Long]) {
    @targetName("pmodLong")
    inline def pmod(right: Expr[Row, Long]): Expr[Row, Long] = PmodLong(left, right)
    @targetName("tryAddLong")
    inline def tryAdd(right: Expr[Row, Long]): Expr[Row, Long] = TryAddLong(left, right)
    @targetName("trySubtractLong")
    inline def trySubtract(right: Expr[Row, Long]): Expr[Row, Long] = TrySubtractLong(left, right)
    @targetName("tryMultiplyLong")
    inline def tryMultiply(right: Expr[Row, Long]): Expr[Row, Long] = TryMultiplyLong(left, right)
    @targetName("tryDivideLong")
    inline def tryDivide(right: Expr[Row, Long]): Expr[Row, Double] = TryDivideLong(left, right)
  }

  extension [Row](t: Expr[Row, Timestamp]) {
    inline def unixTimestamp: Expr[Row, Long] = UnixTimestamp(t)
    inline def hour: Expr[Row, Int] = HourOf(t)
    inline def minute: Expr[Row, Int] = MinuteOf(t)
    inline def second: Expr[Row, Int] = SecondOf(t)
    inline def fromUtc(tz: String): Expr[Row, Timestamp] = FromUtcTimestamp(t, tz)
    inline def toUtc(tz: String): Expr[Row, Timestamp] = ToUtcTimestamp(t, tz)
    inline def convertTimezone(fromTz: String, toTz: String): Expr[Row, Timestamp] =
      ConvertTimezone(t, fromTz, toTz)
    inline def timestampAdd(unit: String, qty: Expr[Row, Int]): Expr[Row, Timestamp] =
      TimestampAdd(unit, qty, t)
  }

  /** Parse an ISO-8601 datetime string into a timestamp (UTC wall-clock interpretation). */
  def toTimestamp[Row](expr: Expr[Row, String]): Expr[Row, Timestamp] = ToTimestamp(expr)

  /** Parse an ISO-8601 date string into a date. */
  def toDate[Row](expr: Expr[Row, String]): Expr[Row, Date] = ToDate(expr)

  def current_date[Row]: Expr[Row, Date] = CurrentDate[Row]()

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
    inline def weekday: Expr[Row, Int] = Weekday(d)
  }

  extension [Row, A](e: Expr[Row, Seq[A]]) {
    inline def arrayAppend(elem: Expr[Row, A]): Expr[Row, Seq[A]] = ArrayAppend(e, elem)
    inline def arrayPrepend(elem: Expr[Row, A]): Expr[Row, Seq[A]] = ArrayPrepend(e, elem)
    inline def arrayInsert(pos: Expr[Row, Int], elem: Expr[Row, A]): Expr[Row, Seq[A]] = ArrayInsert(e, pos, elem)
    inline def arrayRemove(elem: Expr[Row, A]): Expr[Row, Seq[A]] = ArrayRemove(e, elem)
    inline def arrayCompact: Expr[Row, Seq[A]] = ArrayCompact(e)
    inline def arrayPosition(elem: Expr[Row, A]): Expr[Row, Int] = ArrayPosition(e, elem)
    inline def arraysOverlap(other: Expr[Row, Seq[A]]): Expr[Row, Boolean] = ArraysOverlap(e, other)
    inline def getArray(index: Expr[Row, Int]): Expr[Row, A] = GetArray(e, index)
    inline def arrayMax(using ordering: Ordering[A]): Expr[Row, A] = ArrayMax(e, ordering)
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
    inline def posexplode: Expr[Row, Any] = Posexplode(e)
    inline def explodeOuter: Expr[Row, Any] = ExplodeOuter(e)
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
