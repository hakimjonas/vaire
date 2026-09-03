package net.ghoula.vaire.spark

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.column.ColumnType
import net.ghoula.vaire.expr.Expr
import net.ghoula.vaire.interpreter.ExprInterpreter
import net.ghoula.vaire.prelude.*
import net.ghoula.vaire.types.{Date, DayTimeInterval, Timestamp, YearMonthInterval}

class DateTimeParitySpec extends AnyFlatSpec with Matchers with SparkTestBase {

  case class Ev(ts: Timestamp, d: Date)
  given Schema[Ev] = Schema.derived

  private lazy val base: DataFrame = {
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql("""
      SELECT inline(array(
        struct(cast('2024-01-15 10:30:45' as timestamp), cast('2024-01-15' as date)),
        struct(cast('2024-06-30 23:59:59' as timestamp), cast('2024-06-30' as date))
      )) AS (ts_value, d_value)
    """)
  }

  private lazy val materialized = {
    val rows = base.collect()
    RowConverter.toMaterialized(rows, summon[Schema[Ev]]).fold(err => fail(s"$err"), identity)
  }

  private def tsCell: Expr[Ev, Timestamp] = Expr.cell("ts_value", ColumnIndex(0))
  private def dCell: Expr[Ev, Date] = Expr.cell("d_value", ColumnIndex(1))

  private def checkParity[A](expr: Expr[Ev, A], columnType: ColumnType, expected: Vector[Any | Null]): Unit = {
    val inMemory = ExprInterpreter.evalColumn(expr, materialized.columns, columnType) match {
      case Right(col) => (0 until col.length).toVector.map(col.getValue)
      case other => fail(s"In-memory eval failed: $other")
    }
    val (sparkCol, _) = ExprToColumn.convert(expr) match {
      case Right(converted) => converted
      case other => fail(s"Spark conversion failed: $other")
    }
    val sparkValues = base
      .select(sparkCol)
      .collect()
      .map { r =>
        r.get(0) match {
          case t: java.sql.Timestamp =>
            Timestamp.ofEpochMicro(t.getTime * 1000L + (t.getNanos.toLong % 1000000L) / 1000L): Any | Null
          case d: java.sql.Date => d.toLocalDate: Any | Null
          case dur: java.time.Duration =>
            DayTimeInterval.ofMicros(dur.toMillis * 1000L + (dur.getNano.toLong / 1000L)): Any | Null
          case per: java.time.Period => YearMonthInterval.ofMonths(per.toTotalMonths.toInt): Any | Null
          case ldt: java.time.LocalDateTime =>
            Timestamp.ofEpochMicro(
              ldt.toEpochSecond(java.time.ZoneOffset.UTC) * 1000000L + ldt.getNano.toLong / 1000L
            ): Any | Null
          case other => other: Any | Null
        }
      }
      .toVector
    inMemory shouldBe sparkValues
    sparkValues shouldBe expected
  }

  "timestamp accessors" should "extract hour/minute/second in UTC on both backends" in {
    checkParity(tsCell.hour, ColumnType.IntType, Vector[Any | Null](10, 23))
    checkParity(tsCell.minute, ColumnType.IntType, Vector[Any | Null](30, 59))
    checkParity(tsCell.second, ColumnType.IntType, Vector[Any | Null](45, 59))
  }

  it should "round-trip through unix_timestamp and from_unixtime" in {
    val expectedSeconds = Vector[Any | Null](
      java.time.LocalDateTime.parse("2024-01-15T10:30:45").toEpochSecond(java.time.ZoneOffset.UTC),
      java.time.LocalDateTime.parse("2024-06-30T23:59:59").toEpochSecond(java.time.ZoneOffset.UTC)
    )
    checkParity(tsCell.unixTimestamp, ColumnType.LongType, expectedSeconds)
    checkParity(
      Expr.FromUnixtime[Ev](tsCell.unixTimestamp),
      ColumnType.StringType,
      Vector[Any | Null]("2024-01-15 10:30:45", "2024-06-30 23:59:59")
    )
  }

  it should "build timestamps from seconds, millis and micros" in {
    checkParity(
      Expr.timestampSeconds[Ev](Expr.const[Ev, Double](1705314645.0)),
      ColumnType.TimestampType,
      Vector[Any | Null](
        Timestamp.ofEpochMicro(1705314645000000L),
        Timestamp.ofEpochMicro(1705314645000000L)
      )
    )
    checkParity(
      Expr.timestampMillis[Ev](Expr.const[Ev, Long](1705314645123L)),
      ColumnType.TimestampType,
      Vector[Any | Null](
        Timestamp.ofEpochMicro(1705314645123000L),
        Timestamp.ofEpochMicro(1705314645123000L)
      )
    )
    checkParity(
      Expr.timestampMicros[Ev](Expr.const[Ev, Long](1705314645123456L)),
      ColumnType.TimestampType,
      Vector[Any | Null](
        Timestamp.ofEpochMicro(1705314645123456L),
        Timestamp.ofEpochMicro(1705314645123456L)
      )
    )
  }

  it should "build timestamps and intervals from components" in {
    checkParity(
      Expr.makeTimestamp[Ev](
        Expr.const[Ev, Int](2024),
        Expr.const[Ev, Int](1),
        Expr.const[Ev, Int](15),
        Expr.const[Ev, Int](10),
        Expr.const[Ev, Int](30),
        Expr.const[Ev, Double](45.5)
      ),
      ColumnType.TimestampType,
      Vector[Any | Null](
        Timestamp.ofEpochMicro(
          java.time.LocalDateTime
            .parse("2024-01-15T10:30:45.5")
            .toEpochSecond(java.time.ZoneOffset.UTC) * 1000000L + 500000L
        ),
        Timestamp.ofEpochMicro(
          java.time.LocalDateTime
            .parse("2024-01-15T10:30:45.5")
            .toEpochSecond(java.time.ZoneOffset.UTC) * 1000000L + 500000L
        )
      )
    )
    checkParity(
      Expr.makeDtInterval[Ev](
        Expr.const[Ev, Long](1L),
        Expr.const[Ev, Int](2),
        Expr.const[Ev, Int](3),
        Expr.const[Ev, Double](4.0)
      ),
      ColumnType.DayTimeIntervalType,
      Vector[Any | Null](
        DayTimeInterval.ofMicros(86400000000L + 7200000000L + 180000000L + 4000000L),
        DayTimeInterval.ofMicros(86400000000L + 7200000000L + 180000000L + 4000000L)
      )
    )
    checkParity(
      Expr.makeYmInterval[Ev](Expr.const[Ev, Int](1), Expr.const[Ev, Int](2)),
      ColumnType.YearMonthIntervalType,
      Vector[Any | Null](YearMonthInterval.ofMonths(14), YearMonthInterval.ofMonths(14))
    )
  }

  it should "shift between UTC and named zones consistently" in {
    def shift(wallClock: String, from: java.time.ZoneId, to: java.time.ZoneId): Long =
      java.time.LocalDateTime
        .parse(wallClock)
        .atZone(from)
        .toInstant
        .atZone(to)
        .toLocalDateTime
        .toEpochSecond(java.time.ZoneOffset.UTC) * 1000000L

    checkParity(
      tsCell.fromUtc("America/Los_Angeles"),
      ColumnType.TimestampType,
      Vector[Any | Null](
        Timestamp.ofEpochMicro(
          shift("2024-01-15T10:30:45", java.time.ZoneOffset.UTC, java.time.ZoneId.of("America/Los_Angeles"))
        ),
        Timestamp.ofEpochMicro(
          shift("2024-06-30T23:59:59", java.time.ZoneOffset.UTC, java.time.ZoneId.of("America/Los_Angeles"))
        )
      )
    )
  }

  it should "add and diff timestamps in units" in {
    checkParity(
      tsCell.timestampAdd("HOUR", Expr.const[Ev, Int](5)),
      ColumnType.TimestampType,
      Vector[Any | Null](
        Timestamp.ofEpochMicro(
          java.time.LocalDateTime.parse("2024-01-15T15:30:45").toEpochSecond(java.time.ZoneOffset.UTC) * 1000000L
        ),
        Timestamp.ofEpochMicro(
          java.time.LocalDateTime.parse("2024-07-01T04:59:59").toEpochSecond(java.time.ZoneOffset.UTC) * 1000000L
        )
      )
    )
    checkParity(
      Expr.timestampDiff[Ev]("HOUR", tsCell, tsCell.timestampAdd("DAY", Expr.const[Ev, Int](1))),
      ColumnType.LongType,
      Vector[Any | Null](24L, 24L)
    )
  }

  it should "convert timezones and compute weekdays" in {
    // convert_timezone renders the instant's wall clock in the target zone (NTZ):
    // 10:30:45Z in January is 02:30:45 LA (PST, -8); 23:59:59Z in June is 16:59:59 LA (PDT, -7).
    checkParity(
      tsCell.convertTimezone("UTC", "America/Los_Angeles"),
      ColumnType.TimestampType,
      Vector[Any | Null](
        Timestamp.ofEpochMicro(1705285845000000L),
        Timestamp.ofEpochMicro(1719766799000000L)
      )
    )
    checkParity(dCell.weekday, ColumnType.IntType, Vector[Any | Null](0, 6))
  }

  it should "parse date and timestamp strings" in {
    checkParity(
      Expr.toDate[Ev](Expr.const[Ev, String]("2024-03-01")),
      ColumnType.DateType,
      Vector[Any | Null](Date.ofEpochDay(19783L), Date.ofEpochDay(19783L))
    )
    checkParity(
      Expr.toTimestamp[Ev](Expr.const[Ev, String]("2024-01-15 10:30:45")),
      ColumnType.TimestampType,
      Vector[Any | Null](
        Timestamp.ofEpochMicro(
          java.time.LocalDateTime.parse("2024-01-15T10:30:45").toEpochSecond(java.time.ZoneOffset.UTC) * 1000000L
        ),
        Timestamp.ofEpochMicro(
          java.time.LocalDateTime.parse("2024-01-15T10:30:45").toEpochSecond(java.time.ZoneOffset.UTC) * 1000000L
        )
      )
    )
  }

  "nondeterministic current values" should "be type-compatible on both backends" in {
    val (sparkCol, sparkType) = ExprToColumn.convert(Expr.now[Ev]) match {
      case Right(converted) => converted
      case other => fail(s"Spark conversion failed: $other")
    }
    sparkType shouldBe ColumnType.TimestampType
    base.select(sparkCol).collect().length shouldBe 2
    ExprInterpreter.evalColumn(Expr.now[Ev], materialized.columns, ColumnType.TimestampType) match {
      case Right(col) => col.length shouldBe 2
      case other => fail(s"In-memory eval failed: $other")
    }
  }
}
