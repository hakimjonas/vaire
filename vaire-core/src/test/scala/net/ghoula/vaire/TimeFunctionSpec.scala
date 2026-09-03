package net.ghoula.vaire

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.language.strictEquality

import net.ghoula.vaire.prelude.*
import net.ghoula.vaire.types.{DayTimeInterval, Decimal, Time, Timestamp}

class TimeFunctionSpec extends AnyFlatSpec with Matchers {

  given CanEqual[Decimal, Decimal] = CanEqual.derived

  val micros: Array[Long] = Array(
    0L,
    (10 * 3600 + 15 * 60 + 30) * 1000000L + 500000L,
    86399999999L
  )
  val timeCol: Column[?] = Column.time(micros)
  val columns: Vector[Column[?]] = Vector(timeCol)

  def evalOf[A](expr: Expr[Vector[Any], A], columnType: ColumnType): Column[?] =
    ExprInterpreter.evalColumn(expr, columns, columnType) match {
      case Right(col) => col
      case other => fail(s"Evaluation failed: $other")
    }

  "time_to_seconds" should "reinterpret microseconds as DECIMAL(14,6)" in {
    val col = evalOf(Expr.TimeToSeconds[Vector[Any]](Expr.cell("t", ColumnIndex(0))), ColumnType.DecimalType(14, 6))
    col.columnType shouldBe ColumnType.DecimalType(14, 6)
    col.getValue(0) shouldBe Decimal.ofUnscaled(0L)
    col.getValue(1) shouldBe Decimal.ofUnscaled((10 * 3600 + 15 * 60 + 30) * 1000000L + 500000L)
    col.getValue(2) shouldBe Decimal.ofUnscaled(86399999999L)
  }

  "time_to_millis" should "floor-divide to milliseconds" in {
    val col = evalOf(Expr.TimeToMillis[Vector[Any]](Expr.cell("t", ColumnIndex(0))), ColumnType.LongType)
    col.getValue(0) shouldBe 0L
    col.getValue(1) shouldBe (10 * 3600 + 15 * 60 + 30) * 1000L + 500L
    col.getValue(2) shouldBe 86399999L
  }

  "time_to_micros" should "return microseconds unchanged" in {
    val col = evalOf(Expr.TimeToMicros[Vector[Any]](Expr.cell("t", ColumnIndex(0))), ColumnType.LongType)
    col.getValue(1) shouldBe micros(1)
    col.getValue(2) shouldBe 86399999999L
  }

  "time_from_seconds" should "truncate fractional seconds to microseconds" in {
    val seconds = Column.double(Array(0.0, 36930.5, 86399.999999))
    val col = ExprInterpreter
      .evalColumn(
        Expr.TimeFromSeconds[Vector[Any]](Expr.cell("s", ColumnIndex(0))),
        Vector(seconds),
        ColumnType.TimeType
      )
      .toOption
      .get
    col.getValue(0) shouldBe Time.ofMicros(0L)
    col.getValue(1) shouldBe Time.ofMicros(36930500000L)
    col.getValue(2) shouldBe Time.ofMicros(86399999999L)
  }

  it should "reject NaN and infinite seconds" in {
    val seconds = Column.double(Array(Double.NaN))
    val result = ExprInterpreter.evalColumn(
      Expr.TimeFromSeconds[Vector[Any]](Expr.cell("s", ColumnIndex(0))),
      Vector(seconds),
      ColumnType.TimeType
    )
    result.isLeft shouldBe true
  }

  "time_from_millis" should "scale milliseconds to microseconds" in {
    val millis = Column.long(Array(0L, 36930500L, 86399999L))
    val col = ExprInterpreter
      .evalColumn(
        Expr.TimeFromMillis[Vector[Any]](Expr.cell("m", ColumnIndex(0))),
        Vector(millis),
        ColumnType.TimeType
      )
      .toOption
      .get
    col.getValue(1) shouldBe Time.ofMicros(36930500000L)
    col.getValue(2) shouldBe Time.ofMicros(86399999000L)
  }

  "time_from_micros" should "keep microsecond values" in {
    val vals = Column.long(Array(0L, 86399999999L))
    val col = ExprInterpreter
      .evalColumn(
        Expr.TimeFromMicros[Vector[Any]](Expr.cell("m", ColumnIndex(0))),
        Vector(vals),
        ColumnType.TimeType
      )
      .toOption
      .get
    col.getValue(1) shouldBe Time.ofMicros(86399999999L)
  }

  "time_bucket" should "floor timestamps to interval buckets aligned to origin" in {
    val origin = Timestamp.ofEpochMicro(0L)
    val ts = Column.timestamp(Array(0L, 11 * 3600 * 1000000L + 27 * 60 * 1000000L))
    val bucket = Column.dayTimeInterval(Array(15 * 60 * 1000000L, 15 * 60 * 1000000L))
    val col = ExprInterpreter
      .evalColumn(
        Expr
          .TimeBucket[Vector[Any]](Expr.cell("b", ColumnIndex(2)), Expr.cell("ts", ColumnIndex(0)), Expr.const(origin)),
        Vector(ts, Column.int(Array(1)), bucket),
        ColumnType.TimestampType
      )
      .toOption
      .get
    col.getValue(0) shouldBe Timestamp.ofEpochMicro(0L)
    val expectedBucket = Timestamp.ofEpochMicro(11 * 3600 * 1000000L + 15 * 60 * 1000000L)
    col.getValue(1) shouldBe expectedBucket
  }

  it should "reject non-positive bucket sizes" in {
    val ts = Column.timestamp(Array(1000L))
    val bucket = Column.dayTimeInterval(Array(0L))
    val result = ExprInterpreter.evalColumn(
      Expr.TimeBucket[Vector[Any]](
        Expr.cell("b", ColumnIndex(1)),
        Expr.cell("ts", ColumnIndex(0)),
        Expr.const(Timestamp.ofEpochMicro(0L))
      ),
      Vector(ts, bucket),
      ColumnType.TimestampType
    )
    result.isLeft shouldBe true
  }
}
