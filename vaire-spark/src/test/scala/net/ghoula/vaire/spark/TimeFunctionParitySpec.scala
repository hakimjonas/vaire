package net.ghoula.vaire.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.column.{Column, ColumnType}
import net.ghoula.vaire.expr.Expr
import net.ghoula.vaire.interpreter.ExprInterpreter
import net.ghoula.vaire.prelude.*
import net.ghoula.vaire.types.{DayTimeInterval, Time, Timestamp}

class TimeFunctionParitySpec extends AnyFlatSpec with Matchers with SparkTestBase {

  /** Base DataFrame with a per-row TIME column built by Spark SQL. */
  private lazy val base: DataFrame = {
    spark.conf.set("spark.sql.timeType.enabled", "true")
    spark.sql("""
      SELECT inline(array(
        struct(1, make_time(0, 0, 0)),
        struct(2, make_time(10, 15, 30.5)),
        struct(3, make_time(23, 59, 59.999999))
      )) AS (id_value, at_value)
    """)
  }

  private lazy val materialized = {
    val rows = base.collect()
    RowConverter.toMaterialized(rows, summon[Schema[Timed]]).fold(err => fail(s"$err"), identity)
  }

  case class Timed(id: Int, at: Time)
  given Schema[Timed] = Schema.derived

  private def atCell: Expr[Timed, Time] = Expr.cell("at_value", ColumnIndex(1))

  /** Runs the expr on both backends and returns (in-memory values, Spark values). */
  private def evalBoth[A](
    expr: Expr[Timed, A],
    columnType: ColumnType
  ): (Vector[Any | Null], Vector[Any | Null]) = {
    val inMemory = ExprInterpreter.evalColumn(expr, materialized.columns, columnType) match {
      case Right(col) => (0 until col.length).toVector.map(col.getValue)
      case other => fail(s"In-memory eval failed: $other")
    }
    val (sparkCol, _) = ExprToColumn.convert(expr) match {
      case Right(converted) => converted
      case other => fail(s"Spark conversion failed: $other")
    }
    val sparkValues = base.select(sparkCol).collect().map(r => r.get(0): Any | Null).toVector
    (inMemory, sparkValues)
  }

  private def checkParity[A](expr: Expr[Timed, A], columnType: ColumnType, expected: Vector[Any | Null]): Unit = {
    val (inMemory, sparkValues) = evalBoth(expr, columnType)
    val normalized = sparkValues.map {
      case lt: java.time.LocalTime => Time.fromLocalTime(lt): Any | Null
      case other => other
    }
    inMemory shouldBe expected
    normalized shouldBe expected
  }

  "TIME functions" should "agree on both backends for time_to_seconds" in {
    val expectedMicros = Vector[Long](0L, (10 * 3600 + 15 * 60 + 30) * 1000000L + 500000L, 86399999999L)

    val (inMemory, sparkValues) = evalBoth(Expr.TimeToSeconds[Timed](atCell), ColumnType.DecimalType(14, 6))
    inMemory.map { case d: Long => d; case other => fail(s"Expected Decimal, got $other") } shouldBe expectedMicros
    sparkValues.map {
      case bd: java.math.BigDecimal => bd.unscaledValue().longValue();
      case other =>
        fail(s"Expected BigDecimal, got $other")
    } shouldBe expectedMicros
  }

  it should "agree on both backends for time_to_millis and time_to_micros" in {
    checkParity(
      Expr.TimeToMillis[Timed](atCell),
      ColumnType.LongType,
      Vector[Any | Null](0L, (10 * 3600 + 15 * 60 + 30) * 1000L + 500L, 86399999L)
    )
    checkParity(
      Expr.TimeToMicros[Timed](atCell),
      ColumnType.LongType,
      Vector[Any | Null](0L, (10 * 3600 + 15 * 60 + 30) * 1000000L + 500000L, 86399999999L)
    )
  }

  it should "agree on both backends for time_from_millis and time_from_micros" in {
    checkParity(
      Expr.timeFromMillis[Timed](Expr.cell[Timed, Int]("id_value", ColumnIndex(0)).castToLong),
      ColumnType.TimeType,
      Vector[Any | Null](Time.ofMicros(1000L), Time.ofMicros(2000L), Time.ofMicros(3000L))
    )
    checkParity(
      Expr.timeFromMicros[Timed](Expr.cell[Timed, Int]("id_value", ColumnIndex(0)).castToLong),
      ColumnType.TimeType,
      Vector[Any | Null](Time.ofMicros(1L), Time.ofMicros(2L), Time.ofMicros(3L))
    )
  }

  it should "agree on both backends for time_from_seconds" in {
    val secondsCol = materialized.columns.head match {
      case Column.IntColumn(data, nulls) =>
        val out = data.map(_.toDouble)
        Column.double(out, nulls)
      case other => fail(s"Expected IntColumn, got ${other.columnType}")
    }
    val materializedWithSeconds = materialized.copy(columns = materialized.columns :+ secondsCol)
    val expr = Expr.timeFromSeconds[Timed](Expr.cell[Timed, Double]("s", ColumnIndex(2)))

    val inMemory = ExprInterpreter.evalColumn(expr, materializedWithSeconds.columns, ColumnType.TimeType) match {
      case Right(col) => (0 until col.length).toVector.map(col.getValue)
      case other => fail(s"In-memory eval failed: $other")
    }
    val (sparkCol, _) = ExprToColumn.convert(expr) match {
      case Right(converted) => converted
      case other => fail(s"Spark conversion failed: $other")
    }
    val sparkValues = base
      .select(col("id_value").cast("double").as("s"))
      .select(sparkCol)
      .collect()
      .map(r => r.get(0): Any | Null)
      .toVector

    inMemory shouldBe Vector[Time](
      Time.ofMicros(1000000L),
      Time.ofMicros(2000000L),
      Time.ofMicros(3000000L)
    ).toVector.map(t => t: Any | Null)
    sparkValues.map {
      case lt: java.time.LocalTime => Time.fromLocalTime(lt): Any | Null
      case other => other
    } shouldBe inMemory
  }

  it should "agree on both backends for time_bucket with sub-day buckets" in {
    val origin = Timestamp.ofEpochMicro(0L)
    val tsMicros = 11 * 3600L * 1000000L + 27 * 60L * 1000000L
    val expected = Timestamp.ofEpochMicro(11 * 3600L * 1000000L + 15 * 60L * 1000000L)

    val tsCol = Column.timestamp(Array.fill(materialized.rowCount)(tsMicros))
    val withTs = materialized.copy(columns = materialized.columns :+ tsCol)
    val expr = Expr.timeBucket[Timed](
      Expr.const(DayTimeInterval.ofMicros(15 * 60L * 1000000L)),
      Expr.cell[Timed, Timestamp]("ts_value", ColumnIndex(2)),
      Expr.const(origin)
    )

    val inMemory = ExprInterpreter.evalColumn(expr, withTs.columns, ColumnType.TimestampType) match {
      case Right(c) => (0 until c.length).toVector.map(c.getValue)
      case other => fail(s"In-memory eval failed: $other")
    }
    inMemory shouldBe Vector.fill(3)(expected: Any | Null)

    val (sparkCol, _) = ExprToColumn.convert(expr) match {
      case Right(converted) => converted
      case other => fail(s"Spark conversion failed: $other")
    }
    val sparkValues = base
      .select(lit(new java.sql.Timestamp(tsMicros / 1000L)).as("ts_value"))
      .select(sparkCol)
      .collect()
      .map(r => r.get(0): Any | Null)
      .toVector
    val expectedMicros = 11 * 3600L * 1000000L + 15 * 60L * 1000000L
    sparkValues.foreach {
      case t: java.sql.Timestamp => t.getTime * 1000L + t.getNanos.toLong / 1000L shouldBe expectedMicros
      case other => fail(s"Expected Timestamp, got $other")
    }
  }
}
