package net.ghoula.strongbow.spark

import java.nio.file.Files

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.column.{Column, ColumnType}
import net.ghoula.strongbow.errors.ExecutionError
import net.ghoula.strongbow.expr.Expr
import net.ghoula.strongbow.interpreter.ExprInterpreter
import net.ghoula.strongbow.prelude.*
import net.ghoula.strongbow.types.Binary

class SparkOnlySpec extends AnyFlatSpec with Matchers with SparkTestBase {

  case class Event(id: Long, value: Double)
  given Schema[Event] = Schema.derived

  private lazy val base: DataFrame = spark.sql("""
      SELECT inline(array(
        struct(1, cast(1.5 as double)),
        struct(2, cast(2.5 as double)),
        struct(3, cast(3.5 as double)),
        struct(4, cast(10.5 as double))
      )) AS (id_value, value_value)
    """)

  private def valueCell: Expr[Event, Double] = Expr.cell("value_value", ColumnIndex(1))
  private def idCell: Expr[Event, Long] = Expr.cell("id_value", ColumnIndex(0))

  private def evalSpark[A](expr: Expr[Event, A]): (Vector[Any | Null], ColumnType) = {
    val (sparkCol, ct) = ExprToColumn.convert(expr) match {
      case Right(converted) => converted
      case other => fail(s"Spark conversion failed: $other")
    }
    val values = base.select(sparkCol).collect().map(r => r.get(0): Any | Null).toVector
    (values, ct)
  }

  private def assertUnsupported[A](expr: Expr[Event, A], columnType: ColumnType): Unit = {
    ExprInterpreter.evalColumn(expr, Vector(Column.int(Array(1)), Column.double(Array(1.0))), columnType) match {
      case Left(_: ExecutionError.UnsupportedOperation) => (): Unit
      case other => fail(s"Expected UnsupportedOperation, got: $other")
    }
  }

  "kll sketch functions" should "build a sketch, estimate n, and read quantiles on Spark" in {
    val (sketch, aggType) = evalSpark(Expr.kllSketchAggDouble[Event](valueCell))
    aggType shouldBe ColumnType.BinaryType
    sketch.size shouldBe 1
    sketch.head.asInstanceOf[Array[Byte]].length should be > 0

    val sketchCell: Expr[Event, Binary] = Expr.const[Event, Binary](Binary(sketch.head.asInstanceOf[Array[Byte]]))
    val (n, nType) = evalSpark(Expr.kllSketchGetNDouble[Event](sketchCell))
    nType shouldBe ColumnType.LongType
    n.distinct shouldBe Vector[Any | Null](4L)

    val (q, qType) = evalSpark(Expr.kllSketchGetQuantileDouble[Event](sketchCell, Expr.const(0.5)))
    qType shouldBe ColumnType.AnyType
    q.distinct shouldBe Vector[Any | Null](2.5)

    val (rank, rankType) = evalSpark(Expr.kllSketchGetRankDouble[Event](sketchCell, Expr.const(2.5)))
    rankType shouldBe ColumnType.DoubleType
    rank.distinct shouldBe Vector[Any | Null](0.5)

    assertUnsupported(Expr.kllSketchAggDouble[Event](valueCell), ColumnType.BinaryType)
    assertUnsupported(Expr.kllSketchGetNDouble[Event](sketchCell), ColumnType.LongType)
  }

  it should "merge sketches" in {
    val (sketch, _) = evalSpark(Expr.kllSketchAggDouble[Event](valueCell))
    val bytes = sketch.head.asInstanceOf[Array[Byte]]
    val sketchCell: Expr[Event, Binary] = Expr.const[Event, Binary](Binary(bytes))

    val (merged, mergedType) = evalSpark(
      Expr.kllSketchMergeDouble[Event](sketchCell, sketchCell)
    )
    mergedType shouldBe ColumnType.BinaryType
    merged.map(_.asInstanceOf[Array[Byte]].toSeq).distinct.size shouldBe 1

    val mergedCell: Expr[Event, Binary] = Expr.const[Event, Binary](Binary(merged.head.asInstanceOf[Array[Byte]]))
    val (n, _) = evalSpark(Expr.kllSketchGetNDouble[Event](mergedCell))
    n.distinct shouldBe Vector[Any | Null](8L)
  }

  "tuple sketch functions" should "aggregate with summaries and estimate distinct keys on Spark" in {
    val (sketch, aggType) = evalSpark(Expr.tupleSketchAggDouble[Event](idCell, valueCell))
    aggType shouldBe ColumnType.BinaryType
    sketch.size shouldBe 1

    val sketchCell: Expr[Event, Binary] = Expr.const[Event, Binary](Binary(sketch.head.asInstanceOf[Array[Byte]]))

    val (estimate, estimateType) = evalSpark(Expr.tupleSketchEstimateDouble[Event](sketchCell))
    estimateType shouldBe ColumnType.LongType
    estimate.distinct shouldBe Vector[Any | Null](4.0)

    val (theta, thetaType) = evalSpark(Expr.tupleSketchThetaDouble[Event](sketchCell))
    thetaType shouldBe ColumnType.DoubleType
    theta.distinct.head.asInstanceOf[Double] should (be >= 0.0 and be <= 1.0)

    val (summary, summaryType) = evalSpark(Expr.tupleSketchSummaryDouble[Event](sketchCell))
    summaryType shouldBe ColumnType.DoubleType
    summary.distinct shouldBe Vector[Any | Null](18.0)

    val (maxSummary, _) = evalSpark(Expr.tupleSketchSummaryDouble[Event](sketchCell, Some("max")))
    maxSummary.distinct shouldBe Vector[Any | Null](10.5)

    val (union, unionType) = evalSpark(Expr.tupleUnionDouble[Event](sketchCell, sketchCell))
    unionType shouldBe ColumnType.BinaryType
    union.map(_.asInstanceOf[Array[Byte]].toSeq).distinct.size shouldBe 1

    val (intersection, _) = evalSpark(Expr.tupleIntersectionDouble[Event](sketchCell, sketchCell))
    val intersectionCell: Expr[Event, Binary] =
      Expr.const[Event, Binary](Binary(intersection.head.asInstanceOf[Array[Byte]]))
    val (intersectEstimate, _) = evalSpark(Expr.tupleSketchEstimateDouble[Event](intersectionCell))
    intersectEstimate.distinct shouldBe Vector[Any | Null](4.0)

    val (difference, _) = evalSpark(Expr.tupleDifferenceDouble[Event](sketchCell, sketchCell))
    val differenceCell: Expr[Event, Binary] =
      Expr.const[Event, Binary](Binary(difference.head.asInstanceOf[Array[Byte]]))
    val (differenceEstimate, _) = evalSpark(Expr.tupleSketchEstimateDouble[Event](differenceCell))
    differenceEstimate.distinct shouldBe Vector[Any | Null](0.0)

    assertUnsupported(Expr.tupleSketchEstimateDouble[Event](sketchCell), ColumnType.LongType)
    assertUnsupported(Expr.tupleSketchThetaDouble[Event](sketchCell), ColumnType.DoubleType)
    assertUnsupported(Expr.tupleSketchAggDouble[Event](idCell, valueCell), ColumnType.BinaryType)
  }

  "current_path" should "return the file path on Spark and be unsupported in-memory" in {
    val tempDir = Files.createTempDirectory("strongbow-current-path")
    base.write.mode("overwrite").parquet(tempDir.toString)
    val fileDf = spark.read.parquet(tempDir.toString)
    val paths = fileDf.select(functions.current_path()).collect().map(r => r.get(0))
    paths.head.toString should include("spark_catalog")

    assertUnsupported(Expr.currentPath[Event](), ColumnType.StringType)
  }
}
