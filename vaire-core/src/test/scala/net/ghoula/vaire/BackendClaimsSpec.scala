package net.ghoula.vaire

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.column.{Column, ColumnType}
import net.ghoula.vaire.errors.ExecutionError
import net.ghoula.vaire.expr.Expr
import net.ghoula.vaire.interpreter.ExprInterpreter
import net.ghoula.vaire.types.{Binary, ColumnIndex}

/** Pins the in-memory rejection of backend-capability claims. The table and its boundary are in
  * `docs/backend-claims.md`; this suite asserts the in-memory side of each claim.
  */
class BackendClaimsSpec extends AnyFlatSpec with Matchers {

  private val dblCell = Expr.Cell[Any, Double]("v", ColumnIndex(0))
  private val intCell = Expr.Cell[Any, Int]("i", ColumnIndex(1))
  private val boolCell = Expr.Cell[Any, Boolean]("b", ColumnIndex(2))
  private val strCell = Expr.Cell[Any, String]("s", ColumnIndex(3))
  private val longCell = Expr.Cell[Any, Long]("l", ColumnIndex(4))
  private val anyCell = Expr.Cell[Any, String]("any", ColumnIndex(5))

  private val columns: Vector[Column[?]] = Vector(
    Column.double(Array(1.0)),
    Column.int(Array(1)),
    Column.boolean(Array(true)),
    Column.string(Array("x")),
    Column.long(Array(1L)),
    Column.any(Array[Any | Null]("x"))
  )

  private val emptyColumns: Vector[Column[?]] = Vector.empty

  private def assertUnsupported(expr: Expr[Any, ?], expected: String): Unit =
    ExprInterpreter.evalColumn(expr, columns, ColumnType.AnyType) match {
      case Left(ExecutionError.UnsupportedOperation(message)) => message shouldBe expected
      case other => fail(s"expected UnsupportedOperation($expected), got $other")
    }

  private def assertAggregationUnsupported(
    expr: Expr[Any, ?],
    expected: String,
    cols: Vector[Column[?]] = columns
  ): Unit =
    ExprInterpreter.evalAggregation(expr, cols) match {
      case Left(ExecutionError.UnsupportedOperation(message)) => message shouldBe expected
      case other => fail(s"expected UnsupportedOperation($expected), got $other")
    }

  "aggregation expressions" should "be rejected in columnar evalColumn" in {
    val members: Vector[(String, Expr[Any, ?])] = Vector(
      "Sum" -> Expr.Sum(intCell),
      "SumDouble" -> Expr.SumDouble(dblCell),
      "SumLong" -> Expr.SumLong(longCell),
      "Count" -> Expr.Count[Any](),
      "Max" -> Expr.Max(strCell, Ordering.String),
      "Min" -> Expr.Min(strCell, Ordering.String),
      "Avg" -> Expr.Avg(dblCell),
      "CountDistinct" -> Expr.CountDistinct(intCell),
      "CountIf" -> Expr.CountIf(boolCell),
      "StdDev" -> Expr.StdDev(dblCell),
      "StdDevPop" -> Expr.StdDevPop(dblCell),
      "First" -> Expr.First(intCell),
      "Collect" -> Expr.Collect(intCell),
      "PercentileApprox" -> Expr.PercentileApprox(dblCell, 0.5, 100),
      "MaxBy" -> Expr.MaxBy(strCell, dblCell, Ordering.Double.TotalOrdering),
      "MinBy" -> Expr.MinBy(strCell, dblCell, Ordering.Double.TotalOrdering),
      "MaxN" -> Expr.MaxN(intCell, 2, Ordering.Int),
      "MinN" -> Expr.MinN(intCell, 2, Ordering.Int),
      "MaxByN" -> Expr.MaxByN(strCell, dblCell, 2, Ordering.Double.TotalOrdering),
      "MinByN" -> Expr.MinByN(strCell, dblCell, 2, Ordering.Double.TotalOrdering),
      "Variance" -> Expr.Variance(dblCell),
      "VariancePop" -> Expr.VariancePop(dblCell),
      "ApproxCountDistinct" -> Expr.ApproxCountDistinct(intCell),
      "CollectSet" -> Expr.CollectSet(intCell),
      "ExprLast" -> Expr.ExprLast(intCell),
      "AnyValue" -> Expr.AnyValue(intCell),
      "BoolAnd" -> Expr.BoolAnd(boolCell),
      "BoolOr" -> Expr.BoolOr(boolCell),
      "Corr" -> Expr.Corr(dblCell, dblCell),
      "CovarSamp" -> Expr.CovarSamp(dblCell, dblCell),
      "CovarPop" -> Expr.CovarPop(dblCell, dblCell),
      "Median" -> Expr.Median(dblCell),
      "Mode" -> Expr.Mode(intCell)
    )

    members.foreach { case (name, expr) =>
      withClue(s"$name: ") {
        assertUnsupported(expr, "Aggregations not supported in columnar evalColumn")
      }
    }
  }

  "window expressions" should "be rejected in columnar evalColumn" in {
    val members: Vector[(String, Expr[Any, ?])] = Vector(
      "RowNumber" -> Expr.RowNumber[Any](),
      "Rank" -> Expr.Rank[Any](),
      "DenseRank" -> Expr.DenseRank[Any](),
      "Lag" -> Expr.Lag(intCell, 1, None),
      "Lead" -> Expr.Lead(intCell, 1, None),
      "NTile" -> Expr.NTile[Any](4),
      "CumeDist" -> Expr.CumeDist[Any](),
      "PercentRank" -> Expr.PercentRank[Any](),
      "NthValue" -> Expr.NthValue(intCell, 1),
      "FirstValue" -> Expr.FirstValue(intCell),
      "LastValue" -> Expr.LastValue(intCell)
    )

    members.foreach { case (name, expr) =>
      withClue(s"$name: ") {
        assertUnsupported(expr, "Window functions not supported in columnar evalColumn")
      }
    }
  }

  "untyped column comparisons" should "be rejected" in {
    assertUnsupported(
      Expr.Between[Any, String](anyCell, anyCell, anyCell, Ordering.String),
      "Between not supported for untyped columns"
    )
    assertUnsupported(
      Expr.Gt[Any, String](anyCell, anyCell, Ordering.String),
      "Comparison not supported for untyped columns"
    )
  }

  "generator expressions" should "require Dataset-level handling" in {
    val members: Vector[(String, Expr[Any, ?])] = Vector(
      "Posexplode" -> Expr.Posexplode(Expr.const[Any, Seq[Int]](Seq(1))),
      "ExplodeOuter" -> Expr.ExplodeOuter(Expr.const[Any, Seq[Int]](Seq(1))),
      "Inline" -> Expr.Inline(Expr.const[Any, Seq[Seq[Int]]](Seq(Seq(1))))
    )

    members.foreach { case (name, expr) =>
      withClue(s"$name: ") {
        assertUnsupported(expr, "Generator functions require Dataset-level handling")
      }
    }
  }

  "VariantExplode" should "require Dataset-level handling" in {
    assertUnsupported(
      Expr.VariantExplode(Expr.const[Any, String]("x")),
      "VariantExplode requires Dataset-level handling"
    )
  }

  "AES expressions" should "be rejected in memory" in {
    val members: Vector[(String, Expr[Any, ?])] = Vector(
      "AesEncrypt" -> Expr.AesEncrypt(strCell, strCell),
      "AesDecrypt" -> Expr.AesDecrypt(strCell, strCell),
      "TryAesDecrypt" -> Expr.TryAesDecrypt(strCell, strCell)
    )

    members.foreach { case (name, expr) =>
      withClue(s"$name: ") {
        assertUnsupported(
          expr,
          "AES functions use Spark's key-derivation and cipher format; use SparkInterpreter"
        )
      }
    }
  }

  "datasketches and current_path" should "be rejected in memory" in {
    val sketch = Expr.const[Any, Binary](Binary(Array.emptyByteArray))
    val members: Vector[(String, Expr[Any, ?])] = Vector(
      "CurrentPath" -> Expr.CurrentPath[Any](),
      "SketchEstimate" -> Expr.SketchEstimate("theta", sketch),
      "SketchSummary" -> Expr.SketchSummary("theta", sketch, None),
      "SketchTheta" -> Expr.SketchTheta("theta", sketch),
      "SketchBinaryOp" -> Expr.SketchBinaryOp("theta", sketch, sketch, None, None),
      "TupleSketchAgg" -> Expr.TupleSketchAgg("tuple", intCell, dblCell, None, None),
      "SketchSetAgg" -> Expr.SketchSetAgg("theta", sketch, None, None),
      "KllSketchAgg" -> Expr.KllSketchAgg("kll", dblCell, None),
      "KllQuantile" -> Expr.KllQuantile("kll", sketch, Expr.const[Any, Double](0.5)),
      "KllRank" -> Expr.KllRank("kll", sketch, Expr.const[Any, Double](0.5))
    )

    members.foreach { case (name, expr) =>
      withClue(s"$name: ") {
        assertUnsupported(
          expr,
          "Datasketches and current_path are Spark-only; use SparkInterpreter"
        )
      }
    }
  }

  "Spark-only aggregates" should "be rejected on both evalAggregation branches" in {
    val members: Vector[(String, Expr[Any, ?], String)] = Vector(
      ("HistogramNumeric", Expr.HistogramNumeric(dblCell, Expr.const[Any, Int](2)), "HistogramNumeric is Spark-only"),
      (
        "Grouping",
        Expr.Grouping(intCell),
        "Grouping requires a grouping-sets context (Spark-only)"
      ),
      (
        "GroupingId",
        Expr.GroupingId(Vector(intCell)),
        "GroupingId requires a grouping-sets context (Spark-only)"
      )
    )

    members.foreach { case (name, expr, message) =>
      withClue(s"$name non-empty: ") {
        assertAggregationUnsupported(expr, message)
      }
      withClue(s"$name empty: ") {
        assertAggregationUnsupported(expr, message, emptyColumns)
      }
    }
  }
}
