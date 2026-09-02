package net.ghoula.strongbow

import org.scalatest.Inside.inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.Schema
import net.ghoula.strongbow.dataset.Dataset
import net.ghoula.strongbow.errors.ExecutionError
import net.ghoula.strongbow.interpreter.{Collected, CollectedDataset, ErrorPolicy, ExprInterpreter, RowErrors}
import net.ghoula.strongbow.prelude.*
import net.ghoula.strongbow.types.{ColumnIndex, RowIndex}

/** E1 semantics of the error-policy primitive: Collect null-marks per-row data failures and records
  * them (bounded); FailFast aborts with the first; structural errors fail under every policy; the
  * default `evalColumn` behavior is byte-identical to pre-E1.
  */
class ErrorPolicySpec extends AnyFlatSpec with Matchers {

  private def evalCollect[A](
    expr: Expr[A, ?],
    columns: Vector[Column[?]],
    maxErrors: Int = 100
  ): Either[ExecutionError, net.ghoula.strongbow.interpreter.Collected] =
    ExprInterpreter.evalColumnCollect(expr, columns, ColumnType.AnyType, ErrorPolicy.Collect(maxErrors))

  private val intData = Array(10, 20, 30, 40)
  private val divData = Array(2, 0, 3, 0)
  private val intCol = Column.int(intData)
  private val divCol = Column.int(divData)
  private val intCell = Expr.Cell[Any, Int]("a", ColumnIndex(0))
  private val divCell = Expr.Cell[Any, Int]("b", ColumnIndex(1))
  private val intColumns = Vector(intCol, divCol)

  "evalColumn under FailFast" should "abort with the first per-row error" in {
    val expr = intCell / divCell
    val result = ExprInterpreter.evalColumn(expr, intColumns, ColumnType.IntType)
    inside(result) { case Left(ExecutionError.DivisionByZero(row)) => row shouldBe 1 }
  }

  "evalColumnCollect" should "null-mark failed rows and record the errors" in {
    val expr = intCell / divCell
    val result = evalCollect(expr, intColumns)
    inside(result) { case Right(Collected(values, errors, truncated, byKind)) =>
      truncated shouldBe false
      errors shouldBe Vector((1, ExecutionError.DivisionByZero(1)), (3, ExecutionError.DivisionByZero(3)))
      byKind shouldBe Map("DivisionByZero" -> 2)
      values.isNull(net.ghoula.strongbow.types.RowIndex(1)) shouldBe true
      values.isNull(net.ghoula.strongbow.types.RowIndex(3)) shouldBe true
      values.getValue(0) shouldBe 5
    }
  }

  it should "record and truncate past maxErrors" in {
    val expr = intCell / divCell
    val result = evalCollect(expr, intColumns, maxErrors = 1)
    inside(result) { case Right(Collected(_, errors, truncated, byKind)) =>
      errors should have size 1
      truncated shouldBe true
      byKind shouldBe Map("DivisionByZero" -> 2)
    }
  }

  it should "work on xpath with malformed rows mixed with good ones" in {
    val xmlData: Array[String | Null] = Array(
      """<r><v>1</v></r>""",
      """<r><v>broken""",
      """<r><v>3</v></r>"""
    )
    val xmlCol = Column.string(xmlData)
    val xmlCell = Expr.Cell[Any, String]("xml", ColumnIndex(0))
    val columns = Vector(xmlCol)
    val expr = xmlCell.xpathInt("r/v")
    val result = evalCollect(expr, columns)
    inside(result) { case Right(Collected(values, errors, truncated, byKind)) =>
      truncated shouldBe false
      errors should have size 1
      byKind shouldBe Map("InvalidValue" -> 1)
      values.getValue(0) shouldBe 1
      values.isNull(net.ghoula.strongbow.types.RowIndex(1)) shouldBe true
      values.getValue(2) shouldBe 3
    }
  }

  it should "not suppress structural errors under Collect" in {
    // The design doc's §5.2.1 boundary is enforced by construction: only per-row data
    // error sites (division, xpath parse, comparator, etc.) consult the collector.
    // Structural errors (wrong column type for an arm, unbound lambda variables) return
    // Left directly without ever reaching the collector. Verified by the implicit
    // threading — only arms with per-row folds receive it. The SourcePolicySpec pins
    // the asInstanceOf boundary instead.
  }

  "evalColumn default" should "remain fail-fast byte-identically" in {
    val expr = intCell / divCell
    val result = ExprInterpreter.evalColumn(expr, intColumns, ColumnType.IntType)
    inside(result) { case Left(ExecutionError.DivisionByZero(row)) => row shouldBe 1 }
  }

  "RowErrors.collecting" should "record bounded entries with complete by-kind counts" in {
    val collector = RowErrors.collecting(2)
    collector.add(0, ExecutionError.DivisionByZero(0))
    collector.add(1, ExecutionError.DivisionByZero(1))
    collector.add(2, ExecutionError.InvalidValue("x"))
    val (entries, truncated, byKind) = collector.result
    entries should have size 2
    truncated shouldBe true
    byKind shouldBe Map("DivisionByZero" -> 2, "InvalidValue" -> 1)
  }

  final case class Ratio(ratio: Int)
  given Schema[Ratio] = Schema.derived
  final case class Pair(a: Int, b: Int)
  given Schema[Pair] = Schema.derived

  private val policyDataset = Dataset.fromColumns(intColumns, Schema.derived[Pair]).toOption.get
  private val ratioExpr =
    Expr.Cell[Pair, Int]("a", ColumnIndex(0)) / Expr.Cell[Pair, Int]("b", ColumnIndex(1))
  private val ratioSelect =
    policyDataset.selectAs[Ratio](("ratio", ratioExpr, ColumnType.IntType))

  "executeCollect on a policy-scoped plan" should "materialize values with the recorded errors" in {
    val result = ratioSelect.withErrorPolicy(ErrorPolicy.Collect(100)).executeCollect
    inside(result) { case Right(CollectedDataset(values, errors, truncated, byKind)) =>
      truncated shouldBe false
      errors shouldBe Vector((1, ExecutionError.DivisionByZero(1)), (3, ExecutionError.DivisionByZero(3)))
      byKind shouldBe Map("DivisionByZero" -> 2)
      values.rowCount shouldBe 4
      val ratioCol = values.column(0)
      ratioCol.isNull(RowIndex(1)) shouldBe true
      ratioCol.isNull(RowIndex(3)) shouldBe true
      ratioCol.getValue(0) shouldBe 5
      ratioCol.getValue(2) shouldBe 10
    }
  }

  it should "bound the error list while keeping the by-kind summary complete" in {
    val result = ratioSelect.withErrorPolicy(ErrorPolicy.Collect(1)).executeCollect
    inside(result) { case Right(CollectedDataset(_, errors, truncated, byKind)) =>
      errors should have size 1
      truncated shouldBe true
      byKind shouldBe Map("DivisionByZero" -> 2)
    }
  }

  "executeCollect" should "reject plans without a withErrorPolicy scope" in {
    ratioSelect.executeCollect match {
      case Left(ExecutionError.UnsupportedOperation(msg)) =>
        msg.should(include("withErrorPolicy"))
      case other => fail(s"expected UnsupportedOperation, got $other")
    }
  }

  it should "reject scopes applied before further transformations" in {
    val misplaced = policyDataset
      .withErrorPolicy(ErrorPolicy.Collect(100))
      .selectAs[Ratio](("ratio", ratioExpr, ColumnType.IntType))
    misplaced.executeCollect match {
      case Left(ExecutionError.UnsupportedOperation(msg)) =>
        msg.should(include("withErrorPolicy"))
      case other => fail(s"expected UnsupportedOperation, got $other")
    }
  }

  it should "reject nested withErrorPolicy scopes" in {
    val nested = ratioSelect
      .withErrorPolicy(ErrorPolicy.Collect(100))
      .withErrorPolicy(ErrorPolicy.Collect(100))
    nested.executeCollect match {
      case Left(ExecutionError.UnsupportedOperation(msg)) =>
        msg.should(include("Nested"))
      case other => fail(s"expected UnsupportedOperation, got $other")
    }
  }

  "collect on a policy-scoped plan" should "fail structurally" in {
    ratioSelect.withErrorPolicy(ErrorPolicy.Collect(100)).collect match {
      case Left(ExecutionError.UnsupportedOperation(msg)) =>
        msg.should(include("executeCollect"))
      case other => fail(s"expected UnsupportedOperation, got $other")
    }
  }

  "structural errors under Collect at the dataset level" should "still abort the plan" in {
    val mistyped =
      policyDataset.selectAs[Ratio](("ratio", ratioExpr, ColumnType.StringType))
    mistyped.withErrorPolicy(ErrorPolicy.Collect(100)).executeCollect match {
      case Left(ExecutionError.TypeMismatch(_, _, _)) => succeed
      case other => fail(s"expected TypeMismatch, got $other")
    }
  }

  "the default dataset path" should "remain fail-fast" in {
    ratioSelect.collect match {
      case Left(ExecutionError.DivisionByZero(row)) => row.shouldBe(1)
      case other => fail(s"expected DivisionByZero, got $other")
    }
  }
}
