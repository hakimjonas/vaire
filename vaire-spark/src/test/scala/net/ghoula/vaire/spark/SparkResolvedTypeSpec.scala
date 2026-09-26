package net.ghoula.vaire.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.errors.ExecutionError
import net.ghoula.vaire.prelude.*
import net.ghoula.vaire.types.ColumnIndex

/** Backend-parity for the resolved-column-type check on non-join operators.
  *
  * Spark converts expressions to Spark columns and would otherwise ignore the declared
  * `ColumnType`; these specs assert it rejects the same mismatches as the in-memory interpreter.
  */
class SparkResolvedTypeSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  case class Rec(name: String, age: Int, score: Double)
  given recSchema: Schema[Rec] = Schema.derived

  case class Grouped(name: String, n: Long)
  given groupedSchema: Schema[Grouped] = Schema.derived

  case class Windowed(name: String, age: Int, score: Double, n: Int)
  given windowedSchema: Schema[Windowed] = Schema.derived

  private def recs: Dataset[Rec] =
    Dataset
      .fromColumns(
        Vector(Column.string(Array("a", "b")), Column.int(Array(1, 2)), Column.double(Array(1.0, 2.0))),
        summon[Schema[Rec]]
      )
      .toOption
      .get

  private def bothReject[T](plan: Dataset[T]): Unit = {
    DatasetInterpreter.execute(plan) match {
      case Left(_: ExecutionError.TypeMismatch) => ()
      case other => fail(s"in-memory: expected TypeMismatch, got $other")
    }
    sparkInterpreter.execute(plan) match {
      case Left(_: ExecutionError.TypeMismatch) => ()
      case other => fail(s"spark: expected TypeMismatch, got $other")
    }
  }

  "sortByExpr" should "reject a declared type that disagrees with the column in both backends" in {
    bothReject(recs.sortByExpr(Expr.Cell[Rec, Long]("age", ColumnIndex(1)), ColumnType.LongType))
  }

  "sortByExprs" should "reject a declared type that disagrees with the column in both backends" in {
    bothReject(
      recs.sortByExprs(
        Vector(
          SortSpec[Rec, Long](Expr.Cell[Rec, Long]("age", ColumnIndex(1)), Ordering.Long, ColumnType.LongType, true)
        )
      )
    )
  }

  "groupByAgg" should "reject a key declared type that disagrees with the column in both backends" in {
    val keys = Vector(KeySpec[Rec, Any]("name", Expr.Cell[Rec, Any]("name", ColumnIndex(0)), ColumnType.IntType))
    val aggs = Vector(AggSpec("n", Expr.Count[Rec](), ColumnType.LongType))
    bothReject(recs.groupByAgg[Grouped](keys, aggs))
  }

  "withWindow" should "reject a partition type that disagrees with the column in both backends" in {
    val windowSpec = WindowSpec[Rec](
      partitionBy = Vector(KeySpec[Rec, Any]("name", Expr.Cell[Rec, Any]("name", ColumnIndex(0)), ColumnType.IntType)),
      orderBy = Vector(SortSpec[Rec, Int](Expr.Cell("age", ColumnIndex(1)), Ordering.Int, ColumnType.IntType, true))
    )
    val exprs = Vector(WindowExprSpec("n", Expr.RowNumber[Rec](), ColumnType.IntType))
    bothReject(recs.withWindow[Windowed](exprs, windowSpec))
  }
}
