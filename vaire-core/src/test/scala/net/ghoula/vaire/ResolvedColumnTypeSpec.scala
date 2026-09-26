package net.ghoula.vaire

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.errors.ExecutionError
import net.ghoula.vaire.prelude.*
import net.ghoula.vaire.types.ColumnIndex

/** Locks the resolved-column-type check for operators that carry a declared `ColumnType`.
  *
  * `ExprInterpreter.evalColumn` returns a `Cell` column as-is, so the declared type is a claim the
  * operator has to verify (`TypeChecks.resolvedColumnType`). These specs cover the non-join
  * operators: `sortByExpr`, `sortByExprs`, `groupByAgg` keys, and window partition/order keys.
  */
class ResolvedColumnTypeSpec extends AnyFlatSpec with Matchers {

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

  private def expectMismatch[T](plan: Dataset[T]): Unit =
    DatasetInterpreter.execute(plan) match {
      case Left(_: ExecutionError.TypeMismatch) => ()
      case other => fail(s"expected TypeMismatch, got $other")
    }

  private def nameKey(columnType: ColumnType): KeySpec[Rec] =
    KeySpec[Rec, Any]("name", Expr.Cell[Rec, Any]("name", ColumnIndex(0)), columnType)

  private def ageSort(columnType: ColumnType): SortSpec[Rec] =
    SortSpec[Rec, Long](Expr.Cell[Rec, Long]("age", ColumnIndex(1)), Ordering.Long, columnType, true)

  "sortByExpr" should "reject a declared type that disagrees with the column" in {
    expectMismatch(recs.sortByExpr(Expr.Cell[Rec, Long]("age", ColumnIndex(1)), ColumnType.LongType))
  }

  "sortByExprs" should "reject a declared type that disagrees with the column" in {
    expectMismatch(recs.sortByExprs(Vector(ageSort(ColumnType.LongType))))
  }

  "groupByAgg" should "reject a key declared type that disagrees with the column" in {
    val keys = Vector(nameKey(ColumnType.IntType))
    val aggs = Vector(AggSpec("n", Expr.Count[Rec](), ColumnType.LongType))
    expectMismatch(recs.groupByAgg[Grouped](keys, aggs))
  }

  "withWindow" should "reject a partition type that disagrees with the column" in {
    val windowSpec = WindowSpec[Rec](
      partitionBy = Vector(nameKey(ColumnType.IntType)),
      orderBy = Vector(ageSort(ColumnType.IntType))
    )
    val exprs = Vector(WindowExprSpec("n", Expr.RowNumber[Rec](), ColumnType.IntType))
    expectMismatch(recs.withWindow[Windowed](exprs, windowSpec))
  }

  it should "reject an order type that disagrees with the column" in {
    val windowSpec = WindowSpec[Rec](
      partitionBy = Vector(nameKey(ColumnType.StringType)),
      orderBy = Vector(ageSort(ColumnType.LongType))
    )
    val exprs = Vector(WindowExprSpec("n", Expr.RowNumber[Rec](), ColumnType.IntType))
    expectMismatch(recs.withWindow[Windowed](exprs, windowSpec))
  }
}
