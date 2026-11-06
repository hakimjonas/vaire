package net.ghoula.vaire.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.column.{Column, ColumnType}
import net.ghoula.vaire.errors.ExecutionError
import net.ghoula.vaire.expr.Expr
import net.ghoula.vaire.params.{KeySpec, SortSpec, WindowExprSpec, WindowSpec}
import net.ghoula.vaire.prelude.*
import net.ghoula.vaire.types.ColumnIndex

/** Pins the Spark-side rejection of backend-capability claims. The table and its boundary are in
  * `docs/backend-claims.md`; this suite asserts the Spark conversion and Dataset paths.
  */
class BackendClaimsSparkSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  private val xmlCell = Expr.Cell[Any, String]("xml", ColumnIndex(0))

  "try-xpath variants" should "require the in-memory interpreter" in {
    val members: Vector[(String, Expr[Any, ?])] = Vector(
      "try_xpath" -> Expr.TryXpath[Any](xmlCell, "a"),
      "try_xpath_string" -> Expr.TryXpathString[Any](xmlCell, "a"),
      "try_xpath_boolean" -> Expr.TryXpathBoolean[Any](xmlCell, "a"),
      "try_xpath_short" -> Expr.TryXpathShort[Any](xmlCell, "a"),
      "try_xpath_int" -> Expr.TryXpathInt[Any](xmlCell, "a"),
      "try_xpath_long" -> Expr.TryXpathLong[Any](xmlCell, "a"),
      "try_xpath_float" -> Expr.TryXpathFloat[Any](xmlCell, "a"),
      "try_xpath_double" -> Expr.TryXpathDouble[Any](xmlCell, "a")
    )

    members.foreach { case (fn, expr) =>
      withClue(s"$fn: ") {
        ExprToColumn.convert(expr) match {
          case Left(ExecutionError.UnsupportedOperation(message)) =>
            message shouldBe s"$fn requires the in-memory interpreter"
          case other => fail(s"expected UnsupportedOperation, got $other")
        }
      }
    }
  }

  "schema_of_json" should "require a constant input on Spark" in {
    ExprToColumn.convert(Expr.schemaOfJson[Any](xmlCell)) match {
      case Left(ExecutionError.UnsupportedOperation(message)) =>
        message shouldBe "schema_of_json requires a constant JSON string on Spark (foldable input)"
      case other => fail(s"expected UnsupportedOperation, got $other")
    }
  }

  "window expressions outside the Dataset window path" should "be rejected by SparkInterpreter" in {
    val ds = makeScores.withWindow[ScoreWithTile](
      Vector(WindowExprSpec("tile", Expr.NTile[Score](4), ColumnType.IntType)),
      windowSpec
    )

    sparkInterpreter.execute(ds) match {
      case Left(ExecutionError.UnsupportedExpression(message)) =>
        message should include("Unsupported window expr")
      case other => fail(s"expected UnsupportedExpression, got $other")
    }
  }

  private case class Score(dept: String, name: String, score: Int)
  private given Schema[Score] = Schema.derived

  private case class ScoreWithTile(dept: String, name: String, score: Int, tile: Int)
  private given Schema[ScoreWithTile] = Schema.derived

  private def makeScores: Dataset[Score] =
    Dataset
      .fromColumns(
        Vector(
          Column.string(Array("Sales", "Sales", "Eng")),
          Column.string(Array("Alice", "Bob", "Dave")),
          Column.int(Array(90, 80, 95))
        ),
        summon[Schema[Score]]
      )
      .toOption
      .get

  private def windowSpec: WindowSpec[Score] =
    WindowSpec[Score](
      partitionBy = Vector(
        KeySpec[Score, Any](
          "dept",
          Expr.Cell[Score, Any]("dept", ColumnIndex(0)),
          ColumnType.StringType
        )
      ),
      orderBy = Vector(
        SortSpec[Score, Int](
          Expr.Cell("score", ColumnIndex(2)),
          summon[Ordering[Int]],
          ColumnType.IntType,
          false
        )
      )
    )
}
