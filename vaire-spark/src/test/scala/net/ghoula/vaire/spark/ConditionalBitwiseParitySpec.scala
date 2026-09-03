package net.ghoula.vaire.spark

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.column.ColumnType
import net.ghoula.vaire.expr.Expr
import net.ghoula.vaire.interpreter.ExprInterpreter
import net.ghoula.vaire.prelude.*

class ConditionalBitwiseParitySpec extends AnyFlatSpec with Matchers with SparkTestBase {

  case class M(id: Int, score: Double, label: String)
  given Schema[M] = Schema.derived

  private lazy val base: DataFrame = spark.sql("""
      SELECT inline(array(
        struct(1, 1.5, 'b'),
        struct(2, 2.5, 'a'),
        struct(3, cast(null as double), 'c')
      )) AS (id_value, score_value, label_value)
    """)

  private lazy val materialized = {
    val rows = base.collect()
    RowConverter.toMaterialized(rows, summon[Schema[M]]).fold(err => fail(s"$err"), identity)
  }

  private def evalBoth[A](
    expr: Expr[M, A],
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

  private def checkParity[A](expr: Expr[M, A], columnType: ColumnType, expected: Vector[Any | Null]): Unit = {
    val (inMemory, sparkValues) = evalBoth(expr, columnType)
    inMemory shouldBe expected
    sparkValues shouldBe expected
  }

  private def idCell: Expr[M, Int] = Expr.cell("id_value", ColumnIndex(0))
  private def scoreCell: Expr[M, Double] = Expr.cell("score_value", ColumnIndex(1))
  private def labelCell: Expr[M, String] = Expr.cell("label_value", ColumnIndex(2))

  "greatest and least" should "pick row-wise extremes, skipping nulls" in {
    checkParity(
      Expr.greatest[M, Int](idCell, Expr.const[M, Int](2)),
      ColumnType.IntType,
      Vector[Any | Null](2, 2, 3)
    )
    checkParity(
      Expr.least[M, Int](idCell, Expr.const[M, Int](2)),
      ColumnType.IntType,
      Vector[Any | Null](1, 2, 2)
    )
  }

  "nullif" should "nullify equal values on both backends" in {
    checkParity(
      Expr.nullif[M, Int](idCell, Expr.const[M, Int](2)),
      ColumnType.IntType,
      Vector[Any | Null](1, SqlNull.value, 3)
    )
  }

  "ifnull/nvl" should "substitute for SqlNull.value on both backends" in {
    checkParity(
      Expr.ifnull[M, Double](scoreCell, Expr.const[M, Double](0.0)),
      ColumnType.DoubleType,
      Vector[Any | Null](1.5, 2.5, 0.0)
    )
  }

  "nvl2" should "branch on test nullability on both backends" in {
    checkParity(
      Expr.nvl2[M, Double, String](scoreCell, labelCell, Expr.const[M, String]("n/a")),
      ColumnType.StringType,
      Vector[Any | Null]("b", "a", "n/a")
    )
  }

  "nanvl" should "replace NaN with the fallback on both backends" in {
    val nanBase =
      spark.sql("SELECT my_nan AS v_value FROM VALUES (cast('nan' as double)), (cast(2.0 as double)) t(my_nan)")
    val expr = Expr.nanvl[M](Expr.cell[M, Double]("v_value", ColumnIndex(0)), Expr.const[M, Double](0.0))
    val (sparkCol, _) = ExprToColumn.convert(expr) match {
      case Right(converted) => converted
      case other => fail(s"Spark conversion failed: $other")
    }
    val sparkValues = nanBase.select(sparkCol).collect().map(r => r.get(0): Any | Null).toVector
    sparkValues shouldBe Vector[Any | Null](0.0, 2.0)
  }

  "bitwise functions" should "agree on both backends" in {
    checkParity(Expr.BitCount[M](idCell), ColumnType.IntType, Vector[Any | Null](1, 1, 2))
    checkParity(
      Expr.BitGet[M](idCell, Expr.const[M, Int](1)),
      ColumnType.IntType,
      Vector[Any | Null](0, 1, 1)
    )
    checkParity(
      Expr.ShiftLeft[M](idCell, Expr.const[M, Int](1)),
      ColumnType.IntType,
      Vector[Any | Null](2, 4, 6)
    )
    checkParity(
      Expr.ShiftRight[M](idCell, Expr.const[M, Int](1)),
      ColumnType.IntType,
      Vector[Any | Null](0, 1, 1)
    )
    checkParity(
      Expr.ShiftRightUnsigned[M](Expr.const[M, Int](-8), Expr.const[M, Int](1)),
      ColumnType.IntType,
      Vector[Any | Null](2147483644, 2147483644, 2147483644)
    )
    checkParity(
      Expr.BitwiseNot[M](idCell),
      ColumnType.IntType,
      Vector[Any | Null](-2, -3, -4)
    )
  }
}
