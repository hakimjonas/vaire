package net.ghoula.vaire.spark

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.column.ColumnType
import net.ghoula.vaire.errors.ExecutionError
import net.ghoula.vaire.expr.Expr
import net.ghoula.vaire.interpreter.ExprInterpreter
import net.ghoula.vaire.prelude.*

class VariantSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  case class Doc(j: String)
  given Schema[Doc] = Schema.derived

  private lazy val base: DataFrame = spark.sql("""
      SELECT inline(array(
        struct('{"a":1,"b":{"c":[10,20]}}'),
        struct('null'),
        struct('"str"')
      )) AS (j_value)
    """)

  private lazy val materialized = {
    val rows = base.collect()
    RowConverter.toMaterialized(rows, summon[Schema[Doc]]).fold(err => fail(s"$err"), identity)
  }

  private def jCell: Expr[Doc, String] = Expr.cell("j_value", ColumnIndex(0))

  private def evalBoth[A](
    expr: Expr[Doc, A],
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

  "parse_json + try_variant_get" should "agree on both backends" in {
    val expr = Expr.tryVariantGet[Doc](
      Expr.parseJson[Doc](jCell),
      "$.b.c[0]",
      "BIGINT"
    )
    val (inMemory, sparkValues) = evalBoth(expr, ColumnType.VariantType)
    inMemory shouldBe Vector[Any | Null](10L, SqlNull.value, SqlNull.value)
    sparkValues shouldBe inMemory
  }

  it should "extract string and boolean targets" in {
    val strExpr = Expr.tryVariantGet[Doc](Expr.parseJson[Doc](jCell), "$", "VARIANT")
    val (inMemory, sparkValues) = evalBoth(strExpr, ColumnType.VariantType)
    inMemory.size shouldBe 3
    inMemory.zip(sparkValues).map { case (mem, spark) =>
      val memJson = mem match {
        case v: net.ghoula.sarati.ast.json.JsonValue => parsers.json.formatJson(v)
        case other => other.toString
      }
      spark.toString shouldBe memJson
    }
  }

  "try_variant_get" should "return SqlNull.value on type mismatch on both backends" in {
    val expr = Expr.tryVariantGet[Doc](Expr.parseJson[Doc](jCell), "$.a", "STRING")
    val (inMemory, sparkValues) = evalBoth(expr, ColumnType.VariantType)
    inMemory shouldBe Vector[Any | Null]("1", SqlNull.value, SqlNull.value)
    sparkValues shouldBe inMemory
  }

  "is_variant_null" should "agree on both backends" in {
    val expr = Expr.isVariantNull[Doc](Expr.parseJson[Doc](jCell))
    val (inMemory, sparkValues) = evalBoth(expr, ColumnType.BooleanType)
    inMemory shouldBe Vector[Any | Null](false, true, false)
    sparkValues shouldBe inMemory
  }

  "schema_of_variant" should "agree on both backends" in {
    val expr = Expr.schemaOfVariant[Doc](Expr.parseJson[Doc](jCell))
    val (inMemory, sparkValues) = evalBoth(expr, ColumnType.StringType)
    sparkValues shouldBe inMemory
    inMemory.head.toString should include("BIGINT")
  }

  "is_valid_variant" should "accept valid JSON strings on both backends" in {
    val expr = Expr.isValidVariant[Doc](jCell)
    val (inMemory, sparkValues) = evalBoth(expr, ColumnType.BooleanType)
    inMemory shouldBe Vector[Any | Null](true, true, true)
    sparkValues shouldBe inMemory
    inMemory.forall(v => v == SqlNull.value || v == true) shouldBe true
  }

  it should "reject invalid JSON strings on both backends" in {
    val invalid = spark.sql("""SELECT inline(array(struct('nope'), struct('{oops}'))) AS (j_value)""")
    val rows = invalid.collect()
    val md = RowConverter.toMaterialized(rows, summon[Schema[Doc]]).fold(err => fail(s"$err"), identity)
    val expr = Expr.isValidVariant[Doc](jCell)
    val inMemory = ExprInterpreter.evalColumn(expr, md.columns, ColumnType.BooleanType) match {
      case Right(col) => (0 until col.length).toVector.map(col.getValue)
      case other => fail(s"In-memory eval failed: $other")
    }
    val (sparkCol, _) = ExprToColumn.convert(expr) match {
      case Right(converted) => converted
      case other => fail(s"Spark conversion failed: $other")
    }
    val sparkValues = invalid.select(sparkCol).collect().map(r => r.get(0): Any | Null).toVector
    inMemory shouldBe Vector[Any | Null](SqlNull.value, SqlNull.value)
    sparkValues shouldBe inMemory
  }

  "variant_get" should "return SqlNull.value on missing paths on both backends" in {
    val expr = Expr.variantGet[Doc](Expr.parseJson[Doc](jCell), "$.missing", "BIGINT")
    val (inMemory, sparkValues) = evalBoth(expr, ColumnType.VariantType)
    inMemory shouldBe Vector[Any | Null](SqlNull.value, SqlNull.value, SqlNull.value)
    sparkValues shouldBe inMemory
  }

  it should "cast numeric variant values to BOOLEAN on both backends" in {
    val expr = Expr.variantGet[Doc](Expr.parseJson[Doc](jCell), "$.a", "BOOLEAN")
    val (inMemory, sparkValues) = evalBoth(expr, ColumnType.VariantType)
    inMemory shouldBe Vector[Any | Null](true, SqlNull.value, SqlNull.value)
    sparkValues shouldBe inMemory
  }

  it should "error on incompatible casts and return SqlNull.value via try_variant_get" in {
    val strict = Expr.variantGet[Doc](Expr.parseJson[Doc](jCell), "$.b", "BIGINT")
    ExprInterpreter.evalColumn(strict, materialized.columns, ColumnType.VariantType).isLeft shouldBe true
    val (sparkCol, _) = ExprToColumn.convert(strict) match {
      case Right(converted) => converted
      case other => fail(s"Spark conversion failed: $other")
    }
    an[Exception] should be thrownBy base.select(sparkCol).collect()

    val lenient = Expr.tryVariantGet[Doc](Expr.parseJson[Doc](jCell), "$.b", "BIGINT")
    val (inMemory, sparkValues) = evalBoth(lenient, ColumnType.VariantType)
    inMemory shouldBe Vector[Any | Null](SqlNull.value, SqlNull.value, SqlNull.value)
    sparkValues shouldBe inMemory
  }

  it should "error on invalid path syntax in-memory and on Spark" in {
    val expr = Expr.variantGet[Doc](Expr.parseJson[Doc](jCell), "a..b", "BIGINT")
    ExprInterpreter.evalColumn(expr, materialized.columns, ColumnType.VariantType).isLeft shouldBe true
    val (sparkCol, _) = ExprToColumn.convert(expr) match {
      case Right(converted) => converted
      case other => fail(s"Spark conversion failed: $other")
    }
    an[Exception] should be thrownBy base.select(sparkCol).collect()
  }

  "variant_explode" should "be unsupported in-memory" in {
    val expr = Expr.variantExplode[Doc](Expr.parseJson[Doc](jCell))
    ExprInterpreter.evalColumn(expr, materialized.columns, ColumnType.VariantType) match {
      case Left(_: ExecutionError.UnsupportedOperation) => (): Unit
      case other => fail(s"Expected UnsupportedOperation, got: $other")
    }
  }
}
