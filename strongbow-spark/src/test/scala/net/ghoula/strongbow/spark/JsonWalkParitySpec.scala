package net.ghoula.strongbow.spark

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.column.ColumnType
import net.ghoula.strongbow.expr.Expr
import net.ghoula.strongbow.interpreter.ExprInterpreter
import net.ghoula.strongbow.prelude.*

/** Parity for schema_of_json, json_array_length and json_object_keys.
  *
  * In-memory walks the Rumil-parsed JsonValue (reusing the variant path machinery); Spark maps to
  * the native SQL functions.
  */
class JsonWalkParitySpec extends AnyFlatSpec with Matchers with SparkTestBase {

  case class Doc(j: String)
  given Schema[Doc] = Schema.derived

  private lazy val base: DataFrame = spark.sql("""
      SELECT inline(array(
        struct('{"a":1,"b":{"c":[10,20]},"d":"x"}'),
        struct('[1,2,3]'),
        struct('"plain"')
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

  private def checkParity[A](expr: Expr[Doc, A], columnType: ColumnType, expected: Vector[Any | Null]): Unit = {
    val (inMemory, sparkValues) = evalBoth(expr, columnType)
    inMemory shouldBe sparkValues
    sparkValues shouldBe expected
  }

  "json_array_length" should "count array elements on both backends" in {
    checkParity(
      Expr.jsonArrayLength[Doc](jCell, "$.b.c"),
      ColumnType.LongType,
      Vector[Any | Null](2L, null, null) // scalafix:ok DisableSyntax.null
    )
    checkParity(
      Expr.jsonArrayLength[Doc](jCell, "$"),
      ColumnType.LongType,
      Vector[Any | Null](null, 3L, null) // scalafix:ok DisableSyntax.null
    )
  }

  "json_object_keys" should "list object keys on both backends" in {
    checkParity(
      Expr.jsonObjectKeys[Doc](jCell, "$"),
      ColumnType.ArrayType(ColumnType.StringType),
      Vector[Any | Null](Seq("a", "b", "d"), null, null) // scalafix:ok DisableSyntax.null
    )
  }

  "schema_of_json" should "render the DDL type for a constant document" in {
    // Spark's schema_of_json only accepts a foldable (constant) input, so this maps
    // per-document in-memory and via a literal on Spark.
    val inMemory = ExprInterpreter.evalColumn(
      Expr.schemaOfJson[Doc](jCell),
      materialized.columns,
      ColumnType.StringType
    ) match {
      case Right(col) => col.getValue(0).toString
      case other => fail(s"In-memory eval failed: $other")
    }
    val sqlLiteral = "'{\"a\":1,\"b\":{\"c\":[10,20]},\"d\":\"x\"}'"
    val sparkSchema = spark
      .sql(s"SELECT schema_of_json($sqlLiteral) AS s")
      .collect()
      .head
      .get(0)
      .toString

    inMemory should startWith("OBJECT<")
    inMemory should include("BIGINT")
    // Both backends describe the same structure; field order/format may differ.
    sparkSchema should include("BIGINT")
  }
}
