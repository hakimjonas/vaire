package net.ghoula.strongbow.spark

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.column.ColumnType
import net.ghoula.strongbow.expr.Expr
import net.ghoula.strongbow.interpreter.ExprInterpreter
import net.ghoula.strongbow.prelude.*

/** Parity for json_tuple.
  *
  * In-memory walks the Rumil-parsed JsonValue per key (reusing the GetJsonObject conventions);
  * Spark maps to array(get_json_object) over bracket-quoted key paths, which matches Spark's native
  * json_tuple generator for strings, booleans, compounds, JSON nulls, absent keys and invalid
  * documents (the generator cannot be nested in expressions). Number formatting matches Spark via
  * the raw token preserved by rumil's parser and rendered per Jackson node-type semantics (sarati
  * 0.3.12 + rumil 0.3.12). Remaining pinned divergence: keys mixing a single quote with a dot or
  * bracket (unaddressable by any Spark path syntax).
  */
class JsonTupleParitySpec extends AnyFlatSpec with Matchers with SparkTestBase {

  case class Doc(j: String)
  given Schema[Doc] = Schema.derived

  private lazy val base: DataFrame = spark.sql("""
      SELECT inline(array(
        struct('{"a":"v","n":42,"f":9.99,"b":true,"z":null,"o":{"x":1},"r":[1,2],"a.b":"dotted"}'),
        struct('["arr"]'),
        struct('not json'),
        struct('{"a''b":"quoted"}'),
        struct('{"a.b''c":"literal"}'),
        struct('{"e":1e10,"g":1.0}')
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

  "json_tuple" should "extract scalars, compounds and nulls on both backends" in {
    checkParity(
      Expr.jsonTuple[Doc](jCell, "a", "n", "f", "b", "z", "o", "r", "missing"),
      ColumnType.AnyType,
      Vector(
        Seq("v", "42", "9.99", "true", SqlNull.value, "{\"x\":1}", "[1,2]", SqlNull.value),
        Seq(
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value
        ),
        Seq(
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value
        ),
        Seq(
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value
        ),
        Seq(
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value
        ),
        Seq(
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value,
          SqlNull.value
        )
      )
    )
  }

  it should "treat keys as literal top-level names" in {
    checkParity(
      Expr.jsonTuple[Doc](jCell, "a.b", "o"),
      ColumnType.AnyType,
      Vector(
        Seq("dotted", "{\"x\":1}"),
        Seq(SqlNull.value, SqlNull.value),
        Seq(SqlNull.value, SqlNull.value),
        Seq(SqlNull.value, SqlNull.value),
        Seq(SqlNull.value, SqlNull.value),
        Seq(SqlNull.value, SqlNull.value)
      )
    )
  }

  it should "return one null element per requested key for null and invalid rows" in {
    checkParity(
      Expr.jsonTuple[Doc](jCell, "k0", "k1", "k2"),
      ColumnType.AnyType,
      Vector(
        Seq(SqlNull.value, SqlNull.value, SqlNull.value),
        Seq(SqlNull.value, SqlNull.value, SqlNull.value),
        Seq(SqlNull.value, SqlNull.value, SqlNull.value),
        Seq(SqlNull.value, SqlNull.value, SqlNull.value),
        Seq(SqlNull.value, SqlNull.value, SqlNull.value),
        Seq(SqlNull.value, SqlNull.value, SqlNull.value)
      )
    )
  }

  it should "preserve duplicate key requests in order" in {
    checkParity(
      Expr.jsonTuple[Doc](jCell, "n", "a", "n"),
      ColumnType.AnyType,
      Vector(
        Seq("42", "v", "42"),
        Seq(SqlNull.value, SqlNull.value, SqlNull.value),
        Seq(SqlNull.value, SqlNull.value, SqlNull.value),
        Seq(SqlNull.value, SqlNull.value, SqlNull.value),
        Seq(SqlNull.value, SqlNull.value, SqlNull.value),
        Seq(SqlNull.value, SqlNull.value, SqlNull.value)
      )
    )
  }

  it should "address keys containing a single quote via the dot path form" in {
    checkParity(
      Expr.jsonTuple[Doc](jCell, "a'b"),
      ColumnType.AnyType,
      Vector(
        Seq(SqlNull.value),
        Seq(SqlNull.value),
        Seq(SqlNull.value),
        Seq("quoted"),
        Seq(SqlNull.value),
        Seq(SqlNull.value)
      )
    )
  }

  it should "fail Spark conversion for keys mixing a quote with a dot or bracket" in {
    val expr = Expr.jsonTuple[Doc](jCell, "a.b'c")
    ExprToColumn.convert(expr) match {
      case Left(err) => err.toString should include("cannot be addressed")
      case other => fail(s"expected conversion failure, got $other")
    }
    val inMemory = ExprInterpreter.evalColumn(expr, materialized.columns, ColumnType.AnyType) match {
      case Right(col) => (0 until col.length).toVector.map(col.getValue)
      case other => fail(s"In-memory eval failed: $other")
    }
    inMemory(4) shouldBe Seq("literal")
  }

  it should "render number spellings per Spark's Jackson node-type semantics" in {
    // raw tokens drive the rendering: float tokens via Double.toString, integer tokens exact
    checkParity(
      Expr.jsonTuple[Doc](jCell, "e", "g"),
      ColumnType.AnyType,
      Vector(
        Seq(SqlNull.value, SqlNull.value),
        Seq(SqlNull.value, SqlNull.value),
        Seq(SqlNull.value, SqlNull.value),
        Seq(SqlNull.value, SqlNull.value),
        Seq(SqlNull.value, SqlNull.value),
        Seq("1.0E10", "1.0")
      )
    )
  }
}
