package net.ghoula.vaire.spark

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.column.ColumnType
import net.ghoula.vaire.expr.Expr
import net.ghoula.vaire.interpreter.ExprInterpreter
import net.ghoula.vaire.prelude.*

/** Parity for `from_json` / `to_json`.
  *
  * In-memory parses via Rumil and decodes/encodes via Sarati's derived codecs; Spark maps to the
  * native `from_json`/`to_json` with the caller-supplied DDL schema. The spec chains both
  * functions: `from_json` → `to_json` round-trips to the canonical JSON string on both backends.
  *
  * Divergence (documented, not tested as parity): for a malformed JSON string, Vairë's typed decode
  * yields NULL, while Spark's permissive `from_json` yields an all-null struct (which `to_json`
  * serializes as `{}`). Spark's null-struct has no faithful Scala counterpart, so Vairë chooses
  * NULL.
  */
class JsonCodecParitySpec extends AnyFlatSpec with Matchers with SparkTestBase {

  case class Item(sku: String, qty: Int)
  given Schema[Item] = Schema.derived
  case class Order(id: Long, items: Seq[Item])
  given Schema[Order] = Schema.derived

  import net.ghoula.sarati.codec.JsonDecoders.given
  import net.ghoula.sarati.codec.JsonEncoders.given
  given net.ghoula.sarati.codec.Decoder[net.ghoula.sarati.ast.json.JsonValue, Item] =
    net.ghoula.sarati.codec.Decoder.derived
  given net.ghoula.sarati.codec.Encoder[Item, net.ghoula.sarati.ast.json.JsonValue] =
    net.ghoula.sarati.codec.Encoder.derived
  given net.ghoula.sarati.codec.Decoder[net.ghoula.sarati.ast.json.JsonValue, Order] =
    net.ghoula.sarati.codec.Decoder.derived
  given net.ghoula.sarati.codec.Encoder[Order, net.ghoula.sarati.ast.json.JsonValue] =
    net.ghoula.sarati.codec.Encoder.derived

  case class Doc(j: String)
  given Schema[Doc] = Schema.derived

  private lazy val base: DataFrame = spark.sql("""
      SELECT inline(array(
        struct('{"id":7,"items":[{"sku":"a","qty":2},{"sku":"b","qty":1}]}'),
        struct('{"id":8,"items":[]}'),
        struct(cast(null as string))
      )) AS (j_value)
    """)

  private lazy val materialized = {
    val rows = base.collect()
    RowConverter.toMaterialized(rows, summon[Schema[Doc]]).fold(err => fail(s"$err"), identity)
  }

  private def jCell: Expr[Doc, String] = Expr.cell("j_value", ColumnIndex(0))

  private val schemaDdl = "STRUCT<id: BIGINT, items: ARRAY<STRUCT<sku: STRING, qty: INT>>>"

  private def roundTrip: Expr[Doc, String] =
    Expr.toJson[Doc, Order](Expr.fromJson[Doc, Order](jCell, schemaDdl))

  private def evalInMemory: Vector[Any | Null] =
    ExprInterpreter.evalColumn(roundTrip, materialized.columns, ColumnType.StringType) match {
      case Right(col) => (0 until col.length).toVector.map(col.getValue)
      case other => fail(s"In-memory eval failed: $other")
    }

  private def evalSpark: Vector[Any | Null] = {
    val (sparkCol, _) = ExprToColumn.convert(roundTrip) match {
      case Right(converted) => converted
      case other => fail(s"Spark conversion failed: $other")
    }
    base.select(sparkCol).collect().toVector.map(r => r.get(0): Any | Null)
  }

  "from_json -> to_json" should "round-trip valid JSON and null on both backends" in {
    val expected = Vector[Any | Null](
      "{\"id\":7,\"items\":[{\"sku\":\"a\",\"qty\":2},{\"sku\":\"b\",\"qty\":1}]}",
      "{\"id\":8,\"items\":[]}",
      SqlNull.value
    )
    evalInMemory shouldBe expected
    evalSpark shouldBe expected
  }

  "from_json" should "yield NULL for malformed JSON (Vairë semantics)" in {
    val bad = spark.sql("SELECT 'not json' AS j")
    val badMaterialized = RowConverter
      .toMaterialized(bad.collect(), summon[Schema[Doc]])
      .fold(err => fail(s"$err"), identity)
    val inMemory = ExprInterpreter
      .evalColumn(roundTrip, badMaterialized.columns, ColumnType.StringType)
      .fold(err => fail(s"$err"), identity)
    inMemory.getValue(0) shouldBe SqlNull.value
  }
}
