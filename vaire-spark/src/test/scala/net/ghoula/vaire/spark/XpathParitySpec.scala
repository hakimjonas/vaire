package net.ghoula.vaire.spark

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.column.ColumnType
import net.ghoula.vaire.expr.Expr
import net.ghoula.vaire.interpreter.ExprInterpreter
import net.ghoula.vaire.prelude.*

/** Parity for the strict xpath* family (the try variants are in-memory-only: Spark's Column model
  * cannot catch per-row evaluation failures).
  *
  * Both backends parse the same documents: Spark via its namespace-unaware DOM, in-memory via
  * Rumil's XML parser under [[net.ghoula.sarati.ast.xml.xpathXmlConfig]]. Documented divergences
  * (pinned in [[XpathDivergenceSpec]]): documents with a DTD fail in-memory parsing; prefixed name
  * tests match prefix-literally in-memory while Spark's engine ignores prefixes (and fails on
  * `*:b`); CDATA sections participate in `text()` in-memory but not on Spark.
  */
class XpathParitySpec extends AnyFlatSpec with Matchers with SparkTestBase {

  case class Doc(xml: String)
  given Schema[Doc] = Schema.derived

  private lazy val base: DataFrame = spark.sql("""
      SELECT inline(array(
        struct('<r><a><b>1</b></a><a><b>2</b></a><c>3</c></r>'),
        struct('<r><item id="7" kind="x">v1</item><item id="8">v2</item></r>'),
        struct('<r><v>42</v><v> 4.5 </v></r>'),
        struct('<r><m>&amp;lt;</m><t><![CDATA[cd]]></t></r>')
      )) AS (xml_value)
    """)

  private lazy val materialized = {
    val rows = base.collect()
    RowConverter.toMaterialized(rows, summon[Schema[Doc]]).fold(err => fail(s"$err"), identity)
  }

  private def xmlCell: Expr[Doc, String] = Expr.cell("xml_value", ColumnIndex(0))

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

  "xpath" should "return nulls for element nodes on both backends" in {
    checkParity(
      xmlCell.xpath("r/a/b"),
      ColumnType.AnyType,
      Vector(
        Seq(null, null), // scalafix:ok DisableSyntax.null
        Seq.empty,
        Seq.empty,
        Seq.empty
      )
    )
  }

  it should "return text and attribute values on both backends" in {
    checkParity(
      xmlCell.xpath("r/a/b/text()"),
      ColumnType.AnyType,
      Vector(Seq("1", "2"), Seq.empty, Seq.empty, Seq.empty)
    )
    checkParity(
      xmlCell.xpath("r/item/@id"),
      ColumnType.AnyType,
      Vector(Seq.empty, Seq("7", "8"), Seq.empty, Seq.empty)
    )
  }

  it should "fail on both backends when the result is not a node-set" in {
    // count() yields a number: Spark's xpath() fails the query, in-memory errors the column
    val expr = xmlCell.xpath("count(r/a)")
    val inMemory = (0 until 4).toVector.map { i =>
      ExprInterpreter.evalColumn(expr, materialized.columns, ColumnType.AnyType)
    }
    inMemory.foreach(_.isLeft shouldBe true)
    val sparkCol = ExprToColumn.convert(expr).fold(e => fail(s"$e"), identity)._1
    an[Exception] should be thrownBy base.select(sparkCol).collect()
  }

  "xpath_string" should "return the first node's string-value on both backends" in {
    checkParity(
      xmlCell.xpathString("r/a/b"),
      ColumnType.StringType,
      Vector("1", "", "", "")
    )
    checkParity(
      xmlCell.xpathString("r/item/@kind"),
      ColumnType.StringType,
      Vector("", "x", "", "")
    )
  }

  "xpath_boolean" should "apply boolean() on both backends" in {
    checkParity(
      xmlCell.xpathBoolean("r/a"),
      ColumnType.BooleanType,
      Vector(true, false, false, false)
    )
    checkParity(
      xmlCell.xpathBoolean("count(r/a) = 2"),
      ColumnType.BooleanType,
      Vector(true, false, false, false)
    )
  }

  "xpath_int / xpath_long / xpath_double" should "coerce through number() on both backends" in {
    checkParity(
      xmlCell.xpathInt("sum(r//b)"),
      ColumnType.IntType,
      Vector(3, 0, 0, 0)
    )
    checkParity(
      xmlCell.xpathLong("sum(r//v)"),
      ColumnType.LongType,
      Vector(0, 0, 46, 0)
    )
    val doubleResult = evalBoth(xmlCell.xpathDouble("number(r/missing)"), ColumnType.DoubleType)
    doubleResult._1.foreach { case d: Double => d.isNaN shouldBe true; case other => fail(s"$other") }
    doubleResult._2.foreach { case d: Double => d.isNaN shouldBe true; case other => fail(s"$other") }
  }

  "xpath_short / xpath_float" should "truncate and keep NaN on both backends" in {
    checkParity(
      xmlCell.xpathShort("sum(r//v)"),
      ColumnType.ShortType,
      Vector(0: Short, 0: Short, 46: Short, 0: Short)
    )
    val floatResult = evalBoth(xmlCell.xpathFloat("r/missing"), ColumnType.FloatType)
    floatResult._1.foreach { case f: Float => f.isNaN shouldBe true; case other => fail(s"$other") }
    floatResult._2.foreach { case f: Float => f.isNaN shouldBe true; case other => fail(s"$other") }
  }

  "null and empty inputs" should "yield null rows on both backends" in {
    checkParity(
      xmlCell.xpathString("a"),
      ColumnType.StringType,
      Vector("", "", "", "")
    )
    // DTD documents: pinned separately in XpathDivergenceSpec
  }
}
