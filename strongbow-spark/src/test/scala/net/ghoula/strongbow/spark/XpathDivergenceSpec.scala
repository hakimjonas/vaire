package net.ghoula.strongbow.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.column.ColumnType
import net.ghoula.strongbow.expr.Expr
import net.ghoula.strongbow.interpreter.ExprInterpreter
import net.ghoula.strongbow.prelude.*

/** Pinned divergences between the in-memory xpath* family and Spark, each verified against Spark
  * 4.2 during the validation gate:
  *
  *   1. Prefixed name tests: Spark's engine runs over a namespace-unaware DOM — `b` matches both
  *      `b` and `ns:b` (local-name semantics) and `*:b` fails the query ("Prefix must resolve to a
  *      namespace"). In-memory matches prefix-literally per XPath 1.0. (CDATA was the third
  *      divergence - resolved by sarati 0.3.12's cdata flag, tracked as sarati#11; both backends
  *      now exclude it from text(), pinned in XpathParitySpec's text() coverage.)
  */
class XpathDivergenceSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  case class Doc(xml: String)
  given Schema[Doc] = Schema.derived

  private def evalInMemory[A](xml: String, expr: Expr[Doc, A]): Either[String, Any | Null] =
    RowConverter.toMaterialized(Array(org.apache.spark.sql.Row(xml)), summon[Schema[Doc]]) match {
      case Left(err) => Left(s"$err")
      case Right(m) =>
        ExprInterpreter.evalColumn(expr, m.columns, ColumnType.AnyType) match {
          case Right(col) => Right(col.getValue(0))
          case Left(err) => Left(err.toString)
        }
    }

  private def evalSpark[A](xml: String, fn: String, path: String): Either[String, Any | Null] =
    try {
      val esc = xml.replace("'", "''")
      val escP = path.replace("'", "\\'")
      Right(
        spark.sql(s"SELECT $fn('$esc', '$escP') AS v").collect().head.get(0): Any | Null
      )
    } catch {
      case e: Throwable =>
        Left(s"spark failed: ${Option(e.getCause).map(_.getCause).map(_.getMessage).getOrElse(e.getMessage).take(120)}")
    }

  private def cell(xml: String): Expr[Doc, String] = Expr.const(xml)

  "DTD documents" should "expand internal-subset entities on both backends" in {
    val doc = """<!DOCTYPE r [<!ENTITY w "hello">]><r><m>&w;</m></r>"""
    val inMemory = evalInMemory(doc, cell(doc).xpathString("r/m"))
    inMemory shouldBe Right("hello")
    val sparkSide = evalSpark(doc, "xpath_string", "r/m")
    sparkSide shouldBe Right("hello")
  }

  "prefixed name tests" should "match prefix-literally in-memory while Spark uses local names" in {
    val doc = """<ns:r xmlns:ns="urn:x"><ns:b>1</ns:b><b>2</b></ns:r>"""
    // unprefixed `b` with an unprefixed parent: both match. Note `r/b` matches NOTHING
    // in-memory (the root is ns:r; prefix-literal `r` does not match it) while Spark's
    // local-name semantics match ns:r and both b elements.
    val inMemRb = evalInMemory(doc, cell(doc).xpathString("r/b"))
    inMemRb shouldBe Right("")
    val sparkRb = evalSpark(doc, "xpath_string", "r/b")
    sparkRb shouldBe Right("1")
    // Spark's abbreviated //b and absolute /descendant::b disagree with each other on this DOM
    // (verified empirically: //b -> "2", /descendant::b -> "1"); in-memory is consistent
    // (prefix-literal //b -> "2").
    val inMemB = evalInMemory(doc, cell(doc).xpathString("//b"))
    inMemB shouldBe Right("2")
    val sparkB = evalSpark(doc, "xpath_string", "//b")
    sparkB shouldBe Right("2")
    val inMemDesc = evalInMemory(doc, cell(doc).xpathString("/descendant::b"))
    inMemDesc shouldBe Right("2")
    val sparkDesc = evalSpark(doc, "xpath_string", "/descendant::b")
    sparkDesc shouldBe Right("2")
    // prefixed `ns:b`: in-memory matches the prefixed element; Spark selects nothing
    // (note `r/ns:b` is empty in-memory too — prefix-literal `r` does not match ns:r)
    val inMemPrefixed = evalInMemory(doc, cell(doc).xpathString("//ns:b"))
    inMemPrefixed shouldBe Right("1")
    val sparkPrefixed = evalSpark(doc, "xpath_string", "//ns:b")
    sparkPrefixed shouldBe Right("")
  }

  "`*:b`" should "fail on Spark while in-memory matches both" in {
    val doc = """<ns:r xmlns:ns="urn:x"><ns:b>1</ns:b><b>2</b></ns:r>"""
    val inMemory = evalInMemory(doc, cell(doc).xpathString("//*:b"))
    inMemory shouldBe Right("1")
    val sparkSide = evalSpark(doc, "xpath_string", "//*:b")
    sparkSide.isLeft shouldBe true
  }

  "CDATA sections" should "be excluded from text() on both backends since sarati 0.3.12" in {
    val doc = """<r><t><![CDATA[cd]]></t><e>plain</e></r>"""
    val inMemory = evalInMemory(doc, cell(doc).xpath("//text()"))
    inMemory shouldBe Right(Seq("plain"))
    val sparkSide = evalSpark(doc, "xpath", "//text()")
    sparkSide shouldBe Right(Seq("plain"))
    // string-value still includes CDATA on both backends
    val inMemStr = evalInMemory(doc, cell(doc).xpathString("string(r)"))
    inMemStr shouldBe Right("cdplain")
    val sparkStr = evalSpark(doc, "xpath_string", "string(r)")
    sparkStr shouldBe Right("cdplain")
  }
}
