package net.ghoula.vaire

import org.scalatest.Inside.inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.prelude.*
import net.ghoula.vaire.types.ColumnIndex

/** In-memory semantics of the xpath* family and its try variants. */
class XpathSpec extends AnyFlatSpec with Matchers {

  private def eval[Row, A](
    expr: Expr[Row, A],
    columns: Vector[Column[?]],
    idx: Int
  ): Either[errors.ExecutionError, Any] = {
    val colType = ExprInterpreter.inferExprColumnType(expr, columns)
    ExprInterpreter.evalColumn(expr, columns, colType).map(_.getValue(idx))
  }

  private val xmlData: Array[String | Null] = Array(
    """<r><a><b>1</b></a><a><b>2</b></a><c>3</c></r>""",
    """<r><item id="7" kind="x">v1</item></r>""",
    """<a> <b>x1</b> tail <b>x2</b> </a>"""
  )
  private val xmlCol = Column.string(xmlData)
  private val xmlCell = Expr.Cell[Any, String]("xml", ColumnIndex(0))
  private val columns = Vector(xmlCol)

  "Xpath" should "return nulls for element nodes (Spark getNodeValue semantics)" in {
    val expr = xmlCell.xpath("r/a/b")
    eval(expr, columns, 0) shouldBe Right(Seq(null, null)) // scalafix:ok DisableSyntax.null
    eval(expr, columns, 1) shouldBe Right(Seq.empty)
  }

  it should "return text-node content through text()" in {
    val expr = xmlCell.xpath("r/a/b/text()")
    eval(expr, columns, 0) shouldBe Right(Seq("1", "2"))
  }

  it should "return attribute values through the node-set" in {
    val expr = xmlCell.xpath("r/item/@id")
    eval(expr, columns, 1) shouldBe Right(Seq("7"))
  }

  it should "fail with the row index when the result is not a node-set" in {
    val expr = xmlCell.xpath("count(r/a)")
    val result = eval(expr, columns, 0)
    inside(result) { case Left(errors.ExecutionError.InvalidValue(msg)) =>
      msg should include("not a node-set")
      msg should include("row 0")
    }
  }

  "XpathString" should "return the string-value of the first node" in {
    eval(xmlCell.xpathString("r/a/b"), columns, 0) shouldBe Right("1")
    eval(xmlCell.xpathString("r/item/@kind"), columns, 1) shouldBe Right("x")
    eval(xmlCell.xpathString("r/missing"), columns, 0) shouldBe Right("")
  }

  "XpathBoolean" should "apply the XPath boolean() coercion" in {
    eval(xmlCell.xpathBoolean("r/a"), columns, 0) shouldBe Right(true)
    eval(xmlCell.xpathBoolean("r/missing"), columns, 0) shouldBe Right(false)
    eval(xmlCell.xpathBoolean("count(r/a) = 2"), columns, 0) shouldBe Right(true)
  }

  "XpathInt" should "coerce through number() and truncate NaN to zero" in {
    eval(xmlCell.xpathInt("sum(r//b)"), columns, 0) shouldBe Right(3)
    eval(xmlCell.xpathInt("r/missing"), columns, 0) shouldBe Right(0)
    eval(xmlCell.xpathInt("r/missing"), columns, 0) shouldBe Right(0)
  }

  "XpathDouble" should "keep NaN for empty node-sets" in {
    val result = eval(xmlCell.xpathDouble("r/missing + 1"), columns, 0)
    inside(result) { case Right(d: Double) => d.isNaN shouldBe true }
  }

  "Xpath" should "map null and empty inputs to null rows" in {
    val data: Array[String | Null] = Array(null, "") // scalafix:ok DisableSyntax.null
    val col = Column.string(data)
    val cell = Expr.Cell[Any, String]("xml", ColumnIndex(0))
    eval(cell.xpath("a"), Vector(col), 0).map(v => v == null) shouldBe Right(true) // scalafix:ok DisableSyntax.null

    eval(cell.xpath("a"), Vector(col), 1).map(v => v == null) shouldBe Right(true) // scalafix:ok DisableSyntax.null
  }

  it should "fail on malformed XML with the row index" in {
    val data: Array[String | Null] = Array("<a><b></a>")
    val col = Column.string(data)
    val cell = Expr.Cell[Any, String]("xml", ColumnIndex(0))
    val result = eval(cell.xpathString("a"), Vector(col), 0)
    inside(result) { case Left(errors.ExecutionError.InvalidValue(msg)) =>
      msg should include("Invalid XML document at row 0")
    }
  }

  it should "fail on an invalid path in both modes" in {
    val strict = eval(xmlCell.xpathString("r/["), columns, 0)
    strict.isLeft shouldBe true
    val tryMode = eval(xmlCell.tryXpathString("r/["), columns, 0)
    tryMode.isLeft shouldBe true
  }

  "TryXpath" should "map malformed XML to null rows instead of failing" in {
    val data: Array[String | Null] =
      Array("<r><a><b>1</b></a></r>", "<a><b></a>", null) // scalafix:ok DisableSyntax.null
    val col = Column.string(data)
    val cell = Expr.Cell[Any, String]("xml", ColumnIndex(0))
    val columns2 = Vector(col)
    eval(cell.tryXpathString("r/a/b"), columns2, 0) shouldBe Right("1")
    eval(cell.tryXpathString("r/a/b"), columns2, 1).map(v => v == null) shouldBe Right(
      true
    ) // scalafix:ok DisableSyntax.null
    eval(cell.tryXpathString("r/a/b"), columns2, 2).map(v => v == null) shouldBe Right(
      true
    ) // scalafix:ok DisableSyntax.null
  }

  it should "map evaluation failures to null rows" in {
    // count() is not a node-set: strict fails, try yields null
    eval(xmlCell.xpath("count(r/a)"), columns, 0).isLeft shouldBe true
    eval(xmlCell.tryXpath("count(r/a)"), columns, 0).map(v => v == SqlNull.value) shouldBe Right(true)
  }

  "xpathNumber" should "alias XpathDouble" in {
    xmlCell.xpathNumber("r/a").outputType shouldBe xmlCell.xpathDouble("r/a").outputType
    eval(xmlCell.xpathNumber("sum(r//b)"), columns, 0) shouldBe Right(3.0)
  }

  it should "preserve whitespace text nodes per the xpath config" in {
    eval(xmlCell.xpathString("a"), columns, 2) shouldBe Right(" x1 tail x2 ")
  }
}
