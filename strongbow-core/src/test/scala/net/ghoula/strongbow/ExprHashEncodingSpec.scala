package net.ghoula.strongbow

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.prelude.*
import net.ghoula.strongbow.types.ColumnIndex

class ExprHashEncodingSpec extends AnyFlatSpec with Matchers {

  private def eval[Row, A](
    expr: Expr[Row, A],
    columns: Vector[Column[?]],
    idx: Int
  ): Either[errors.ExecutionError, Any] = {
    val effectiveColumns =
      if (columns.isEmpty || columns.head.length == 0) Vector(Column.int(Array(0)))
      else columns
    val colType = ExprInterpreter.inferExprColumnType(expr, effectiveColumns)
    ExprInterpreter.evalColumn(expr, effectiveColumns, colType).map(_.getValue(idx))
  }

  private val strData: Array[String | Null] = Array("hello", "world", "foo bar")
  private val strCol = Column.string(strData)
  private val columns = Vector(strCol)
  private val cell = Expr.Cell[Any, String]("s", ColumnIndex(0))

  "Md5" should "compute MD5 hash of a string" in {
    val expr = cell.md5
    val result = eval(expr, columns, 0)
    result shouldBe Right("5d41402abc4b2a76b9719d911017c592")
  }

  it should "compute MD5 for different inputs" in {
    val result = eval(cell.md5, columns, 1)
    result shouldBe Right("7d793037a0760186574b0282f2f435e7")
  }

  "Sha1" should "compute SHA-1 hash of a string" in {
    val expr = cell.sha1
    val result = eval(expr, columns, 0)
    result shouldBe Right("aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d")
  }

  "Sha2" should "compute SHA-256 hash by default (bitLength=0)" in {
    val expr = cell.sha2(0)
    val result = eval(expr, columns, 0)
    result shouldBe Right("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
  }

  it should "compute SHA-256 hash with bitLength=256" in {
    val expr = cell.sha2(256)
    val result = eval(expr, columns, 0)
    result shouldBe Right("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
  }

  it should "compute SHA-512 hash" in {
    val expr = cell.sha2(512)
    val result = eval(expr, columns, 0)
    result shouldBe Right(
      "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043"
    )
  }

  "UrlEncode" should "encode a string for URLs" in {
    val data: Array[String | Null] = Array("hello world", "foo=bar&baz=qux")
    val col = Column.string(data)
    val cols = Vector(col)
    val c = Expr.Cell[Any, String]("s", ColumnIndex(0))
    eval(c.urlEncode, cols, 0) shouldBe Right("hello+world")
    eval(c.urlEncode, cols, 1) shouldBe Right("foo%3Dbar%26baz%3Dqux")
  }

  "UrlDecode" should "decode a URL-encoded string" in {
    val data: Array[String | Null] = Array("hello+world", "foo%3Dbar%26baz%3Dqux")
    val col = Column.string(data)
    val cols = Vector(col)
    val c = Expr.Cell[Any, String]("s", ColumnIndex(0))
    eval(c.urlDecode, cols, 0) shouldBe Right("hello world")
    eval(c.urlDecode, cols, 1) shouldBe Right("foo=bar&baz=qux")
  }

  "Base64Encode" should "encode a string to base64" in {
    val expr = cell.base64Encode
    val result = eval(expr, columns, 0)
    result shouldBe Right("aGVsbG8=")
  }

  "Base64Decode" should "decode a base64 string" in {
    val data: Array[String | Null] = Array("aGVsbG8=", "d29ybGQ=")
    val col = Column.string(data)
    val cols = Vector(col)
    val c = Expr.Cell[Any, String]("s", ColumnIndex(0))
    eval(c.base64Decode, cols, 0) shouldBe Right("hello")
    eval(c.base64Decode, cols, 1) shouldBe Right("world")
  }

  "Base64 roundtrip" should "encode then decode back to original" in {
    val expr = cell.base64Encode
    val encoded = eval(expr, columns, 0)
    encoded shouldBe Right("aGVsbG8=")
    val data2: Array[String | Null] = Array("aGVsbG8=")
    val col2 = Column.string(data2)
    val cols2 = Vector(col2)
    val c2 = Expr.Cell[Any, String]("s", ColumnIndex(0))
    eval(c2.base64Decode, cols2, 0) shouldBe Right("hello")
  }

  "Hex" should "hex-encode string bytes" in {
    val expr = cell.hex
    val result = eval(expr, columns, 0)
    result shouldBe Right("68656c6c6f")
  }

  it should "hex-encode world" in {
    val result = eval(cell.hex, columns, 1)
    result shouldBe Right("776f726c64")
  }

  "GetJsonObject" should "extract a top-level field" in {
    val jsonData: Array[String | Null] = Array("""{"name":"Alice","age":30}""", """{"name":"Bob"}""")
    val jsonCol = Column.string(jsonData)
    val jsonColumns = Vector(jsonCol)
    val jsonCell = Expr.Cell[Any, String]("json", ColumnIndex(0))
    eval(jsonCell.getJsonObject("$.name"), jsonColumns, 0) shouldBe Right("Alice")
    eval(jsonCell.getJsonObject("$.name"), jsonColumns, 1) shouldBe Right("Bob")
  }

  it should "extract nested fields" in {
    val jsonData: Array[String | Null] = Array("""{"a":{"b":{"c":"deep"}}}""")
    val jsonCol = Column.string(jsonData)
    val jsonColumns = Vector(jsonCol)
    val jsonCell = Expr.Cell[Any, String]("json", ColumnIndex(0))
    eval(jsonCell.getJsonObject("$.a.b.c"), jsonColumns, 0) shouldBe Right("deep")
  }

  it should "return numeric values as strings" in {
    val jsonData: Array[String | Null] = Array("""{"count":42,"price":9.99}""")
    val jsonCol = Column.string(jsonData)
    val jsonColumns = Vector(jsonCol)
    val jsonCell = Expr.Cell[Any, String]("json", ColumnIndex(0))
    eval(jsonCell.getJsonObject("$.count"), jsonColumns, 0) shouldBe Right("42")
    eval(jsonCell.getJsonObject("$.price"), jsonColumns, 0) shouldBe Right("9.99")
  }

  it should "return SqlNull.value for missing path" in {
    val jsonData: Array[String | Null] = Array("""{"name":"Alice"}""")
    val jsonCol = Column.string(jsonData)
    val jsonColumns = Vector(jsonCol)
    val jsonCell = Expr.Cell[Any, String]("json", ColumnIndex(0))
    val result = eval(jsonCell.getJsonObject("$.missing"), jsonColumns, 0)
    result shouldBe Right(SqlNull.value)
  }

  it should "return SqlNull.value for invalid JSON" in {
    val jsonData: Array[String | Null] = Array("not json")
    val jsonCol = Column.string(jsonData)
    val jsonColumns = Vector(jsonCol)
    val jsonCell = Expr.Cell[Any, String]("json", ColumnIndex(0))
    eval(jsonCell.getJsonObject("$.key"), jsonColumns, 0) shouldBe Right(
      SqlNull.value
    )
  }

  "JsonTuple" should "extract one element per key in order" in {
    val jsonData: Array[String | Null] = Array("""{"a":"v","n":42,"b":true}""")
    val jsonCol = Column.string(jsonData)
    val jsonColumns = Vector(jsonCol)
    val jsonCell = Expr.Cell[Any, String]("json", ColumnIndex(0))
    eval(Expr.jsonTuple(jsonCell, "a", "n", "b"), jsonColumns, 0) shouldBe Right(
      Seq("v", "42", "true")
    )
  }

  it should "render compound values as compact JSON text" in {
    val jsonData: Array[String | Null] = Array("""{"o":{"x":1},"r":[1,2]}""")
    val jsonCol = Column.string(jsonData)
    val jsonColumns = Vector(jsonCol)
    val jsonCell = Expr.Cell[Any, String]("json", ColumnIndex(0))
    eval(Expr.jsonTuple(jsonCell, "o", "r"), jsonColumns, 0) shouldBe Right(
      Seq("""{"x":1}""", "[1,2]")
    )
  }

  it should "return null elements for absent, null and invalid rows" in {
    val jsonData: Array[String | Null] =
      Array("""{"a":"v","z":null}""", """["arr"]""", "not json")
    val jsonCol = Column.string(jsonData)
    val jsonColumns = Vector(jsonCol)
    val jsonCell = Expr.Cell[Any, String]("json", ColumnIndex(0))
    val result = eval(Expr.jsonTuple(jsonCell, "a", "z", "missing"), jsonColumns, 0)
    result match {
      case Right(values: Seq[?]) =>
        values should have size 3
        values(0) shouldBe "v"
        values(1) shouldBe SqlNull.value
        values(2) shouldBe SqlNull.value
      case other => fail(s"unexpected: $other")
    }
    eval(Expr.jsonTuple(jsonCell, "a", "z", "missing"), jsonColumns, 1) shouldBe Right(
      Seq(SqlNull.value, SqlNull.value, SqlNull.value)
    )
    eval(Expr.jsonTuple(jsonCell, "a", "z", "missing"), jsonColumns, 2) shouldBe Right(
      Seq(SqlNull.value, SqlNull.value, SqlNull.value)
    )
  }

  it should "look up keys literally without path syntax" in {
    val jsonData: Array[String | Null] = Array("""{"a.b":"dotted","a":"plain"}""")
    val jsonCol = Column.string(jsonData)
    val jsonColumns = Vector(jsonCol)
    val jsonCell = Expr.Cell[Any, String]("json", ColumnIndex(0))
    eval(Expr.jsonTuple(jsonCell, "a.b", "a"), jsonColumns, 0) shouldBe Right(
      Seq("dotted", "plain")
    )
  }

  it should "report AnyType outputType" in {
    Expr.jsonTuple(cell, "a").outputType shouldBe Some(ColumnType.AnyType)
  }

  "outputType" should "return StringType for all Phase 4 expressions" in {
    cell.md5.outputType shouldBe Some(ColumnType.StringType)
    cell.sha1.outputType shouldBe Some(ColumnType.StringType)
    cell.sha2(256).outputType shouldBe Some(ColumnType.StringType)
    cell.urlEncode.outputType shouldBe Some(ColumnType.StringType)
    cell.urlDecode.outputType shouldBe Some(ColumnType.StringType)
    cell.base64Encode.outputType shouldBe Some(ColumnType.StringType)
    cell.base64Decode.outputType shouldBe Some(ColumnType.StringType)
    cell.hex.outputType shouldBe Some(ColumnType.StringType)
    cell.getJsonObject("$.key").outputType shouldBe Some(ColumnType.StringType)
  }
}
