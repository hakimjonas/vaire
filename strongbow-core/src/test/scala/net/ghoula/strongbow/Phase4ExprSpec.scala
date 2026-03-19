package net.ghoula.strongbow

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.errors.ExecutionError
import net.ghoula.strongbow.prelude.*
import net.ghoula.strongbow.types.{ColumnIndex, RowIndex}

class Phase4ExprSpec extends AnyFlatSpec with Matchers {

  private val strData: Array[String] = Array("hello", "world", "foo bar")
  private val strCol = Column.string(strData)
  private val columns = Vector(strCol)
  private val cell = Expr.Cell[Any, String]("s", ColumnIndex(0))

  "Md5" should "compute MD5 hash of a string" in {
    val expr = cell.md5
    val result = ExprInterpreter.eval(expr, columns, RowIndex(0))
    result shouldBe Right("5d41402abc4b2a76b9719d911017c592")
  }

  it should "compute MD5 for different inputs" in {
    val result = ExprInterpreter.eval(cell.md5, columns, RowIndex(1))
    result shouldBe Right("7d793037a0760186574b0282f2f435e7")
  }

  "Sha1" should "compute SHA-1 hash of a string" in {
    val expr = cell.sha1
    val result = ExprInterpreter.eval(expr, columns, RowIndex(0))
    result shouldBe Right("aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d")
  }

  "Sha2" should "compute SHA-256 hash by default (bitLength=0)" in {
    val expr = cell.sha2(0)
    val result = ExprInterpreter.eval(expr, columns, RowIndex(0))
    result shouldBe Right("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
  }

  it should "compute SHA-256 hash with bitLength=256" in {
    val expr = cell.sha2(256)
    val result = ExprInterpreter.eval(expr, columns, RowIndex(0))
    result shouldBe Right("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
  }

  it should "compute SHA-512 hash" in {
    val expr = cell.sha2(512)
    val result = ExprInterpreter.eval(expr, columns, RowIndex(0))
    result shouldBe Right(
      "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043"
    )
  }

  "UrlEncode" should "encode a string for URLs" in {
    val data: Array[String] = Array("hello world", "foo=bar&baz=qux")
    val col = Column.string(data)
    val cols = Vector(col)
    val c = Expr.Cell[Any, String]("s", ColumnIndex(0))
    ExprInterpreter.eval(c.urlEncode, cols, RowIndex(0)) shouldBe Right("hello+world")
    ExprInterpreter.eval(c.urlEncode, cols, RowIndex(1)) shouldBe Right("foo%3Dbar%26baz%3Dqux")
  }

  "UrlDecode" should "decode a URL-encoded string" in {
    val data: Array[String] = Array("hello+world", "foo%3Dbar%26baz%3Dqux")
    val col = Column.string(data)
    val cols = Vector(col)
    val c = Expr.Cell[Any, String]("s", ColumnIndex(0))
    ExprInterpreter.eval(c.urlDecode, cols, RowIndex(0)) shouldBe Right("hello world")
    ExprInterpreter.eval(c.urlDecode, cols, RowIndex(1)) shouldBe Right("foo=bar&baz=qux")
  }

  "Base64Encode" should "encode a string to base64" in {
    val expr = cell.base64Encode
    val result = ExprInterpreter.eval(expr, columns, RowIndex(0))
    result shouldBe Right("aGVsbG8=")
  }

  "Base64Decode" should "decode a base64 string" in {
    val data: Array[String] = Array("aGVsbG8=", "d29ybGQ=")
    val col = Column.string(data)
    val cols = Vector(col)
    val c = Expr.Cell[Any, String]("s", ColumnIndex(0))
    ExprInterpreter.eval(c.base64Decode, cols, RowIndex(0)) shouldBe Right("hello")
    ExprInterpreter.eval(c.base64Decode, cols, RowIndex(1)) shouldBe Right("world")
  }

  "Base64 roundtrip" should "encode then decode back to original" in {
    val expr = cell.base64Encode
    val encoded = ExprInterpreter.eval(expr, columns, RowIndex(0))
    encoded shouldBe Right("aGVsbG8=")
    val data2: Array[String] = Array("aGVsbG8=")
    val col2 = Column.string(data2)
    val cols2 = Vector(col2)
    val c2 = Expr.Cell[Any, String]("s", ColumnIndex(0))
    ExprInterpreter.eval(c2.base64Decode, cols2, RowIndex(0)) shouldBe Right("hello")
  }

  "Hex" should "hex-encode string bytes" in {
    val expr = cell.hex
    val result = ExprInterpreter.eval(expr, columns, RowIndex(0))
    result shouldBe Right("68656c6c6f")
  }

  it should "hex-encode world" in {
    val result = ExprInterpreter.eval(cell.hex, columns, RowIndex(1))
    result shouldBe Right("776f726c64")
  }

  "GetJsonObject" should "return UnsupportedOperation" in {
    val expr = cell.getJsonObject("$.key")
    val result = ExprInterpreter.eval(expr, columns, RowIndex(0))
    result shouldBe Left(ExecutionError.UnsupportedOperation("GetJsonObject requires strongbow-io module"))
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
