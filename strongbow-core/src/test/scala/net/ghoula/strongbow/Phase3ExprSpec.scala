package net.ghoula.strongbow

import org.scalatest.Inside.inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.errors.ExecutionError
import net.ghoula.strongbow.prelude.*
import net.ghoula.strongbow.types.ColumnIndex

class Phase3ExprSpec extends AnyFlatSpec with Matchers {

  private def eval[Row, A](expr: Expr[Row, A], columns: Vector[Column[?]], idx: Int): Either[ExecutionError, Any] = {
    val effectiveColumns =
      if (columns.isEmpty || columns.head.length == 0) Vector(Column.int(Array(0)))
      else columns
    val colType = ExprInterpreter.inferExprColumnType(expr, effectiveColumns)
    ExprInterpreter.evalColumn(expr, effectiveColumns, colType).map(_.getValue(idx))
  }

  private val arrData: Array[Any] = Array(
    Seq(1, 2, 3),
    Seq(4, 5),
    Seq(1, 2, 2, 3)
  )
  private val arrCol = Column.any(arrData)
  private val arrColumns = Vector(arrCol)
  private val arrCell = Expr.Cell[Any, Seq[Int]]("arr", ColumnIndex(0))

  "ArraySize" should "return the size of an array" in {
    val expr = arrCell.arraySize
    val results = (0 until 3).map(i => eval(expr, arrColumns, i))
    results shouldBe Seq(Right(3), Right(2), Right(4))
  }

  "ArrayContains" should "check if array contains a value" in {
    val expr = arrCell.arrayContains(Expr.const(2))
    val results = (0 until 3).map(i => eval(expr, arrColumns, i))
    results shouldBe Seq(Right(true), Right(false), Right(true))
  }

  "Explode" should "return UnsupportedOperation" in {
    val expr = arrCell.explode
    val result = eval(expr, arrColumns, 0)
    result shouldBe Left(ExecutionError.UnsupportedOperation("Explode requires Dataset-level handling"))
  }

  "ArraySort" should "return unsupported for untyped columns in evalColumn path" in {
    val data: Array[Any] = Array(Seq(3, 1, 2), Seq(5, 4))
    val col = Column.any(data)
    val columns = Vector(col)
    val cell = Expr.Cell[Any, Seq[Int]]("arr", ColumnIndex(0))
    val expr = cell.arraySort
    val result = eval(expr, columns, 0)
    result.isLeft shouldBe true
  }

  "ArrayDistinct" should "remove duplicates from array" in {
    val expr = arrCell.arrayDistinct
    val result = eval(expr, arrColumns, 2)
    result shouldBe Right(Seq(1, 2, 3))
  }

  "ArrayUnion" should "return distinct union of two arrays" in {
    val data2: Array[Any] = Array(Seq(2, 3, 4), Seq(5, 6), Seq(3, 4))
    val col2 = Column.any(data2)
    val columns = Vector(arrCol, col2)
    val cell2 = Expr.Cell[Any, Seq[Int]]("arr2", ColumnIndex(1))
    val expr = arrCell.arrayUnion(cell2)
    val result = eval(expr, columns, 0)
    result shouldBe Right(Seq(1, 2, 3, 4))
  }

  "ArrayIntersect" should "return common elements of two arrays" in {
    val data2: Array[Any] = Array(Seq(2, 3, 4), Seq(5, 6), Seq(3, 4))
    val col2 = Column.any(data2)
    val columns = Vector(arrCol, col2)
    val cell2 = Expr.Cell[Any, Seq[Int]]("arr2", ColumnIndex(1))
    val expr = arrCell.arrayIntersect(cell2)
    val result = eval(expr, columns, 0)
    result shouldBe Right(Seq(2, 3))
  }

  "ArrayExcept" should "return elements in left but not right" in {
    val data2: Array[Any] = Array(Seq(2, 3, 4), Seq(5, 6), Seq(3, 4))
    val col2 = Column.any(data2)
    val columns = Vector(arrCol, col2)
    val cell2 = Expr.Cell[Any, Seq[Int]]("arr2", ColumnIndex(1))
    val expr = arrCell.arrayExcept(cell2)
    val result = eval(expr, columns, 0)
    result shouldBe Right(Seq(1))
  }

  "Flatten" should "flatten nested arrays" in {
    val data: Array[Any] = Array(
      Seq(Seq(1, 2), Seq(3, 4)),
      Seq(Seq(5), Seq(6, 7, 8))
    )
    val col = Column.any(data)
    val columns = Vector(col)
    val cell = Expr.Cell[Any, Seq[Seq[Int]]]("nested", ColumnIndex(0))
    val expr = cell.flatten
    val results = (0 until 2).map(i => eval(expr, columns, i))
    results shouldBe Seq(Right(Seq(1, 2, 3, 4)), Right(Seq(5, 6, 7, 8)))
  }

  "ElementAt" should "return element at 1-based positive index" in {
    val expr = arrCell.elementAt(Expr.const(2))
    val result = eval(expr, arrColumns, 0)
    result shouldBe Right(2)
  }

  it should "return element at negative index from end" in {
    val expr = arrCell.elementAt(Expr.const(-1))
    val result = eval(expr, arrColumns, 0)
    result shouldBe Right(3)
  }

  it should "return null for positive index past end" in {
    val expr = arrCell.elementAt(Expr.const(10))
    val result = eval(expr, arrColumns, 0)
    result shouldBe Right(null) // scalafix:ok DisableSyntax.null
  }

  it should "return null for zero index" in {
    val expr = arrCell.elementAt(Expr.const(0))
    val result = eval(expr, arrColumns, 0)
    result shouldBe Right(null) // scalafix:ok DisableSyntax.null
  }

  it should "return null for negative index past start" in {
    val expr = arrCell.elementAt(Expr.const(-10))
    val result = eval(expr, arrColumns, 0)
    result shouldBe Right(null) // scalafix:ok DisableSyntax.null
  }

  "ArraySlice" should "return a sub-array" in {
    val expr = arrCell.arraySlice(1, 2)
    val result = eval(expr, arrColumns, 0)
    result shouldBe Right(Seq(1, 2))
  }

  it should "handle start beyond first element" in {
    val expr = arrCell.arraySlice(2, 2)
    val result = eval(expr, arrColumns, 0)
    result shouldBe Right(Seq(2, 3))
  }

  private val mapData: Array[Any] = Array(
    Map("a" -> 1, "b" -> 2),
    Map("x" -> 10, "y" -> 20, "z" -> 30)
  )
  private val mapCol = Column.any(mapData)
  private val mapColumns = Vector(mapCol)
  private val mapCell = Expr.Cell[Any, Map[String, Int]]("m", ColumnIndex(0))

  "MapKeys" should "return all keys of a map" in {
    val expr = mapCell.mapKeys
    val result = eval(expr, mapColumns, 0)
    inside(result) { case Right(v: Seq[?]) => v.toSet shouldBe Set("a", "b") }
  }

  "MapValues" should "return all values of a map" in {
    val expr = mapCell.mapValues
    val result = eval(expr, mapColumns, 0)
    inside(result) { case Right(v: Seq[?]) => v.toSet shouldBe Set(1, 2) }
  }

  "MapContainsKey" should "check if map contains a key" in {
    val exprTrue = mapCell.mapContainsKey(Expr.const("a"))
    val exprFalse = mapCell.mapContainsKey(Expr.const("z"))
    eval(exprTrue, mapColumns, 0) shouldBe Right(true)
    eval(exprFalse, mapColumns, 0) shouldBe Right(false)
  }

  "MapEntries" should "return key-value pairs as a sequence of tuples" in {
    val expr = mapCell.mapEntries
    val result = eval(expr, mapColumns, 0)
    inside(result) { case Right(v: Seq[?]) => v.toSet shouldBe Set(("a", 1), ("b", 2)) }
  }

  "MapFromArrays" should "create a map from key and value arrays" in {
    val keysData: Array[Any] = Array(Seq("a", "b", "c"))
    val valsData: Array[Any] = Array(Seq(1, 2, 3))
    val keysCol = Column.any(keysData)
    val valsCol = Column.any(valsData)
    val columns = Vector(keysCol, valsCol)
    val keysCell = Expr.Cell[Any, Seq[String]]("keys", ColumnIndex(0))
    val valsCell = Expr.Cell[Any, Seq[Int]]("vals", ColumnIndex(1))
    val expr = Expr.mapFromArrays(keysCell, valsCell)
    val result = eval(expr, columns, 0)
    result shouldBe Right(Map("a" -> 1, "b" -> 2, "c" -> 3))
  }

  it should "truncate when key and value arrays have different lengths" in {
    val keysData: Array[Any] = Array(Seq("a", "b"))
    val valsData: Array[Any] = Array(Seq(1, 2, 3))
    val keysCol = Column.any(keysData)
    val valsCol = Column.any(valsData)
    val columns = Vector(keysCol, valsCol)
    val keysCell = Expr.Cell[Any, Seq[String]]("keys", ColumnIndex(0))
    val valsCell = Expr.Cell[Any, Seq[Int]]("vals", ColumnIndex(1))
    val expr = Expr.mapFromArrays(keysCell, valsCell)
    val result = eval(expr, columns, 0)
    result shouldBe Right(Map("a" -> 1, "b" -> 2))
  }

  "MapConcat" should "merge two maps" in {
    val data2: Array[Any] = Array(
      Map("c" -> 3, "d" -> 4),
      Map("w" -> 40)
    )
    val col2 = Column.any(data2)
    val columns = Vector(mapCol, col2)
    val cell2 = Expr.Cell[Any, Map[String, Int]]("m2", ColumnIndex(1))
    val expr = mapCell.mapConcat(cell2)
    val result = eval(expr, columns, 0)
    result shouldBe Right(Map("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4))
  }

  "ColumnType.ArrayType" should "exist in the enum" in {
    val ct = ColumnType.ArrayType(ColumnType.IntType)
    ct match {
      case ColumnType.ArrayType(elem) => elem shouldBe ColumnType.IntType
      case _ => fail("Expected ArrayType")
    }
  }

  "ColumnType.MapType" should "exist in the enum" in {
    val ct = ColumnType.MapType(ColumnType.StringType, ColumnType.IntType)
    ct match {
      case ColumnType.MapType(k, v) =>
        k shouldBe ColumnType.StringType
        v shouldBe ColumnType.IntType
      case _ => fail("Expected MapType")
    }
  }

  "Column.empty" should "handle ArrayType" in {
    val col = Column.empty(ColumnType.ArrayType(ColumnType.IntType))
    col.length shouldBe 0
  }

  it should "handle MapType" in {
    val col = Column.empty(ColumnType.MapType(ColumnType.StringType, ColumnType.IntType))
    col.length shouldBe 0
  }

  "Column.fromValues" should "handle ArrayType" in {
    val values = Vector[Any](Seq(1, 2), Seq(3, 4))
    val result = Column.fromValues(values, ColumnType.ArrayType(ColumnType.IntType))
    result.isRight shouldBe true
    result.toOption.get.length shouldBe 2
  }

  it should "handle MapType" in {
    val values = Vector[Any](Map("a" -> 1), Map("b" -> 2))
    val result = Column.fromValues(values, ColumnType.MapType(ColumnType.StringType, ColumnType.IntType))
    result.isRight shouldBe true
    result.toOption.get.length shouldBe 2
  }

  "outputType" should "return IntType for ArraySize" in {
    arrCell.arraySize.outputType shouldBe Some(ColumnType.IntType)
  }

  it should "return BooleanType for ArrayContains" in {
    arrCell.arrayContains(Expr.const(1)).outputType shouldBe Some(ColumnType.BooleanType)
  }

  it should "return BooleanType for MapContainsKey" in {
    mapCell.mapContainsKey(Expr.const("a")).outputType shouldBe Some(ColumnType.BooleanType)
  }

  it should "return AnyType for collection-returning expressions" in {
    arrCell.arrayDistinct.outputType shouldBe Some(ColumnType.AnyType)
    mapCell.mapKeys.outputType shouldBe Some(ColumnType.AnyType)
    mapCell.mapValues.outputType shouldBe Some(ColumnType.AnyType)
  }
}
