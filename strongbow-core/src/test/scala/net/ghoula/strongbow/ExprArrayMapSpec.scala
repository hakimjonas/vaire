package net.ghoula.strongbow

import org.scalatest.Inside.inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.errors.ExecutionError
import net.ghoula.strongbow.prelude.*
import net.ghoula.strongbow.types.ColumnIndex

class ExprArrayMapSpec extends AnyFlatSpec with Matchers {

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

  it should "return SqlNull.value for positive index past end" in {
    val expr = arrCell.elementAt(Expr.const(10))
    val result = eval(expr, arrColumns, 0)
    result shouldBe Right(SqlNull.value)
  }

  it should "return SqlNull.value for zero index" in {
    val expr = arrCell.elementAt(Expr.const(0))
    val result = eval(expr, arrColumns, 0)
    result shouldBe Right(SqlNull.value)
  }

  it should "return SqlNull.value for negative index past start" in {
    val expr = arrCell.elementAt(Expr.const(-10))
    val result = eval(expr, arrColumns, 0)
    result shouldBe Right(SqlNull.value)
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

  it should "return AnyType for lambda-family expressions and BooleanType for predicates" in {
    arrCell.transform(x => x + Expr.const(1)).outputType shouldBe Some(ColumnType.AnyType)
    arrCell.filter(x => x > Expr.const(1)).outputType shouldBe Some(ColumnType.AnyType)
    arrCell.exists(x => x > Expr.const(1)).outputType shouldBe Some(ColumnType.BooleanType)
    arrCell.forall(x => x > Expr.const(1)).outputType shouldBe Some(ColumnType.BooleanType)
  }

  "Transform" should "apply a lambda to every element" in {
    val expr = arrCell.transform(x => x + Expr.const(1))
    val results = (0 until 3).map(i => eval(expr, arrColumns, i))
    results shouldBe Seq(Right(Seq(2, 3, 4)), Right(Seq(5, 6)), Right(Seq(2, 3, 3, 4)))
  }

  it should "bind the element index in the two-argument form" in {
    val expr = arrCell.transform((x, i) => x + i)
    val results = (0 until 3).map(i => eval(expr, arrColumns, i))
    results shouldBe Seq(Right(Seq(1, 3, 5)), Right(Seq(4, 6)), Right(Seq(1, 3, 4, 6)))
  }

  it should "let the body reference outer columns" in {
    val addData = Array(10, 20, 30)
    val addCol = Column.int(addData)
    val addCell = Expr.Cell[Any, Int]("add", ColumnIndex(1))
    val columns = Vector(arrCol, addCol)
    val expr = arrCell.transform(x => x + addCell)
    val results = (0 until 3).map(i => eval(expr, columns, i))
    results shouldBe Seq(Right(Seq(11, 12, 13)), Right(Seq(24, 25)), Right(Seq(31, 32, 32, 33)))
  }

  it should "support nested transforms" in {
    val nestedData: Array[Any] = Array(Seq(Seq(1, 2), Seq(3)), Seq(Seq(4)))
    val nestedCol = Column.any(nestedData)
    val nestedCell = Expr.Cell[Any, Seq[Seq[Int]]]("nested", ColumnIndex(0))
    val columns = Vector(nestedCol)
    val expr = nestedCell.transform(xs => xs.transform(y => y * Expr.const(2)))
    val results = (0 until 2).map(i => eval(expr, columns, i))
    results shouldBe Seq(Right(Seq(Seq(2, 4), Seq(6))), Right(Seq(Seq(8))))
  }

  it should "map null arrays to null and empty arrays to empty" in {
    val data: Array[Any] = Array(Seq(1, 2), SqlNull.value, Seq.empty[Int])
    val col = Column.any(data)
    val cell = Expr.Cell[Any, Seq[Int]]("arr", ColumnIndex(0))
    val columns = Vector(col)
    val expr = cell.transform(x => x + Expr.const(1))
    eval(expr, columns, 0) shouldBe Right(Seq(2, 3))
    eval(expr, columns, 1).map(v => v == SqlNull.value) shouldBe Right(true)
    eval(expr, columns, 2) shouldBe Right(Seq.empty)
  }

  "Filter" should "keep elements whose predicate holds" in {
    val expr = arrCell.filter(x => x > Expr.const(1))
    val results = (0 until 3).map(i => eval(expr, arrColumns, i))
    results shouldBe Seq(Right(Seq(2, 3)), Right(Seq(4, 5)), Right(Seq(2, 2, 3)))
  }

  it should "bind the element index in the two-argument form" in {
    val expr = arrCell.filter((x, i) => x > i)
    val results = (0 until 3).map(i => eval(expr, arrColumns, i))
    results shouldBe Seq(Right(Seq(1, 2, 3)), Right(Seq(4, 5)), Right(Seq(1, 2)))
  }

  it should "drop elements with null elements under the predicate" in {
    val data: Array[Any] = Array(Seq(1, SqlNull.value, 3))
    val col = Column.any(data)
    val cell = Expr.Cell[Any, Seq[Int]]("arr", ColumnIndex(0))
    val columns = Vector(col)
    val expr = cell.filter(x => x > Expr.const(1))
    eval(expr, columns, 0) shouldBe Right(Seq(3))
  }

  it should "map null arrays to null and empty arrays to empty" in {
    val data: Array[Any] = Array(Seq(1, 2), SqlNull.value, Seq.empty[Int])
    val col = Column.any(data)
    val cell = Expr.Cell[Any, Seq[Int]]("arr", ColumnIndex(0))
    val columns = Vector(col)
    val expr = cell.filter(x => x > Expr.const(1))
    eval(expr, columns, 0) shouldBe Right(Seq(2))
    eval(expr, columns, 1).map(v => v == SqlNull.value) shouldBe Right(true)
    eval(expr, columns, 2) shouldBe Right(Seq.empty)
  }

  "Exists" should "follow Spark's three-valued logic" in {
    val expr = arrCell.exists(x => x > Expr.const(1))
    val results = (0 until 3).map(i => eval(expr, arrColumns, i))
    results shouldBe Seq(Right(true), Right(true), Right(true))
  }

  it should "yield null when no predicate is true and some is null" in {
    val data: Array[Any] = Array(Seq(1, SqlNull.value), Seq(1, 2), Seq(SqlNull.value))
    val col = Column.any(data)
    val cell = Expr.Cell[Any, Seq[Int]]("arr", ColumnIndex(0))
    val columns = Vector(col)
    val expr = cell.exists(x => x > Expr.const(1))
    eval(expr, columns, 0).map(v => v == SqlNull.value) shouldBe Right(true)
    eval(expr, columns, 1) shouldBe Right(true)
    eval(expr, columns, 2).map(v => v == SqlNull.value) shouldBe Right(true)
  }

  it should "return false for empty arrays" in {
    val data: Array[Any] = Array(Seq.empty[Int])
    val col = Column.any(data)
    val cell = Expr.Cell[Any, Seq[Int]]("arr", ColumnIndex(0))
    eval(cell.exists(x => x > Expr.const(1)), Vector(col), 0) shouldBe Right(false)
  }

  it should "map null arrays to null" in {
    val data: Array[Any] = Array(SqlNull.value)
    val col = Column.any(data)
    val cell = Expr.Cell[Any, Seq[Int]]("arr", ColumnIndex(0))
    eval(cell.exists(x => x > Expr.const(1)), Vector(col), 0).map(v => v == SqlNull.value) shouldBe Right(true)
  }

  "ForAll" should "follow Spark's three-valued logic" in {
    val expr = arrCell.forall(x => x > Expr.const(1))
    val results = (0 until 3).map(i => eval(expr, arrColumns, i))
    results shouldBe Seq(Right(false), Right(true), Right(false))
  }

  it should "yield null when no predicate is false and some is null" in {
    val data: Array[Any] = Array(Seq(2, SqlNull.value), Seq(2, 3), Seq(2, 1))
    val col = Column.any(data)
    val cell = Expr.Cell[Any, Seq[Int]]("arr", ColumnIndex(0))
    val columns = Vector(col)
    val expr = cell.forall(x => x > Expr.const(1))
    eval(expr, columns, 0).map(v => v == SqlNull.value) shouldBe Right(true)
    eval(expr, columns, 1) shouldBe Right(true)
    eval(expr, columns, 2) shouldBe Right(false)
  }

  it should "return true for empty arrays" in {
    val data: Array[Any] = Array(Seq.empty[Int])
    val col = Column.any(data)
    val cell = Expr.Cell[Any, Seq[Int]]("arr", ColumnIndex(0))
    eval(cell.forall(x => x > Expr.const(1)), Vector(col), 0) shouldBe Right(true)
  }

  "Aggregate" should "fold the array from zero with the merge lambda" in {
    val expr = arrCell.aggregate(Expr.const(0))((acc, x) => acc + x)
    val results = (0 until 3).map(i => eval(expr, arrColumns, i))
    results shouldBe Seq(Right(6), Right(9), Right(8))
  }

  it should "apply the finish lambda when given" in {
    val expr = arrCell.aggregate(Expr.const(0))((acc, x) => acc + x, acc => acc * Expr.const(10))
    val results = (0 until 3).map(i => eval(expr, arrColumns, i))
    results shouldBe Seq(Right(60), Right(90), Right(80))
  }

  it should "bind the accumulator and element in order" in {
    val expr = arrCell.aggregate(Expr.const(0))((acc, x) => acc * Expr.const(10) + x)
    eval(expr, arrColumns, 0) shouldBe Right(123)
  }

  it should "let the merge reference outer columns" in {
    val addData = Array(1, 2, 3)
    val addCol = Column.int(addData)
    val addCell = Expr.Cell[Any, Int]("add", ColumnIndex(1))
    val columns = Vector(arrCol, addCol)
    val expr = arrCell.aggregate(Expr.const(0))((acc, x) => acc + x + addCell)
    val results = (0 until 3).map(i => eval(expr, columns, i))
    results shouldBe Seq(Right(9), Right(13), Right(20))
  }

  it should "fold empty arrays to zero and null arrays to null" in {
    val data: Array[Any] = Array(Seq.empty[Int], SqlNull.value)
    val col = Column.any(data)
    val cell = Expr.Cell[Any, Seq[Int]]("arr", ColumnIndex(0))
    val columns = Vector(col)
    val expr = cell.aggregate(Expr.const(0))((acc, x) => acc + x)
    eval(expr, columns, 0) shouldBe Right(0)
    eval(expr, columns, 1).map(v => v == SqlNull.value) shouldBe Right(true)
  }

  "ZipWith" should "pad the shorter array with null binders on both sides" in {
    val leftData: Array[Any] = Array(Seq(1, 2, 3), Seq(1), Seq.empty[Int], SqlNull.value)
    val rightData: Array[Any] = Array(Seq(10, 20), Seq(30, 40, 50), Seq(60), Seq(70))
    val leftCol = Column.any(leftData)
    val rightCol = Column.any(rightData)
    val leftCell = Expr.Cell[Any, Seq[Int]]("l", ColumnIndex(0))
    val rightCell = Expr.Cell[Any, Seq[Int]]("r", ColumnIndex(1))
    val columns = Vector(leftCol, rightCol)
    val expr = leftCell.zipWith(rightCell)((x, y) => x.getOrElse(0) + y.getOrElse(0))
    val results = (0 until 4).map(i => eval(expr, columns, i))
    results shouldBe Seq(
      Right(Seq(11, 22, 3)),
      Right(Seq(31, 40, 50)),
      Right(Seq(60)),
      Right(SqlNull.value)
    )
  }

  it should "map null input arrays to null results" in {
    val leftData: Array[Any] = Array(SqlNull.value)
    val rightData: Array[Any] = Array(Seq(1))
    val leftCol = Column.any(leftData)
    val rightCol = Column.any(rightData)
    val leftCell = Expr.Cell[Any, Seq[Int]]("l", ColumnIndex(0))
    val rightCell = Expr.Cell[Any, Seq[Int]]("r", ColumnIndex(1))
    val columns = Vector(leftCol, rightCol)
    val expr = leftCell.zipWith(rightCell)((x, y) => x.getOrElse(0) + y.getOrElse(0))
    eval(expr, columns, 0).map(v => v == SqlNull.value) shouldBe Right(true)
  }

  private val hofMapData: Array[Any] = Array(
    Map("a" -> 1, "b" -> 2, "c" -> 3),
    Map("x" -> 10),
    Map.empty[String, Int]
  )
  private val hofMapCol = Column.any(hofMapData)
  private val mapCell2 = Expr.Cell[Any, Map[String, Int]]("m", ColumnIndex(0))
  private val hofMapColumns = Vector(hofMapCol)

  "MapFilter" should "keep entries whose predicate holds" in {
    val expr = mapCell2.mapFilter((_, v) => v > Expr.const(1))
    val results = (0 until 3).map(i => eval(expr, hofMapColumns, i))
    results shouldBe Seq(Right(Map("b" -> 2, "c" -> 3)), Right(Map("x" -> 10)), Right(Map.empty))
  }

  it should "map null maps to null" in {
    val data: Array[Any] = Array(SqlNull.value)
    val col = Column.any(data)
    val cell = Expr.Cell[Any, Map[String, Int]]("m", ColumnIndex(0))
    val expr = cell.mapFilter((_, v) => v > Expr.const(1))
    eval(expr, Vector(col), 0).map(v => v == SqlNull.value) shouldBe Right(true)
  }

  "TransformKeys" should "transform map keys" in {
    val expr = mapCell2.transformKeys((k, _) => k ++ Expr.const("_v"))
    val results = (0 until 3).map(i => eval(expr, hofMapColumns, i))
    results shouldBe Seq(
      Right(Map("a_v" -> 1, "b_v" -> 2, "c_v" -> 3)),
      Right(Map("x_v" -> 10)),
      Right(Map.empty)
    )
  }

  it should "fail on duplicate transformed keys" in {
    val expr = mapCell2.transformKeys((_, _) => Expr.const("same"))
    val result = eval(expr, hofMapColumns, 0)
    result.isLeft shouldBe true
  }

  "TransformValues" should "transform map values" in {
    val expr = mapCell2.transformValues((_, v) => v * Expr.const(10))
    val results = (0 until 3).map(i => eval(expr, hofMapColumns, i))
    results shouldBe Seq(
      Right(Map("a" -> 10, "b" -> 20, "c" -> 30)),
      Right(Map("x" -> 100)),
      Right(Map.empty)
    )
  }

  it should "reference keys in the body" in {
    val expr = mapCell2.transformValues((_, v) => v + Expr.const(100))
    eval(expr, hofMapColumns, 1) shouldBe Right(Map("x" -> 110))
  }

  "MapZipWith" should "merge two maps with null binders for missing keys" in {
    val leftData: Array[Any] = Array(Map("a" -> 1, "b" -> 2))
    val rightData: Array[Any] = Array(Map("b" -> 20, "c" -> 30))
    val leftCol = Column.any(leftData)
    val rightCol = Column.any(rightData)
    val leftCell = Expr.Cell[Any, Map[String, Int]]("l", ColumnIndex(0))
    val rightCell = Expr.Cell[Any, Map[String, Int]]("r", ColumnIndex(1))
    val columns = Vector(leftCol, rightCol)
    val expr = leftCell.mapZipWith(rightCell)((_, v1, v2) => v1.getOrElse(0) + v2.getOrElse(0))
    eval(expr, columns, 0) shouldBe Right(Map("a" -> 1, "b" -> 22, "c" -> 30))
  }
}
