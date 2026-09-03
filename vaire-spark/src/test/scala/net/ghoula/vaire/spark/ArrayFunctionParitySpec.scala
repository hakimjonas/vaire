package net.ghoula.vaire.spark

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.column.ColumnType
import net.ghoula.vaire.expr.Expr
import net.ghoula.vaire.interpreter.ExprInterpreter
import net.ghoula.vaire.prelude.*

class ArrayFunctionParitySpec extends AnyFlatSpec with Matchers with SparkTestBase {

  case class Rec(id: Int, xs: Seq[Int], labels: Seq[String])
  given Schema[Rec] = Schema.derived

  private lazy val base: DataFrame = spark.sql("""
      SELECT inline(array(
        struct(1, array(1, 2, 3), array('a', 'b')),
        struct(2, array(4), array('c', null, 'd')),
        struct(3, array(), array())
      )) AS (id_value, xs_value, labels_value)
    """)

  private lazy val materialized = {
    val rows = base.collect()
    RowConverter.toMaterialized(rows, summon[Schema[Rec]]).fold(err => fail(s"$err"), identity)
  }

  private def xsCell: Expr[Rec, Seq[Int]] = Expr.cell("xs_value", ColumnIndex(1))
  private def labelsCell: Expr[Rec, Seq[String]] = Expr.cell("labels_value", ColumnIndex(2))

  private def checkParity[A](expr: Expr[Rec, A], columnType: ColumnType, expected: Vector[Any | Null]): Unit = {
    val inMemory = ExprInterpreter.evalColumn(expr, materialized.columns, columnType) match {
      case Right(col) => (0 until col.length).toVector.map(col.getValue)
      case other => fail(s"In-memory eval failed: $other")
    }
    val (sparkCol, _) = ExprToColumn.convert(expr) match {
      case Right(converted) => converted
      case other => fail(s"Spark conversion failed: $other")
    }
    val sparkValues = base.select(sparkCol).collect().map(r => r.get(0): Any | Null).toVector
    inMemory shouldBe sparkValues
    sparkValues shouldBe expected
  }

  private def constI(v: Int): Expr[Rec, Int] = Expr.const[Rec, Int](v)
  private def constS(v: String): Expr[Rec, String] = Expr.const[Rec, String](v)

  "array functions" should "append, prepend, remove and compact on both backends" in {
    checkParity(
      xsCell.arrayAppend(constI(9)),
      ColumnType.AnyType,
      Vector[Any | Null](Seq(1, 2, 3, 9), Seq(4, 9), Seq(9))
    )
    checkParity(
      xsCell.arrayPrepend(constI(0)),
      ColumnType.AnyType,
      Vector[Any | Null](Seq(0, 1, 2, 3), Seq(0, 4), Seq(0))
    )
    checkParity(
      labelsCell.arrayRemove(constS("a")),
      ColumnType.AnyType,
      Vector[Any | Null](Seq("b"), Seq("c", SqlNull.value, "d"), Seq.empty)
    )
    checkParity(
      labelsCell.arrayCompact,
      ColumnType.AnyType,
      Vector[Any | Null](Seq("a", "b"), Seq("c", "d"), Seq.empty)
    )
  }

  it should "insert, repeat and join on both backends" in {
    checkParity(
      xsCell.arrayInsert(constI(2), constI(99)),
      ColumnType.AnyType,
      Vector[Any | Null](Seq(1, 99, 2, 3), Seq(4, 99), Seq(SqlNull.value, 99))
    )
    checkParity(
      Expr.arrayRepeat[Rec, Int](constI(7), constI(3)),
      ColumnType.AnyType,
      Vector[Any | Null](Seq(7, 7, 7), Seq(7, 7, 7), Seq(7, 7, 7))
    )
    checkParity(
      Expr.arrayJoin[Rec](labelsCell, ","),
      ColumnType.StringType,
      Vector[Any | Null]("a,b", "c,d", "")
    )
    checkParity(
      Expr.arrayJoin[Rec](labelsCell, ",", Some("*")),
      ColumnType.StringType,
      Vector[Any | Null]("a,b", "c,*,d", "")
    )
  }

  it should "find max, min, position and overlap on both backends" in {
    checkParity(xsCell.arrayMax, ColumnType.AnyType, Vector[Any | Null](3, 4, SqlNull.value))
    checkParity(xsCell.arrayMin, ColumnType.AnyType, Vector[Any | Null](1, 4, SqlNull.value))
    checkParity(
      labelsCell.arrayPosition(constS("b")),
      ColumnType.IntType,
      Vector[Any | Null](2, 0, 0)
    )
    checkParity(
      xsCell.arraysOverlap(Expr.const[Rec, Seq[Int]](Seq(3, 9))),
      ColumnType.BooleanType,
      Vector[Any | Null](true, false, false)
    )
  }

  it should "zip arrays and build maps from entries on both backends" in {
    val zipped = Expr.arraysZip[Rec](xsCell, labelsCell)
    val inMemory = ExprInterpreter.evalColumn(zipped, materialized.columns, ColumnType.AnyType) match {
      case Right(col) => (0 until col.length).toVector.map(col.getValue)
      case other => fail(s"In-memory eval failed: $other")
    }
    val (sparkCol, _) = ExprToColumn.convert(zipped) match {
      case Right(converted) => converted
      case other => fail(s"Spark conversion failed: $other")
    }
    val sparkMaps = base
      .select(sparkCol)
      .collect()
      .map { r =>
        r.get(0) match {
          case s: scala.collection.Seq[?] =>
            s.map {
              case row: org.apache.spark.sql.Row =>
                (0 until row.size).map(i => i.toString -> row.get(i)).toMap
              case other => other
            }.toList
          case other => fail(s"Expected Seq, got ${other.getClass}")
        }: Any | Null
      }
      .toVector
    inMemory.map {
      case s: scala.collection.Seq[?] =>
        s.map {
          case m: Map[?, ?] => m.map { case (k, v) => k -> (v: Any | Null) }
          case other => fail(s"Expected Map, got $other")
        }
      case other => fail(s"Expected Seq, got $other")
    } shouldBe sparkMaps.map {
      case s: scala.collection.Seq[?] =>
        s.map {
          case m: Map[?, ?] => m.map { case (k, v) => k -> (v: Any | Null) }
          case other => fail(s"Expected Map, got $other")
        }
      case other => fail(s"Expected Seq, got $other")
    }
  }

  it should "get by zero-based index on both backends" in {
    checkParity(
      xsCell.getArray(constI(1)),
      ColumnType.AnyType,
      Vector[Any | Null](2, SqlNull.value, SqlNull.value)
    )
  }

  it should "build maps from entries on both backends" in {
    checkParity(
      Expr.mapFromEntries[Rec, String, Int](
        Expr.const[Rec, Seq[(String, Int)]](Seq("k" -> 1, "m" -> 2))
      ),
      ColumnType.AnyType,
      Vector[Any | Null](Map("k" -> 1, "m" -> 2), Map("k" -> 1, "m" -> 2), Map("k" -> 1, "m" -> 2))
    )
  }

  "generator functions" should "be unsupported in-memory but map to Spark" in {
    val px = Expr.Posexplode[Rec](xsCell)
    ExprInterpreter.evalColumn(px, materialized.columns, ColumnType.AnyType).isLeft shouldBe true
    val (sparkCol, _) = ExprToColumn.convert(px) match {
      case Right(converted) => converted
      case other => fail(s"Spark conversion failed: $other")
    }
    base.select(sparkCol).collect().length shouldBe 4
  }
}
