package net.ghoula.vaire

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.language.strictEquality

import net.ghoula.vaire.prelude.*

class RecursiveSchemaSpec extends AnyFlatSpec with Matchers {

  "Schema.derived" should "derive nested case classes without in-scope field schemas" in {
    case class Inner(count: Int)
    case class Outer(name: String, inner: Inner, xs: Seq[Int])

    given Schema[Outer] = Schema.derived

    val schema = summon[Schema[Outer]]
    schema.columnCount shouldBe 3
    schema.columnTypes shouldBe Vector(
      ColumnType.StringType,
      ColumnType.IntType,
      ColumnType.ArrayType(ColumnType.IntType)
    )

    val value = Outer("a", Inner(7), Seq(1, 2, 3))
    val encoded = schema.encode(value)
    encoded.length shouldBe 3
    schema.decode(encoded) shouldBe Right(value)
  }

  it should "derive deeply nested case classes" in {
    case class Leaf(v: Int)
    case class Middle(leaf: Leaf, label: String)
    case class Root(middle: Middle, flag: Boolean)

    given Schema[Root] = Schema.derived

    val schema = summon[Schema[Root]]
    schema.columnCount shouldBe 3

    val value = Root(Middle(Leaf(42), "m"), true)
    schema.decode(schema.encode(value)) shouldBe Right(value)
  }

  it should "derive maps with typed keys and values" in {
    case class Bag(counts: Map[String, Int])

    given Schema[Bag] = Schema.derived

    val schema = summon[Schema[Bag]]
    schema.columnCount shouldBe 1
    schema.columnTypes shouldBe Vector(
      ColumnType.MapType(ColumnType.StringType, ColumnType.IntType)
    )

    val value = Bag(Map("a" -> 1, "b" -> 2))
    schema.decode(schema.encode(value)) shouldBe Right(value)
  }

  it should "still honor explicitly provided field schemas" in {
    case class Wrapper(value: Int)
    case class Holder(w: Wrapper)

    given Schema[Wrapper] = Schema.derived
    given Schema[Holder] = Schema.derived

    val schema = summon[Schema[Holder]]
    schema.columnCount shouldBe 1
    schema.decode(schema.encode(Holder(Wrapper(5)))) shouldBe Right(Holder(Wrapper(5)))
  }

  "Schema.structColumn" should "expose T as a single StructType column" in {
    case class Point(x: Int, y: String)
    given Schema[Point] = Schema.derived

    val schema = Schema.structColumn[Point]
    schema.columnCount shouldBe 1
    schema.columnTypes shouldBe Vector(
      ColumnType.StructType(Vector(("x_value", ColumnType.IntType), ("y_value", ColumnType.StringType)))
    )
    schema.nestedSchemas shouldBe Vector(Some(summon[Schema[Point]]))

    val value = Point(3, "c")
    schema.decode(schema.encode(value)) shouldBe Right(value)
  }

  it should "round-trip through MaterializedDataset as a deboxed struct column" in {
    case class Point(x: Int, y: String)
    given Schema[Point] = Schema.derived

    val points = Vector(Point(1, "a"), Point(2, "b"))
    MaterializedDataset.fromVector(points)(using Schema.structColumn[Point]) match {
      case Right(md) =>
        md.columnCount shouldBe 1
        md.columns.head match {
          case _: Column.StructColumn[?] => succeed
          case other => fail(s"Expected StructColumn, got: ${other.getClass.getSimpleName}")
        }
        md.columns.head.columnType shouldBe
          ColumnType.StructType(Vector(("x_value", ColumnType.IntType), ("y_value", ColumnType.StringType)))
        md.toVectorUnsafe shouldBe points
      case other => fail(s"Expected materialized dataset, got: $other")
    }
  }
}
