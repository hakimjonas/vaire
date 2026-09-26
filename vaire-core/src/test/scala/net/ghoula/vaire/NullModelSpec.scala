package net.ghoula.vaire

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.BitSet

import net.ghoula.vaire.column.{Column, ColumnType}
import net.ghoula.vaire.errors.SchemaError
import net.ghoula.vaire.prelude.*

/** Locks the single-column null model.
  *
  * `Option[A]` is one nullable column: the inner column when `A` is a single column, a struct
  * column when `A` flattens to several. A null is `None`. A non-optional column must hold no nulls,
  * which `fromColumns` enforces.
  */
class NullModelSpec extends AnyFlatSpec with Matchers {

  private def nullValue: Any = Column.int(Array(0), BitSet(0)).getValue(0)

  "optionSchema" should "be a single nullable column for a primitive" in {
    val schema = summon[Schema[Option[Int]]]
    schema.columnCount shouldBe 1
    schema.columnTypes shouldBe Vector(ColumnType.OptionType(ColumnType.IntType))
    schema.encode(Some(3)) shouldBe Vector(3)
    schema.encode(None) shouldBe Vector(nullValue)
    schema.decode(Vector(3)) shouldBe Right(Some(3))
    schema.decode(Vector(nullValue)) shouldBe Right(None)
  }

  it should "wrap a multi-column inner in a struct column" in {
    val schema = summon[Schema[Option[(Int, String)]]]
    schema.columnCount shouldBe 1
    schema.columnTypes should matchPattern { case Vector(ColumnType.OptionType(ColumnType.StructType(_))) =>
      ()
    }
    schema.encode(Some((1, "a"))).length shouldBe 1
    schema.encode(None) shouldBe Vector(nullValue)
    schema.decode(Vector((1, "a"))) shouldBe Right(Some((1, "a")))
    schema.decode(Vector(nullValue)) shouldBe Right(None)
  }

  "fromColumns" should "reject nulls in a non-optional column" in {
    Dataset.fromColumns(Vector(Column.int(Array(1, 2), BitSet(0))), Schema.intSchema) match {
      case Left(errors) => errors.toList should contain(SchemaError.NullInNonNullableColumn(0, "value"))
      case Right(_) => fail("expected NullInNonNullableColumn")
    }
  }

  it should "accept nulls in an optional column" in {
    Dataset
      .fromColumns(Vector(Column.int(Array(1, 2), BitSet(0))), Schema.optionSchema[Int])
      .isRight shouldBe true
  }
}
