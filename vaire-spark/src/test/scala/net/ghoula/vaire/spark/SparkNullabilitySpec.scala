package net.ghoula.vaire.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.types.IntegerType

import scala.collection.immutable.BitSet

import net.ghoula.vaire.prelude.*

/** Backend parity for the single-column null model.
  *
  * A nullable Vairë field is one nullable Spark column; a non-optional field is non-nullable. The
  * values round-trip, with a null mapping to `None`.
  */
class SparkNullabilitySpec extends AnyFlatSpec with Matchers with SparkTestBase {

  private def optInts(values: Array[Int], nulls: BitSet): Dataset[Option[Int]] =
    Dataset.fromColumns(Vector(Column.int(values, nulls)), Schema.optionSchema[Int]).toOption.get

  "a nullable Vairë field" should "round-trip through Spark as one nullable column" in {
    val ds = optInts(Array(1, 0), BitSet(1)) // [Some(1), None]

    val df = sparkInterpreter.toDataFrame(ds).toOption.get
    df.schema.fields.length shouldBe 1
    df.schema.fields(0).nullable shouldBe true
    df.schema.fields(0).dataType shouldBe IntegerType

    val rows = df.collect()
    rows(0).getInt(0) shouldBe 1
    rows(1).isNullAt(0) shouldBe true

    sparkInterpreter.execute(ds).toOption.get.toVectorUnsafe shouldBe Vector(Some(1), None)
  }

  "a non-optional Vairë field" should "map to a non-nullable Spark column" in {
    val ds = Dataset.fromColumns(Vector(Column.int(Array(1, 2))), Schema.intSchema).toOption.get
    sparkInterpreter.toDataFrame(ds).toOption.get.schema.fields(0).nullable shouldBe false
  }
}
