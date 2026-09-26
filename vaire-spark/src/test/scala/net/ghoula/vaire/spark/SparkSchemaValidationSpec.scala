package net.ghoula.vaire.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import net.ghoula.vaire.errors.ExecutionError
import net.ghoula.vaire.prelude.*

/** Locks the Spark source boundary: a nullable Spark column needs an `Option` Vairë field, and
  * `narrow` asserts non-null against the data.
  */
class SparkSchemaValidationSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  private def nullableIntDf(rows: Any*): org.apache.spark.sql.DataFrame = {
    val structType = StructType(Array(StructField("value", IntegerType, nullable = true)))
    spark.createDataFrame(java.util.Arrays.asList(rows.map(Row(_))*), structType)
  }

  "a Spark source" should "reject a nullable column mapped to a non-optional Vairë field" in {
    val ds = SparkDatasets.fromDataFrame(nullableIntDf(1), Schema.intSchema)
    sparkInterpreter.execute(ds) match {
      case Left(_: ExecutionError.InvalidPlan) => ()
      case other => fail(s"expected InvalidPlan, got $other")
    }
  }

  it should "accept a nullable column mapped to an Option Vairë field" in {
    val ds = SparkDatasets.fromDataFrame(nullableIntDf(1), Schema.optionSchema[Int])
    sparkInterpreter.execute(ds).toOption.get.toVectorUnsafe shouldBe Vector(Some(1))
  }

  "narrow" should "succeed when the data has no nulls and fail when it does" in {
    val clean = SparkDatasets.fromDataFrame(nullableIntDf(1, 2), Schema.optionSchema[Int])
    sparkInterpreter.execute(clean.narrow[Int]).toOption.get.toVectorUnsafe shouldBe Vector(1, 2)

    val dirtyDf = spark.sql("SELECT * FROM VALUES (1), (CAST(NULL AS INT)) AS t(value)")
    val dirty = SparkDatasets.fromDataFrame(dirtyDf, Schema.optionSchema[Int])
    sparkInterpreter.execute(dirty.narrow[Int]) match {
      case Left(_: ExecutionError.InvalidPlan) => ()
      case other => fail(s"expected InvalidPlan, got $other")
    }
  }
}
