package net.ghoula.vaire.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.prelude.*

/** Compiles and runs the Spark example in `README.md` so it cannot rot.
  *
  * The `Expr.Cell` names in a derived schema are `<field>_value`, and the Spark translation
  * resolves cells with `col(name)`, so a DataFrame read from a source with plain column names is
  * renamed to match before it is wrapped.
  */
class ReadmeExamplesSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  case class Trade(symbol: String, price: Double, quantity: Int)
  given Schema[Trade] = Schema.derived

  "the Spark example" should "run the plan and round-trip a DataFrame" in {
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

    val interpreter = SparkInterpreter(spark)

    // A DataFrame as `spark.read.parquet("/data/trades")` would produce.
    val sourceSchema = StructType(
      Seq(
        StructField("symbol", StringType, nullable = false),
        StructField("price", DoubleType, nullable = false),
        StructField("quantity", IntegerType, nullable = false)
      )
    )
    val rows = Seq(Row("ACME", 120.5, 10), Row("GLOB", 85.0, 40), Row("ACME", 99.9, 5))
    val df = spark
      .createDataFrame(spark.sparkContext.parallelize(rows), sourceSchema)
      .toDF("symbol_value", "price_value", "quantity_value")

    val trades = SparkDatasets.fromDataFrame(df, summon[Schema[Trade]])
    val filtered = trades.where(_.price > 100.0)

    interpreter.toDataFrame(filtered).toOption.get.count() shouldBe 1L
    filtered.collect(using interpreter).toOption.get.toSet shouldBe Set(Trade("ACME", 120.5, 10))
  }
}
