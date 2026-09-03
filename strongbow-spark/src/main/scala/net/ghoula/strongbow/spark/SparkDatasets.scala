package net.ghoula.strongbow.spark

import org.apache.spark.sql.DataFrame

import net.ghoula.strongbow.Schema
import net.ghoula.strongbow.dataset.Dataset

/** Factory methods for creating Strongbow Datasets backed by Spark DataFrames.
  *
  * @example
  *   {{{
  * val df = spark.read.parquet("/data/sales")
  * val ds = SparkDatasets.fromDataFrame(df, salesSchema)
  * val result = ds.filter(amountCell > Expr.lit(1000.0))
  * val outDf = sparkInterpreter.toDataFrame(result).toOption.get
  * outDf.write.parquet("/output/filtered")
  *   }}}
  */
object SparkDatasets {

  /** A Dataset plan rooted at an existing Spark DataFrame. */
  def fromDataFrame[T](df: DataFrame, schema: Schema[T]): Dataset[T] =
    Dataset.Root(SparkSource(df), schema)
}
