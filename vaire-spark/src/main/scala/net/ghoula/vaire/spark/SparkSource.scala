package net.ghoula.vaire.spark

import org.apache.spark.sql.DataFrame

import net.ghoula.vaire.dataset.DataSource

/** Data source backed by a Spark DataFrame.
  *
  * Enables zero-overhead Spark pipelines: the data stays in Catalyst, no array conversion needed.
  */
case class SparkSource(df: DataFrame) extends DataSource
