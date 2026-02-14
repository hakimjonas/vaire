package net.ghoula.strongbow.spark

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import net.ghoula.strongbow.{Column => SBColumn, MaterializedDataset, Schema}

/** Helper to create Spark DataFrames from Strongbow's columnar storage. */
object DataFrameBuilder {

  /** Build a DataFrame from a MaterializedDataset. */
  def fromMaterialized[T](
    spark: SparkSession,
    dataset: MaterializedDataset[T]
  ): DataFrame = {
    val structType = SchemaConverter.toStructType(dataset.schema)
    val rows = (0 until dataset.rowCount).map { rowIdx =>
      val values = dataset.columns.map(_.getValue(rowIdx))
      Row.fromSeq(values)
    }

    val javaRows = java.util.Arrays.asList(rows*)
    spark.createDataFrame(javaRows, structType)
  }

  /** Build a DataFrame from columns and schema directly. */
  def fromColumns[T](
    spark: SparkSession,
    columns: Vector[SBColumn],
    schema: Schema[T]
  ): DataFrame = {
    val structType = SchemaConverter.toStructType(schema)
    val rowCount = if (columns.isEmpty) 0 else columns.head.length
    val rows = (0 until rowCount).map { rowIdx =>
      val values = columns.map(_.getValue(rowIdx))
      Row.fromSeq(values)
    }

    val javaRows = java.util.Arrays.asList(rows*)
    spark.createDataFrame(javaRows, structType)
  }
}
