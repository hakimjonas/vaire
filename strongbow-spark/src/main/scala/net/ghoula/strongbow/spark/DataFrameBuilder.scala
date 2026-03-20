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
    val cols = dataset.columns
    val colCount = cols.length
    val rows = Array.tabulate(dataset.rowCount) { rowIdx =>
      Row.fromSeq(cols.map(_.getValue(rowIdx)))
    }
    val javaRows = java.util.Arrays.asList(rows*)
    spark.createDataFrame(javaRows, structType)
  }

  /** Build a DataFrame from columns and schema directly. */
  def fromColumns[T](
    spark: SparkSession,
    columns: Vector[SBColumn[?]],
    schema: Schema[T]
  ): DataFrame = {
    val structType = SchemaConverter.toStructType(schema)
    val rowCount = if (columns.isEmpty) 0 else columns.head.length
    val colCount = columns.length
    val rows = Array.tabulate(rowCount) { rowIdx =>
      Row.fromSeq(columns.map(_.getValue(rowIdx)))
    }
    val javaRows = java.util.Arrays.asList(rows*)
    spark.createDataFrame(javaRows, structType)
  }
}
