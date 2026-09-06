package net.ghoula.vaire.spark

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import net.ghoula.vaire.Schema
import net.ghoula.vaire.column.{Column => SBColumn}
import net.ghoula.vaire.dataset.MaterializedDataset

/** Helper to create Spark DataFrames from Vairë's columnar storage. */
object DataFrameBuilder {

  /** Build a DataFrame from a MaterializedDataset. */
  def fromMaterialized[T](
    spark: SparkSession,
    dataset: MaterializedDataset[T]
  ): DataFrame = {
    val structType = SchemaConverter.toStructType(dataset.schema)
    val cols = dataset.columns
    val rows = Array.tabulate(dataset.rowCount) { rowIdx =>
      Row.fromSeq(cols.map(SparkValues.columnValue(_, rowIdx)))
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
    val rows = Array.tabulate(rowCount) { rowIdx =>
      Row.fromSeq(columns.map(SparkValues.columnValue(_, rowIdx)))
    }
    val javaRows = java.util.Arrays.asList(rows*)
    spark.createDataFrame(javaRows, structType)
  }
}
