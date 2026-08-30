package net.ghoula.strongbow.spark

import net.ghoula.strongbow.column.Column
import net.ghoula.strongbow.types.Time

/** Converts Strongbow column values to the representation Spark expects at Row boundaries.
  *
  * Time values become java.time.LocalTime, decimals become java.math.BigDecimal, struct values
  * become Rows, and the conversion recurses into array/map/struct children.
  */
private[strongbow] object SparkValues {

  def columnValue(col: Column[?], index: Int): Any | Null = col match {
    case Column.TimeColumn(data, nulls) =>
      if (nulls.contains(index)) null // scalafix:ok DisableSyntax.null
      else Time.ofMicros(data(index)).toLocalTime
    case Column.DecimalColumn(data, _, scale, nulls) =>
      if (nulls.contains(index)) null // scalafix:ok DisableSyntax.null
      else java.math.BigDecimal.valueOf(data(index), scale)
    case Column.ArrayColumn(elements, offsets, nulls) =>
      if (nulls.contains(index)) null // scalafix:ok DisableSyntax.null
      else (offsets(index) until offsets(index + 1)).map(j => columnValue(elements, j)).toSeq
    case Column.MapColumn(keys, values, offsets, nulls) =>
      if (nulls.contains(index)) null // scalafix:ok DisableSyntax.null
      else
        (offsets(index) until offsets(index + 1))
          .map(j => columnValue(keys, j) -> columnValue(values, j))
          .toMap
    case Column.StructColumn(columns, _, nulls) =>
      if (nulls.contains(index)) null // scalafix:ok DisableSyntax.null
      else org.apache.spark.sql.Row.fromSeq(columns.map(c => columnValue(c, index)))
    case other => other.getValue(index)
  }
}
