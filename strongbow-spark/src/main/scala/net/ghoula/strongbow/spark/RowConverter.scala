package net.ghoula.strongbow.spark

import org.apache.spark.sql.Row

import net.ghoula.strongbow.Schema
import net.ghoula.strongbow.errors.DecodeError

/** Type bridge between Strongbow values and Spark Rows using Schema[T]. */
object RowConverter {

  /** Encode a Strongbow value to a Spark Row via Schema. */
  def toRow[T](value: T, schema: Schema[T]): Row = {
    val encoded = schema.encode(value)
    Row.fromSeq(encoded)
  }

  /** Decode a Spark Row to a Strongbow value via Schema. */
  def fromRow[T](row: Row, schema: Schema[T]): Either[DecodeError, T] = {
    val values = (0 until row.size).map { i =>
      if (row.isNullAt(i)) null // scalafix:ok DisableSyntax.null
      else row.get(i)
    }.toVector
    schema.decode(values)
  }

  /** Decode a Spark Row, throwing on failure. For use in mapPartitions. */
  def fromRowUnsafe[T](row: Row, schema: Schema[T]): T = {
    fromRow(row, schema) match {
      case Right(value) => value
      case Left(err) => throw new RuntimeException(s"Row decode failed: $err") // scalafix:ok DisableSyntax.throw
    }
  }
}
