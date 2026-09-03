package net.ghoula.vaire.spark

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.column.ColumnType
import net.ghoula.vaire.expr.Expr
import net.ghoula.vaire.interpreter.ExprInterpreter
import net.ghoula.vaire.prelude.*

/** Parity for the non-AES hash functions (crc32, xxhash64, hash).
  *
  * The in-memory implementations port Spark's exact algorithms (CRC-32, XXH64 seed 42, Murmur3 x86
  * 32 seed 42) over the UTF-8 bytes, so both backends must agree byte-for-byte.
  */
class CryptoParitySpec extends AnyFlatSpec with Matchers with SparkTestBase {

  case class Doc(s: String)
  given Schema[Doc] = Schema.derived

  private lazy val base: DataFrame = spark.sql("""
      SELECT inline(array(
        struct('hello'),
        struct(''),
        struct('The quick brown fox jumps over the lazy dog'),
        struct(cast(null as string))
      )) AS (s_value)
    """)

  private lazy val materialized =
    RowConverter.toMaterialized(base.collect(), summon[Schema[Doc]]).fold(err => fail(s"$err"), identity)

  private def cell: Expr[Doc, String] = Expr.cell("s_value", ColumnIndex(0))

  private def checkParity[A](expr: Expr[Doc, A], columnType: ColumnType): Unit = {
    val inMemory = ExprInterpreter.evalColumn(expr, materialized.columns, columnType) match {
      case Right(col) => (0 until col.length).toVector.map(col.getValue)
      case other => fail(s"In-memory eval failed: $other")
    }
    val (sparkCol, _) = ExprToColumn.convert(expr) match {
      case Right(c) => c
      case other => fail(s"Spark conversion failed: $other")
    }
    val sparkValues = base.select(sparkCol).collect().toVector.map(r => r.get(0): Any | Null)
    inMemory shouldBe sparkValues
  }

  "crc32" should "match Spark on both backends" in {
    checkParity(cell.crc32, ColumnType.LongType)
  }

  "xxhash64" should "match Spark on both backends" in {
    checkParity(cell.xxhash64, ColumnType.LongType)
  }

  "hash" should "match Spark on both backends" in {
    checkParity(cell.hash, ColumnType.IntType)
  }
}
