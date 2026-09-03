package net.ghoula.vaire.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.prelude.*

class SparkSortByExprsSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  case class Record(name: String, age: Int, score: Double)
  given Schema[Record] = Schema.derived

  // Schema-derived names: name_value, age_value, score_value
  private val ageCell: Expr[Record, Int] = Expr.Cell("age_value", ColumnIndex(1))
  private val scoreCell: Expr[Record, Double] = Expr.Cell("score_value", ColumnIndex(2))

  private def makeRecords: Dataset[Record] = {
    val nameCol = Column.string(Array("Alice", "Bob", "Charlie", "Dave", "Eve"))
    val ageCol = Column.int(Array(30, 25, 30, 25, 35))
    val scoreCol = Column.double(Array(90.0, 80.0, 85.0, 95.0, 70.0))
    Dataset.fromColumns(Vector(nameCol, ageCol, scoreCol), summon[Schema[Record]]).toOption.get
  }

  "SparkInterpreter SortByExprs" should "produce same results as in-memory" in {
    val ds = makeRecords

    val sorted = ds.sortByExprs(
      Vector(
        SortSpec(ageCell, summon[Ordering[Int]], ColumnType.IntType, true),
        SortSpec(scoreCell, summon[Ordering[Double]], ColumnType.DoubleType, true)
      )
    )

    val inMemory = DatasetInterpreter.execute(sorted).map(_.toVectorUnsafe)
    val sparkResult = sparkInterpreter.execute(sorted).map(_.toVectorUnsafe)

    sparkResult shouldBe inMemory
  }

  it should "handle mixed ASC/DESC" in {
    val ds = makeRecords

    val sorted = ds.sortByExprs(
      Vector(
        SortSpec(ageCell, summon[Ordering[Int]], ColumnType.IntType, true),
        SortSpec(scoreCell, summon[Ordering[Double]], ColumnType.DoubleType, false)
      )
    )

    val inMemory = DatasetInterpreter.execute(sorted).map(_.toVectorUnsafe)
    val sparkResult = sparkInterpreter.execute(sorted).map(_.toVectorUnsafe)

    sparkResult shouldBe inMemory
  }
}
