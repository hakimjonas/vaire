package net.ghoula.strongbow

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.specs.SortSpec
import net.ghoula.strongbow.types.ColumnIndex

class SortByExprsSpec extends AnyFlatSpec with Matchers {

  case class Record(name: String, age: Int, score: Double)
  given Schema[Record] = Schema.derived

  private def makeRecords: Dataset[Record] = {
    val nameCol = Column.string(Array("Alice", "Bob", "Charlie", "Dave", "Eve"))
    val ageCol = Column.int(Array(30, 25, 30, 25, 35))
    val scoreCol = Column.double(Array(90.0, 80.0, 85.0, 95.0, 70.0))
    Dataset.fromColumns(Vector(nameCol, ageCol, scoreCol), summon[Schema[Record]]).toOption.get
  }

  "SortByExprs" should "sort by two columns" in {
    val ds = makeRecords

    val sorted = ds.sortByExprs(
      Vector(
        SortSpec[Record, Int](Expr.Cell("age", ColumnIndex(1)), summon[Ordering[Int]], ColumnType.IntType, true),
        SortSpec[Record, Double](
          Expr.Cell("score", ColumnIndex(2)),
          summon[Ordering[Double]],
          ColumnType.DoubleType,
          true
        )
      )
    )

    val result = DatasetInterpreter.execute(sorted).toOption.get.toVectorUnsafe
    result.map(_.name) shouldBe Vector("Bob", "Dave", "Charlie", "Alice", "Eve")
  }

  it should "handle mixed ASC/DESC" in {
    val ds = makeRecords

    val sorted = ds.sortByExprs(
      Vector(
        SortSpec[Record, Int](Expr.Cell("age", ColumnIndex(1)), summon[Ordering[Int]], ColumnType.IntType, true),
        SortSpec[Record, Double](
          Expr.Cell("score", ColumnIndex(2)),
          summon[Ordering[Double]],
          ColumnType.DoubleType,
          false
        )
      )
    )

    val result = DatasetInterpreter.execute(sorted).toOption.get.toVectorUnsafe
    result.map(_.name) shouldBe Vector("Dave", "Bob", "Alice", "Charlie", "Eve")
  }

  it should "sort by three columns" in {
    val nameCol = Column.string(Array("A", "B", "C", "D", "E", "F"))
    val col1 = Column.int(Array(1, 1, 2, 2, 1, 2))
    val col2 = Column.int(Array(10, 10, 10, 20, 20, 10))
    val col3 = Column.double(Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0))

    case class Row3(name: String, c1: Int, c2: Int, c3: Double)
    given Schema[Row3] = Schema.derived

    val ds = Dataset.fromColumns(Vector(nameCol, col1, col2, col3), summon[Schema[Row3]]).toOption.get

    val sorted = ds.sortByExprs(
      Vector(
        SortSpec[Row3, Int](Expr.Cell("c1", ColumnIndex(1)), summon[Ordering[Int]], ColumnType.IntType, true),
        SortSpec[Row3, Int](Expr.Cell("c2", ColumnIndex(2)), summon[Ordering[Int]], ColumnType.IntType, true),
        SortSpec[Row3, Double](Expr.Cell("c3", ColumnIndex(3)), summon[Ordering[Double]], ColumnType.DoubleType, true)
      )
    )

    val result = DatasetInterpreter.execute(sorted).toOption.get.toVectorUnsafe
    result.map(_.name) shouldBe Vector("A", "B", "E", "C", "F", "D")
  }

  it should "handle single-row dataset" in {
    val nameCol = Column.string(Array("Alice"))
    val ageCol = Column.int(Array(30))
    val scoreCol = Column.double(Array(90.0))
    val ds = Dataset.fromColumns(Vector(nameCol, ageCol, scoreCol), summon[Schema[Record]]).toOption.get

    val sorted = ds.sortByExprs(
      Vector(
        SortSpec[Record, Int](Expr.Cell("age", ColumnIndex(1)), summon[Ordering[Int]], ColumnType.IntType, true)
      )
    )

    val result = DatasetInterpreter.execute(sorted).toOption.get.toVectorUnsafe
    result.length shouldBe 1
    result.head.name shouldBe "Alice"
  }
}
