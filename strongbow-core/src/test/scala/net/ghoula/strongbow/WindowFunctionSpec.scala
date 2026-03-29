package net.ghoula.strongbow

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.prelude.*
import net.ghoula.strongbow.types.ColumnIndex

class WindowFunctionSpec extends AnyFlatSpec with Matchers {

  case class Score(dept: String, name: String, score: Int)
  given scoreSchema: Schema[Score] = Schema.derived

  case class ScoreWithRank(dept: String, name: String, score: Int, rowNum: Int)
  given swrSchema: Schema[ScoreWithRank] = Schema.derived

  case class ScoreWithRankAndDense(dept: String, name: String, score: Int, rnk: Int, denseRnk: Int)
  given swrdSchema: Schema[ScoreWithRankAndDense] = Schema.derived

  private def makeScores: Dataset[Score] = {
    val deptCol = Column.string(Array("Sales", "Sales", "Sales", "Eng", "Eng"))
    val nameCol = Column.string(Array("Alice", "Bob", "Charlie", "Dave", "Eve"))
    val scoreCol = Column.int(Array(90, 80, 90, 95, 85))
    Dataset.fromColumns(Vector(deptCol, nameCol, scoreCol), scoreSchema).toOption.get
  }

  "ROW_NUMBER" should "assign sequential numbers within partitions" in {
    val ds = makeScores

    val windowSpec = WindowSpec[Score](
      partitionBy = Vector(
        KeySpec[Score, Any]("dept", Expr.Cell[Score, Any]("dept", ColumnIndex(0)), ColumnType.StringType)
      ),
      orderBy = Vector(
        SortSpec[Score, Int](Expr.Cell("score", ColumnIndex(2)), summon[Ordering[Int]], ColumnType.IntType, false)
      )
    )

    val windowExprs = Vector(
      WindowExprSpec("rowNum", Expr.RowNumber[Score](), ColumnType.IntType)
    )

    val result = ds.withWindow[ScoreWithRank](windowExprs, windowSpec)
    val rows = DatasetInterpreter.execute(result).toOption.get.toVectorUnsafe

    // Sales: Alice(90) and Charlie(90) should be 1,2 (order among ties is implementation-defined)
    // Sales: Bob(80) should be 3
    val salesRows = rows.filter(_.dept == "Sales")
    salesRows.map(_.rowNum).sorted shouldBe Vector(1, 2, 3)
    salesRows.find(_.name == "Bob").get.rowNum shouldBe 3

    // Eng: Dave(95) should be 1, Eve(85) should be 2
    val engRows = rows.filter(_.dept == "Eng")
    engRows.find(_.name == "Dave").get.rowNum shouldBe 1
    engRows.find(_.name == "Eve").get.rowNum shouldBe 2
  }

  "RANK" should "handle ties correctly" in {
    val ds = makeScores

    val windowSpec = WindowSpec[Score](
      partitionBy = Vector(
        KeySpec[Score, Any]("dept", Expr.Cell[Score, Any]("dept", ColumnIndex(0)), ColumnType.StringType)
      ),
      orderBy = Vector(
        SortSpec[Score, Int](Expr.Cell("score", ColumnIndex(2)), summon[Ordering[Int]], ColumnType.IntType, false)
      )
    )

    val windowExprs = Vector(
      WindowExprSpec("rnk", Expr.Rank[Score](), ColumnType.IntType),
      WindowExprSpec("denseRnk", Expr.DenseRank[Score](), ColumnType.IntType)
    )

    val result = ds.withWindow[ScoreWithRankAndDense](windowExprs, windowSpec)
    val rows = DatasetInterpreter.execute(result).toOption.get.toVectorUnsafe

    // Sales: Alice(90) and Charlie(90) tied → both rank 1, Bob(80) → rank 3
    val salesRows = rows.filter(_.dept == "Sales")
    val aliceRank = salesRows.find(_.name == "Alice").get.rnk
    val charlieRank = salesRows.find(_.name == "Charlie").get.rnk
    val bobRank = salesRows.find(_.name == "Bob").get.rnk

    aliceRank shouldBe 1
    charlieRank shouldBe 1
    bobRank shouldBe 3

    // Dense rank: Alice/Charlie → 1, Bob → 2
    val bobDense = salesRows.find(_.name == "Bob").get.denseRnk
    bobDense shouldBe 2
  }

  "LAG" should "return previous row value within partition" in {
    case class ValRow(grp: String, val1: Int)
    given Schema[ValRow] = Schema.derived

    case class ValRowWithLag(grp: String, val1: Int, lagVal: Int)
    given Schema[ValRowWithLag] = Schema.derived

    val grpCol = Column.string(Array("A", "A", "A"))
    val valCol = Column.int(Array(10, 20, 30))
    val ds = Dataset.fromColumns(Vector(grpCol, valCol), summon[Schema[ValRow]]).toOption.get

    val windowSpec = WindowSpec[ValRow](
      partitionBy = Vector(
        KeySpec[ValRow, Any]("grp", Expr.Cell[ValRow, Any]("grp", ColumnIndex(0)), ColumnType.StringType)
      ),
      orderBy = Vector(
        SortSpec[ValRow, Int](Expr.Cell("val1", ColumnIndex(1)), summon[Ordering[Int]], ColumnType.IntType, true)
      )
    )

    val windowExprs = Vector(
      WindowExprSpec(
        "lagVal",
        Expr.Lag[ValRow, Any](
          Expr.Cell[ValRow, Any]("val1", ColumnIndex(1)),
          1,
          Some(0)
        ),
        ColumnType.IntType
      )
    )

    val result = ds.withWindow[ValRowWithLag](windowExprs, windowSpec)
    val rows = DatasetInterpreter.execute(result).toOption.get.toVectorUnsafe

    // Sorted by val1 ASC: 10, 20, 30
    // lag(val1, 1, 0): 0, 10, 20
    val sorted = rows.sortBy(_.val1)
    sorted(0).lagVal shouldBe 0 // first row, default
    sorted(1).lagVal shouldBe 10
    sorted(2).lagVal shouldBe 20
  }

  "LEAD" should "return next row value within partition" in {
    case class ValRow(grp: String, val1: Int)
    given Schema[ValRow] = Schema.derived

    case class ValRowWithLead(grp: String, val1: Int, leadVal: Int)
    given Schema[ValRowWithLead] = Schema.derived

    val grpCol = Column.string(Array("A", "A", "A"))
    val valCol = Column.int(Array(10, 20, 30))
    val ds = Dataset.fromColumns(Vector(grpCol, valCol), summon[Schema[ValRow]]).toOption.get

    val windowSpec = WindowSpec[ValRow](
      partitionBy = Vector(
        KeySpec[ValRow, Any]("grp", Expr.Cell[ValRow, Any]("grp", ColumnIndex(0)), ColumnType.StringType)
      ),
      orderBy = Vector(
        SortSpec[ValRow, Int](Expr.Cell("val1", ColumnIndex(1)), summon[Ordering[Int]], ColumnType.IntType, true)
      )
    )

    val windowExprs = Vector(
      WindowExprSpec(
        "leadVal",
        Expr.Lead[ValRow, Any](
          Expr.Cell[ValRow, Any]("val1", ColumnIndex(1)),
          1,
          Some(99)
        ),
        ColumnType.IntType
      )
    )

    val result = ds.withWindow[ValRowWithLead](windowExprs, windowSpec)
    val rows = DatasetInterpreter.execute(result).toOption.get.toVectorUnsafe

    // Sorted by val1 ASC: 10, 20, 30
    // lead(val1, 1, 99): 20, 30, 99
    val sorted = rows.sortBy(_.val1)
    sorted(0).leadVal shouldBe 20
    sorted(1).leadVal shouldBe 30
    sorted(2).leadVal shouldBe 99
  }
}
