package net.ghoula.strongbow

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.prelude.*

class ExprMacroSpec extends AnyFlatSpec with Matchers {

  case class User(id: Int, name: String, age: Int)
  given Schema[User] = Schema.derived

  private def createDataset(users: Vector[User]): Dataset[User] = {
    val idCol = Column.int(users.map(_.id).toArray)
    val nameCol = Column.string(users.map(_.name).toArray)
    val ageCol = Column.int(users.map(_.age).toArray)

    Dataset.fromColumns(Vector(idCol, nameCol, ageCol), summon[Schema[User]]) match {
      case Right(ds) => ds
      case Left(errors) => fail(s"Failed to create dataset: ${errors.toList}")
    }
  }

  private val testUsers = Vector(
    User(1, "Alice", 25),
    User(2, "Bob", 30),
    User(3, "Charlie", 17),
    User(4, "Diana", 65)
  )

  // --- column macro tests ---

  "column macro" should "produce Cell and IntType for _.age" in {
    val (expr, colType) = ExprMacro.column[User, Int](_.age)

    expr shouldBe Expr.Cell[User, Int]("age", ColumnIndex(2))
    colType shouldBe ColumnType.IntType
  }

  it should "produce Cell and StringType for _.name" in {
    val (expr, colType) = ExprMacro.column[User, String](_.name)

    expr shouldBe Expr.Cell[User, String]("name", ColumnIndex(1))
    colType shouldBe ColumnType.StringType
  }

  it should "produce Cell and IntType for _.id" in {
    val (expr, colType) = ExprMacro.column[User, Int](_.id)

    expr shouldBe Expr.Cell[User, Int]("id", ColumnIndex(0))
    colType shouldBe ColumnType.IntType
  }

  // --- predicate macro tests ---

  "predicate macro" should "compile _.age > 25" in {
    val pred = ExprMacro.predicate[User](_.age > 25)

    pred shouldBe a[Expr.Gt[?, ?]]

    val dataset = createDataset(testUsers)
    val result = dataset.filter(pred).collect.toOption.get

    result shouldBe Vector(User(2, "Bob", 30), User(4, "Diana", 65))
  }

  it should "compile compound u => u.age >= 18 && u.age < 65" in {
    val pred = ExprMacro.predicate[User](u => u.age >= 18 && u.age < 65)

    pred shouldBe a[Expr.And[?]]

    val dataset = createDataset(testUsers)
    val result = dataset.filter(pred).collect.toOption.get

    result shouldBe Vector(User(1, "Alice", 25), User(2, "Bob", 30))
  }

  it should "compile _.name == \"Alice\"" in {
    val pred = ExprMacro.predicate[User](_.name == "Alice")

    pred shouldBe a[Expr.Eq[?, ?]]

    val dataset = createDataset(testUsers)
    val result = dataset.filter(pred).collect.toOption.get

    result shouldBe Vector(User(1, "Alice", 25))
  }

  it should "compile _.age != 25" in {
    val pred = ExprMacro.predicate[User](_.age != 25)

    val dataset = createDataset(testUsers)
    val result = dataset.filter(pred).collect.toOption.get

    result shouldBe Vector(User(2, "Bob", 30), User(3, "Charlie", 17), User(4, "Diana", 65))
  }

  it should "compile arithmetic _.age * 2 + 1 > 50" in {
    val pred = ExprMacro.predicate[User](_.age * 2 + 1 > 50)

    val dataset = createDataset(testUsers)
    val result = dataset.filter(pred).collect.toOption.get

    // age * 2 + 1 > 50: Alice=51(yes), Bob=61(yes), Charlie=35(no), Diana=131(yes)
    result shouldBe Vector(User(1, "Alice", 25), User(2, "Bob", 30), User(4, "Diana", 65))
  }

  // --- where extension tests ---

  "where extension" should "produce same results as manual filter" in {
    val dataset = createDataset(testUsers)

    // Manual
    val ageCell = Expr.Cell[User, Int]("age", ColumnIndex(2))
    val manualResult = dataset.filter(ageCell > Expr.lit(25)).collect.toOption.get

    // Macro
    val macroResult = dataset.where(_.age > 25).collect.toOption.get

    macroResult shouldBe manualResult
  }

  // --- sortByColumn extension tests ---

  "sortByColumn extension" should "produce sorted output matching sortByExpr" in {
    val dataset = createDataset(testUsers)

    // Manual
    val ageCell = Expr.Cell[User, Int]("age", ColumnIndex(2))
    val manualResult = dataset.sortByExpr(ageCell, ColumnType.IntType).collect.toOption.get

    // Macro
    val macroResult = dataset.sortByColumn(_.age).collect.toOption.get

    macroResult shouldBe manualResult
  }

  // --- groupByColumn extension tests ---

  "groupByColumn extension" should "produce same groups as groupByExpr" in {
    val dataset = createDataset(testUsers)

    // Manual
    val ageCell = Expr.Cell[User, Int]("age", ColumnIndex(2))
    val manualResult = GroupByInterpreter.execute(dataset.groupByExpr(ageCell, ColumnType.IntType))

    // Macro
    val macroResult = GroupByInterpreter.execute(dataset.groupByColumn(_.age))

    macroResult should contain theSameElementsAs manualResult
  }

  // --- parity check ---

  "macro-compiled expressions" should "match manual Expr construction when evaluated" in {
    val dataset = createDataset(testUsers)

    // Manual expression: age >= 18 && age < 65
    val ageCell = Expr.Cell[User, Int]("age", ColumnIndex(2))
    val manualPred = Expr.And(
      Expr.Gte(ageCell, Expr.Const[User, Int](18), summon[Ordering[Int]]),
      Expr.Lt(ageCell, Expr.Const[User, Int](65), summon[Ordering[Int]])
    )
    val manualResult = dataset.filter(manualPred).collect.toOption.get

    // Macro-compiled
    val macroResult = dataset.where(u => u.age >= 18 && u.age < 65).collect.toOption.get

    macroResult shouldBe manualResult
  }
}
