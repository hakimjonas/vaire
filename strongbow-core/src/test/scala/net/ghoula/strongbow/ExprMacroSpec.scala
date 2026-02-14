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

  it should "compile _.age < 18 || _.age > 60 (Or predicate)" in {
    val pred = ExprMacro.predicate[User](u => u.age < 18 || u.age > 60)

    pred shouldBe a[Expr.Or[?]]

    val dataset = createDataset(testUsers)
    val result = dataset.filter(pred).collect.toOption.get

    result shouldBe Vector(User(3, "Charlie", 17), User(4, "Diana", 65))
  }

  // --- String ops in predicate tests (8.2c) ---

  it should "compile string concat _.name + \"!\" == \"Alice!\"" in {
    val pred = ExprMacro.predicate[User](_.name + "!" == "Alice!")

    val dataset = createDataset(testUsers)
    val result = dataset.filter(pred).collect.toOption.get

    result shouldBe Vector(User(1, "Alice", 25))
  }

  it should "compile string length _.name.length > 3" in {
    val pred = ExprMacro.predicate[User](_.name.length > 3)

    val dataset = createDataset(testUsers)
    val result = dataset.filter(pred).collect.toOption.get

    // Alice(5), Charlie(7), Diana(5) all > 3; Bob(3) not > 3
    result shouldBe Vector(User(1, "Alice", 25), User(3, "Charlie", 17), User(4, "Diana", 65))
  }

  it should "compile string length + int arithmetic _.name.length + 1 > 5" in {
    val pred = ExprMacro.predicate[User](_.name.length + 1 > 5)

    val dataset = createDataset(testUsers)
    val result = dataset.filter(pred).collect.toOption.get

    // Alice.length+1=6(yes), Bob.length+1=4(no), Charlie.length+1=8(yes), Diana.length+1=6(yes)
    result shouldBe Vector(User(1, "Alice", 25), User(3, "Charlie", 17), User(4, "Diana", 65))
  }

  // --- Not / boolean field access tests (8.2b) ---

  case class Member(name: String, age: Int, isActive: Boolean)
  given Schema[Member] = Schema.derived

  private def createMemberDataset(members: Vector[Member]): Dataset[Member] = {
    val nameCol = Column.string(members.map(_.name).toArray)
    val ageCol = Column.int(members.map(_.age).toArray)
    val activeCol = Column.boolean(members.map(_.isActive).toArray)

    Dataset.fromColumns(Vector(nameCol, ageCol, activeCol), summon[Schema[Member]]) match {
      case Right(ds) => ds
      case Left(errors) => fail(s"Failed to create dataset: ${errors.toList}")
    }
  }

  private val testMembers = Vector(
    Member("Alice", 25, true),
    Member("Bob", 30, true),
    Member("Charlie", 17, false),
    Member("Diana", 65, false)
  )

  "predicate macro" should "compile boolean field access _.isActive" in {
    val pred = ExprMacro.predicate[Member](_.isActive)

    pred shouldBe a[Expr.Cell[?, ?]]

    val dataset = createMemberDataset(testMembers)
    val result = dataset.filter(pred).collect.toOption.get

    result shouldBe Vector(Member("Alice", 25, true), Member("Bob", 30, true))
  }

  it should "compile negated boolean field !_.isActive" in {
    val pred = ExprMacro.predicate[Member](m => !m.isActive)

    pred shouldBe a[Expr.Not[?]]

    val dataset = createMemberDataset(testMembers)
    val result = dataset.filter(pred).collect.toOption.get

    result shouldBe Vector(Member("Charlie", 17, false), Member("Diana", 65, false))
  }

  it should "compile !_.isActive && _.age > 25 (Not combined with comparison)" in {
    val pred = ExprMacro.predicate[Member](m => !m.isActive && m.age > 25)

    pred shouldBe a[Expr.And[?]]

    val dataset = createMemberDataset(testMembers)
    val result = dataset.filter(pred).collect.toOption.get

    result shouldBe Vector(Member("Diana", 65, false))
  }

  // --- Nested case class field access tests (8.2d) ---

  case class Address(street: String, city: String)
  given Schema[Address] = Schema.derived

  case class Person(name: String, age: Int, address: Address)
  given Schema[Person] = Schema.derived

  private def createPersonDataset(people: Vector[Person]): Dataset[Person] = {
    val nameCol = Column.string(people.map(_.name).toArray)
    val ageCol = Column.int(people.map(_.age).toArray)
    val streetCol = Column.string(people.map(_.address.street).toArray)
    val cityCol = Column.string(people.map(_.address.city).toArray)

    Dataset.fromColumns(Vector(nameCol, ageCol, streetCol, cityCol), summon[Schema[Person]]) match {
      case Right(ds) => ds
      case Left(errors) => fail(s"Failed to create dataset: ${errors.toList}")
    }
  }

  private val testPeople = Vector(
    Person("Alice", 25, Address("123 Main St", "Boston")),
    Person("Bob", 30, Address("456 Oak Ave", "New York")),
    Person("Charlie", 17, Address("789 Elm Dr", "Boston")),
    Person("Diana", 65, Address("321 Pine Rd", "Chicago"))
  )

  "predicate macro" should "compile nested field _.address.city == \"Boston\"" in {
    val pred = ExprMacro.predicate[Person](_.address.city == "Boston")

    val dataset = createPersonDataset(testPeople)
    val result = dataset.filter(pred).collect.toOption.get

    result shouldBe Vector(
      Person("Alice", 25, Address("123 Main St", "Boston")),
      Person("Charlie", 17, Address("789 Elm Dr", "Boston"))
    )
  }

  it should "compile nested + flat _.address.city == \"Boston\" && _.age > 20" in {
    val pred = ExprMacro.predicate[Person](p => p.address.city == "Boston" && p.age > 20)

    val dataset = createPersonDataset(testPeople)
    val result = dataset.filter(pred).collect.toOption.get

    result shouldBe Vector(Person("Alice", 25, Address("123 Main St", "Boston")))
  }

  "column macro" should "resolve nested field _.address.city to correct index" in {
    val (expr, colType) = ExprMacro.column[Person, String](_.address.city)

    // Person: name(0), age(1), address.street(2), address.city(3)
    expr shouldBe Expr.Cell[Person, String]("address.city", ColumnIndex(3))
    colType shouldBe ColumnType.StringType
  }

  // --- compileExpr tests (8.2e) ---

  "compileExpr" should "compile Int arithmetic expression _.age * 2 + 1" in {
    val (expr, colType) = ExprMacro.compileExpr[User, Int](_.age * 2 + 1)

    colType shouldBe ColumnType.IntType
    expr shouldBe a[Expr.Add[?]]
  }

  it should "compile String field access _.name" in {
    val (expr, colType) = ExprMacro.compileExpr[User, String](_.name)

    colType shouldBe ColumnType.StringType
    expr shouldBe Expr.Cell[User, String]("name", ColumnIndex(1))
  }

  it should "compile Boolean predicate _.age > 25" in {
    val (expr, colType) = ExprMacro.compileExpr[User, Boolean](_.age > 25)

    colType shouldBe ColumnType.BooleanType
    expr shouldBe a[Expr.Gt[?, ?]]
  }

  // --- Long/Double arithmetic tests (8.2f) ---

  case class Measurement(label: String, count: Long, value: Double)
  given Schema[Measurement] = Schema.derived

  private def createMeasurementDataset(measurements: Vector[Measurement]): Dataset[Measurement] = {
    val labelCol = Column.string(measurements.map(_.label).toArray)
    val countCol = Column.long(measurements.map(_.count).toArray)
    val valueCol = Column.double(measurements.map(_.value).toArray)

    Dataset.fromColumns(Vector(labelCol, countCol, valueCol), summon[Schema[Measurement]]) match {
      case Right(ds) => ds
      case Left(errors) => fail(s"Failed to create dataset: ${errors.toList}")
    }
  }

  private val testMeasurements = Vector(
    Measurement("a", 10L, 2.5),
    Measurement("b", 20L, 3.0),
    Measurement("c", 5L, 1.5),
    Measurement("d", 30L, 4.0)
  )

  // Long arithmetic tests
  "predicate macro" should "compile Long arithmetic _.count * 2L + 5L > 25L" in {
    val pred = ExprMacro.predicate[Measurement](_.count * 2L + 5L > 25L)

    val dataset = createMeasurementDataset(testMeasurements)
    val result = dataset.filter(pred).collect.toOption.get

    // a: 10*2+5=25(no), b: 20*2+5=45(yes), c: 5*2+5=15(no), d: 30*2+5=65(yes)
    result shouldBe Vector(Measurement("b", 20L, 3.0), Measurement("d", 30L, 4.0))
  }

  it should "compile Long subtraction _.count - 15L > 0L" in {
    val pred = ExprMacro.predicate[Measurement](_.count - 15L > 0L)

    val dataset = createMeasurementDataset(testMeasurements)
    val result = dataset.filter(pred).collect.toOption.get

    // a: -5(no), b: 5(yes), c: -10(no), d: 15(yes)
    result shouldBe Vector(Measurement("b", 20L, 3.0), Measurement("d", 30L, 4.0))
  }

  it should "compile Long division _.count / 10L > 1L" in {
    val pred = ExprMacro.predicate[Measurement](_.count / 10L > 1L)

    val dataset = createMeasurementDataset(testMeasurements)
    val result = dataset.filter(pred).collect.toOption.get

    // a: 1(no), b: 2(yes), c: 0(no), d: 3(yes)
    result shouldBe Vector(Measurement("b", 20L, 3.0), Measurement("d", 30L, 4.0))
  }

  // Double arithmetic tests
  it should "compile Double multiplication _.value * 2.0 > 4.0" in {
    val pred = ExprMacro.predicate[Measurement](_.value * 2.0 > 4.0)

    val dataset = createMeasurementDataset(testMeasurements)
    val result = dataset.filter(pred).collect.toOption.get

    // a: 5.0(yes), b: 6.0(yes), c: 3.0(no), d: 8.0(yes)
    result shouldBe Vector(
      Measurement("a", 10L, 2.5),
      Measurement("b", 20L, 3.0),
      Measurement("d", 30L, 4.0)
    )
  }

  it should "compile Double addition _.value + 1.0 > 3.0" in {
    val pred = ExprMacro.predicate[Measurement](_.value + 1.0 > 3.0)

    val dataset = createMeasurementDataset(testMeasurements)
    val result = dataset.filter(pred).collect.toOption.get

    // a: 3.5(yes), b: 4.0(yes), c: 2.5(no), d: 5.0(yes)
    result shouldBe Vector(
      Measurement("a", 10L, 2.5),
      Measurement("b", 20L, 3.0),
      Measurement("d", 30L, 4.0)
    )
  }

  it should "compile Double subtraction _.value - 2.0 > 0.0" in {
    val pred = ExprMacro.predicate[Measurement](_.value - 2.0 > 0.0)

    val dataset = createMeasurementDataset(testMeasurements)
    val result = dataset.filter(pred).collect.toOption.get

    // a: 0.5(yes), b: 1.0(yes), c: -0.5(no), d: 2.0(yes)
    result shouldBe Vector(
      Measurement("a", 10L, 2.5),
      Measurement("b", 20L, 3.0),
      Measurement("d", 30L, 4.0)
    )
  }

  it should "compile Double division _.value / 2.0 > 1.0" in {
    val pred = ExprMacro.predicate[Measurement](_.value / 2.0 > 1.0)

    val dataset = createMeasurementDataset(testMeasurements)
    val result = dataset.filter(pred).collect.toOption.get

    // a: 1.25(yes), b: 1.5(yes), c: 0.75(no), d: 2.0(yes)
    result shouldBe Vector(
      Measurement("a", 10L, 2.5),
      Measurement("b", 20L, 3.0),
      Measurement("d", 30L, 4.0)
    )
  }

  // Extension method tests — manual construction verifying correct variant
  "Long extension methods" should "produce correct arithmetic variants" in {
    val cell = Expr.Cell[Measurement, Long]("count", ColumnIndex(1))
    val c = Expr.Const[Measurement, Long](5L)

    (cell + c) shouldBe a[Expr.AddLong[?]]
    (cell - c) shouldBe a[Expr.SubLong[?]]
    (cell * c) shouldBe a[Expr.MulLong[?]]
    (cell / c) shouldBe a[Expr.DivLong[?]]
  }

  "Double extension methods" should "produce correct arithmetic variants" in {
    val cell = Expr.Cell[Measurement, Double]("value", ColumnIndex(2))
    val c = Expr.Const[Measurement, Double](2.0)

    (cell + c) shouldBe a[Expr.AddDouble[?]]
    (cell - c) shouldBe a[Expr.SubDouble[?]]
    (cell * c) shouldBe a[Expr.MulDouble[?]]
    (cell / c) shouldBe a[Expr.DivDouble[?]]
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
