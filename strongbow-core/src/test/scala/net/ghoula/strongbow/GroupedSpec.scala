package net.ghoula.strongbow

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.prelude.*

/** Tests for Grouped operations and aggregations. */
class GroupedSpec extends AnyFlatSpec with Matchers {

  // Test data: users with (id, name, age)
  case class User(id: Int, name: String, age: Int)
  given Schema[User] = Schema.derived

  given Schema[Int] = Schema.intSchema
  given Schema[String] = Schema.stringSchema

  "groupBy" should "partition data by key" in {
    val users = Vector(
      User(1, "Alice", 25),
      User(2, "Bob", 30),
      User(3, "Charlie", 25),
      User(4, "Diana", 30)
    )

    val dataset = createDataset(users)
    val grouped: Grouped[Int, User] = dataset.groupBy(_.age)

    // Execute to get key-value pairs
    val result = GroupByInterpreter.execute(grouped)

    // Should have (age, user) pairs
    result should contain theSameElementsAs Vector(
      (25, User(1, "Alice", 25)),
      (30, User(2, "Bob", 30)),
      (25, User(3, "Charlie", 25)),
      (30, User(4, "Diana", 30))
    )
  }

  "reduceByKey" should "combine values for same key" in {
    val dataset = createIntDataset(Vector(1, 2, 3, 4, 5, 6))
    val grouped: Grouped[Boolean, Int] = dataset.groupBy(_ % 2 == 0) // Even/odd

    val reduced = grouped.reduceByKey(_ + _)
    val result = GroupByInterpreter.execute(reduced).toMap

    result(false) shouldBe 9 // 1 + 3 + 5
    result(true) shouldBe 12 // 2 + 4 + 6
  }

  "mapValues" should "transform values without changing keys" in {
    val dataset = createIntDataset(Vector(1, 2, 3, 4, 5))
    val grouped: Grouped[Boolean, Int] = dataset.groupBy(_ % 2 == 0)

    val mapped = grouped.mapValues(_ * 10)
    val result = GroupByInterpreter.execute(mapped)

    result should contain theSameElementsAs Vector(
      (false, 10),
      (false, 30),
      (false, 50),
      (true, 20),
      (true, 40)
    )
  }

  "filterKeys" should "remove groups by predicate" in {
    val dataset = createIntDataset(Vector(1, 2, 3, 4, 5))
    val grouped: Grouped[Int, Int] = dataset.groupBy(x => x)

    val filtered = grouped.filterKeys(_ > 3)
    val result = GroupByInterpreter.execute(filtered)

    result should contain theSameElementsAs Vector(
      (4, 4),
      (5, 5)
    )
  }

  "innerJoin" should "combine matching keys" in {
    val left = createStringDataset(Vector("a", "b", "c"))
    val right = createStringDataset(Vector("b", "c", "d"))

    val groupedLeft: Grouped[String, String] = left.groupBy(identity)
    val groupedRight: Grouped[String, String] = right.groupBy(identity)

    val joined = groupedLeft.join(groupedRight)
    val result = GroupByInterpreter.execute(joined).toSet

    result shouldBe Set(
      ("b", ("b", "b")),
      ("c", ("c", "c"))
    )
  }

  "leftJoin" should "keep all left keys" in {
    val left = createStringDataset(Vector("a", "b"))
    val right = createStringDataset(Vector("b", "c"))

    val groupedLeft: Grouped[String, String] = left.groupBy(identity)
    val groupedRight: Grouped[String, String] = right.groupBy(identity)

    val joined = groupedLeft.leftJoin(groupedRight)
    val result = GroupByInterpreter.execute(joined).toSet

    result shouldBe Set(
      ("a", ("a", None)),
      ("b", ("b", Some("b")))
    )
  }

  "rightJoin" should "keep all right keys" in {
    val left = createStringDataset(Vector("a", "b"))
    val right = createStringDataset(Vector("b", "c"))

    val groupedLeft: Grouped[String, String] = left.groupBy(identity)
    val groupedRight: Grouped[String, String] = right.groupBy(identity)

    val joined = groupedLeft.rightJoin(groupedRight)
    val result = GroupByInterpreter.execute(joined).toSet

    result shouldBe Set(
      ("b", (Some("b"), "b")),
      ("c", (None, "c"))
    )
  }

  "fullJoin" should "keep all keys from both sides" in {
    val left = createStringDataset(Vector("a", "b"))
    val right = createStringDataset(Vector("b", "c"))

    val groupedLeft: Grouped[String, String] = left.groupBy(identity)
    val groupedRight: Grouped[String, String] = right.groupBy(identity)

    val joined = groupedLeft.fullJoin(groupedRight)
    val result = GroupByInterpreter.execute(joined).toSet

    result shouldBe Set(
      ("a", (Some("a"), None)),
      ("b", (Some("b"), Some("b"))),
      ("c", (None, Some("c")))
    )
  }

  "flatMapValues" should "expand values" in {
    val dataset = createIntDataset(Vector(1, 2, 3))
    val grouped: Grouped[Boolean, Int] = dataset.groupBy(_ % 2 == 0)

    // Duplicate each value
    val expanded = grouped.flatMapValues(x => List(x, x))
    val result = GroupByInterpreter.execute(expanded)

    result should contain theSameElementsAs Vector(
      (false, 1),
      (false, 1),
      (true, 2),
      (true, 2),
      (false, 3),
      (false, 3)
    )
  }

  "values" should "extract only values" in {
    val dataset = createIntDataset(Vector(1, 2, 3))
    val grouped: Grouped[Boolean, Int] = dataset.groupBy(_ % 2 == 0)

    val valuesDs = grouped.values
    val result = DatasetInterpreter.execute(valuesDs) match {
      case Right(ds) => ds
      case Left(err) => fail(s"Execution failed: $err")
    }

    result.toVectorUnsafe should contain theSameElementsAs Vector(1, 2, 3)
  }

  "keys" should "extract only keys" in {
    val dataset = createIntDataset(Vector(1, 2, 3, 4))
    val grouped: Grouped[Boolean, Int] = dataset.groupBy(_ % 2 == 0)

    val keysDs = grouped.keys
    val result = DatasetInterpreter.execute(keysDs) match {
      case Right(ds) => ds
      case Left(err) => fail(s"Execution failed: $err")
    }

    result.toVectorUnsafe should contain theSameElementsAs Vector(false, true, false, true)
  }

  "leftAntiJoin" should "exclude matching keys" in {
    val users = createDataset(Vector(
      User(1, "Alice", 25),
      User(2, "Bob", 30),
      User(3, "Charlie", 35)
    ))

    val toExclude = createDataset(Vector(
      User(2, "Bob", 30)
    ))

    val usersGrouped = users.groupBy(_.id)
    val excludeGrouped = toExclude.groupBy(_.id)

    val result = usersGrouped.leftAntiJoin(excludeGrouped)
      .toPairs
      .collect
      .toOption
      .get

    result.map(_._2.name) should contain theSameElementsAs Vector("Alice", "Charlie")
  }

  "sortByKey" should "order by key" in {
    val dataset = createIntDataset(Vector(3, 1, 4, 1, 5))
    val grouped = dataset.groupBy(identity)

    val sorted = grouped.sortByKey.keys.collect.toOption.get

    sorted shouldBe Vector(1, 1, 3, 4, 5)
  }

  "union on Grouped" should "combine key-value pairs" in {
    val g1 = createIntDataset(Vector(1, 2)).groupBy(identity)
    val g2 = createIntDataset(Vector(3, 4)).groupBy(identity)

    val unioned = (g1 ++ g2).keys.collect.toOption.get

    unioned should contain theSameElementsAs Vector(1, 2, 3, 4)
  }

  // Helper methods
  private def createDataset(users: Vector[User]): Dataset[User] = {
    val idCol = Column.int(users.map(_.id).toArray)
    val nameCol = Column.string(users.map(_.name).toArray)
    val ageCol = Column.int(users.map(_.age).toArray)

    Dataset.fromColumns(Vector(idCol, nameCol, ageCol), summon[Schema[User]]) match {
      case Right(ds) => ds
      case Left(errors) => fail(s"Failed to create dataset: ${errors.toList}")
    }
  }

  private def createIntDataset(values: Vector[Int]): Dataset[Int] = {
    val col = Column.int(values.toArray)
    Dataset.fromColumns(Vector(col), Schema.intSchema) match {
      case Right(ds) => ds
      case Left(errors) => fail(s"Failed to create dataset: ${errors.toList}")
    }
  }

  private def createStringDataset(values: Vector[String]): Dataset[String] = {
    val col = Column.string(values.toArray)
    Dataset.fromColumns(Vector(col), Schema.stringSchema) match {
      case Right(ds) => ds
      case Left(errors) => fail(s"Failed to create dataset: ${errors.toList}")
    }
  }
}
