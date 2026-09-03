package net.ghoula.vaire

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.language.strictEquality

import net.ghoula.vaire.prelude.*

// Allow universal equality in tests
given CanEqual[Any, Any] = CanEqual.derived

class SchemaDerivedSpec extends AnyFlatSpec with Matchers {

  "Schema.derived" should "work for simple case class" in {
    case class Person(name: String, age: Int)
    given Schema[Person] = Schema.derived

    val schema = summon[Schema[Person]]

    schema.columnCount shouldBe 2
    schema.columnNames should contain("name_value")
    schema.columnNames should contain("age_value")
    schema.columnTypes shouldBe Vector(ColumnType.StringType, ColumnType.IntType)
  }

  "Schema.derived" should "encode case class correctly" in {
    case class Person(name: String, age: Int)
    given Schema[Person] = Schema.derived

    val schema = summon[Schema[Person]]
    val person = Person("Alice", 30)

    val encoded = schema.encode(person)

    encoded.length shouldBe 2
    assert(encoded(0) == "Alice")
    assert(encoded(1) == 30)
  }

  "Schema.derived" should "decode case class correctly" in {
    case class Person(name: String, age: Int)
    given Schema[Person] = Schema.derived

    val schema = summon[Schema[Person]]
    val values = Vector("Alice", 30)

    val decoded = schema.decode(values)

    decoded match {
      case Right(person) =>
        person.name shouldBe "Alice"
        person.age shouldBe 30
      case Left(err) => fail(s"Decode failed: $err")
    }
  }

  "Schema.derived" should "round-trip encode/decode" in {
    case class User(id: Int, name: String, email: String, active: Boolean)
    given Schema[User] = Schema.derived

    val schema = summon[Schema[User]]
    val original = User(123, "Bob", "bob@example.com", true)

    val encoded = schema.encode(original)
    val decoded = schema.decode(encoded)

    decoded match {
      case Right(user) =>
        user.id shouldBe original.id
        user.name shouldBe original.name
        user.email shouldBe original.email
        user.active shouldBe original.active
      case Left(err) => fail(s"Decode failed: $err")
    }
  }

  "Schema.derived" should "work with nested products" in {
    case class Address(street: String, city: String)
    case class Person(name: String, address: Address)

    given Schema[Address] = Schema.derived
    given Schema[Person] = Schema.derived

    val schema = summon[Schema[Person]]

    schema.columnCount shouldBe 3 // name + street + city
    schema.columnNames should have length 3

    val person = Person("Alice", Address("123 Main St", "Boston"))
    val encoded = schema.encode(person)
    encoded.length shouldBe 3
    assert(encoded(0) == "Alice")
    assert(encoded(1) == "123 Main St")
    assert(encoded(2) == "Boston")

    val decoded = schema.decode(encoded)
    decoded match {
      case Right(p) =>
        p.name shouldBe person.name
        p.address.street shouldBe person.address.street
        p.address.city shouldBe person.address.city
      case Left(err) => fail(s"Decode failed: $err")
    }
  }

  "Schema.derived" should "work with multiple fields" in {
    case class Record(
      a: Int,
      b: String,
      c: Double,
      d: Long,
      e: Boolean
    )
    given Schema[Record] = Schema.derived

    val schema = summon[Schema[Record]]

    schema.columnCount shouldBe 5
    schema.columnTypes shouldBe Vector(
      ColumnType.IntType,
      ColumnType.StringType,
      ColumnType.DoubleType,
      ColumnType.LongType,
      ColumnType.BooleanType
    )

    val record = Record(1, "test", 3.14, 999L, false)
    val encoded = schema.encode(record)
    val decoded = schema.decode(encoded)

    decoded match {
      case Right(r) =>
        r.a shouldBe record.a
        r.b shouldBe record.b
        r.c shouldBe record.c
        r.d shouldBe record.d
        r.e shouldBe record.e
      case Left(err) => fail(s"Decode failed: $err")
    }
  }

  "Schema.derived" should "detect wrong arity on decode" in {
    case class Person(name: String, age: Int)
    given Schema[Person] = Schema.derived

    val schema = summon[Schema[Person]]
    val values = Vector("Alice", 30, 999) // Too many values

    val decoded = schema.decode(values)

    decoded match {
      case Left(DecodeError.WrongArity(expected, actual)) =>
        expected shouldBe 2
        actual shouldBe 3
      case other => fail(s"Expected WrongArity error, got: $other")
    }
  }

  "Schema.derived" should "detect wrong arity with too few values" in {
    case class Person(name: String, age: Int)
    given Schema[Person] = Schema.derived

    val schema = summon[Schema[Person]]
    val values = Vector("Alice") // Too few values

    val decoded = schema.decode(values)

    decoded match {
      case Left(DecodeError.WrongArity(expected, actual)) =>
        expected shouldBe 2
        actual shouldBe 1
      case other => fail(s"Expected WrongArity error, got: $other")
    }
  }

  "Schema.derived" should "work with tuple types" in {
    given Schema[(Int, String)] = Schema.derived

    val schema = summon[Schema[(Int, String)]]

    schema.columnCount shouldBe 2
    schema.columnTypes shouldBe Vector(ColumnType.IntType, ColumnType.StringType)

    val tuple = (42, "hello")
    val encoded = schema.encode(tuple)
    val decoded = schema.decode(encoded)

    decoded shouldBe Right(tuple)
  }

  "Schema.derivedTupleSchema" should "auto-derive for Tuple3" in {
    // No explicit Schema given needed - derivedTupleSchema handles it
    val schema = summon[Schema[(Int, String, Double)]]

    schema.columnCount shouldBe 3
    schema.columnTypes shouldBe Vector(ColumnType.IntType, ColumnType.StringType, ColumnType.DoubleType)

    val tuple = (42, "hello", 3.14)
    val encoded = schema.encode(tuple)
    val decoded = schema.decode(encoded)

    decoded shouldBe Right(tuple)
  }

  "Schema.derivedTupleSchema" should "auto-derive for Tuple4" in {
    val schema = summon[Schema[(Int, String, Double, Boolean)]]

    schema.columnCount shouldBe 4

    val tuple = (1, "test", 2.5, true)
    val encoded = schema.encode(tuple)
    val decoded = schema.decode(encoded)

    decoded shouldBe Right(tuple)
  }

  "Schema.derivedTupleSchema" should "auto-derive for Tuple5" in {
    val schema = summon[Schema[(Int, String, Double, Long, Boolean)]]

    schema.columnCount shouldBe 5

    val tuple = (1, "test", 2.5, 100L, false)
    val encoded = schema.encode(tuple)
    val decoded = schema.decode(encoded)

    decoded shouldBe Right(tuple)
  }

  "Schema.derived" should "integrate with Dataset" in {
    case class User(id: Int, name: String, age: Int)
    given Schema[User] = Schema.derived

    val users = Vector(
      User(1, "Alice", 30),
      User(2, "Bob", 25),
      User(3, "Charlie", 35)
    )

    val dataset = MaterializedDataset.fromVector(users) match {
      case Right(mat) => Dataset.Root(InMemorySource(mat.columns), summon[Schema[User]])
      case Left(err) => fail(s"Dataset creation failed: $err")
    }

    val collected = dataset.collect.getOrElse(fail("Collect failed"))

    collected.length shouldBe users.length
    collected should contain theSameElementsAs users
  }

  "Schema.derived" should "work with filtering" in {
    case class Product(id: Int, name: String, price: Double)
    given Schema[Product] = Schema.derived

    val products = Vector(
      Product(1, "Widget", 9.99),
      Product(2, "Gadget", 19.99),
      Product(3, "Doohickey", 5.99)
    )

    val dataset = MaterializedDataset.fromVector(products) match {
      case Right(mat) => Dataset.Root(InMemorySource(mat.columns), summon[Schema[Product]])
      case Left(err) => fail(s"Dataset creation failed: $err")
    }

    val filtered = dataset.map(p => p.price > 10.0)
    val result = filtered.collect.getOrElse(fail("Filter failed"))

    result.should(have.length(3))
    result.should(contain(true))
    result.should(contain(false))
  }
}
