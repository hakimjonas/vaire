package net.ghoula.strongbow

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.prelude.*

class DatasetJoinSpec extends AnyFlatSpec with Matchers {

  case class Employee(id: Int, name: String, deptId: Int)
  case class Department(id: Int, deptName: String)

  // Manual schemas for test data
  given Schema[Employee] with {
    def columnCount: Int = 3
    def columnNames: Vector[String] = Vector("id", "name", "deptId")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.IntType, ColumnType.StringType, ColumnType.IntType)
    def encode(value: Employee): Vector[Any] = Vector(value.id, value.name, value.deptId)
    def decode(values: Vector[Any]): Either[errors.DecodeError, Employee] = {
      if (values.length != 3) {
        Left(errors.DecodeError.WrongArity(3, values.length))
      } else {
        Right(
          Employee(values(0).asInstanceOf[Int], values(1).asInstanceOf[String], values(2).asInstanceOf[Int])
        ) // scalafix:ok DisableSyntax.asInstanceOf
      }
    }
  }

  given Schema[Department] with {
    def columnCount: Int = 2
    def columnNames: Vector[String] = Vector("id", "deptName")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.IntType, ColumnType.StringType)
    def encode(value: Department): Vector[Any] = Vector(value.id, value.deptName)
    def decode(values: Vector[Any]): Either[errors.DecodeError, Department] = {
      if (values.length != 2) {
        Left(errors.DecodeError.WrongArity(2, values.length))
      } else {
        Right(
          Department(values(0).asInstanceOf[Int], values(1).asInstanceOf[String])
        ) // scalafix:ok DisableSyntax.asInstanceOf
      }
    }
  }

  "inner join" should "return only matching rows" in {
    val employees = createDataset(
      Vector(
        Employee(1, "Alice", 10),
        Employee(2, "Bob", 20),
        Employee(3, "Charlie", 10),
        Employee(4, "Dave", 30)
      )
    )

    val departments = createDataset(
      Vector(
        Department(10, "Engineering"),
        Department(20, "Sales")
      )
    )

    val joined = employees.join(departments, (e, d) => e.deptId == d.id)
    val result = joined.collect.getOrElse(fail("Join failed"))

    result should have length 3
    result should contain((Employee(1, "Alice", 10), Department(10, "Engineering")))
    result should contain((Employee(2, "Bob", 20), Department(20, "Sales")))
    result should contain((Employee(3, "Charlie", 10), Department(10, "Engineering")))
  }

  "inner join" should "handle arbitrary predicates" in {
    val nums1 = createDataset(Vector(1, 2, 3, 4))
    val nums2 = createDataset(Vector(2, 3, 5))

    val joined = nums1.join(nums2, (a, b) => a <= b)
    val result = joined.collect.getOrElse(fail("Join failed"))

    result should have length 9
    result should contain((1, 2))
    result should contain((1, 3))
    result should contain((1, 5))
    result should contain((2, 2))
    result should contain((2, 3))
    result should contain((2, 5))
    result should contain((3, 3))
    result should contain((3, 5))
    result should contain((4, 5))
  }

  "left join" should "include all left rows with None for unmatched" in {
    val employees = createDataset(
      Vector(
        Employee(1, "Alice", 10),
        Employee(2, "Bob", 20),
        Employee(3, "Charlie", 30)
      )
    )

    val departments = createDataset(
      Vector(
        Department(10, "Engineering"),
        Department(20, "Sales")
      )
    )

    val joined = employees.leftJoin(departments, (e, d) => e.deptId == d.id)
    val result = joined.collect.getOrElse(fail("Left join failed"))

    result should have length 3
    result should contain((Employee(1, "Alice", 10), Some(Department(10, "Engineering"))))
    result should contain((Employee(2, "Bob", 20), Some(Department(20, "Sales"))))

    // Charlie has deptId 30 which doesn't exist, should have None
    val charlieRow = result.find(_._1.name == "Charlie")
    charlieRow shouldBe defined
    charlieRow.get._2 shouldBe None
  }

  "right join" should "include all right rows with None for unmatched" in {
    val employees = createDataset(
      Vector(
        Employee(1, "Alice", 10),
        Employee(2, "Bob", 20)
      )
    )

    val departments = createDataset(
      Vector(
        Department(10, "Engineering"),
        Department(20, "Sales"),
        Department(30, "Marketing")
      )
    )

    val joined = employees.rightJoin(departments, (e, d) => e.deptId == d.id)
    val result = joined.collect.getOrElse(fail("Right join failed"))

    result should have length 3
    result should contain((Some(Employee(1, "Alice", 10)), Department(10, "Engineering")))
    result should contain((Some(Employee(2, "Bob", 20)), Department(20, "Sales")))

    // Marketing has no employees, should have None
    val marketingRow = result.find(_._2.deptName == "Marketing")
    marketingRow shouldBe defined
    marketingRow.get._1 shouldBe None
  }

  "full join" should "include all rows with None for unmatched" in {
    val employees = createDataset(
      Vector(
        Employee(1, "Alice", 10),
        Employee(2, "Bob", 20),
        Employee(3, "Charlie", 40)
      )
    )

    val departments = createDataset(
      Vector(
        Department(10, "Engineering"),
        Department(30, "Marketing")
      )
    )

    val joined = employees.fullJoin(departments, (e, d) => e.deptId == d.id)
    val result = joined.collect.getOrElse(fail("Full join failed"))

    result should have length 4

    // Matched row
    result should contain((Some(Employee(1, "Alice", 10)), Some(Department(10, "Engineering"))))

    // Employee with no matching department
    val bobRow = result.find(r => r._1.exists(_.name == "Bob"))
    bobRow shouldBe defined
    bobRow.get._2 shouldBe None

    val charlieRow = result.find(r => r._1.exists(_.name == "Charlie"))
    charlieRow shouldBe defined
    charlieRow.get._2 shouldBe None

    // Department with no matching employee
    val marketingRow = result.find(r => r._2.exists(_.deptName == "Marketing"))
    marketingRow shouldBe defined
    marketingRow.get._1 shouldBe None
  }

  "anti join" should "return left rows with no match in right" in {
    val employees = createDataset(
      Vector(
        Employee(1, "Alice", 10),
        Employee(2, "Bob", 20),
        Employee(3, "Charlie", 30),
        Employee(4, "Dave", 40)
      )
    )

    val departments = createDataset(
      Vector(
        Department(10, "Engineering"),
        Department(20, "Sales")
      )
    )

    val joined = employees.antiJoin(departments, (e, d) => e.deptId == d.id)
    val result = joined.collect.getOrElse(fail("Anti join failed"))

    result should have length 2
    result should contain(Employee(3, "Charlie", 30))
    result should contain(Employee(4, "Dave", 40))
  }

  "join" should "handle empty datasets" in {
    val emptyEmployees = createDataset[Employee](Vector.empty)
    val departments = createDataset(Vector(Department(10, "Engineering")))

    val joined = emptyEmployees.join(departments, (e, d) => e.deptId == d.id)
    val result = joined.collect.getOrElse(fail("Join with empty left failed"))

    result shouldBe empty
  }

  "join" should "work with empty right dataset" in {
    val employees = createDataset(Vector(Employee(1, "Alice", 10)))
    val emptyDepartments = createDataset[Department](Vector.empty)

    val joined = employees.join(emptyDepartments, (e, d) => e.deptId == d.id)
    val result = joined.collect.getOrElse(fail("Join with empty right failed"))

    result shouldBe empty
  }

  // --- Expression-based join tests ---

  val empDeptIdExpr: Expr[Employee, Int] = Expr.Cell[Employee, Int]("deptId", types.ColumnIndex(2))
  val deptIdExpr: Expr[Department, Int] = Expr.Cell[Department, Int]("id", types.ColumnIndex(0))

  "expression-based inner join" should "produce same results as lambda-based" in {
    val employees = createDataset(
      Vector(
        Employee(1, "Alice", 10),
        Employee(2, "Bob", 20),
        Employee(3, "Charlie", 10),
        Employee(4, "Dave", 30)
      )
    )

    val departments = createDataset(
      Vector(
        Department(10, "Engineering"),
        Department(20, "Sales")
      )
    )

    val joined = employees.joinOn(departments, empDeptIdExpr, deptIdExpr, ColumnType.IntType, ColumnType.IntType)
    val result = joined.collect.getOrElse(fail("Expression join failed"))

    result should have length 3
    result should contain((Employee(1, "Alice", 10), Department(10, "Engineering")))
    result should contain((Employee(2, "Bob", 20), Department(20, "Sales")))
    result should contain((Employee(3, "Charlie", 10), Department(10, "Engineering")))
  }

  "expression-based left join" should "include unmatched left rows" in {
    val employees = createDataset(
      Vector(
        Employee(1, "Alice", 10),
        Employee(2, "Bob", 20),
        Employee(3, "Charlie", 30)
      )
    )

    val departments = createDataset(
      Vector(
        Department(10, "Engineering"),
        Department(20, "Sales")
      )
    )

    val joined = employees.leftJoinOn(departments, empDeptIdExpr, deptIdExpr, ColumnType.IntType, ColumnType.IntType)
    val result = joined.collect.getOrElse(fail("Expression left join failed"))

    result should have length 3
    result should contain((Employee(1, "Alice", 10), Some(Department(10, "Engineering"))))
    result should contain((Employee(2, "Bob", 20), Some(Department(20, "Sales"))))

    val charlieRow = result.find(_._1.name == "Charlie")
    charlieRow shouldBe defined
    charlieRow.get._2 shouldBe None
  }

  "expression-based right join" should "include unmatched right rows" in {
    val employees = createDataset(
      Vector(
        Employee(1, "Alice", 10),
        Employee(2, "Bob", 20)
      )
    )

    val departments = createDataset(
      Vector(
        Department(10, "Engineering"),
        Department(20, "Sales"),
        Department(30, "Marketing")
      )
    )

    val joined = employees.rightJoinOn(departments, empDeptIdExpr, deptIdExpr, ColumnType.IntType, ColumnType.IntType)
    val result = joined.collect.getOrElse(fail("Expression right join failed"))

    result should have length 3
    result should contain((Some(Employee(1, "Alice", 10)), Department(10, "Engineering")))
    result should contain((Some(Employee(2, "Bob", 20)), Department(20, "Sales")))

    val marketingRow = result.find(_._2.deptName == "Marketing")
    marketingRow shouldBe defined
    marketingRow.get._1 shouldBe None
  }

  "expression-based full join" should "include all rows" in {
    val employees = createDataset(
      Vector(
        Employee(1, "Alice", 10),
        Employee(2, "Bob", 20),
        Employee(3, "Charlie", 40)
      )
    )

    val departments = createDataset(
      Vector(
        Department(10, "Engineering"),
        Department(30, "Marketing")
      )
    )

    val joined = employees.fullJoinOn(departments, empDeptIdExpr, deptIdExpr, ColumnType.IntType, ColumnType.IntType)
    val result = joined.collect.getOrElse(fail("Expression full join failed"))

    result should have length 4

    result should contain((Some(Employee(1, "Alice", 10)), Some(Department(10, "Engineering"))))

    val bobRow = result.find(r => r._1.exists(_.name == "Bob"))
    bobRow shouldBe defined
    bobRow.get._2 shouldBe None

    val charlieRow = result.find(r => r._1.exists(_.name == "Charlie"))
    charlieRow shouldBe defined
    charlieRow.get._2 shouldBe None

    val marketingRow = result.find(r => r._2.exists(_.deptName == "Marketing"))
    marketingRow shouldBe defined
    marketingRow.get._1 shouldBe None
  }

  "expression-based anti join" should "exclude matched rows" in {
    val employees = createDataset(
      Vector(
        Employee(1, "Alice", 10),
        Employee(2, "Bob", 20),
        Employee(3, "Charlie", 30),
        Employee(4, "Dave", 40)
      )
    )

    val departments = createDataset(
      Vector(
        Department(10, "Engineering"),
        Department(20, "Sales")
      )
    )

    val joined = employees.antiJoinOn(departments, empDeptIdExpr, deptIdExpr, ColumnType.IntType, ColumnType.IntType)
    val result = joined.collect.getOrElse(fail("Expression anti join failed"))

    result should have length 2
    result should contain(Employee(3, "Charlie", 30))
    result should contain(Employee(4, "Dave", 40))
  }

  "expression-based join" should "handle empty datasets" in {
    val emptyEmployees = createDataset[Employee](Vector.empty)
    val departments = createDataset(Vector(Department(10, "Engineering")))

    val joined = emptyEmployees.joinOn(departments, empDeptIdExpr, deptIdExpr, ColumnType.IntType, ColumnType.IntType)
    val result = joined.collect.getOrElse(fail("Join with empty left failed"))

    result shouldBe empty
  }

  private def createDataset[T](values: Vector[T])(using schema: Schema[T]): Dataset[T] = {
    MaterializedDataset.fromVector(values) match {
      case Right(mat) =>
        // Convert MaterializedDataset to Dataset
        Dataset.Root(mat.columns, schema)
      case Left(err) => fail(s"Dataset creation failed: $err")
    }
  }
}
