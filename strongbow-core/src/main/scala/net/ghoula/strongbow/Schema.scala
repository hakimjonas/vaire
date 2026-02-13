package net.ghoula.strongbow

import net.ghoula.strongbow.errors.DecodeError

/** Type-level schema representation.
  *
  * Schema[T] provides evidence about the structure of type T, enabling compile-time validation and
  * runtime codec derivation.
  */
trait Schema[T] {
  def columnCount: Int
  def columnNames: Vector[String]
  def columnTypes: Vector[ColumnType]

  /** Encode a value of type T to a vector of column values. */
  def encode(value: T): Vector[Any]

  /** Decode a vector of column values to type T. */
  def decode(values: Vector[Any]): Either[DecodeError, T]
}

object Schema {
  def apply[T](using schema: Schema[T]): Schema[T] = schema

  /** Simple schema for primitive types. */
  given intSchema: Schema[Int] with {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.IntType)
    def encode(value: Int): Vector[Any] = Vector(value)
    def decode(values: Vector[Any]): Either[DecodeError, Int] = {
      if (values.length != 1) {
        Left(DecodeError.WrongArity(1, values.length))
      } else {
        values.head match {
          case i: Int => Right(i)
          case other => Left(DecodeError.TypeMismatch("Int", other.getClass.getSimpleName))
        }
      }
    }
  }

  given stringSchema: Schema[String] with {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.StringType)
    def encode(value: String): Vector[Any] = Vector(value)
    def decode(values: Vector[Any]): Either[DecodeError, String] = {
      if (values.length != 1) {
        Left(DecodeError.WrongArity(1, values.length))
      } else {
        values.head match {
          case s: String => Right(s)
          case other => Left(DecodeError.TypeMismatch("String", other.getClass.getSimpleName))
        }
      }
    }
  }

  given longSchema: Schema[Long] with {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.LongType)
    def encode(value: Long): Vector[Any] = Vector(value)
    def decode(values: Vector[Any]): Either[DecodeError, Long] = {
      if (values.length != 1) {
        Left(DecodeError.WrongArity(1, values.length))
      } else {
        values.head match {
          case l: Long => Right(l)
          case other => Left(DecodeError.TypeMismatch("Long", other.getClass.getSimpleName))
        }
      }
    }
  }

  given doubleSchema: Schema[Double] with {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.DoubleType)
    def encode(value: Double): Vector[Any] = Vector(value)
    def decode(values: Vector[Any]): Either[DecodeError, Double] = {
      if (values.length != 1) {
        Left(DecodeError.WrongArity(1, values.length))
      } else {
        values.head match {
          case d: Double => Right(d)
          case other => Left(DecodeError.TypeMismatch("Double", other.getClass.getSimpleName))
        }
      }
    }
  }

  given booleanSchema: Schema[Boolean] with {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.BooleanType)
    def encode(value: Boolean): Vector[Any] = Vector(value)
    def decode(values: Vector[Any]): Either[DecodeError, Boolean] = {
      if (values.length != 1) {
        Left(DecodeError.WrongArity(1, values.length))
      } else {
        values.head match {
          case b: Boolean => Right(b)
          case other => Left(DecodeError.TypeMismatch("Boolean", other.getClass.getSimpleName))
        }
      }
    }
  }

  /** Generic tuple schema for pairs - enables automatic Schema[(A, B)] derivation */
  given tuple2Schema[A, B](using schemaA: Schema[A], schemaB: Schema[B]): Schema[(A, B)] with {
    def columnCount: Int = schemaA.columnCount + schemaB.columnCount

    def columnNames: Vector[String] =
      schemaA.columnNames.map("_1_" + _) ++ schemaB.columnNames.map("_2_" + _)

    def columnTypes: Vector[ColumnType] =
      schemaA.columnTypes ++ schemaB.columnTypes

    def encode(value: (A, B)): Vector[Any] =
      schemaA.encode(value._1) ++ schemaB.encode(value._2)

    def decode(values: Vector[Any]): Either[DecodeError, (A, B)] = {
      val expectedCount = columnCount
      if (values.length != expectedCount) {
        Left(DecodeError.WrongArity(expectedCount, values.length))
      } else {
        val (aValues, bValues) = values.splitAt(schemaA.columnCount)
        for {
          a <- schemaA.decode(aValues)
          b <- schemaB.decode(bValues)
        } yield (a, b)
      }
    }
  }

  // TODO: Add derivation for case classes using Scala 3 Mirror
}
