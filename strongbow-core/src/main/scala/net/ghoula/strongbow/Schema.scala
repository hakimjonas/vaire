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

  // TODO: Add derivation for case classes using Scala 3 Mirror
}
