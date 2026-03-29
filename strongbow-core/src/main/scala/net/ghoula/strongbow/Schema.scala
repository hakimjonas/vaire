package net.ghoula.strongbow

import scala.annotation.tailrec
import scala.deriving.Mirror
import scala.quoted.{Expr, Quotes, Type}

import net.ghoula.strongbow.column.ColumnType
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

  private def decodeSingle[T](
    typeName: String
  )(dec: PartialFunction[Any, T])(values: Vector[Any]): Either[DecodeError, T] =
    if (values.length != 1) Left(DecodeError.WrongArity(1, values.length))
    else dec.lift(values.head).toRight(DecodeError.TypeMismatch(typeName, values.head.getClass.getSimpleName))

  given intSchema: Schema[Int] with {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.IntType)
    def encode(value: Int): Vector[Any] = Vector(value)
    def decode(values: Vector[Any]): Either[DecodeError, Int] = decodeSingle("Int") { case i: Int => i }(values)
  }

  given stringSchema: Schema[String] with {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.StringType)
    def encode(value: String): Vector[Any] = Vector(value)
    def decode(values: Vector[Any]): Either[DecodeError, String] =
      decodeSingle("String") { case s: String => s }(values)
  }

  given longSchema: Schema[Long] with {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.LongType)
    def encode(value: Long): Vector[Any] = Vector(value)
    def decode(values: Vector[Any]): Either[DecodeError, Long] = decodeSingle("Long") { case l: Long => l }(values)
  }

  given doubleSchema: Schema[Double] with {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.DoubleType)
    def encode(value: Double): Vector[Any] = Vector(value)
    def decode(values: Vector[Any]): Either[DecodeError, Double] =
      decodeSingle("Double") { case d: Double => d }(values)
  }

  given dateSchema: Schema[types.Date] with {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.DateType)
    def encode(value: types.Date): Vector[Any] = Vector(value.toLocalDate)
    def decode(values: Vector[Any]): Either[DecodeError, types.Date] = decodeSingle("Date") {
      case d: java.time.LocalDate => types.Date.fromLocalDate(d)
      case i: Int => types.Date.ofEpochDay(i.toLong)
    }(values)
  }

  given booleanSchema: Schema[Boolean] with {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.BooleanType)
    def encode(value: Boolean): Vector[Any] = Vector(value)
    def decode(values: Vector[Any]): Either[DecodeError, Boolean] =
      decodeSingle("Boolean") { case b: Boolean => b }(values)
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

  /** Generic tuple schema for tuples of arity 3+.
    *
    * Uses Schema.derived macro to auto-generate schema for any Tuple type. Scala 3 tuples are
    * product types with Mirror.ProductOf, so the derivation macro handles field access via _1, _2,
    * etc. automatically.
    *
    * tuple2Schema takes priority for Tuple2 (more specific match). This covers Tuple3 through
    * Tuple22.
    */
  inline given derivedTupleSchema[T <: Tuple](using mirror: Mirror.ProductOf[T]): Schema[T] = Schema.derived

  /** Schema for Option[A] - nullable columns with presence bit */
  given optionSchema[A](using inner: Schema[A]): Schema[Option[A]] with {
    def columnCount: Int = inner.columnCount + 1

    def columnNames: Vector[String] =
      Vector("_isDefined") ++ inner.columnNames.map("_value_" + _)

    def columnTypes: Vector[ColumnType] =
      Vector(ColumnType.BooleanType) ++ inner.columnTypes

    def encode(value: Option[A]): Vector[Any] = value match {
      case Some(a) => Vector(true) ++ inner.encode(a)
      case None => Vector(false) ++ Vector.fill(inner.columnCount)(null) // scalafix:ok DisableSyntax.null
    }

    def decode(values: Vector[Any]): Either[DecodeError, Option[A]] = {
      val expectedCount = columnCount
      if (values.length != expectedCount) {
        Left(DecodeError.WrongArity(expectedCount, values.length))
      } else {
        values.head match {
          case b: Boolean =>
            if (b) inner.decode(values.tail).map(Some(_))
            else Right(None)
          case other => Left(DecodeError.TypeMismatch("Boolean", other.getClass.getSimpleName))
        }
      }
    }
  }

  /** Automatic schema derivation for case classes using Scala 3 Mirror.
    *
    * Usage:
    * {{{
    * case class User(id: Int, name: String, age: Int)
    * given Schema[User] = Schema.derived
    * }}}
    */
  inline def derived[T](using mirror: Mirror.ProductOf[T]): Schema[T] = ${
    deriveSchemaImpl[T, mirror.MirroredElemTypes, mirror.MirroredElemLabels]('mirror)
  }

  private def getLabels[Labels <: Tuple: Type](using q: Quotes): List[String] = {
    import q.reflect.*

    def extract[L <: Tuple: Type]: List[String] = Type.of[L] match {
      case '[EmptyTuple] => Nil
      case '[label *: rest] =>
        Type.of[label] match {
          case '[l] =>
            TypeRepr.of[l] match {
              case ConstantType(StringConstant(s)) => s :: extract[rest]
              case other =>
                report.errorAndAbort(
                  s"Invalid field label type: expected string literal, found ${other.show}",
                  Position.ofMacroExpansion
                )
            }
        }
    }

    extract[Labels]
  }

  private def fieldAccess[T: Type, H: Type](
    aExpr: Expr[T],
    label: String,
    index: Int
  )(using q: Quotes): Expr[H] = {
    import q.reflect.*

    val isRegularTuple = TypeRepr.of[T] <:< TypeRepr.of[Tuple]

    if (isRegularTuple) {
      Select.unique(aExpr.asTerm, s"_${index + 1}").asExprOf[H]
    } else {
      val typeSymbol = TypeRepr.of[T].typeSymbol
      val fieldMember = typeSymbol.fieldMember(label)
      val hasFieldMember = !fieldMember.isNoSymbol

      if (hasFieldMember) {
        Select.unique(aExpr.asTerm, label).asExprOf[H]
      } else {
        val indexExpr = Expr(index)
        '{
          $aExpr.asInstanceOf[Product].productElement($indexExpr).asInstanceOf[H]
        } // scalafix:ok DisableSyntax.asInstanceOf
      }
    }
  }

  private def findMissingSchemas[Elems <: Tuple: Type](
    labels: List[String]
  )(using q: Quotes): List[(String, String)] = {

    @tailrec
    def collect[E <: Tuple: Type](
      remainingLabels: List[String],
      acc: List[(String, String)]
    ): List[(String, String)] =
      Type.of[E] match {
        case '[EmptyTuple] => acc.reverse
        case '[h *: t] =>
          val label = remainingLabels.head
          val fieldTypeStr = Type.show[h]
          val hasSchema = Expr.summon[Schema[h]].isDefined
          val newAcc = if (hasSchema) acc else (label, fieldTypeStr) :: acc
          collect[t](remainingLabels.tail, newAcc)
      }

    collect[Elems](labels, Nil)
  }

  private def deriveSchemaImpl[T: Type, Elems <: Tuple: Type, Labels <: Tuple: Type](
    m: Expr[Mirror.ProductOf[T]]
  )(using q: Quotes): Expr[Schema[T]] = {
    import q.reflect.*

    val fieldLabels = getLabels[Labels]

    val missing = findMissingSchemas[Elems](fieldLabels)
    if (missing.nonEmpty) {
      val header =
        s"Cannot derive Schema for ${Type.show[T]}: missing schemas for ${missing.length} field(s).\n"
      val details = missing.zipWithIndex.map { case ((name, tpe), i) =>
        s"  ${i + 1}. Field '$name' of type $tpe\n" +
          s"     Add: given Schema[$tpe] = ..."
      }.mkString("\n\n")

      report.errorAndAbort(header + "\n" + details, Position.ofMacroExpansion)
    }

    def summonSchemas[E <: Tuple: Type]: List[Expr[Schema[?]]] =
      Type.of[E] match {
        case '[EmptyTuple] => Nil
        case '[h *: t] =>
          val schema = Expr.summon[Schema[h]].get
          schema :: summonSchemas[t]
      }

    val schemas = summonSchemas[Elems]

    val columnCountExpr = schemas.foldLeft[Expr[Int]]('{ 0 }) { (acc, schema) =>
      '{ $acc + $schema.columnCount }
    }

    val columnNamesExpr = {
      val nameExprs = fieldLabels.zip(schemas).map { case (label, schema) =>
        val labelExpr = Expr(label)
        '{ $schema.columnNames.map(name => ${ labelExpr }.toString + "_" + name) }
      }
      nameExprs.foldLeft[Expr[Vector[String]]]('{ Vector.empty }) { (acc, names) =>
        '{ $acc ++ $names }
      }
    }

    val columnTypesExpr = schemas.foldLeft[Expr[Vector[ColumnType]]]('{ Vector.empty }) { (acc, schema) =>
      '{ $acc ++ $schema.columnTypes }
    }

    def generateEncode[E <: Tuple: Type](
      valueExpr: Expr[T],
      index: Int,
      labels: List[String],
      schemas: List[Expr[Schema[?]]]
    ): Expr[Vector[Any]] =
      Type.of[E] match {
        case '[EmptyTuple] => '{ Vector.empty }
        case '[h *: t] =>
          val label = labels.head
          val schema = schemas.head.asExprOf[Schema[h]]
          val fieldExpr = fieldAccess[T, h](valueExpr, label, index)
          val restExpr = generateEncode[t](valueExpr, index + 1, labels.tail, schemas.tail)
          '{ $schema.encode($fieldExpr) ++ $restExpr }
      }

    def generateDecode[E <: Tuple: Type](
      valuesExpr: Expr[Vector[Any]],
      schemas: List[Expr[Schema[?]]]
    ): Expr[Either[DecodeError, List[Any]]] =
      Type.of[E] match {
        case '[EmptyTuple] => '{ Right(Nil) }
        case '[h *: t] =>
          val schema = schemas.head.asExprOf[Schema[h]]
          '{
            val (headValues, tailValues) = $valuesExpr.splitAt($schema.columnCount)
            for {
              head <- $schema.decode(headValues)
              tail <- ${ generateDecode[t]('tailValues, schemas.tail) }
            } yield head :: tail
          }
      }

    '{
      new Schema[T] {
        def columnCount: Int = $columnCountExpr
        def columnNames: Vector[String] = $columnNamesExpr
        def columnTypes: Vector[ColumnType] = $columnTypesExpr

        def encode(value: T): Vector[Any] = ${
          generateEncode[Elems]('value, 0, fieldLabels, schemas)
        }

        def decode(values: Vector[Any]): Either[DecodeError, T] = {
          if (values.length != columnCount) {
            Left(DecodeError.WrongArity(columnCount, values.length))
          } else {
            ${ generateDecode[Elems]('values, schemas) }.map { decodedFields =>
              $m.fromProduct(Tuple.fromArray(decodedFields.toArray))
            }
          }
        }
      }
    }
  }
}
