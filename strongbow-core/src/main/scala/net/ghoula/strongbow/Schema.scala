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

  /** Sub-schemas for struct-typed columns, indexed by column position.
    *
    * Flat schemas flatten nested case classes and need no entries. Schemas that expose a StructType
    * column (e.g. Schema.structColumn) provide the nested schema so Spark Rows can be rebuilt into
    * typed struct columns.
    */
  def nestedSchemas: Vector[Option[Schema[?]]] = Vector.fill(columnCount)(None)

  /** Encode a value whose runtime type is T.
    *
    * Erasure boundary: the Schema contract exchanges Vector[Any], so the type parameter cannot be
    * recovered by pattern matching — the same sanctioned boundary as AnyColumn storage access.
    */
  def encodeAny(value: Any): Vector[Any] =
    encode(value.asInstanceOf[T]) // scalafix:ok DisableSyntax.asInstanceOf
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

  given floatSchema: Schema[Float] with {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.FloatType)
    def encode(value: Float): Vector[Any] = Vector(value)
    def decode(values: Vector[Any]): Either[DecodeError, Float] =
      decodeSingle("Float") { case f: Float => f }(values)
  }

  given shortSchema: Schema[Short] with {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.ShortType)
    def encode(value: Short): Vector[Any] = Vector(value)
    def decode(values: Vector[Any]): Either[DecodeError, Short] =
      decodeSingle("Short") { case s: Short => s }(values)
  }

  given byteSchema: Schema[Byte] with {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.ByteType)
    def encode(value: Byte): Vector[Any] = Vector(value)
    def decode(values: Vector[Any]): Either[DecodeError, Byte] =
      decodeSingle("Byte") { case b: Byte => b }(values)
  }

  given timestampSchema: Schema[types.Timestamp] with {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.TimestampType)
    def encode(value: types.Timestamp): Vector[Any] = Vector(value.toEpochMicro)
    def decode(values: Vector[Any]): Either[DecodeError, types.Timestamp] =
      decodeSingle("Timestamp") { case l: Long => types.Timestamp.ofEpochMicro(l) }(values)
  }

  given timestampNTZSchema: Schema[types.TimestampNTZ] with {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.TimestampNTZType)
    def encode(value: types.TimestampNTZ): Vector[Any] = Vector(value.toEpochMicro)
    def decode(values: Vector[Any]): Either[DecodeError, types.TimestampNTZ] =
      decodeSingle("TimestampNTZ") { case l: Long => types.TimestampNTZ.ofEpochMicro(l) }(values)
  }

  given timeSchema: Schema[types.Time] with {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.TimeType)
    def encode(value: types.Time): Vector[Any] = Vector(value.toMicros)
    def decode(values: Vector[Any]): Either[DecodeError, types.Time] = decodeSingle("Time") {
      case l: Long => types.Time.ofMicros(l)
      case lt: java.time.LocalTime => types.Time.fromLocalTime(lt)
    }(values)
  }

  given yearMonthIntervalSchema: Schema[types.YearMonthInterval] with {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.YearMonthIntervalType)
    def encode(value: types.YearMonthInterval): Vector[Any] = Vector(value.toMonths)
    def decode(values: Vector[Any]): Either[DecodeError, types.YearMonthInterval] =
      decodeSingle("YearMonthInterval") { case i: Int => types.YearMonthInterval.ofMonths(i) }(values)
  }

  given dayTimeIntervalSchema: Schema[types.DayTimeInterval] with {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.DayTimeIntervalType)
    def encode(value: types.DayTimeInterval): Vector[Any] = Vector(value.toMicros)
    def decode(values: Vector[Any]): Either[DecodeError, types.DayTimeInterval] =
      decodeSingle("DayTimeInterval") { case l: Long => types.DayTimeInterval.ofMicros(l) }(values)
  }

  given binarySchema: Schema[types.Binary] with {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.BinaryType)
    def encode(value: types.Binary): Vector[Any] = Vector(value.toBytes)
    def decode(values: Vector[Any]): Either[DecodeError, types.Binary] =
      decodeSingle("Binary") { case ba: Array[Byte @unchecked] => types.Binary(ba) }(values)
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

  private def decodeDecimalValue(values: Vector[Any]): Either[DecodeError, types.Decimal] =
    if (values.length != 1) Left(DecodeError.WrongArity(1, values.length))
    else
      values.head match {
        case l: Long => Right(types.Decimal.ofUnscaled(l))
        case bd: java.math.BigDecimal =>
          types.Decimal.fromBigDecimal(bd).toRight(DecodeError.TypeMismatch("Decimal", bd.toString))
        case other => Left(DecodeError.TypeMismatch("Decimal", other.getClass.getSimpleName))
      }

  given decimalSchema: Schema[types.Decimal] with {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.DecimalType(10, 0))
    def encode(value: types.Decimal): Vector[Any] = Vector(value.toUnscaled)
    def decode(values: Vector[Any]): Either[DecodeError, types.Decimal] = decodeDecimalValue(values)
  }

  def decimalWith(precision: Int, scale: Int): Schema[types.Decimal] = new Schema[types.Decimal] {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.DecimalType(precision, scale))
    def encode(value: types.Decimal): Vector[Any] = Vector(value.toUnscaled)
    def decode(values: Vector[Any]): Either[DecodeError, types.Decimal] = decodeDecimalValue(values)
  }

  given booleanSchema: Schema[Boolean] with {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.BooleanType)
    def encode(value: Boolean): Vector[Any] = Vector(value)
    def decode(values: Vector[Any]): Either[DecodeError, Boolean] =
      decodeSingle("Boolean") { case b: Boolean => b }(values)
  }

  /** Schema for Seq[A] stored as a single ArrayType column. */
  given seqSchema[A](using elemSchema: Schema[A]): Schema[Seq[A]] with {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] = Vector(ColumnType.ArrayType(elemSchema.columnTypes.head))
    def encode(value: Seq[A]): Vector[Any] = Vector(value)
    def decode(values: Vector[Any]): Either[DecodeError, Seq[A]] =
      if (values.length != 1) Left(DecodeError.WrongArity(1, values.length))
      else
        values.head match {
          case s: Seq[?] =>
            s.foldLeft[Either[DecodeError, Vector[A]]](Right(Vector.empty)) { (acc, e) =>
              acc.flatMap(vs => elemSchema.decode(Vector(e)).map(vs :+ _)).map(_.toSeq)
            }.map(_.toSeq)
          case other => Left(DecodeError.TypeMismatch("Seq", other.getClass.getSimpleName))
        }
  }

  /** Schema for Map[K, V] stored as a single MapType column. */
  given mapSchema[K, V](using keySchema: Schema[K], valueSchema: Schema[V]): Schema[Map[K, V]] with {
    def columnCount: Int = 1
    def columnNames: Vector[String] = Vector("value")
    def columnTypes: Vector[ColumnType] =
      Vector(ColumnType.MapType(keySchema.columnTypes.head, valueSchema.columnTypes.head))
    def encode(value: Map[K, V]): Vector[Any] = Vector(value)
    def decode(values: Vector[Any]): Either[DecodeError, Map[K, V]] =
      if (values.length != 1) Left(DecodeError.WrongArity(1, values.length))
      else
        values.head match {
          case m: Map[?, ?] =>
            m.foldLeft[Either[DecodeError, Vector[(K, V)]]](Right(Vector.empty)) { (acc, entry) =>
              for {
                entries <- acc
                k <- keySchema.decode(Vector(entry._1))
                v <- valueSchema.decode(Vector(entry._2))
              } yield entries :+ ((k, v))
            }.map(_.toMap)
          case other => Left(DecodeError.TypeMismatch("Map", other.getClass.getSimpleName))
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

  /** Generic tuple schema for tuples of arity 3+.
    *
    * Uses Schema.derived to auto-generate schema for any Tuple type. Scala 3 tuples are product
    * types with Mirror.ProductOf, so the derivation handles field access via _1, _2, etc.
    * automatically.
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
    * Nested case-class fields derive recursively: when no Schema[h] is in scope but a
    * Mirror.ProductOf[h] is available, the field schema is derived. Nested schemas flatten into the
    * parent's column layout.
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

  /** Schema viewing T itself as a single StructType column.
    *
    * The column value is a T instance (or a Product with T's field layout, e.g. a Spark Row).
    * `nestedSchemas` exposes the flat inner schema so Spark's nested Rows can be rebuilt into typed
    * struct columns.
    */
  inline def structColumn[T](using
    m: Mirror.ProductOf[T],
    inner: Schema[T]
  ): Schema[T] = ${ structColumnImpl[T]('m, 'inner) }

  private def structColumnImpl[T: Type](
    m: Expr[Mirror.ProductOf[T]],
    inner: Expr[Schema[T]]
  )(using q: Quotes): Expr[Schema[T]] = {
    val typeName: String = Type.show[T]
    '{
      new Schema[T] {
        private val innerSchema = $inner
        def columnCount: Int = 1
        def columnNames: Vector[String] = Vector("value")
        def columnTypes: Vector[ColumnType] =
          Vector(ColumnType.StructType(innerSchema.columnNames.zip(innerSchema.columnTypes)))
        override def nestedSchemas: Vector[Option[Schema[?]]] = Vector(Some(innerSchema))
        def encode(value: T): Vector[Any] = Vector(value)
        def decode(values: Vector[Any]): Either[DecodeError, T] = values match {
          case Vector(p: Product) => Right($m.fromProduct(p))
          case Vector(v) =>
            Left(DecodeError.TypeMismatch(${ Expr(typeName): Expr[String] }, v.getClass.getSimpleName))
          case other => Left(DecodeError.WrongArity(1, other.length))
        }
      }
    }
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
          val product: Product = $aExpr.asInstanceOf[Product] // scalafix:ok DisableSyntax.asInstanceOf
          val element: Any = product.productElement($indexExpr)
          element.asInstanceOf[H] // scalafix:ok DisableSyntax.asInstanceOf
        }
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
          val hasMirror = Expr.summon[Mirror.ProductOf[h]].isDefined
          val newAcc = if (hasSchema || hasMirror) acc else (label, fieldTypeStr) :: acc
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
          val schema = Expr.summon[Schema[h]].getOrElse {
            Expr.summon[Mirror.ProductOf[h]] match {
              case Some(m) => '{ Schema.derived[h](using $m) }
              case None =>
                report.errorAndAbort(
                  s"No Schema or Mirror.ProductOf available for ${Type.show[h]}",
                  Position.ofMacroExpansion
                )
            }
          }
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
