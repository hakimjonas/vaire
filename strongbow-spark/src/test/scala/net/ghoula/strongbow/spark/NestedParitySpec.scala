package net.ghoula.strongbow.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.prelude.*
import net.ghoula.strongbow.types.Time

class NestedParitySpec extends AnyFlatSpec with Matchers with SparkTestBase {

  case class Rec(id: Int, xs: Seq[Int], tags: Map[String, Int])
  given Schema[Rec] = Schema.derived

  case class Point(x: Int, y: String)
  given Schema[Point] = Schema.derived

  case class Entry(id: Int, score: Int, label: String)
  given Schema[Entry] = Schema.derived

  case class OnlyY(y: String)
  given Schema[OnlyY] = Schema.derived

  val recs: Vector[Rec] = Vector(
    Rec(1, Seq(1, 2, 3), Map("a" -> 1)),
    Rec(2, Seq.empty, Map.empty),
    Rec(3, Seq(4), Map("b" -> 2, "c" -> 3))
  )

  val entries: Vector[Entry] = Vector(
    Entry(1, 10, "a"),
    Entry(2, 20, "b"),
    Entry(3, 30, "c")
  )

  def rootOf[T](values: Vector[T])(using schema: Schema[T]): Dataset[T] =
    MaterializedDataset.fromVector(values) match {
      case Right(md) => Dataset.Root(InMemorySource(md.columns), schema)
      case Left(err) => fail(s"Dataset creation failed: $err")
    }

  "array and map columns" should "round-trip through a Spark DataFrame without boxing" in {
    MaterializedDataset.fromVector(recs) match {
      case Right(md) =>
        md.columns(1) match {
          case _: Column.ArrayColumn[?] => succeed
          case other => fail(s"Expected deboxed ArrayColumn, got: ${other.getClass.getSimpleName}")
        }
        md.columns(2) match {
          case _: Column.MapColumn[?, ?] => succeed
          case other => fail(s"Expected deboxed MapColumn, got: ${other.getClass.getSimpleName}")
        }

        val df = DataFrameBuilder.fromMaterialized(spark, md)
        df.schema.fieldNames.toSeq shouldBe Seq("id_value", "xs_value", "tags_value")
        df.schema("xs_value").dataType.simpleString shouldBe "array<int>"
        df.schema("tags_value").dataType.simpleString shouldBe "map<string,int>"

        val rows = df.collect()
        RowConverter.toMaterialized(rows, summon[Schema[Rec]]) match {
          case Right(roundTripped) =>
            roundTripped.rowCount shouldBe 3
            roundTripped.toVectorUnsafe shouldBe recs
          case other => fail(s"Round-trip failed: $other")
        }
      case other => fail(s"Dataset creation failed: $other")
    }
  }

  "time values" should "convert between Strongbow Time and Spark Rows" in {
    val schema = Schema.timeSchema
    val micros = (10 * 3600 + 15 * 60 + 30) * 1000000L + 123456L
    val row = RowConverter.toRow(Time.ofMicros(micros), schema)
    row.get(0) shouldBe java.time.LocalTime.of(10, 15, 30, 123456000)
    RowConverter.fromRow(row, schema) shouldBe Right(Time.ofMicros(micros))
  }

  it should "read TIME columns produced by Spark SQL" in {
    spark.conf.set("spark.sql.timeType.enabled", "true")
    val df = spark.sql("SELECT make_time(10, 15, 30.123456) AS at_value")
    df.schema("at_value").dataType.typeName shouldBe "time(6)"

    val rows = df.collect()
    rows(0).get(0) shouldBe java.time.LocalTime.of(10, 15, 30, 123456000)
    RowConverter.fromRow(rows(0), Schema.timeSchema) shouldBe
      Right(Time.ofMicros((10 * 3600 + 15 * 60 + 30) * 1000000L + 123456L))
  }

  "struct columns" should "round-trip through a Spark DataFrame via SparkValues" in {
    val points = Vector(Point(1, "a"), Point(2, "b"))
    MaterializedDataset.fromVector(points)(using Schema.structColumn[Point]) match {
      case Right(md) =>
        val df = DataFrameBuilder.fromMaterialized(spark, md)
        df.schema.fields.head.dataType.simpleString shouldBe "struct<x_value:int,y_value:string>"

        val rows = df.collect()
        rows(0).getStruct(0).getInt(0) shouldBe 1
        rows(0).getStruct(0).getString(1) shouldBe "a"
        RowConverter.toMaterialized(rows, Schema.structColumn[Point]) match {
          case Right(roundTripped) =>
            roundTripped.toVectorUnsafe shouldBe points
          case other => fail(s"Round-trip failed: $other")
        }
      case other => fail(s"Dataset creation failed: $other")
    }
  }

  "Expr.Struct" should "produce identical results on both interpreters" in {
    val structExpr = Expr.struct[Entry, Point](
      ("x_value", Expr.cell[Entry, Int]("score_value", ColumnIndex(1)), ColumnType.IntType),
      ("y_value", Expr.cell[Entry, String]("label_value", ColumnIndex(2)), ColumnType.StringType)
    )
    val structCt = structExpr.outputType.get
    val expected = Vector(Point(10, "a"), Point(20, "b"), Point(30, "c"))

    val projected =
      rootOf(entries).selectAs[Point](("p", structExpr, structCt))(using Schema.structColumn[Point])

    DatasetInterpreter.execute(projected) match {
      case Right(inMemory) =>
        inMemory.toVectorUnsafe shouldBe expected
        sparkInterpreter.execute(projected) match {
          case Right(viaSpark) =>
            viaSpark.toVectorUnsafe shouldBe expected
          case other => fail(s"Spark execution failed: $other")
        }
      case other => fail(s"In-memory execution failed: $other")
    }
  }

  "Expr.GetField" should "produce identical results on both interpreters" in {
    val structExpr = Expr.struct[Entry, Point](
      ("x_value", Expr.cell[Entry, Int]("score_value", ColumnIndex(1)), ColumnType.IntType),
      ("y_value", Expr.cell[Entry, String]("label_value", ColumnIndex(2)), ColumnType.StringType)
    )
    val getFieldExpr: Expr[Entry, String] = structExpr.getField[String](ColumnIndex(1), "y_value")
    val expected = Vector(OnlyY("a"), OnlyY("b"), OnlyY("c"))

    val projected = rootOf(entries).selectAs[OnlyY](("y", getFieldExpr, ColumnType.StringType))

    DatasetInterpreter.execute(projected) match {
      case Right(inMemory) =>
        inMemory.toVectorUnsafe shouldBe expected
        sparkInterpreter.execute(projected) match {
          case Right(viaSpark) =>
            viaSpark.toVectorUnsafe shouldBe expected
          case other => fail(s"Spark execution failed: $other")
        }
      case other => fail(s"In-memory execution failed: $other")
    }
  }
}
