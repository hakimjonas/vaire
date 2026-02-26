package net.ghoula.strongbow

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.specs.{AggSpec, KeySpec}
import net.ghoula.strongbow.types.ColumnIndex

class GroupByAggSpec extends AnyFlatSpec with Matchers {

  case class Sale(region: String, product: String, amount: Double, quantity: Int)
  given saleSchema: Schema[Sale] = Schema.derived

  case class RegionTotal(region: String, totalAmount: Double, totalQty: Long)
  given regionTotalSchema: Schema[RegionTotal] = Schema.derived

  case class RegionProductTotal(region: String, product: String, totalAmount: Double)
  given regionProductTotalSchema: Schema[RegionProductTotal] = Schema.derived

  case class SingleAggResult(region: String, cnt: Long)
  given singleAggResultSchema: Schema[SingleAggResult] = Schema.derived

  private def makeSales: Dataset[Sale] = {
    val regionCol = Column.string(Array("East", "West", "East", "West", "East"))
    val productCol = Column.string(Array("A", "B", "A", "A", "B"))
    val amountCol = Column.double(Array(100.0, 200.0, 150.0, 50.0, 300.0))
    val qtyCol = Column.int(Array(10, 20, 15, 5, 30))
    Dataset.fromColumns(Vector(regionCol, productCol, amountCol, qtyCol), saleSchema).toOption.get
  }

  "GroupByAgg" should "group by single key with multiple aggs" in {
    val sales = makeSales

    val keys = Vector(
      KeySpec[Sale, Any]("region", Expr.Cell[Sale, Any]("region", ColumnIndex(0)), ColumnType.StringType)
    )
    val aggs = Vector(
      AggSpec("totalAmount", Expr.SumDouble(Expr.Cell[Sale, Double]("amount", ColumnIndex(2))), ColumnType.DoubleType),
      AggSpec("totalQty", Expr.Count[Sale](), ColumnType.LongType)
    )

    val grouped = sales.groupByAgg[RegionTotal](keys, aggs)
    val result = DatasetInterpreter.execute(grouped).toOption.get.toVectorUnsafe

    result.length shouldBe 2
    val east = result.find(_.region == "East").get
    east.totalAmount shouldBe 550.0
    east.totalQty shouldBe 3L

    val west = result.find(_.region == "West").get
    west.totalAmount shouldBe 250.0
    west.totalQty shouldBe 2L
  }

  it should "group by multiple keys" in {
    val sales = makeSales

    val keys = Vector(
      KeySpec[Sale, Any]("region", Expr.Cell[Sale, Any]("region", ColumnIndex(0)), ColumnType.StringType),
      KeySpec[Sale, Any]("product", Expr.Cell[Sale, Any]("product", ColumnIndex(1)), ColumnType.StringType)
    )
    val aggs = Vector(
      AggSpec("totalAmount", Expr.SumDouble(Expr.Cell[Sale, Double]("amount", ColumnIndex(2))), ColumnType.DoubleType)
    )

    val grouped = sales.groupByAgg[RegionProductTotal](keys, aggs)
    val result = DatasetInterpreter.execute(grouped).toOption.get.toVectorUnsafe

    result.length shouldBe 4 // East-A, East-B, West-A, West-B
    val eastA = result.find(r => r.region == "East" && r.product == "A").get
    eastA.totalAmount shouldBe 250.0

    val eastB = result.find(r => r.region == "East" && r.product == "B").get
    eastB.totalAmount shouldBe 300.0
  }

  it should "handle HAVING via filter" in {
    val sales = makeSales

    val keys = Vector(
      KeySpec[Sale, Any]("region", Expr.Cell[Sale, Any]("region", ColumnIndex(0)), ColumnType.StringType)
    )
    val aggs = Vector(
      AggSpec("totalAmount", Expr.SumDouble(Expr.Cell[Sale, Double]("amount", ColumnIndex(2))), ColumnType.DoubleType),
      AggSpec("totalQty", Expr.Count[Sale](), ColumnType.LongType)
    )

    val grouped = sales.groupByAgg[RegionTotal](keys, aggs)
    // HAVING: totalQty > 2
    val having = grouped.filter(
      Expr.Gt(
        Expr.Cell[RegionTotal, Long]("totalQty", ColumnIndex(2)),
        Expr.Const(2L),
        summon[Ordering[Long]]
      )
    )
    val result = DatasetInterpreter.execute(having).toOption.get.toVectorUnsafe

    result.length shouldBe 1
    result.head.region shouldBe "East"
  }

  it should "handle empty groups (no rows match)" in {
    val regionCol = Column.string(Array.empty[String])
    val productCol = Column.string(Array.empty[String])
    val amountCol = Column.double(Array.empty[Double])
    val qtyCol = Column.int(Array.empty[Int])
    val emptyDs = Dataset.fromColumns(Vector(regionCol, productCol, amountCol, qtyCol), saleSchema).toOption.get

    val keys = Vector(
      KeySpec[Sale, Any]("region", Expr.Cell[Sale, Any]("region", ColumnIndex(0)), ColumnType.StringType)
    )
    val aggs = Vector(
      AggSpec("cnt", Expr.Count[Sale](), ColumnType.LongType)
    )

    val grouped = emptyDs.groupByAgg[SingleAggResult](keys, aggs)
    val result = DatasetInterpreter.execute(grouped).toOption.get.toVectorUnsafe

    result shouldBe Vector.empty // no groups produced
  }

  "GroupByAgg with 8 aggregations" should "work without arity limit" in {
    // Simulates TPC-H Q1's 8 aggregations
    case class Row8(key: String, v1: Double, v2: Double, v3: Int)
    given Schema[Row8] = Schema.derived

    case class Agg8Result(
      key: String,
      sum1: Double,
      sum2: Double,
      avg1: Double,
      avg2: Double,
      cnt: Long,
      max1: Double,
      min1: Double,
      sumInt: Int
    )
    given Schema[Agg8Result] = Schema.derived

    val keyCol = Column.string(Array("A", "A", "B", "A", "B"))
    val v1Col = Column.double(Array(10.0, 20.0, 30.0, 40.0, 50.0))
    val v2Col = Column.double(Array(1.0, 2.0, 3.0, 4.0, 5.0))
    val v3Col = Column.int(Array(1, 2, 3, 4, 5))
    val ds = Dataset.fromColumns(Vector(keyCol, v1Col, v2Col, v3Col), summon[Schema[Row8]]).toOption.get

    val keys = Vector(
      KeySpec[Row8, Any]("key", Expr.Cell[Row8, Any]("key", ColumnIndex(0)), ColumnType.StringType)
    )
    val aggs = Vector(
      AggSpec("sum1", Expr.SumDouble(Expr.Cell[Row8, Double]("v1", ColumnIndex(1))), ColumnType.DoubleType),
      AggSpec("sum2", Expr.SumDouble(Expr.Cell[Row8, Double]("v2", ColumnIndex(2))), ColumnType.DoubleType),
      AggSpec("avg1", Expr.Avg(Expr.Cell[Row8, Double]("v1", ColumnIndex(1))), ColumnType.DoubleType),
      AggSpec("avg2", Expr.Avg(Expr.Cell[Row8, Double]("v2", ColumnIndex(2))), ColumnType.DoubleType),
      AggSpec("cnt", Expr.Count[Row8](), ColumnType.LongType),
      AggSpec("max1", Expr.SumDouble(Expr.Cell[Row8, Double]("v1", ColumnIndex(1))), ColumnType.DoubleType),
      AggSpec("min1", Expr.SumDouble(Expr.Cell[Row8, Double]("v2", ColumnIndex(2))), ColumnType.DoubleType),
      AggSpec("sumInt", Expr.Sum(Expr.Cell[Row8, Int]("v3", ColumnIndex(3))), ColumnType.IntType)
    )

    val grouped = ds.groupByAgg[Agg8Result](keys, aggs)
    val result = DatasetInterpreter.execute(grouped).toOption.get.toVectorUnsafe

    result.length shouldBe 2
    val groupA = result.find(_.key == "A").get
    groupA.sum1 shouldBe 70.0 // 10+20+40
    groupA.cnt shouldBe 3L
    groupA.sumInt shouldBe 7 // 1+2+4
  }

  // --- AggBuilder ergonomic tests ---

  "AggBuilders" should "produce same results as manual AggSpec" in {
    import net.ghoula.strongbow.agg.AggBuilders as agg
    import net.ghoula.strongbow.agg.as

    val sales = makeSales

    val keys = Vector(
      KeySpec[Sale, Any]("region", Expr.Cell[Sale, Any]("region", ColumnIndex(0)), ColumnType.StringType)
    )
    val aggs = Vector(
      agg.sumDouble[Sale](_.amount).as("totalAmount"),
      agg.count[Sale].as("totalQty")
    )

    val grouped = sales.groupByAgg[RegionTotal](keys, aggs)
    val result = DatasetInterpreter.execute(grouped).toOption.get.toVectorUnsafe

    result.length shouldBe 2
    val east = result.find(_.region == "East").get
    east.totalAmount shouldBe 550.0
    east.totalQty shouldBe 3L
  }

  it should "support .as() renaming" in {
    import net.ghoula.strongbow.agg.AggBuilders as agg
    import net.ghoula.strongbow.agg.as

    val spec = agg.count[Sale].as("myCount")
    spec.name shouldBe "myCount"
  }

  it should "support sum for Int fields" in {
    import net.ghoula.strongbow.agg.AggBuilders as agg
    import net.ghoula.strongbow.agg.as

    val sales = makeSales

    case class RegionSum(region: String, totalQty: Int)
    given Schema[RegionSum] = Schema.derived

    val keys = Vector(
      KeySpec[Sale, Any]("region", Expr.Cell[Sale, Any]("region", ColumnIndex(0)), ColumnType.StringType)
    )
    val aggs = Vector(
      agg.sum[Sale](_.quantity).as("totalQty")
    )

    val grouped = sales.groupByAgg[RegionSum](keys, aggs)
    val result = DatasetInterpreter.execute(grouped).toOption.get.toVectorUnsafe

    result.length shouldBe 2
    val east = result.find(_.region == "East").get
    east.totalQty shouldBe 55 // 10+15+30
  }
}
