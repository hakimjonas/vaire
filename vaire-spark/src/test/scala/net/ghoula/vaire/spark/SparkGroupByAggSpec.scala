package net.ghoula.vaire.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.prelude.*

class SparkGroupByAggSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  case class Sale(region: String, product: String, amount: Double, quantity: Int)
  given saleSchema: Schema[Sale] = Schema.derived

  case class RegionTotal(region: String, totalAmount: Double, cnt: Long)
  given regionTotalSchema: Schema[RegionTotal] = Schema.derived

  case class RegionProductTotal(region: String, product: String, totalAmount: Double)
  given regionProductTotalSchema: Schema[RegionProductTotal] = Schema.derived

  // Schema-derived names: region_value, product_value, amount_value, quantity_value
  private val regionCell: Expr[Sale, Any] = Expr.Cell("region_value", ColumnIndex(0))
  private val productCell: Expr[Sale, Any] = Expr.Cell("product_value", ColumnIndex(1))
  private val amountCell: Expr[Sale, Double] = Expr.Cell("amount_value", ColumnIndex(2))

  private def makeSales: Dataset[Sale] = {
    val regionCol = Column.string(Array("East", "West", "East", "West", "East"))
    val productCol = Column.string(Array("A", "B", "A", "A", "B"))
    val amountCol = Column.double(Array(100.0, 200.0, 150.0, 50.0, 300.0))
    val qtyCol = Column.int(Array(10, 20, 15, 5, 30))
    Dataset.fromColumns(Vector(regionCol, productCol, amountCol, qtyCol), saleSchema).toOption.get
  }

  "SparkInterpreter GroupByAgg" should "produce same results as in-memory" in {
    val sales = makeSales

    val keys = Vector(
      KeySpec[Sale, Any]("region", regionCell, ColumnType.StringType)
    )
    val aggs = Vector(
      AggSpec("totalAmount", Expr.SumDouble(amountCell), ColumnType.DoubleType),
      AggSpec("cnt", Expr.Count[Sale](), ColumnType.LongType)
    )

    val grouped = sales.groupByAgg[RegionTotal](keys, aggs)

    val inMemory = DatasetInterpreter.execute(grouped).map(_.toVectorUnsafe.sortBy(_.region))
    val sparkResult = sparkInterpreter.execute(grouped).map(_.toVectorUnsafe.sortBy(_.region))

    sparkResult shouldBe inMemory
  }

  it should "handle multi-key GROUP BY" in {
    val sales = makeSales

    val keys = Vector(
      KeySpec[Sale, Any]("region", regionCell, ColumnType.StringType),
      KeySpec[Sale, Any]("product", productCell, ColumnType.StringType)
    )
    val aggs = Vector(
      AggSpec("totalAmount", Expr.SumDouble(amountCell), ColumnType.DoubleType)
    )

    val grouped = sales.groupByAgg[RegionProductTotal](keys, aggs)

    val inMemory = DatasetInterpreter.execute(grouped).map(_.toVectorUnsafe.sortBy(r => (r.region, r.product)))
    val sparkResult = sparkInterpreter.execute(grouped).map(_.toVectorUnsafe.sortBy(r => (r.region, r.product)))

    sparkResult shouldBe inMemory
  }

  it should "handle HAVING via filter" in {
    val sales = makeSales

    val keys = Vector(
      KeySpec[Sale, Any]("region", regionCell, ColumnType.StringType)
    )
    val aggs = Vector(
      AggSpec("totalAmount", Expr.SumDouble(amountCell), ColumnType.DoubleType),
      AggSpec("cnt", Expr.Count[Sale](), ColumnType.LongType)
    )

    val grouped = sales.groupByAgg[RegionTotal](keys, aggs)
    // Note: the output schema has columns region_value, totalAmount_value, cnt_value
    // But GroupByAgg output names are user-defined: "region", "totalAmount", "cnt"
    val having = grouped.filter(
      Expr.Gt(
        Expr.Cell[RegionTotal, Long]("cnt", ColumnIndex(2)),
        Expr.Const(2L),
        summon[Ordering[Long]]
      )
    )

    val inMemory = DatasetInterpreter.execute(having).map(_.toVectorUnsafe)
    val sparkResult = sparkInterpreter.execute(having).map(_.toVectorUnsafe)

    sparkResult shouldBe inMemory
  }
}
