package net.ghoula.strongbow

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.prelude.*

class AggregateByKeySpec extends AnyFlatSpec with Matchers {

  case class Sale(product: String, quantity: Int, revenue: Double)
  given Schema[Sale] = Schema.derived

  "aggregateByKey (2-arity)" should "compute two aggregations per key" in {
    val sales = createSalesDataset()

    val result = sales
      .groupBy(_.product)
      .aggregateByKey(
        agg1 = (s: Sale) => s.quantity,
        agg2 = (s: Sale) => s.revenue,
        reduce1 = (a: Int, b: Int) => a + b,
        reduce2 = (a: Double, b: Double) => a + b
      )
      .toPairs
      .collect
      .toOption
      .get

    val laptopStats = result.find(_._1 == "Laptop").get._2
    laptopStats._1 shouldBe 5 // total quantity
    laptopStats._2 shouldBe (2499.95 +- 0.01) // total revenue

    val mouseStats = result.find(_._1 == "Mouse").get._2
    mouseStats._1 shouldBe 10
    mouseStats._2 shouldBe (299.90 +- 0.01)
  }

  "aggregateByKey (3-arity)" should "compute three aggregations per key" in {
    val sales = createSalesDataset()

    val result = sales
      .groupBy(_.product)
      .aggregateByKey(
        agg1 = (_: Sale) => 1,
        agg2 = (s: Sale) => s.quantity,
        agg3 = (s: Sale) => s.revenue,
        reduce1 = (a: Int, b: Int) => a + b,
        reduce2 = (a: Int, b: Int) => a + b,
        reduce3 = (a: Double, b: Double) => a + b
      )
      .toPairs
      .collect
      .toOption
      .get

    val laptopStats = result.find(_._1 == "Laptop").get._2
    laptopStats._1 shouldBe 3 // count
    laptopStats._2 shouldBe 5 // total quantity
    laptopStats._3 shouldBe (2499.95 +- 0.01) // total revenue
  }

  "aggregateByKey (4-arity)" should "compute four aggregations per key" in {
    val sales = createSalesDataset()

    val result = sales
      .groupBy(_.product)
      .aggregateByKey(
        agg1 = (_: Sale) => 1,
        agg2 = (s: Sale) => s.quantity,
        agg3 = (s: Sale) => s.revenue,
        agg4 = (s: Sale) => s.revenue,
        reduce1 = (a: Int, b: Int) => a + b,    // count
        reduce2 = (a: Int, b: Int) => a + b,    // sum quantity
        reduce3 = (a: Double, b: Double) => a + b, // sum revenue
        reduce4 = (a: Double, b: Double) => math.max(a, b) // max revenue
      )
      .toPairs
      .collect
      .toOption
      .get

    val laptopStats = result.find(_._1 == "Laptop").get._2
    laptopStats._1 shouldBe 3
    laptopStats._2 shouldBe 5
    laptopStats._3 shouldBe (2499.95 +- 0.01)
    laptopStats._4 shouldBe (999.99 +- 0.01)
  }

  "aggregateByKey" should "work with single-key groups" in {
    val sales = Vector(
      Sale("Widget", 1, 10.0),
      Sale("Gadget", 2, 20.0)
    )

    val dataset = MaterializedDataset.fromVector(sales).toOption.get
    val ds = Dataset.Root(dataset.columns, summon[Schema[Sale]])

    val result = ds
      .groupBy(_.product)
      .aggregateByKey(
        agg1 = (s: Sale) => s.quantity,
        agg2 = (s: Sale) => s.revenue,
        reduce1 = (a: Int, b: Int) => a + b,
        reduce2 = (a: Double, b: Double) => a + b
      )
      .toPairs
      .collect
      .toOption
      .get

    result should have length 2
    result.find(_._1 == "Widget").get._2 shouldBe (1, 10.0)
    result.find(_._1 == "Gadget").get._2 shouldBe (2, 20.0)
  }

  "aggregateByKey" should "chain with other grouped operations" in {
    val sales = createSalesDataset()

    val result = sales
      .groupBy(_.product)
      .aggregateByKey(
        agg1 = (s: Sale) => s.quantity,
        agg2 = (s: Sale) => s.revenue,
        reduce1 = (a: Int, b: Int) => a + b,
        reduce2 = (a: Double, b: Double) => a + b
      )
      .filterKeys(_ == "Laptop")
      .toPairs
      .collect
      .toOption
      .get

    result should have length 1
    result.head._1 shouldBe "Laptop"
  }

  private def createSalesDataset(): Dataset[Sale] = {
    val sales = Vector(
      Sale("Laptop", 2, 999.99),
      Sale("Mouse", 5, 149.95),
      Sale("Laptop", 1, 999.99),
      Sale("Mouse", 3, 89.97),
      Sale("Laptop", 2, 499.97),
      Sale("Mouse", 2, 59.98)
    )

    val mat = MaterializedDataset.fromVector(sales).toOption.get
    Dataset.Root(mat.columns, summon[Schema[Sale]])
  }
}
