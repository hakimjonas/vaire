package net.ghoula.vaire

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.vaire.prelude.*

/** Compiles the README examples so they cannot rot. Each block here mirrors one in `README.md`. */
class ReadmeExamplesSpec extends AnyFlatSpec with Matchers {

  case class Trade(symbol: String, price: Double, quantity: Int)
  given Schema[Trade] = Schema.derived

  case class SymbolTotal(symbol: String, total: Double)
  given Schema[SymbolTotal] = Schema.derived

  case class SymbolPrice(symbol: String, price: Double)
  given Schema[SymbolPrice] = Schema.derived

  private val trades: Dataset[Trade] = Dataset
    .fromColumns(
      Vector(
        Column.string(Array("ACME", "GLOB", "ACME")),
        Column.double(Array(120.5, 85.0, 99.9)),
        Column.int(Array(10, 40, 5))
      ),
      summon[Schema[Trade]]
    )
    .toOption
    .get

  "the quick-start example" should "group and collect" in {
    val totals = trades
      .where(_.quantity > 5)
      .groupByAgg[SymbolTotal](
        keys =
          Vector(KeySpec("symbol", Expr.Cell[Trade, String]("symbol_value", ColumnIndex(0)), ColumnType.StringType)),
        aggs = Vector(agg.sumDouble[Trade](_.price).as("total"))
      )

    totals.collect.toOption.get.toSet shouldBe Set(SymbolTotal("ACME", 120.5), SymbolTotal("GLOB", 85.0))
  }

  "the common operations" should "compile" in {
    trades.where(_.price > 100.0)
    trades.withFields(t => t.copy(quantity = t.quantity * 2))
    trades.sortByColumn(_.price)
    trades.project[SymbolPrice]
    succeed
  }

  "the join example" should "compile" in {
    val prices = Dataset
      .fromColumns(Vector(Column.string(Array("ACME")), Column.double(Array(120.5))), summon[Schema[SymbolPrice]])
      .toOption
      .get
    val tradeKey: Expr[Trade, String] = Expr.Cell("symbol_value", ColumnIndex(0))
    val priceKey: Expr[SymbolPrice, String] = Expr.Cell("symbol_value", ColumnIndex(0))

    trades.joinOn(prices, tradeKey, priceKey, ColumnType.StringType, ColumnType.StringType)
    succeed
  }
}
