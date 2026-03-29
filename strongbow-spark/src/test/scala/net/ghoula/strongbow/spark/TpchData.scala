package net.ghoula.strongbow.spark

import net.ghoula.strongbow.prelude.*

/** Synthetic TPC-H data generators for proof-of-concept queries.
  *
  * Generates lineitem and part data with realistic distributions — no dbgen dependency. Uses a
  * seeded PRNG for reproducibility. Field types follow TPC-H but use Double for DECIMAL (strongbow
  * has no BigDecimal) and String for DATE (ISO-8601 format).
  */
object TpchData {

  // --- LineItem ---

  case class LineItem(
    l_orderkey: Long,
    l_partkey: Long,
    l_suppkey: Long,
    l_linenumber: Int,
    l_quantity: Double,
    l_extendedprice: Double,
    l_discount: Double,
    l_tax: Double,
    l_returnflag: String,
    l_linestatus: String,
    l_shipdate: String,
    l_commitdate: String,
    l_receiptdate: String,
    l_shipinstruct: String,
    l_shipmode: String,
    l_comment: String
  )

  given lineItemSchema: Schema[LineItem] = Schema.derived

  // --- Part ---

  case class Part(
    p_partkey: Long,
    p_name: String,
    p_mfgr: String,
    p_brand: String,
    p_type: String,
    p_size: Int,
    p_container: String,
    p_retailprice: Double,
    p_comment: String
  )

  given partSchema: Schema[Part] = Schema.derived

  // --- Generators ---

  private val returnFlags = Array("A", "N", "R")
  private val lineStatuses = Array("F", "O")
  private val shipInstructs = Array("DELIVER IN PERSON", "COLLECT COD", "TAKE BACK RETURN", "NONE")
  private val shipModes = Array("REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB")
  private val partTypes = Array(
    "STANDARD ANODIZED TIN",
    "STANDARD ANODIZED STEEL",
    "PROMO ANODIZED TIN",
    "PROMO BURNISHED STEEL",
    "ECONOMY PLATED COPPER",
    "PROMO BRUSHED BRASS",
    "STANDARD POLISHED BRASS",
    "ECONOMY BURNISHED NICKEL",
    "PROMO POLISHED COPPER",
    "STANDARD BRUSHED TIN"
  )
  private val containers = Array(
    "SM CASE",
    "SM BOX",
    "SM PACK",
    "SM PKG",
    "MED BAG",
    "MED BOX",
    "MED PKG",
    "MED PACK",
    "LG CASE",
    "LG BOX",
    "LG PACK",
    "LG PKG"
  )
  private val brands = Array(
    "Brand#11",
    "Brand#12",
    "Brand#13",
    "Brand#14",
    "Brand#15",
    "Brand#21",
    "Brand#22",
    "Brand#23",
    "Brand#24",
    "Brand#25"
  )
  private val manufacturers =
    Array("Manufacturer#1", "Manufacturer#2", "Manufacturer#3", "Manufacturer#4", "Manufacturer#5")

  /** Generate a date between startYear-01-01 and endYear-12-31. */
  private def randomDate(rng: scala.util.Random, startYear: Int, endYear: Int): String = {
    val year = startYear + rng.nextInt(endYear - startYear + 1)
    val month = 1 + rng.nextInt(12)
    val day = 1 + rng.nextInt(28) // safe day range
    f"$year%04d-$month%02d-$day%02d"
  }

  /** Generate `n` LineItem rows with reproducible data.
    *
    * Distribution matches TPC-H spec:
    *   - quantity: 1-50
    *   - extendedprice: 900-100000
    *   - discount: 0.00-0.10
    *   - tax: 0.00-0.08
    *   - shipdate: 1992-01-01 to 1998-12-01
    */
  def generateLineItems(n: Int, seed: Long = 42L): Vector[LineItem] = {
    val rng = new scala.util.Random(seed)
    Vector.tabulate(n) { i =>
      val quantity = 1 + rng.nextInt(50)
      val price = 900.0 + rng.nextDouble() * 99100.0
      val discount = math.round(rng.nextDouble() * 0.10 * 100.0) / 100.0
      val tax = math.round(rng.nextDouble() * 0.08 * 100.0) / 100.0
      LineItem(
        l_orderkey = (i / 4 + 1).toLong,
        l_partkey = (1 + rng.nextInt(200000)).toLong,
        l_suppkey = (1 + rng.nextInt(10000)).toLong,
        l_linenumber = (i % 4) + 1,
        l_quantity = quantity.toDouble,
        l_extendedprice = math.round(price * 100.0) / 100.0,
        l_discount = discount,
        l_tax = tax,
        l_shipdate = randomDate(rng, 1992, 1998),
        l_commitdate = randomDate(rng, 1992, 1998),
        l_receiptdate = randomDate(rng, 1992, 1998),
        l_shipinstruct = shipInstructs(rng.nextInt(shipInstructs.length)),
        l_shipmode = shipModes(rng.nextInt(shipModes.length)),
        l_returnflag = returnFlags(rng.nextInt(returnFlags.length)),
        l_linestatus = lineStatuses(rng.nextInt(lineStatuses.length)),
        l_comment = s"comment-$i"
      )
    }
  }

  /** Generate `n` Part rows with reproducible data. */
  def generateParts(n: Int, seed: Long = 99L): Vector[Part] = {
    val rng = new scala.util.Random(seed)
    Vector.tabulate(n) { i =>
      Part(
        p_partkey = (i + 1).toLong,
        p_name = s"part-${i + 1}",
        p_mfgr = manufacturers(rng.nextInt(manufacturers.length)),
        p_brand = brands(rng.nextInt(brands.length)),
        p_type = partTypes(rng.nextInt(partTypes.length)),
        p_size = 1 + rng.nextInt(50),
        p_container = containers(rng.nextInt(containers.length)),
        p_retailprice = math.round((900.0 + rng.nextDouble() * 1100.0) * 100.0) / 100.0,
        p_comment = s"part-comment-${i + 1}"
      )
    }
  }

  /** Build a strongbow Dataset from LineItem rows. */
  def lineItemDataset(items: Vector[LineItem]): Dataset[LineItem] = {
    val cols = Vector(
      Column.long(items.map(_.l_orderkey).toArray),
      Column.long(items.map(_.l_partkey).toArray),
      Column.long(items.map(_.l_suppkey).toArray),
      Column.int(items.map(_.l_linenumber).toArray),
      Column.double(items.map(_.l_quantity).toArray),
      Column.double(items.map(_.l_extendedprice).toArray),
      Column.double(items.map(_.l_discount).toArray),
      Column.double(items.map(_.l_tax).toArray),
      Column.string(items.map(_.l_returnflag).toArray),
      Column.string(items.map(_.l_linestatus).toArray),
      Column.string(items.map(_.l_shipdate).toArray),
      Column.string(items.map(_.l_commitdate).toArray),
      Column.string(items.map(_.l_receiptdate).toArray),
      Column.string(items.map(_.l_shipinstruct).toArray),
      Column.string(items.map(_.l_shipmode).toArray),
      Column.string(items.map(_.l_comment).toArray)
    )
    Dataset.fromColumns(cols, lineItemSchema).toOption.get
  }

  /** Build a strongbow Dataset from Part rows. */
  def partDataset(parts: Vector[Part]): Dataset[Part] = {
    val cols = Vector(
      Column.long(parts.map(_.p_partkey).toArray),
      Column.string(parts.map(_.p_name).toArray),
      Column.string(parts.map(_.p_mfgr).toArray),
      Column.string(parts.map(_.p_brand).toArray),
      Column.string(parts.map(_.p_type).toArray),
      Column.int(parts.map(_.p_size).toArray),
      Column.string(parts.map(_.p_container).toArray),
      Column.double(parts.map(_.p_retailprice).toArray),
      Column.string(parts.map(_.p_comment).toArray)
    )
    Dataset.fromColumns(cols, partSchema).toOption.get
  }
}
