package net.ghoula.strongbow.bench

import scala.util.Random

import net.ghoula.strongbow.prelude.*

/** Sanity benchmarks for newly implemented methods.
  *
  * Tests at 10K and 50K rows:
  *   - Dataset.join() (cross-dataset inner/left join)
  *   - AggregateByKey (2-arity, 3-arity)
  *   - Map with Schema (newly fixed)
  *   - Schema.derived round-trip
  */
object NewMethodsBench {

  case class Sale(product: String, quantity: Int, revenue: Double)
  given Schema[Sale] = Schema.derived

  case class Product(id: Int, name: String, category: String)
  given Schema[Product] = Schema.derived

  case class BenchResult(
    operation: String,
    rows: Int,
    medianMs: Double,
    p95Ms: Double
  )

  def time(warmups: Int, runs: Int)(f: => Any): (Double, Double) = {
    // Warmup
    (0 until warmups).foreach(_ => f)

    System.gc()
    Thread.sleep(100)

    // Measure
    val times = (0 until runs).map { _ =>
      val start = System.nanoTime()
      f
      val end = System.nanoTime()
      (end - start) / 1_000_000.0
    }.sorted

    val median = times(runs / 2)
    val p95 = times((runs * 0.95).toInt)
    (median, p95)
  }

  def createSalesDataset(n: Int): Dataset[Sale] = {
    val products = Array("Laptop", "Mouse", "Keyboard", "Monitor", "Headset", "Webcam", "Dock", "Cable")
    val sales = Vector.fill(n) {
      Sale(products(Random.nextInt(products.length)), Random.nextInt(100) + 1, Random.nextDouble() * 1000.0)
    }

    val mat = MaterializedDataset.fromVector(sales).toOption.get
    Dataset.Root(mat.columns, summon[Schema[Sale]])
  }

  def createProductsDataset(n: Int): Dataset[Product] = {
    val categories = Array("Electronics", "Accessories", "Peripherals")
    val products = Vector.tabulate(n) { i =>
      Product(i, s"Product_$i", categories(i % categories.length))
    }

    val mat = MaterializedDataset.fromVector(products).toOption.get
    Dataset.Root(mat.columns, summon[Schema[Product]])
  }

  def benchDatasetJoin(salesRows: Int, productRows: Int): Vector[BenchResult] = {
    val sales = createSalesDataset(salesRows)
    val products = createProductsDataset(productRows)

    val results = Vector.newBuilder[BenchResult]

    // Inner join
    val (medianInner, p95Inner) = time(10, 30) {
      val joined = sales.join(products, (s: Sale, p: Product) => s.product == p.name)
      DatasetInterpreter.execute(joined).toOption.get.rowCount
    }
    results += BenchResult("InnerJoin (Dataset)", salesRows, medianInner, p95Inner)

    // Left join
    val (medianLeft, p95Left) = time(10, 30) {
      val joined = sales.leftJoin(products, (s: Sale, p: Product) => s.product == p.name)
      DatasetInterpreter.execute(joined).toOption.get.rowCount
    }
    results += BenchResult("LeftJoin (Dataset)", salesRows, medianLeft, p95Left)

    // Anti join
    val (medianAnti, p95Anti) = time(10, 30) {
      val joined = sales.antiJoin(products, (s: Sale, p: Product) => s.product == p.name)
      DatasetInterpreter.execute(joined).toOption.get.rowCount
    }
    results += BenchResult("AntiJoin (Dataset)", salesRows, medianAnti, p95Anti)

    results.result()
  }

  def benchAggregateByKey(rows: Int): Vector[BenchResult] = {
    val sales = createSalesDataset(rows)

    val results = Vector.newBuilder[BenchResult]

    // 2-arity aggregateByKey
    val (median2, p952) = time(10, 30) {
      val grouped = sales
        .groupBy(_.product)
        .aggregateByKey(
          agg1 = (s: Sale) => s.quantity,
          agg2 = (s: Sale) => s.revenue,
          reduce1 = (a: Int, b: Int) => a + b,
          reduce2 = (a: Double, b: Double) => a + b
        )
      val pairs = GroupByInterpreter.execute(grouped)
      pairs.length
    }
    results += BenchResult("AggregateByKey2", rows, median2, p952)

    // 3-arity aggregateByKey
    val (median3, p953) = time(10, 30) {
      val grouped = sales
        .groupBy(_.product)
        .aggregateByKey(
          agg1 = (_: Sale) => 1,
          agg2 = (s: Sale) => s.quantity,
          agg3 = (s: Sale) => s.revenue,
          reduce1 = (a: Int, b: Int) => a + b,
          reduce2 = (a: Int, b: Int) => a + b,
          reduce3 = (a: Double, b: Double) => a + b
        )
      val pairs = GroupByInterpreter.execute(grouped)
      pairs.length
    }
    results += BenchResult("AggregateByKey3", rows, median3, p953)

    // Comparison: reduceByKey (existing)
    val (medianReduce, p95Reduce) = time(10, 30) {
      val grouped = sales
        .groupBy(_.product)
        .reduceByKey((a, b) => Sale(a.product, a.quantity + b.quantity, a.revenue + b.revenue))
      val pairs = GroupByInterpreter.execute(grouped)
      pairs.length
    }
    results += BenchResult("ReduceByKey (baseline)", rows, medianReduce, p95Reduce)

    results.result()
  }

  def benchMapWithSchema(rows: Int): Vector[BenchResult] = {
    val sales = createSalesDataset(rows)

    val results = Vector.newBuilder[BenchResult]

    // Map to Boolean
    val (medianBool, p95Bool) = time(10, 30) {
      val mapped = sales.map(s => s.revenue > 500.0)
      DatasetInterpreter.execute(mapped).toOption.get.rowCount
    }
    results += BenchResult("Map[Sale => Boolean]", rows, medianBool, p95Bool)

    // Map to Double
    val (medianDouble, p95Double) = time(10, 30) {
      val mapped = sales.map(_.revenue)
      DatasetInterpreter.execute(mapped).toOption.get.rowCount
    }
    results += BenchResult("Map[Sale => Double]", rows, medianDouble, p95Double)

    // Map to Tuple (uses derivedTupleSchema)
    val (medianTuple, p95Tuple) = time(10, 30) {
      val mapped = sales.map(s => (s.product, s.revenue))
      DatasetInterpreter.execute(mapped).toOption.get.rowCount
    }
    results += BenchResult("Map[Sale => (String,Double)]", rows, medianTuple, p95Tuple)

    // FlatMap
    val (medianFlat, p95Flat) = time(10, 30) {
      val mapped = sales.flatMap(s => if (s.revenue > 500.0) Some(s.revenue) else None)
      DatasetInterpreter.execute(mapped).toOption.get.rowCount
    }
    results += BenchResult("FlatMap[Sale => Option[Double]]", rows, medianFlat, p95Flat)

    results.result()
  }

  def benchSchemaDerived(rows: Int): Vector[BenchResult] = {
    val results = Vector.newBuilder[BenchResult]

    // Schema.derived encode/decode round-trip
    val sales = Vector.fill(rows) {
      Sale(s"Product_${Random.nextInt(100)}", Random.nextInt(100), Random.nextDouble() * 1000.0)
    }

    val schema = summon[Schema[Sale]]

    val (medianEncode, p95Encode) = time(20, 50) {
      var i = 0
      while (i < sales.length) {
        schema.encode(sales(i))
        i += 1
      }
    }
    results += BenchResult("Schema.derived encode", rows, medianEncode, p95Encode)

    val encoded = sales.map(schema.encode)
    val (medianDecode, p95Decode) = time(20, 50) {
      var i = 0
      while (i < encoded.length) {
        schema.decode(encoded(i))
        i += 1
      }
    }
    results += BenchResult("Schema.derived decode", rows, medianDecode, p95Decode)

    // fromVector (full pipeline: encode + column build)
    val (medianFromVec, p95FromVec) = time(10, 30) {
      MaterializedDataset.fromVector(sales).toOption.get
    }
    results += BenchResult("MaterializedDataset.fromVector", rows, medianFromVec, p95FromVec)

    results.result()
  }

  def printResults(title: String, results: Vector[BenchResult]): Unit = {
    println(s"\n$title")
    println("-".repeat(70))
    printf("%-35s %8s %10s %10s%n", "Operation", "Rows", "Median", "P95")
    println("-".repeat(70))
    results.foreach { r =>
      printf("%-35s %8d %8.2f ms %8.2f ms%n", r.operation, r.rows, r.medianMs, r.p95Ms)
    }
  }

  def main(args: Array[String]): Unit = {
    println("=".repeat(70))
    println("Strongbow New Methods Sanity Benchmark")
    println("=".repeat(70))

    val smallScale = 10_000
    val medScale = 50_000

    // Dataset joins (smaller scale - nested loop is O(n*m))
    print("Running Dataset join benchmarks...")
    val joinResults = benchDatasetJoin(1_000, 100)
    printResults("Dataset Joins (1K sales x 100 products)", joinResults)

    // AggregateByKey
    print("Running aggregateByKey benchmarks...")
    val aggResults10k = benchAggregateByKey(smallScale)
    val aggResults50k = benchAggregateByKey(medScale)
    printResults(s"AggregateByKey ($smallScale rows)", aggResults10k)
    printResults(s"AggregateByKey ($medScale rows)", aggResults50k)

    // Map with Schema
    print("Running Map benchmarks...")
    val mapResults10k = benchMapWithSchema(smallScale)
    val mapResults50k = benchMapWithSchema(medScale)
    printResults(s"Map with Schema ($smallScale rows)", mapResults10k)
    printResults(s"Map with Schema ($medScale rows)", mapResults50k)

    // Schema.derived
    print("Running Schema.derived benchmarks...")
    val schemaResults = benchSchemaDerived(smallScale)
    printResults(s"Schema.derived ($smallScale rows)", schemaResults)

    println("\n" + "=".repeat(70))
    println("Benchmark complete.")
    println("=".repeat(70))
  }
}
