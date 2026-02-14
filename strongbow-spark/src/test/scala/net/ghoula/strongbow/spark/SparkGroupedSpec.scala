package net.ghoula.strongbow.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import net.ghoula.strongbow.{Column, Dataset, DatasetInterpreter, Grouped, Schema}

/** Parity tests for Grouped operations through SparkInterpreter. */
class SparkGroupedSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  private def intDataset(values: Int*): Dataset[Int] = {
    val col = Column.int(values.toArray)
    Dataset.fromColumns(Vector(col), Schema.intSchema).toOption.get
  }

  "groupBy + toPairs" should "produce same results" in {
    given Schema[(Int, Int)] = Schema.tuple2Schema[Int, Int]
    val ds = intDataset(1, 2, 3, 1, 2)
    val plan = ds.groupBy(identity).toPairs

    val inMemory = DatasetInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  "groupBy + keys" should "produce same results" in {
    val ds = intDataset(1, 2, 3, 1, 2)
    val plan = ds.groupBy(x => x % 2).keys

    val inMemory = DatasetInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  "groupBy + values" should "produce same results" in {
    val ds = intDataset(1, 2, 3, 1, 2)
    val plan = ds.groupBy(x => x % 2).values

    val inMemory = DatasetInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  "reduceByKey" should "produce same results" in {
    given Schema[(Int, Int)] = Schema.tuple2Schema[Int, Int]
    val ds = intDataset(1, 2, 3, 1, 2)
    val plan = ds.groupBy(identity).reduceByKey(_ + _).toPairs

    val inMemory = DatasetInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  "mapValues" should "produce same results" in {
    given Schema[(Int, Int)] = Schema.tuple2Schema[Int, Int]
    val ds = intDataset(1, 2, 3)
    val plan = ds.groupBy(identity).mapValues(_ * 10).toPairs

    val inMemory = DatasetInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  "filterKeys" should "produce same results" in {
    given Schema[(Int, Int)] = Schema.tuple2Schema[Int, Int]
    val ds = intDataset(1, 2, 3, 4, 5)
    val plan = ds.groupBy(identity).filterKeys(_ > 3).toPairs

    val inMemory = DatasetInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  "flatMapValues" should "produce same results" in {
    given Schema[(Int, Int)] = Schema.tuple2Schema[Int, Int]
    val ds = intDataset(1, 2, 3)
    val plan = ds.groupBy(identity).flatMapValues(x => List(x, x * 10)).toPairs

    val inMemory = DatasetInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  "grouped innerJoin" should "produce same results" in {
    given Schema[(Int, (Int, Int))] = Schema.tuple2Schema[Int, (Int, Int)]
    val ds1 = intDataset(1, 2, 3)
    val ds2 = intDataset(2, 3, 4)
    val g1 = ds1.groupBy(identity)
    val g2 = ds2.groupBy(identity)
    val plan = g1.join(g2).toPairs

    val inMemory = DatasetInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  "grouped leftJoin" should "produce same results" in {
    given optSchema: Schema[Option[Int]] = Schema.optionSchema[Int]
    given tupleSchema: Schema[(Int, Option[Int])] = Schema.tuple2Schema[Int, Option[Int]]
    given pairSchema: Schema[(Int, (Int, Option[Int]))] = Schema.tuple2Schema[Int, (Int, Option[Int])]
    val ds1 = intDataset(1, 2, 3)
    val ds2 = intDataset(2, 3, 4)
    val plan = ds1.groupBy(identity).leftJoin(ds2.groupBy(identity)).toPairs

    val inMemory = DatasetInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  "grouped leftAntiJoin" should "produce same results" in {
    given Schema[(Int, Int)] = Schema.tuple2Schema[Int, Int]
    val ds1 = intDataset(1, 2, 3)
    val ds2 = intDataset(2, 3, 4)
    val plan = ds1.groupBy(identity).leftAntiJoin(ds2.groupBy(identity)).toPairs

    val inMemory = DatasetInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  "sortByKey" should "produce same results" in {
    given Schema[(Int, Int)] = Schema.tuple2Schema[Int, Int]
    val ds = intDataset(3, 1, 4, 1, 5)
    val plan = ds.groupBy(identity).sortByKey.toPairs

    val inMemory = DatasetInterpreter.execute(plan).map(_.toVectorUnsafe)
    val sparkResult = sparkInterpreter.execute(plan).map(_.toVectorUnsafe)
    sparkResult shouldBe inMemory
  }

  "union on Grouped" should "produce same results" in {
    given Schema[(Int, Int)] = Schema.tuple2Schema[Int, Int]
    val ds1 = intDataset(1, 2, 3)
    val ds2 = intDataset(4, 5, 6)
    val plan = (ds1.groupBy(identity) ++ ds2.groupBy(identity)).toPairs

    val inMemory = DatasetInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  "grouped rightJoin" should "produce same results" in {
    given optSchema: Schema[Option[Int]] = Schema.optionSchema[Int]
    given tupleSchema: Schema[(Option[Int], Int)] = Schema.tuple2Schema[Option[Int], Int]
    given pairSchema: Schema[(Int, (Option[Int], Int))] = Schema.tuple2Schema[Int, (Option[Int], Int)]
    val ds1 = intDataset(1, 2, 3)
    val ds2 = intDataset(2, 3, 4)
    val plan = ds1.groupBy(identity).rightJoin(ds2.groupBy(identity)).toPairs

    val inMemory = DatasetInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  "grouped fullJoin" should "produce same results" in {
    given optSchema: Schema[Option[Int]] = Schema.optionSchema[Int]
    given tupleSchema: Schema[(Option[Int], Option[Int])] = Schema.tuple2Schema[Option[Int], Option[Int]]
    given pairSchema: Schema[(Int, (Option[Int], Option[Int]))] = Schema.tuple2Schema[Int, (Option[Int], Option[Int])]
    val ds1 = intDataset(1, 2, 3)
    val ds2 = intDataset(2, 3, 4)
    val plan = ds1.groupBy(identity).fullJoin(ds2.groupBy(identity)).toPairs

    val inMemory = DatasetInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  "aggregateByKey2" should "produce same results" in {
    given Schema[(Int, Int)] = Schema.tuple2Schema[Int, Int]
    given Schema[(Int, (Int, Int))] = Schema.tuple2Schema[Int, (Int, Int)]
    val ds = intDataset(1, 2, 1, 2, 1)
    val plan = ds
      .groupBy(identity)
      .aggregateByKey[Int, Int](
        identity,
        _ => 1,
        _ + _,
        _ + _
      )
      .toPairs

    val inMemory = DatasetInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  "aggregateByKey3" should "produce same results" in {
    given Schema[(Int, Int)] = Schema.tuple2Schema[Int, Int]
    given Schema[(Int, Int, Int)] = Schema.derived
    given Schema[(Int, (Int, Int, Int))] = Schema.tuple2Schema[Int, (Int, Int, Int)]
    val ds = intDataset(1, 2, 1, 2, 1)
    val plan = ds
      .groupBy(identity)
      .aggregateByKey[Int, Int, Int](
        identity,
        _ => 1,
        identity,
        _ + _,
        _ + _,
        _ + _
      )
      .toPairs

    val inMemory = DatasetInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  "aggregateByKey4" should "produce same results" in {
    given Schema[(Int, Int)] = Schema.tuple2Schema[Int, Int]
    given Schema[(Int, Int, Int, Int)] = Schema.derived
    given Schema[(Int, (Int, Int, Int, Int))] = Schema.tuple2Schema[Int, (Int, Int, Int, Int)]
    val ds = intDataset(1, 2, 1, 2, 1)
    val plan = ds
      .groupBy(identity)
      .aggregateByKey[Int, Int, Int, Int](
        identity,
        _ => 1,
        identity,
        _ => 0,
        _ + _,
        _ + _,
        _ + _,
        _ + _
      )
      .toPairs

    val inMemory = DatasetInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }

  "aggregateByKey5" should "produce same results" in {
    given Schema[(Int, Int)] = Schema.tuple2Schema[Int, Int]
    given Schema[(Int, Int, Int, Int, Int)] = Schema.derived
    given Schema[(Int, (Int, Int, Int, Int, Int))] = Schema.tuple2Schema[Int, (Int, Int, Int, Int, Int)]
    val ds = intDataset(1, 2, 1, 2, 1)
    val plan = ds
      .groupBy(identity)
      .aggregateByKey[Int, Int, Int, Int, Int](
        identity,
        _ => 1,
        identity,
        _ => 0,
        _ => 10,
        _ + _,
        _ + _,
        _ + _,
        _ + _,
        _ + _
      )
      .toPairs

    val inMemory = DatasetInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    val sparkResult = sparkInterpreter.execute(plan).map(_.toVectorUnsafe.sorted)
    sparkResult shouldBe inMemory
  }
}
