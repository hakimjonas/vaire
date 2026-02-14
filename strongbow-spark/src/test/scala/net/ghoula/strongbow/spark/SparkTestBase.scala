package net.ghoula.strongbow.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Shared test trait providing a singleton SparkSession for all Spark tests. */
trait SparkTestBase extends BeforeAndAfterAll { self: Suite =>

  protected lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("strongbow-test")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  protected lazy val sparkInterpreter: SparkInterpreter = SparkInterpreter(spark)

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }
}
