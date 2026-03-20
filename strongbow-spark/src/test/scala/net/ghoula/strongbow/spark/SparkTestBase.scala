package net.ghoula.strongbow.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Shared test trait providing a singleton SparkSession for all Spark tests.
  *
  * By default, tests run in local[2] mode. To run against a Spark standalone cluster, pass
  * `-Dspark.test.master=spark://127.0.0.1:7077` to the JVM.
  */
trait SparkTestBase extends BeforeAndAfterAll { self: Suite =>

  private val master: String =
    sys.props.getOrElse("spark.test.master", "local[2]").nn

  private val isCluster: Boolean = master.startsWith("spark://")

  protected lazy val spark: SparkSession = {
    val builder = SparkSession
      .builder()
      .master(master)
      .appName("strongbow-test")
      .config("spark.sql.shuffle.partitions", "2")

    if (isCluster) {
      builder
        .config("spark.driver.host", "127.0.0.1")
        .config(
          "spark.executor.extraClassPath",
          "/opt/strongbow/core-classes:/opt/strongbow/spark-classes"
        )
        .config("spark.ui.enabled", "true")
    } else {
      builder.config("spark.ui.enabled", "false")
    }

    builder.getOrCreate()
  }

  protected lazy val sparkInterpreter: SparkInterpreter = SparkInterpreter(spark)

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }
}
