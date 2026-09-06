package net.ghoula.vaire.spark

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Singleton Spark session shared by the whole Spark test suite.
  *
  * One local SparkSession per JVM, reused across every suite — the same pattern as Spark's own
  * `SharedSparkContext`. Starting a SparkContext (~2-3s) is the dominant cost of this suite, so a
  * single shared session keeps the PR gate fast and its memory/CPU footprint small. The session is
  * stopped once, at JVM shutdown.
  */
object SparkTestSession {
  private val master: String =
    sys.props.getOrElse("spark.test.master", "local[2]").nn

  private val isCluster: Boolean = master.startsWith("spark://")

  private val initialized = new AtomicBoolean(false)

  private lazy val session: SparkSession = {
    val builder = SparkSession
      .builder()
      .master(master)
      .appName("vaire-test")
      .config("spark.sql.shuffle.partitions", "2")

    if (isCluster) {
      builder
        .config("spark.driver.host", "127.0.0.1")
        .config(
          "spark.executor.extraClassPath",
          "/opt/vaire/core-classes:/opt/vaire/spark-classes"
        )
        .config("spark.ui.enabled", "true")
    } else {
      builder.config("spark.ui.enabled", "false")
    }

    builder.getOrCreate()
  }

  Runtime.getRuntime.addShutdownHook(new Thread("vaire-spark-shutdown") {
    override def run(): Unit = if (initialized.get()) session.stop()
  })

  def spark: SparkSession = {
    initialized.set(true)
    session
  }
}

/** Shared test trait providing a singleton SparkSession for all Spark tests.
  *
  * By default, tests run in local[2] mode. To run against a Spark standalone cluster, pass
  * `-Dspark.test.master=spark://127.0.0.1:7077` to the JVM.
  */
trait SparkTestBase extends BeforeAndAfterAll { self: Suite =>

  protected lazy val spark: SparkSession = SparkTestSession.spark

  protected lazy val sparkInterpreter: SparkInterpreter = SparkInterpreter(spark)
}
