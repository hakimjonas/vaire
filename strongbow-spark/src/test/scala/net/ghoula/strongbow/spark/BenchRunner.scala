package net.ghoula.strongbow.spark

import org.scalatest.tools.Runner

object BenchRunner {
  def main(args: Array[String]): Unit = {
    val suites = args match {
      case a if a.isEmpty => Array("-s", "net.ghoula.strongbow.spark.SparkComparativeBench")
      case _ => args.flatMap(s => Array("-s", s))
    }
    val exitCode = Runner.run(suites ++ Array("-oDF"))
    if (!exitCode) sys.exit(1)
  }
}
