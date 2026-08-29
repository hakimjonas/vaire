package net.ghoula.strongbow.spark

import org.scalatest.{Suite, Tag}

object Benchmark extends Tag("net.ghoula.strongbow.Benchmark")

object Slow extends Tag("net.ghoula.strongbow.Slow")

/** Suite-level mixin that tags every test as `Slow`.
  *
  * Used for the heavy/long-running suites (5M-row stress, TPC-H end-to-end) that are excluded from
  * the fast CI gate (`FAST_TESTS=1`) and run in the dedicated `stress` workflow instead.
  */
trait SlowTests extends Suite {
  this: Suite =>

  override def tags: Map[String, Set[String]] =
    testNames.foldLeft(super.tags) { (acc, name) =>
      acc.updated(name, acc.getOrElse(name, Set.empty) + Slow.name)
    }
}
