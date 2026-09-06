package net.ghoula.vaire

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.io.Source
import scala.jdk.CollectionConverters.*

/** Pins the code-standard claims published in the README (Key Properties section): zero
  * `var`/`throw`/`return` statements in production sources, and every `asInstanceOf` site carrying
  * an explicit scalafix suppression at a documented erasure boundary. If this spec fails, either
  * the code drifted from the published claims or the README needs updating — both are audit
  * findings, not test bugs.
  */
class SourcePolicySpec extends AnyFlatSpec with Matchers {

  private val repoRoot: java.io.File = {
    // sbt's test working directory may be the module base or the repo root depending on
    // invocation; walk up until the two source trees are both visible.
    val start = Option(java.nio.file.Paths.get(System.getProperty("user.dir")).toAbsolutePath)
    val chain = Iterator
      .iterate(start)(_.flatMap(p => Option(p.getParent)))
      .takeWhile(_.isDefined)
      .flatten
    chain
      .find(c =>
        java.nio.file.Files.exists(c.resolve("vaire-core/src/main")) &&
          java.nio.file.Files.exists(c.resolve("vaire-spark/src/main"))
      )
      .get
      .toFile
  }

  private def mainSources: Vector[java.io.File] = {
    val roots = Vector(
      java.nio.file.Paths.get(repoRoot.getPath, "vaire-core/src/main"),
      java.nio.file.Paths.get(repoRoot.getPath, "vaire-spark/src/main")
    )
    roots.flatMap { root =>
      val stream = java.nio.file.Files.walk(root)
      try
        stream
          .iterator()
          .asScala
          .filter(p => p.toString.endsWith(".scala"))
          .map(_.toFile)
          .toVector
      finally stream.close()
    }
  }

  private def lines(f: java.io.File): Vector[String] = {
    val src = Source.fromFile(f, "UTF-8")
    try src.getLines().toVector
    finally src.close()
  }

  private def importsScalaConvert(s: String): Boolean = s.contains("scala.collection.convert")

  "production sources" should "contain no var, throw or return statements" in {
    val offenders = mainSources.flatMap { f =>
      lines(f).zipWithIndex.collect {
        case (line, idx) if line.matches("\\s*(var |throw |return ).*") =>
          s"${f.getName}:${idx + 1} $line"
      }
    }
    withClue(s"offenders: ${offenders.mkString(", ")}") { offenders shouldBe empty }
  }

  it should "carry a scalafix suppression on every asInstanceOf code site" in {
    val offenders = mainSources.flatMap { f =>
      lines(f).zipWithIndex.collect {
        // comment lines may discuss the policy; only code lines carry the boundary
        case (line, idx)
            if line.contains("asInstanceOf") && !line.trim.startsWith("//") && !line.trim.startsWith("*") &&
              !line.contains("scalafix:ok DisableSyntax.asInstanceOf") =>
          s"${f.getName}:${idx + 1} ${line.trim}"
      }
    }
    withClue(s"offenders: ${offenders.mkString(", ")}") { offenders shouldBe empty }
  }

  it should "not import scala.collection.convert except where the Spark boundary requires it" in {
    // The only sanctioned use is the Spark interpreter wiring; anywhere else it is a silent
    // implicit-conversion hole that defeats strictEquality.
    val offenders = mainSources.flatMap { f =>
      lines(f).zipWithIndex.collect {
        case (line, idx) if importsScalaConvert(line) && !f.getName.contains("SparkInterpreter") =>
          s"${f.getName}:${idx + 1} ${line.trim}"
      }
    }
    withClue(s"offenders: ${offenders.mkString(", ")}") { offenders shouldBe empty }
  }
}
