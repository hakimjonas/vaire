package net.ghoula.vaire.doctool

import java.nio.file.{Files, Path, Paths}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The coverage gate's contract: what counts as a member, what attaches a doc, and what the ratchet
  * lets through.
  */
class DocCoverageSpec extends AnyFlatSpec with Matchers {

  private val tempDir: Path = Files.createTempDirectory("doctool-spec")

  private def analyzeSrc(text: String): (FileReport, Vector[Violation]) = {
    val file = tempDir.resolve(s"Fixture${Integer.toHexString(text.hashCode)}.scala")
    Files.writeString(file, text)
    DocCoverage.analyzeFile(file.getFileName.toString, file) match {
      case Right(result) => result
      case Left(message) => fail(message)
    }
  }

  private def violationNames(vs: Vector[Violation]): Vector[String] = vs.map(_.member)

  "the member count" should "count documented and undocumented public defs" in {
    val (report, violations) = analyzeSrc(
      """/** documented. */
        |def good: Int = 1
        |
        |def bad: Int = 2
        |""".stripMargin
    )
    report shouldBe FileReport(1, 2)
    violationNames(violations) shouldBe Vector("bad")
  }

  it should "count classes, objects, traits, enums, cases, types and vals" in {
    val (report, violations) = analyzeSrc(
      """class C
        |object O
        |trait T
        |enum E {
        |  case A
        |  /** documented case. */
        |  case B
        |}
        |type Alias = Int
        |val value = 1
        |""".stripMargin
    )
    report.total shouldBe 8
    violationNames(violations).toSet shouldBe Set("C", "O", "T", "E", "A", "Alias", "value")
  }

  it should "count extension methods" in {
    val (report, violations) = analyzeSrc(
      """extension (i: Int) {
        |  /** documented. */
        |  def good: Int = i
        |  def bad: Int = i
        |}
        |""".stripMargin
    )
    report shouldBe FileReport(1, 2)
    violationNames(violations) shouldBe Vector("bad")
  }

  it should "not descend into def or val bodies" in {
    val (report, _) = analyzeSrc(
      """/** documented. */
        |def outer: Int = {
        |  def nestedHelper = 1
        |  val nestedValue = 2
        |  3
        |}
        |""".stripMargin
    )
    report shouldBe FileReport(1, 1)
  }

  it should "not count given instances or exports" in {
    val (report, violations) = analyzeSrc(
      """given intOrdering: Ordering[Int] = Ordering.Int
        |
        |class C {
        |  /** documented. */
        |  def m = 1
        |}
        |""".stripMargin
    )
    report shouldBe FileReport(1, 2)
    violationNames(violations) shouldBe Vector("C")
  }

  it should "skip members of private and qualified-private aggregates" in {
    val (report, _) = analyzeSrc(
      """private[doctool] object Internal {
        |  def helper = 1
        |}
        |
        |class Public {
        |  private case class Hidden(a: Int) {
        |    def alsoHidden = 2
        |  }
        |
        |  /** documented. */
        |  def m = 3
        |}
        |""".stripMargin
    )
    report shouldBe FileReport(1, 2)
  }

  it should "skip private, protected, override and implicit members" in {
    val (report, violations) = analyzeSrc(
      """class C {
        |  private def a = 1
        |  private[doctool] def b = 2
        |  protected def c = 3
        |  override def toString: String = "C"
        |  implicit def d(i: Int): Int = i
        |  /** documented. */
        |  def e = 4
        |}
        |""".stripMargin
    )
    report shouldBe FileReport(1, 2)
    violationNames(violations) shouldBe Vector("C")
  }

  "doc attachment" should "attach across annotations but not across code" in {
    val (report, violations) = analyzeSrc(
      """/** documented. */
        |@java.lang.Deprecated
        |def annotated: Int = 1
        |
        |/* block comment is not scaladoc */
        |def blockCommented: Int = 2
        |
        |// line comment is not scaladoc
        |def lineCommented: Int = 3
        |""".stripMargin
    )
    report shouldBe FileReport(1, 3)
    violationNames(violations).toSet shouldBe Set("blockCommented", "lineCommented")
  }

  it should "not let a previous member's doc leak through a closing brace" in {
    val (report, violations) = analyzeSrc(
      """class First {
        |  /** first's doc. */
        |  def m = 1
        |}
        |
        |class Second {
        |  def n = 2
        |}
        |""".stripMargin
    )
    violationNames(violations).toSet shouldBe Set("First", "Second", "n")
    report.total shouldBe 4
  }

  it should "require non-empty doc content" in {
    val (report, violations) = analyzeSrc(
      """/**/
        |def emptyDoc: Int = 1
        |
        |/**   */
        |def blankDoc: Int = 2
        |""".stripMargin
    )
    violationNames(violations).toSet shouldBe Set("emptyDoc", "blankDoc")
    report.total shouldBe 2
  }

  "the ratchet" should "reject new undocumented members" in {
    val baseline = Map("f.scala" -> FileReport(2, 2))
    val current = Map("f.scala" -> FileReport(2, 3))
    val result = DocCoverage.check(current, baseline)
    result.ok shouldBe false
    result.regressions shouldBe Vector("f.scala: 1 undocumented (baseline 0)")
  }

  it should "accept documented additions and improvements" in {
    val baseline = Map("f.scala" -> FileReport(2, 4))
    val improved = Map("f.scala" -> FileReport(4, 4))
    val added = Map("f.scala" -> FileReport(4, 5))
    DocCoverage.check(improved, baseline).ok shouldBe true
    DocCoverage.check(added, baseline).ok shouldBe true
  }

  it should "accept deleting documented members but reject trading docs for gaps" in {
    val baseline = Map("f.scala" -> FileReport(4, 4))
    val deleted = Map("f.scala" -> FileReport(3, 3))
    val traded = Map("f.scala" -> FileReport(3, 4))
    DocCoverage.check(deleted, baseline).ok shouldBe true
    DocCoverage.check(traded, baseline).ok shouldBe false
  }

  it should "fail any undocumented member in a file absent from the baseline" in {
    val result = DocCoverage.check(Map("new.scala" -> FileReport(3, 5)), Map.empty)
    result.ok shouldBe false
  }

  "the snapshot" should "round-trip" in {
    val reports = Map(
      "a.scala" -> FileReport(3, 5),
      "b/c.scala" -> FileReport(0, 1)
    )
    val path = Files.createTempFile("snapshot", ".json")
    DocCoverage.writeSnapshot(path, reports)
    DocCoverage.readSnapshot(path) shouldBe reports
  }

  private def sourceDir(text: String): Path = {
    val dir = Files.createTempDirectory("doc-coverage-cli-src")
    Files.writeString(dir.resolve("Fixture.scala"), text)
    dir
  }

  private def analysisOf(dir: Path): DocCoverage.Analysis =
    DocCoverage.analyze(Paths.get("").toAbsolutePath, Seq(dir)) match {
      case Right(analysis) => analysis
      case Left(message) => fail(message)
    }

  private def snapshotFor(reports: Map[String, FileReport]): Path = {
    val path = Files.createTempFile("doc-coverage-cli-snapshot", ".json")
    DocCoverage.writeSnapshot(path, reports)
    path
  }

  private def captureOut(body: => Int): (Int, String) = {
    val buffer = new java.io.ByteArrayOutputStream()
    val stream = new java.io.PrintStream(buffer, true, "UTF-8")
    try {
      val code = Console.withOut(stream)(body)
      stream.flush()
      (code, buffer.toString("UTF-8"))
    } finally stream.close()
  }

  private def run(args: String*): (Int, String) =
    captureOut(DocCoverageMain.run(args.toArray))

  "the CLI decision" should "return zero for snapshot written and coverage held" in {
    DocCoverageMain.exitCode(DocCoverageMain.Outcome.SnapshotWritten) shouldBe 0
    DocCoverageMain.exitCode(DocCoverageMain.Outcome.CoverageHeld) shouldBe 0
  }

  it should "return non-zero for usage error, analysis failure and regression" in {
    DocCoverageMain.exitCode(DocCoverageMain.Outcome.UsageError) should not be 0
    DocCoverageMain.exitCode(DocCoverageMain.Outcome.AnalysisFailed) should not be 0
    DocCoverageMain.exitCode(DocCoverageMain.Outcome.CoverageRegressed) should not be 0
  }

  "the CLI run" should "print usage and fail with no arguments" in {
    val (code, output) = run()
    code should not be 0
    output should not be empty
    output should include("usage: DocCoverageMain")
  }

  it should "print usage and fail with an unrecognised mode" in {
    val (code, output) = run("lint", "snapshot.json", "src")
    code should not be 0
    output should not be empty
    output should include("usage: DocCoverageMain")
  }

  it should "fail with non-empty output when a source cannot be analysed" in {
    val dir = sourceDir("object Broken {")
    val snapshot = snapshotFor(Map.empty)
    val (code, output) = run("check", snapshot.toString, dir.toString)
    code should not be 0
    output should not be empty
    output should include("parse failed")
  }

  it should "pass when the snapshot matches the sources" in {
    val dir = sourceDir("/** documented. */\ndef good: Int = 1\n")
    val snapshot = snapshotFor(analysisOf(dir).reports)
    val (code, output) = run("check", snapshot.toString, dir.toString)
    code shouldBe 0
    output should include("ratchet ok: coverage did not regress")
  }

  it should "fail with non-empty output when coverage regresses" in {
    val dir = sourceDir("def bad: Int = 1\n")
    val baseline = analysisOf(dir).reports.map { case (file, report) => file -> FileReport(report.total, report.total) }
    val snapshot = snapshotFor(baseline)
    val (code, output) = run("check", snapshot.toString, dir.toString)
    code should not be 0
    output should not be empty
    output should include("doc coverage regressed:")
  }

  it should "write the snapshot and pass in snapshot mode" in {
    val dir = sourceDir("def bad: Int = 1\n")
    val reports = analysisOf(dir).reports
    val path = Files.createTempDirectory("doc-coverage-cli-out").resolve("snapshot.json")
    val (code, output) = run("snapshot", path.toString, dir.toString)
    code shouldBe 0
    output should include("snapshot written:")
    DocCoverage.readSnapshot(path) shouldBe reports
  }
}
