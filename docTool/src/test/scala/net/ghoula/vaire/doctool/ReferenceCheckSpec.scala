package net.ghoula.vaire.doctool

import java.nio.file.{Files, Path}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The reference gate's contract: path-shaped references only, extension anchored, relative to the
  * containing file and the repository root.
  *
  * Fixture syntax is assembled rather than written literally, so this suite does not itself contain
  * references that the gate would have to resolve.
  */
class ReferenceCheckSpec extends AnyFlatSpec with Matchers {

  private val tempDir: Path = Files.createTempDirectory("reference-check-spec")

  private def code(target: String): String = "`" + target + "`"

  private def link(target: String): String = "[x](" + target + ")"

  private def fenced(body: String): String = "```\n" + body + "\n```"

  private def write(relative: String, text: String): Path = {
    val path = tempDir.resolve(relative)
    Files.createDirectories(path.getParent)
    Files.writeString(path, text)
    path
  }

  "the extractor" should "ignore an extension inside a longer token" in {
    ReferenceCheck.referencesIn(code("cell.md5")) shouldBe empty
  }

  it should "ignore bare Scala mentions in prose" in {
    ReferenceCheck.referencesIn(code("KeyIndex.scala") + " and " + code("JoinOperationsBench.scala")) shouldBe empty
  }

  it should "ignore path-shaped Scala references because the gate covers documents" in {
    ReferenceCheck.referencesIn(
      code("vaire-core/src/main/scala/net/ghoula/vaire/internal/KeyedJoin.scala")
    ) shouldBe empty
  }

  it should "find a path-shaped document in inline code" in {
    ReferenceCheck.referencesIn("see " + code("docs/missing.md")) shouldBe Vector((1, "docs/missing.md"))
  }

  it should "strip a line suffix and an anchor" in {
    ReferenceCheck.referencesIn(code("docs/missing.md:12") + " " + code("docs/backend-claims.md#claims")) shouldBe
      Vector((1, "docs/missing.md"), (1, "docs/backend-claims.md"))
  }

  it should "find a Markdown link target but not an anchor-only link" in {
    ReferenceCheck.referencesIn(link("missing.md") + " " + link("#section")) shouldBe
      Vector((1, "missing.md"))
  }

  it should "skip fenced code blocks" in {
    ReferenceCheck.referencesIn(fenced(link("missing.md") + " " + code("docs/missing.md"))) shouldBe empty
  }

  "the checker" should "resolve a link relative to the containing file" in {
    write("docs/b.md", "b")
    val file = write("docs/sub/a.md", link("../b.md"))
    ReferenceCheck.check(tempDir, Vector(file)).findings shouldBe empty
  }

  it should "resolve a path-shaped reference from the repository root" in {
    write("docs/b.md", "b")
    val file = write("docs/a.md", code("docs/b.md"))
    ReferenceCheck.check(tempDir, Vector(file)).findings shouldBe empty
  }

  it should "report the file, line and target when a link does not resolve" in {
    val file = write("docs/a.md", "intro\n" + link("missing.md") + "\n")
    ReferenceCheck.check(tempDir, Vector(file)).findings shouldBe
      Vector(ReferenceFinding("docs/a.md", 2, "missing.md"))
  }

  it should "accept the corrected issue-template README path" in {
    write("README.md", "root")
    val file = write(".github/ISSUE_TEMPLATE/bug_report.yml", link("../../README.md"))
    ReferenceCheck.check(tempDir, Vector(file)).findings shouldBe empty
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

  "the CLI decision" should "return zero when the scan has no findings" in {
    ReferenceCheckMain.exitCode(Right(ReferenceResult(Vector.empty, 21, 184))) shouldBe 0
  }

  it should "return non-zero for findings or a failed scan" in {
    val finding = ReferenceFinding("docs/missing.md", 1, "missing.md")
    ReferenceCheckMain.exitCode(Right(ReferenceResult(Vector(finding), 1, 1))) should not be 0
    ReferenceCheckMain.exitCode(Left("git ls-files failed")) should not be 0
  }

  // The finding branch of ReferenceCheckMain.run cannot be reached from this spec: it needs a
  // tracked file whose reference does not resolve, and the spec does not create one in the working
  // tree. The decision function above covers the exit code; the branch's formatting stays covered
  // by the checker's own tests, which feed findings directly to ReferenceCheck.check.
  "the CLI run" should "return zero on the working tree" in {
    val (code, output) = captureOut(ReferenceCheckMain.run(Array.empty))
    code shouldBe 0
    output should include("reference check ok:")
  }
}
