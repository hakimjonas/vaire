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
}
