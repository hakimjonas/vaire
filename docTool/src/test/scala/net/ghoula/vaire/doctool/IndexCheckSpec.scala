package net.ghoula.vaire.doctool

import java.nio.file.{Files, Path, Paths}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The index gate's contract: every row matches its document's header, every document has one row,
  * and the audience vocabulary stays closed to `user` and `contributor`.
  *
  * Fixture paths are assembled rather than written literally, so this suite does not itself contain
  * references that the reference gate would have to resolve.
  */
class IndexCheckSpec extends AnyFlatSpec with Matchers {

  private def tempDir(): Path = Files.createTempDirectory("index-check-spec")

  private def header(status: String, audience: String): String =
    s"# Document\n\nStatus: $status\nAudience: $audience.\n"

  private def table(rows: (String, String, String)*): String =
    "# Documentation index\n\nStatus: current\nAudience: contributor.\n\n## Documents\n\n" +
      "| Document | Audience | Status |\n|---|---|---|\n" +
      rows.map { case (file, audience, status) => s"| [`$file`]($file) | $audience | $status |" }.mkString("\n") +
      "\n"

  private def write(root: Path, relative: String, text: String): Path = {
    val path = root.resolve(relative)
    Files.createDirectories(path.getParent)
    Files.writeString(path, text)
    path
  }

  private def index(root: Path, rows: (String, String, String)*): Path =
    write(root, "docs/README.md", table(rows*))

  private def check(root: Path): Either[String, IndexResult] =
    IndexCheck.check(root, Paths.get("docs/README.md"))

  private def messages(root: Path): Vector[String] =
    check(root).toOption.get.findings.map(_.message)

  "the checker" should "accept rows that match the document headers" in {
    val root = tempDir()
    index(root, ("README.md", "contributor", "current"), ("a.md", "user", "current"))
    write(root, "docs/a.md", header("current", "user"))
    check(root).map(_.ok) shouldBe Right(true)
  }

  it should "report a row whose file does not exist" in {
    val root = tempDir()
    index(root, ("README.md", "contributor", "current"), ("missing.md", "user", "current"))
    messages(root) should contain("docs/missing.md: row target does not exist")
  }

  it should "report a document with no row" in {
    val root = tempDir()
    index(root, ("README.md", "contributor", "current"))
    write(root, "docs/unlisted.md", header("current", "user"))
    messages(root) should contain("docs/unlisted.md: document has no row")
  }

  it should "report a duplicate row" in {
    val root = tempDir()
    index(root, ("README.md", "contributor", "current"), ("a.md", "user", "current"), ("a.md", "user", "current"))
    write(root, "docs/a.md", header("current", "user"))
    messages(root) should contain("docs/a.md: duplicate row")
  }

  it should "report a status mismatch with both values" in {
    val root = tempDir()
    index(root, ("README.md", "contributor", "current"), ("a.md", "user", "current"))
    write(root, "docs/a.md", header("authoritative", "user"))
    messages(root) should contain("docs/a.md: status: index says 'current', header says 'authoritative'")
  }

  it should "report an audience mismatch with both values" in {
    val root = tempDir()
    index(root, ("README.md", "contributor", "current"), ("a.md", "contributor", "current"))
    write(root, "docs/a.md", header("current", "user"))
    messages(root) should contain("docs/a.md: audience: index says 'contributor', header says 'user'")
  }

  it should "report a document missing one header line" in {
    val root = tempDir()
    index(root, ("README.md", "contributor", "current"), ("a.md", "user", "current"))
    write(root, "docs/a.md", "# Document\n\nStatus: current\n")
    messages(root) should contain("docs/a.md: missing Audience: line")
  }

  it should "report an audience outside the closed vocabulary" in {
    val root = tempDir()
    index(root, ("README.md", "contributor", "current"), ("a.md", "internal", "current"))
    write(root, "docs/a.md", header("current", "internal"))
    messages(root) should contain("docs/a.md: audience 'internal' is outside user | contributor")
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

  private def runIndex(path: Path): (Int, String) =
    captureOut(IndexCheckMain.run(Array(path.toString)))

  "the CLI decision" should "return zero for a clean index" in {
    IndexCheckMain.exitCode(Right(IndexResult(Vector.empty, 5, 5))) shouldBe 0
  }

  it should "return non-zero for findings or a failed read" in {
    val finding = IndexFinding("docs/README.md", 1, "drift")
    IndexCheckMain.exitCode(Right(IndexResult(Vector(finding), 1, 1))) should not be 0
    IndexCheckMain.exitCode(Left("cannot read docs/README.md")) should not be 0
  }

  "the CLI run" should "return zero for a matching temporary index" in {
    val root = tempDir()
    val path = index(root, ("README.md", "contributor", "current"), ("a.md", "user", "current"))
    write(root, "docs/a.md", header("current", "user"))
    val (code, output) = runIndex(path)
    code shouldBe 0
    output should include("index check ok:")
  }

  it should "return non-zero and print for a mismatched temporary index" in {
    val root = tempDir()
    val path = index(root, ("README.md", "contributor", "current"), ("a.md", "contributor", "current"))
    write(root, "docs/a.md", header("current", "user"))
    val (code, output) = runIndex(path)
    code should not be 0
    output should not be empty
    output should include("documentation index drift:")
  }
}
