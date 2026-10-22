package net.ghoula.vaire.doctool

import java.nio.file.{Files, Path, Paths}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The claim ledger's contract: source extraction, both completeness directions, and the class
  * counts.
  */
class ClaimLedgerSpec extends AnyFlatSpec with Matchers {

  private val tempDir: Path = Files.createTempDirectory("claim-ledger-spec")

  private def writeSource(text: String): Path = {
    val file = tempDir.resolve("src/Fixture.scala")
    Files.createDirectories(file.getParent)
    Files.writeString(file, text)
    file
  }

  "the extractor" should "find plain and interpolated rejection messages" in {
    writeSource(
      """package fixture
        |
        |object Fixture {
        |  def a = Left(ExecutionError.UnsupportedOperation("plain message"))
        |  def b(fn: String) =
        |    Left(ExecutionError.UnsupportedOperation(s"$fn requires the in-memory interpreter"))
        |  def c(other: Any) =
        |    Left(ExecutionError.UnsupportedExpression(s"Unsupported window expr: $other"))
        |}
        |""".stripMargin
    )

    val sites = ClaimLedger.extract(tempDir, Seq(Paths.get("src"))) match {
      case Right(found) => found
      case Left(error) => fail(error)
    }

    sites.map(_.message) shouldBe Vector(
      "plain message",
      "${fn} requires the in-memory interpreter",
      "Unsupported window expr: ${other}"
    )
    sites.map(_.line) shouldBe Vector(4, 6, 8)
  }

  it should "report a parse failure instead of a partial list" in {
    writeSource("object Broken {")
    ClaimLedger.extract(tempDir, Seq(Paths.get("src"))) match {
      case Left(message) => message should include("cannot parse")
      case Right(sites) => fail(s"expected a parse error, got $sites")
    }
  }

  "the completeness check" should "report a source message missing from the ledger" in {
    val sites = Vector(
      RejectionSite("a.scala", 1, "A"),
      RejectionSite("b.scala", 2, "B")
    )
    val result = ClaimLedger.check(sites, Vector(Claim("A", "in-class", None)))

    result.missingFromLedger.map(_.message) shouldBe Vector("B")
    result.listedWithoutSite shouldBe empty
    result.ok shouldBe false
  }

  it should "report a ledger message with no source site" in {
    val sites = Vector(RejectionSite("a.scala", 1, "A"))
    val result = ClaimLedger.check(sites, Vector(Claim("A", "in-class", None), Claim("C", "in-class", None)))

    result.missingFromLedger shouldBe empty
    result.listedWithoutSite.map(_.message) shouldBe Vector("C")
    result.ok shouldBe false
  }

  it should "reject duplicate claims and excluded claims without a reason" in {
    val claims = Vector(
      Claim("A", "in-class", None),
      Claim("A", "in-class", None),
      Claim("B", "excluded", None)
    )
    val result = ClaimLedger.check(Vector(RejectionSite("a.scala", 1, "A"), RejectionSite("a.scala", 2, "B")), claims)

    result.duplicateClaims shouldBe Vector("A")
    result.invalidClaims shouldBe Vector("B: excluded without a reason")
    result.ok shouldBe false
  }

  it should "count sites and messages per class" in {
    val sites = Vector(
      RejectionSite("a.scala", 1, "A"),
      RejectionSite("a.scala", 2, "A"),
      RejectionSite("b.scala", 1, "B")
    )
    val claims = Vector(
      Claim("A", "in-class", None),
      Claim("B", "excluded", Some("not a capability"))
    )
    val result = ClaimLedger.check(sites, claims)

    result.inClassSites shouldBe 2
    result.inClassMessages shouldBe 1
    result.excludedSites shouldBe 1
    result.excludedMessages shouldBe 1
    result.totalSites shouldBe 3
    result.totalMessages shouldBe 2
    result.ok shouldBe true
  }

  private def repoRoot: Path = Paths.get("").toAbsolutePath.normalize()

  private def sourceMessages: Vector[String] =
    ClaimLedger.extract(
      repoRoot,
      Seq(Paths.get("vaire-core/src/main"), Paths.get("vaire-spark/src/main"))
    ) match {
      case Right(sites) => sites.map(_.message).distinct
      case Left(error) => fail(error)
    }

  private def ledger(messages: Seq[String]): Path = {
    val entries = messages.map(message => ujson.Obj("message" -> ujson.Str(message), "class" -> ujson.Str("in-class")))
    val file = Files.createTempFile("claim-ledger-cli", ".json")
    Files.writeString(file, ujson.Obj("messages" -> ujson.Arr(entries*)).render())
    file
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

  "the CLI decision" should "return zero for a successful ledger check" in {
    ClaimLedgerMain.exitCode(Right(ClaimLedger.check(Vector.empty, Vector.empty))) shouldBe 0
  }

  it should "return non-zero for a failed ledger check" in {
    ClaimLedgerMain.exitCode(Left("cannot read ledger")) should not be 0
    ClaimLedgerMain.exitCode(
      Right(ClaimLedger.check(Vector(RejectionSite("a.scala", 1, "A")), Vector.empty))
    ) should not be 0
  }

  "the CLI run" should "return zero for a complete temporary ledger" in {
    val file = ledger(sourceMessages)
    val (code, output) = captureOut(ClaimLedgerMain.run(Array(file.toString)))
    code shouldBe 0
    output should include("claim ledger ok:")
  }

  it should "return non-zero and print when a source message is missing" in {
    val file = ledger(sourceMessages.tail)
    val (code, output) = captureOut(ClaimLedgerMain.run(Array(file.toString)))
    code should not be 0
    output should not be empty
    output should include("source messages missing from")
  }

  it should "return non-zero and print when a ledger message has no source site" in {
    val file = ledger(sourceMessages :+ "message-with-no-source-site")
    val (code, output) = captureOut(ClaimLedgerMain.run(Array(file.toString)))
    code should not be 0
    output should not be empty
    output should include("listed messages with no source site")
  }
}
