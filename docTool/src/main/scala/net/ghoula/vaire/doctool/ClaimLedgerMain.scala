package net.ghoula.vaire.doctool

import java.nio.file.Paths

/** CLI for the backend-claim ledger. Exits 1 when a source rejection is missing from the checked
  * list, when a listed message has no source site, or when the list is malformed.
  */
object ClaimLedgerMain {

  private val SourceDirs = Vector(Paths.get("vaire-core/src/main"), Paths.get("vaire-spark/src/main"))
  private val LedgerPath = "backend-claims.json"

  def main(args: Array[String]): Unit = {
    val root = Paths.get("").toAbsolutePath.normalize()
    val ledgerPath = args.headOption.map(Paths.get(_)).getOrElse(Paths.get(LedgerPath))
    val result: Either[String, ClaimLedger.Result] = for {
      sites <- ClaimLedger.extract(root, SourceDirs)
      claims <- ClaimLedger.load(root, ledgerPath)
    } yield ClaimLedger.check(sites, claims)

    result match {
      case Left(message) =>
        println(message)
        sys.exit(1)
      case Right(checked) if checked.ok =>
        println(
          s"claim ledger ok: ${checked.inClassSites} in-class sites / ${checked.inClassMessages} messages; " +
            s"${checked.excludedSites} excluded sites / ${checked.excludedMessages} messages " +
            s"(${checked.totalSites} sites / ${checked.totalMessages} messages)"
        )
      case Right(checked) =>
        if checked.missingFromLedger.nonEmpty then {
          println(s"claim ledger: source messages missing from $ledgerPath:")
          checked.missingFromLedger.foreach(site => println(s"  ${site.file}:${site.line}: ${site.message}"))
        }
        if checked.listedWithoutSite.nonEmpty then {
          println(s"claim ledger: listed messages with no source site:")
          checked.listedWithoutSite.foreach(claim => println(s"  ${claim.message}"))
        }
        if checked.duplicateClaims.nonEmpty then {
          println("claim ledger: duplicate entries:")
          checked.duplicateClaims.foreach(message => println(s"  $message"))
        }
        if checked.invalidClaims.nonEmpty then {
          println("claim ledger: invalid entries:")
          checked.invalidClaims.foreach(message => println(s"  $message"))
        }
        sys.exit(1)
    }
  }
}
