package net.ghoula.vaire.doctool

import java.nio.file.Paths

/** CLI for the documentation-index gate. Exits 1 when a row and its document disagree, when a row
  * names no file, when a document has no row, or when a row is duplicated.
  */
object IndexCheckMain {

  private val IndexPath = Paths.get("docs/README.md")

  def main(args: Array[String]): Unit = {
    val root = Paths.get("").toAbsolutePath.normalize()
    val indexPath = args.headOption.map(Paths.get(_)).getOrElse(IndexPath)
    IndexCheck.check(root, indexPath) match {
      case Left(message) =>
        println(message)
        sys.exit(1)
      case Right(result) if result.ok =>
        println(s"index check ok: ${result.documents} documents match ${result.rows} rows")
      case Right(result) =>
        println("documentation index drift:")
        result.findings.foreach { finding =>
          val location = if finding.line > 0 then s"${finding.index}:${finding.line}" else finding.index
          println(s"  $location: ${finding.message}")
        }
        sys.exit(1)
    }
  }
}
