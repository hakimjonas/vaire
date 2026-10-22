package net.ghoula.vaire.doctool

import java.nio.file.Paths

/** CLI for the documentation-index gate. Exits 1 when a row and its document disagree, when a row
  * names no file, when a document has no row, or when a row is duplicated.
  */
object IndexCheckMain {

  private val IndexPath = Paths.get("docs/README.md")

  /** The exit code for a completed index check. */
  def exitCode(result: Either[String, IndexResult]): Int =
    result match {
      case Right(checked) if checked.ok => 0
      case _ => 1
    }

  /** Run the index gate and print its report. */
  def run(args: Array[String]): Int = {
    val root = Paths.get("").toAbsolutePath.normalize()
    val indexPath = args.headOption.map(Paths.get(_)).getOrElse(IndexPath)
    val result = IndexCheck.check(root, indexPath)

    result match {
      case Left(message) =>
        println(message)
      case Right(checked) if checked.ok =>
        println(s"index check ok: ${checked.documents} documents match ${checked.rows} rows")
      case Right(checked) =>
        println("documentation index drift:")
        checked.findings.foreach { finding =>
          val location = if finding.line > 0 then s"${finding.index}:${finding.line}" else finding.index
          println(s"  $location: ${finding.message}")
        }
    }
    exitCode(result)
  }

  def main(args: Array[String]): Unit = sys.exit(run(args))
}
