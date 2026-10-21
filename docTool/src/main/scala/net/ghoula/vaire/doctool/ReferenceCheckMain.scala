package net.ghoula.vaire.doctool

import java.nio.file.Paths

/** CLI for the reference gate.
  *
  * Walks every tracked text file, extracts path-shaped document and JSON references, and exits 1
  * when a target does not resolve from the repository root or from the containing file.
  */
object ReferenceCheckMain {

  def main(args: Array[String]): Unit = {
    val root = Paths.get("").toAbsolutePath.normalize()
    ReferenceCheck.trackedFiles(root) match {
      case Left(message) =>
        println(message)
        sys.exit(1)
      case Right(files) =>
        val result = ReferenceCheck.check(root, files)
        if result.findings.isEmpty then
          println(
            s"reference check ok: ${result.references} references in ${result.files} files resolve"
          )
        else {
          println("unresolved document references:")
          result.findings.foreach(finding => println(s"  ${finding.file}:${finding.line}: ${finding.target}"))
          sys.exit(1)
        }
    }
  }
}
