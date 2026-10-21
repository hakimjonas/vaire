package net.ghoula.vaire.doctool

import java.nio.file.Paths

/** CLI for the reference gate.
  *
  * Walks every tracked text file, extracts path-shaped document and JSON references, and exits 1
  * when a target does not resolve from the repository root or from the containing file.
  */
object ReferenceCheckMain {

  /** The exit code for a completed scan. */
  def exitCode(result: Either[String, ReferenceResult]): Int =
    result match {
      case Right(checked) if checked.findings.isEmpty => 0
      case _ => 1
    }

  /** Run the gate and print its report. */
  def run(args: Array[String]): Int = {
    val _ = args
    val root = Paths.get("").toAbsolutePath.normalize()
    val result = ReferenceCheck.trackedFiles(root).map(files => ReferenceCheck.check(root, files))

    result match {
      case Left(message) =>
        println(message)
      case Right(checked) if checked.findings.isEmpty =>
        println(
          s"reference check ok: ${checked.references} references in ${checked.files} files resolve"
        )
      case Right(checked) =>
        println("unresolved document references:")
        checked.findings.foreach(finding => println(s"  ${finding.file}:${finding.line}: ${finding.target}"))
    }
    exitCode(result)
  }

  def main(args: Array[String]): Unit = sys.exit(run(args))
}
