package net.ghoula.vaire.doctool

import java.nio.file.Paths

/** CLI for the scaladoc coverage gate. Modes:
  *   - `check <snapshotPath> <sourceDirs...>` — ratchet compare against the snapshot; exits 1 on
  *     regression.
  *   - `snapshot <snapshotPath> <sourceDirs...>` — rewrite the snapshot with current counts.
  *
  * Paths resolve against the working directory (the sbt launcher's directory, the repo root).
  */
object DocCoverageMain {

  /** What one invocation produced. */
  enum Outcome derives CanEqual {
    case UsageError, AnalysisFailed, SnapshotWritten, CoverageHeld, CoverageRegressed
  }

  /** The exit code for a completed invocation. */
  def exitCode(outcome: Outcome): Int =
    outcome match {
      case Outcome.SnapshotWritten | Outcome.CoverageHeld => 0
      case Outcome.UsageError | Outcome.AnalysisFailed | Outcome.CoverageRegressed => 1
    }

  /** Run the coverage gate and print its report. */
  def run(args: Array[String]): Int = {
    val outcome = args.toList match {
      case mode :: snapshotPath :: dirs if dirs.nonEmpty && (mode == "check" || mode == "snapshot") =>
        val root = Paths.get("").toAbsolutePath
        DocCoverage.analyze(root, dirs.map(root.resolve)) match {
          case Left(message) =>
            println(message)
            Outcome.AnalysisFailed
          case Right(analysis) =>
            val snapshot = root.resolve(snapshotPath)
            val result = DocCoverage.check(analysis.reports, DocCoverage.readSnapshot(snapshot))
            println(result.summary)
            mode match {
              case "snapshot" =>
                DocCoverage.writeSnapshot(snapshot, analysis.reports)
                println(s"snapshot written: ${snapshot.getFileName} (${analysis.reports.size} files)")
                Outcome.SnapshotWritten
              case "check" if result.ok =>
                println("ratchet ok: coverage did not regress")
                Outcome.CoverageHeld
              case "check" =>
                printRegressions(result, analysis.violations)
                Outcome.CoverageRegressed
            }
        }

      case _ =>
        println("usage: DocCoverageMain <check|snapshot> <snapshotPath> <sourceDirs...>")
        Outcome.UsageError
    }
    exitCode(outcome)
  }

  def main(args: Array[String]): Unit = sys.exit(run(args))

  private def printRegressions(
    result: DocCoverage.CheckResult,
    violations: Map[String, Vector[Violation]]
  ): Unit = {
    println("doc coverage regressed:")
    result.regressions.foreach(file => println(s"  $file"))
    val regressed = result.regressions.map(_.takeWhile(_ != ':'))
    regressed.foreach { file =>
      violations.get(file).foreach { vs =>
        vs.take(12).foreach { v =>
          println(s"    - ${v.member} (line ${v.line})")
        }
        if vs.length > 12 then println(s"    ... and ${vs.length - 12} more")
      }
    }
  }
}
