package net.ghoula.vaire.doctool

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import scala.sys.process.Process
import scala.util.Try

/** One document reference that does not resolve. */
final case class ReferenceFinding(file: String, line: Int, target: String) derives CanEqual

/** Result of one reference-check run. */
final case class ReferenceResult(findings: Vector[ReferenceFinding], references: Int, files: Int) derives CanEqual

/** Checks path-shaped references in tracked text files.
  *
  * A reference is either a Markdown link target ending in a covered extension, or an inline-code
  * token that contains `/` and ends in a covered extension. Documents and JSON data files are
  * covered; bare mentions such as `KeyIndex.scala` are prose, not paths, and are ignored. The
  * extension is anchored at the end of the token, so `cell.md5` is not a `cell.md` hit.
  */
object ReferenceCheck {

  private val DocumentExtension = """(?i)\.(?:md|markdown|rst|txt|json)$""".r
  private val InlineCode = """`([^`\n]+)`""".r
  private val MarkdownLink = """\[[^\]]*\]\(([^)]+)\)""".r
  private val LineSuffix = """:\d+(?:-\d+)?$""".r
  private val Fence = "```"

  private val TextExtensions =
    Set(
      "md",
      "markdown",
      "rst",
      "txt",
      "scala",
      "java",
      "sbt",
      "yml",
      "yaml",
      "json",
      "conf",
      "sh",
      "properties",
      "xml"
    )

  /** Every tracked file under `root`, as absolute paths. */
  def trackedFiles(root: Path): Either[String, Vector[Path]] =
    Try(Process(Seq("git", "-C", root.toString, "ls-files", "-z"), root.toFile).!!).toEither.left
      .map(error => s"git ls-files failed: ${error.getMessage}")
      .map { output =>
        output
          .split('\u0000')
          .iterator
          .filter(_.nonEmpty)
          .map(root.resolve)
          .filter(Files.isRegularFile(_))
          .toVector
      }

  /** Check every reference in `files`; paths are reported relative to `root`. */
  def check(root: Path, files: Seq[Path]): ReferenceResult = {
    val rootAbs = root.toAbsolutePath.normalize()
    val analyzed = files
      .filter(isTextFile)
      .filter(Files.isRegularFile(_))
      .toVector
      .map(file => file -> referencesIn(Files.readString(file, StandardCharsets.UTF_8)))

    val findings = analyzed.flatMap { case (file, references) =>
      val relative = rootAbs.relativize(file.toAbsolutePath.normalize()).toString
      references.flatMap { case (line, target) =>
        if resolve(rootAbs, file, target).isEmpty then Some(ReferenceFinding(relative, line, target))
        else None
      }
    }

    ReferenceResult(findings, analyzed.map(_._2.size).sum, analyzed.size)
  }

  /** Line number and normalized target for each reference in `text`. */
  def referencesIn(text: String): Vector[(Int, String)] = {
    val found = Vector.newBuilder[(Int, String)]
    var inFence = false

    text.linesIterator.zipWithIndex.foreach { case (line, index) =>
      if line.trim.startsWith(Fence) then inFence = !inFence
      else if !inFence then {
        InlineCode.findAllMatchIn(line).foreach { matched =>
          val token = matched.group(1).nn.trim
          val normalized = normalize(token)
          if token.contains("/") && !token.exists(_.isWhitespace) && isDocument(normalized) then
            found += ((index + 1, normalized))
        }
        MarkdownLink.findAllMatchIn(line).foreach { matched =>
          val raw = matched.group(1).nn
          val normalized = normalize(raw)
          if isDocument(normalized) && !isStructural(raw) then found += ((index + 1, normalized))
        }
      }
    }

    found.result().distinct
  }

  /** Resolve `target` against the containing file and the repository root. */
  def resolve(root: Path, file: Path, target: String): Option[Path] = {
    val clean = normalize(target)
    val candidates = Vector(
      Option(file.toAbsolutePath.normalize().getParent).map(_.resolve(clean).normalize()),
      Some(root.resolve(clean).normalize())
    ).flatten
    candidates.find(path => path.startsWith(root) && Files.exists(path))
  }

  private def isTextFile(path: Path): Boolean = {
    val name = path.getFileName.toString
    val dot = name.lastIndexOf('.')
    dot >= 0 && TextExtensions.contains(name.substring(dot + 1).toLowerCase)
  }

  private def isDocument(target: String): Boolean =
    DocumentExtension.findFirstIn(target).isDefined

  private def isStructural(raw: String): Boolean = {
    val target = raw.trim
    target.isEmpty ||
    target.startsWith("#") ||
    target.startsWith("http://") ||
    target.startsWith("https://") ||
    target.startsWith("mailto:") ||
    target.exists(_.isWhitespace) ||
    target.contains("*") ||
    target.contains("{") ||
    target.contains("<")
  }

  private def normalize(raw: String): String = {
    val withoutAngles = raw.trim.stripPrefix("<").stripSuffix(">")
    val withoutAnchor = withoutAngles.takeWhile(_ != '#')
    val withoutLine = LineSuffix.replaceFirstIn(withoutAnchor, "")
    withoutLine.trim.stripSuffix(",").stripSuffix(";").stripSuffix(".")
  }
}
