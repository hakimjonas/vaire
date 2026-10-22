package net.ghoula.vaire.doctool

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters.*
import scala.util.Try

/** One mismatch between the documentation index and a document's own header. */
final case class IndexFinding(index: String, line: Int, message: String) derives CanEqual

/** Result of one index-check run. */
final case class IndexResult(findings: Vector[IndexFinding], documents: Int, rows: Int) derives CanEqual {
  def ok: Boolean = findings.isEmpty
}

/** Checks the documentation index against the documents it lists.
  *
  * Every row of the index's document table is compared with the `Status:` and `Audience:` lines of
  * the file it names. A row with no file, a file under `docs/` with no row, a duplicate row, a
  * missing header line, or a value that differs from the row fails. The audience vocabulary is
  * closed to `user` and `contributor`, so the comparison needs no interpretation.
  */
object IndexCheck {

  private val AudienceValues = Set("user", "contributor")
  private val TableHeader = "| Document | Audience | Status |"
  private val Separator = """^:?-+:?$""".r
  private val MarkdownLink = """\[[^\]]*\]\(([^)]+)\)""".r
  private val InlineCode = """`([^`\n]+)`""".r

  private final case class Row(line: Int, document: String, target: String, audience: String, status: String)

  /** Check `indexPath`, resolved against `root`, against every Markdown file in its directory. */
  def check(root: Path, indexPath: Path): Either[String, IndexResult] = {
    val rootAbs = root.toAbsolutePath.normalize()
    val indexAbs = rootAbs.resolve(indexPath).normalize()
    Try(Files.readString(indexAbs, StandardCharsets.UTF_8)).toEither.left
      .map(error => s"cannot read ${rootAbs.relativize(indexAbs)}: ${error.getMessage}")
      .flatMap(text => parseRows(text).map(rows => compare(rootAbs, indexAbs, rows)))
  }

  private def compare(root: Path, index: Path, rows: Vector[Row]): IndexResult = {
    val indexRelative = root.relativize(index).toString
    val docsDir = index.getParent
    val findings = Vector.newBuilder[IndexFinding]
    val seen = scala.collection.mutable.Set.empty[Path]

    rows.foreach { row =>
      val target = if row.target.isEmpty then None else Some(docsDir.resolve(row.target).normalize())

      target match {
        case None =>
          findings += IndexFinding(indexRelative, row.line, s"${row.document}: row names no document")
        case Some(file) =>
          val relative = root.relativize(file).toString
          if seen.contains(file) then findings += IndexFinding(indexRelative, row.line, s"$relative: duplicate row")
          seen += file

          if !Files.isRegularFile(file) then
            findings += IndexFinding(indexRelative, row.line, s"$relative: row target does not exist")
          else
            Try(Files.readString(file, StandardCharsets.UTF_8)).toEither match {
              case Left(error) =>
                findings += IndexFinding(indexRelative, row.line, s"$relative: cannot read: ${error.getMessage}")
              case Right(text) =>
                compareField(findings, indexRelative, row, relative, text, "Status", row.status)
                compareField(findings, indexRelative, row, relative, text, "Audience", row.audience)
            }
      }
    }

    val listed = rows.flatMap(row => Option(row.target).filter(_.nonEmpty)).map(docsDir.resolve(_).normalize()).toSet
    documentsIn(docsDir)
      .filterNot(listed.contains)
      .foreach { file =>
        findings += IndexFinding(indexRelative, 0, s"${root.relativize(file)}: document has no row")
      }

    IndexResult(findings.result(), documentsIn(docsDir).size, rows.size)
  }

  private def compareField(
    findings: scala.collection.mutable.Builder[IndexFinding, Vector[IndexFinding]],
    indexRelative: String,
    row: Row,
    relative: String,
    text: String,
    field: String,
    expected: String
  ): Unit = {
    val actual = headerValue(text, field)
    val label = field.toLowerCase
    actual match {
      case None =>
        findings += IndexFinding(indexRelative, row.line, s"$relative: missing $field: line")
      case Some(value) =>
        if value != expected then
          findings += IndexFinding(
            indexRelative,
            row.line,
            s"$relative: $label: index says '$expected', header says '$value'"
          )
        if field == "Audience" && !AudienceValues.contains(value) then
          findings += IndexFinding(
            indexRelative,
            row.line,
            s"$relative: audience '$value' is outside user | contributor"
          )
    }
  }

  private def parseRows(text: String): Either[String, Vector[Row]] = {
    val lines = text.linesIterator.zipWithIndex.toVector
    val headerIndex = lines.indexWhere { case (line, _) => line.trim == TableHeader }
    if headerIndex < 0 then Left(s"cannot find the document table header '$TableHeader'")
    else {
      val table = lines.drop(headerIndex + 1).takeWhile { case (line, _) => line.trim.startsWith("|") }
      table.foldLeft[Either[String, Vector[Row]]](Right(Vector.empty)) { case (acc, (line, index)) =>
        acc.flatMap { rows =>
          val cells = line.trim.stripPrefix("|").stripSuffix("|").split("\\|", -1).map(_.trim).toVector
          if cells.forall(cell => Separator.matches(cell)) then Right(rows)
          else if cells.size != 3 then Left(s"${index + 1}: expected 3 columns, found ${cells.size}")
          else {
            val document = cells(0)
            val target = extractTarget(document).getOrElse("")
            Right(rows :+ Row(index + 1, document, target, cells(1), cells(2)))
          }
        }
      }
    }
  }

  private def extractTarget(cell: String): Option[String] =
    MarkdownLink
      .findFirstMatchIn(cell)
      .map(_.group(1).nn.trim)
      .orElse(InlineCode.findFirstMatchIn(cell).map(_.group(1).nn.trim))

  private def headerValue(text: String, field: String): Option[String] =
    text.linesIterator
      .map(_.trim)
      .collectFirst {
        case line if line.startsWith(s"$field:") =>
          line.drop(field.length + 1).trim.stripSuffix(".")
      }

  private def documentsIn(dir: Path): Vector[Path] =
    if !Files.isDirectory(dir) then Vector.empty
    else {
      val stream = Files.list(dir)
      try
        stream
          .iterator()
          .asScala
          .filter(path => Files.isRegularFile(path) && path.getFileName.toString.endsWith(".md"))
          .toVector
          .sortBy(_.toString)
      finally stream.close()
    }
}
