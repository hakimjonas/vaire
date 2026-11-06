package net.ghoula.vaire.doctool

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters.*
import scala.util.Try
import scala.meta.*
import scala.meta.dialects.Scala3

/** One typed rejection site found in `src/main`. */
final case class RejectionSite(file: String, line: Int, message: String) derives CanEqual

/** One entry in the checked claim ledger. */
final case class Claim(message: String, classification: String, reason: Option[String]) derives CanEqual

/** The claim ledger's contract.
  *
  * The class is: a typed rejection that tells the caller the executing backend does not provide an
  * **expression or dataset transformation** — an expression case, a generator, a window form, a
  * Spark-only function family, or a conversion the other backend cannot express. Rejections outside
  * that class are still listed with `class: "excluded"` and a reason, so the ledger is complete
  * against every string literal passed to `UnsupportedOperation` or `UnsupportedExpression` in
  * either module's `src/main`.
  *
  * The check compares in both directions: a source message missing from the ledger fails, and a
  * ledger message with no source site fails.
  */
object ClaimLedger {

  final case class Result(
    sites: Vector[RejectionSite],
    claims: Vector[Claim],
    missingFromLedger: Vector[RejectionSite],
    listedWithoutSite: Vector[Claim],
    duplicateClaims: Vector[String],
    invalidClaims: Vector[String],
    inClassSites: Int,
    inClassMessages: Int,
    excludedSites: Int,
    excludedMessages: Int
  ) derives CanEqual {
    def ok: Boolean =
      missingFromLedger.isEmpty && listedWithoutSite.isEmpty && duplicateClaims.isEmpty && invalidClaims.isEmpty
    def totalSites: Int = sites.size
    def totalMessages: Int = claims.size
  }

  /** Extract every rejection site from the Scala sources under `dirs`. */
  def extract(root: Path, dirs: Seq[Path]): Either[String, Vector[RejectionSite]] = {
    val files = dirs.toVector.flatMap { dir =>
      val absolute = root.resolve(dir)
      if Files.isDirectory(absolute) then
        Files
          .walk(absolute)
          .iterator()
          .asScala
          .filter(path => Files.isRegularFile(path) && path.toString.endsWith(".scala"))
          .toVector
      else Vector.empty
    }

    files.foldLeft[Either[String, Vector[RejectionSite]]](Right(Vector.empty)) { (acc, file) =>
      acc.flatMap { sites =>
        parseFile(root, file).map(parsed => sites ++ parsed)
      }
    }
  }

  /** Load the checked ledger. */
  def load(root: Path, path: Path): Either[String, Vector[Claim]] =
    Try {
      val json = ujson.read(Files.readString(root.resolve(path), StandardCharsets.UTF_8))
      json("messages").arr.toVector.map { entry =>
        val reason = entry.obj.get("reason").map(_.str)
        Claim(entry("message").str, entry("class").str, reason)
      }
    }.toEither.left.map(error => s"cannot read ${path}: ${error.getMessage}")

  /** Compare sites against the ledger. */
  def check(sites: Vector[RejectionSite], claims: Vector[Claim]): Result = {
    val siteMessages = sites.map(_.message).toSet
    val claimMessages = claims.map(_.message)
    val claimSet = claimMessages.toSet
    val sitesByMessage = sites.groupBy(_.message)

    val missingFromLedger = sites.filterNot(site => claimSet.contains(site.message))
    val listedWithoutSite = claims.filterNot(claim => siteMessages.contains(claim.message))
    val duplicateClaims =
      claimMessages.groupBy(identity).collect { case (message, values) if values.size > 1 => message }.toVector.sorted
    val invalidClaims = claims.flatMap { claim =>
      if claim.classification != "in-class" && claim.classification != "excluded" then
        Some(s"${claim.message}: unknown class '${claim.classification}'")
      else if claim.classification == "excluded" && claim.reason.forall(_.trim.isEmpty) then
        Some(s"${claim.message}: excluded without a reason")
      else None
    }

    val inClass = claims.filter(_.classification == "in-class")
    val excluded = claims.filter(_.classification == "excluded")
    Result(
      sites = sites,
      claims = claims,
      missingFromLedger = missingFromLedger,
      listedWithoutSite = listedWithoutSite,
      duplicateClaims = duplicateClaims,
      invalidClaims = invalidClaims,
      inClassSites = inClass.map(claim => sitesByMessage.getOrElse(claim.message, Vector.empty).size).sum,
      inClassMessages = inClass.size,
      excludedSites = excluded.map(claim => sitesByMessage.getOrElse(claim.message, Vector.empty).size).sum,
      excludedMessages = excluded.size
    )
  }

  private def parseFile(root: Path, file: Path): Either[String, Vector[RejectionSite]] = {
    val relative = root.relativize(file.toAbsolutePath.normalize()).toString
    val input = Input.VirtualFile(relative, Files.readString(file, StandardCharsets.UTF_8))
    input.parse[Source] match {
      case Parsed.Success(tree) =>
        val sites = tree.collect {
          case application: Term.Apply if isRejection(application.fun) =>
            application.argClause.values.headOption.flatMap(messageOf) match {
              case Some(message) =>
                Some(RejectionSite(relative, application.pos.startLine + 1, message))
              case None =>
                Some(RejectionSite(relative, application.pos.startLine + 1, "<non-literal argument>"))
            }
        }.flatten
        Right(sites.toVector)
      case error: Parsed.Error =>
        Left(s"cannot parse $relative: ${error.message}")
    }
  }

  private def isRejection(term: Tree): Boolean = term match {
    case Term.Select(_, Term.Name(name)) =>
      name == "UnsupportedOperation" || name == "UnsupportedExpression"
    case Term.Name(name) =>
      name == "UnsupportedOperation" || name == "UnsupportedExpression"
    case _ => false
  }

  private def messageOf(term: Tree): Option[String] = term match {
    case Lit.String(value) => Some(value)
    case Term.Interpolate(_, parts, args) =>
      def text(part: Lit): String = part match {
        case Lit.String(value) => value
        case _ => ""
      }
      Some(
        text(parts.head) + args
          .zip(parts.tail)
          .map { case (argument, part) =>
            val syntax = argument.syntax
            val inner =
              if syntax.startsWith("{") && syntax.endsWith("}") then syntax.drop(1).dropRight(1)
              else syntax
            "${" + inner + "}" + text(part)
          }
          .mkString
      )
    case _ => None
  }
}
