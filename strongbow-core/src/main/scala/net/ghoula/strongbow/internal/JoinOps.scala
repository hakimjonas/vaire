package net.ghoula.strongbow.internal

private[strongbow] object JoinOps {

  def fullJoin[A, B](
    lefts: Vector[A],
    rights: Vector[B],
    condition: (A, B) => Boolean
  ): Vector[(Option[A], Option[B])] = {
    val pairs = for { l <- lefts; r <- rights; if condition(l, r) } yield (l, r)
    val matchedLefts = pairs.map(_._1).toSet
    val matchedRights = pairs.map(_._2).toSet
    val inner = pairs.map((l, r) => (Some(l), Some(r)))
    val unmatchedLeft = lefts.filterNot(matchedLefts.contains).map(l => (Some(l), None))
    val unmatchedRight = rights.filterNot(matchedRights.contains).map(r => (None, Some(r)))
    inner ++ unmatchedLeft ++ unmatchedRight
  }
}
