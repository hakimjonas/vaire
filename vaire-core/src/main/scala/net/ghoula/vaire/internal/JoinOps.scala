package net.ghoula.vaire.internal

private[vaire] object JoinOps {

  def fullJoin[A, B](
    lefts: Vector[A],
    rights: Vector[B],
    condition: (A, B) => Boolean
  ): Vector[(Option[A], Option[B])] = {
    val matchesByLeft: Vector[Vector[Int]] =
      lefts.map(left => rights.indices.filter(rightIndex => condition(left, rights(rightIndex))).toVector)
    val matchedRight: Set[Int] = matchesByLeft.iterator.flatten.toSet

    val matchedPairs = lefts.iterator.zip(matchesByLeft.iterator).flatMap { case (left, matches) =>
      matches.iterator.map(rightIndex => (Option(left), Option(rights(rightIndex))))
    }
    val unmatchedLeft = lefts.iterator.zip(matchesByLeft.iterator).collect {
      case (left, matches) if matches.isEmpty => (Option(left), Option.empty[B])
    }
    val unmatchedRight = rights.iterator.zipWithIndex.collect {
      case (right, rightIndex) if !matchedRight.contains(rightIndex) => (Option.empty[A], Option(right))
    }

    (matchedPairs ++ unmatchedLeft ++ unmatchedRight).toVector
  }
}
