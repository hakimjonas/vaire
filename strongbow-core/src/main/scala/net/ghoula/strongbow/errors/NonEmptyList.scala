package net.ghoula.strongbow.errors

/** Non-empty list for error accumulation.
  *
  * Ensures at least one error is present, avoiding invalid empty error states.
  */
final case class NonEmptyList[+A](head: A, tail: List[A]) {
  def toList: List[A] = head :: tail

  def map[B](f: A => B): NonEmptyList[B] = {
    NonEmptyList(f(head), tail.map(f))
  }

  def ++[B >: A](other: NonEmptyList[B]): NonEmptyList[B] = {
    NonEmptyList(head, tail ++ (other.head :: other.tail))
  }
}

object NonEmptyList {
  def one[A](a: A): NonEmptyList[A] = NonEmptyList(a, Nil)

  def fromListUnsafe[A](list: List[A]): NonEmptyList[A] = list match {
    case h :: t => NonEmptyList(h, t)
    case Nil => throw new IllegalArgumentException("Cannot create NonEmptyList from empty list")
  }

  def fromList[A](list: List[A]): Option[NonEmptyList[A]] = list match {
    case h :: t => Some(NonEmptyList(h, t))
    case Nil => None
  }
}
