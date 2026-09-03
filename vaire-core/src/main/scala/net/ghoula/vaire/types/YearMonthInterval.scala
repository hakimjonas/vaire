package net.ghoula.vaire.types

/** Zero-cost year-month interval wrapper over months as Int.
  *
  * Stores intervals as total months. Matches Spark's YearMonthIntervalType internal representation.
  */
opaque type YearMonthInterval = Int

/** Construction of year-month intervals. */
object YearMonthInterval {

  /** An interval from total months. */
  inline def ofMonths(months: Int): YearMonthInterval = months

  extension (i: YearMonthInterval) {

    /** Total months. */
    inline def toMonths: Int = i
  }

  given CanEqual[YearMonthInterval, YearMonthInterval] = CanEqual.derived
  given Ordering[YearMonthInterval] = Ordering.Int
}
