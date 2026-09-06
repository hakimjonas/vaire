package net.ghoula.vaire.types

/** Zero-cost day-time interval wrapper over microseconds as Long.
  *
  * Stores intervals as total microseconds. Matches Spark's DayTimeIntervalType internal
  * representation.
  */
opaque type DayTimeInterval = Long

/** Construction of day-time intervals. */
object DayTimeInterval {

  /** An interval from total microseconds. */
  inline def ofMicros(micros: Long): DayTimeInterval = micros

  extension (i: DayTimeInterval) {

    /** Total microseconds. */
    inline def toMicros: Long = i
  }

  given CanEqual[DayTimeInterval, DayTimeInterval] = CanEqual.derived
  given Ordering[DayTimeInterval] = Ordering.Long
}
