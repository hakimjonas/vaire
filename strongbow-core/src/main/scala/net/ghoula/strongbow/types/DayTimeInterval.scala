package net.ghoula.strongbow.types

/** Zero-cost day-time interval wrapper over microseconds as Long.
  *
  * Stores intervals as total microseconds. Matches Spark's DayTimeIntervalType internal
  * representation.
  */
opaque type DayTimeInterval = Long

object DayTimeInterval {
  inline def ofMicros(micros: Long): DayTimeInterval = micros

  extension (i: DayTimeInterval) {
    inline def toMicros: Long = i
  }

  given CanEqual[DayTimeInterval, DayTimeInterval] = CanEqual.derived
  given Ordering[DayTimeInterval] = Ordering.Long
}
