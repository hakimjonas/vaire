package net.ghoula.vaire.types

/** Zero-cost time-of-day wrapper over microseconds since midnight.
  *
  * Stores time values as Long (microseconds since midnight, 0 to 86399999999), matching Spark's
  * TimeType precision boundary. Prevents accidental confusion with plain Long values.
  */
opaque type Time = Long

/** Construction and constants for Time. */
object Time {

  /** Midnight (the smallest representable time). */
  val MIN: Time = ofMicros(0L)

  /** The last microsecond before the next midnight. */
  val MAX: Time = ofMicros(86399999999L)

  /** A time from microseconds since midnight. */
  inline def ofMicros(micros: Long): Time = micros

  /** A time from nanoseconds since midnight (truncated to microseconds). */
  inline def ofNanoOfDay(nanos: Long): Time = nanos / 1000L

  /** A time from a java.time.LocalTime. */
  inline def fromLocalTime(lt: java.time.LocalTime): Time = lt.toNanoOfDay / 1000L

  extension (t: Time) {

    /** Microseconds since midnight. */
    inline def toMicros: Long = t

    /** Nanoseconds since midnight. */
    inline def toNanos: Long = t * 1000L

    /** The underlying java.time.LocalTime. */
    inline def toLocalTime: java.time.LocalTime = java.time.LocalTime.ofNanoOfDay(t * 1000L)
  }

  given CanEqual[Time, Time] = CanEqual.derived
  given Ordering[Time] = Ordering.Long
}
