package net.ghoula.strongbow.types

/** Zero-cost time-of-day wrapper over microseconds since midnight.
  *
  * Stores time values as Long (microseconds since midnight, 0 to 86399999999), matching Spark's
  * TimeType precision boundary. Prevents accidental confusion with plain Long values.
  */
opaque type Time = Long

object Time {
  val MIN: Time = ofMicros(0L)
  val MAX: Time = ofMicros(86399999999L)

  inline def ofMicros(micros: Long): Time = micros

  inline def ofNanoOfDay(nanos: Long): Time = nanos / 1000L

  inline def fromLocalTime(lt: java.time.LocalTime): Time = lt.toNanoOfDay / 1000L

  extension (t: Time) {
    inline def toMicros: Long = t
    inline def toNanos: Long = t * 1000L
    inline def toLocalTime: java.time.LocalTime = java.time.LocalTime.ofNanoOfDay(t * 1000L)
  }

  given CanEqual[Time, Time] = CanEqual.derived
  given Ordering[Time] = Ordering.Long
}
