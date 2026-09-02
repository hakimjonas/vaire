package net.ghoula.strongbow.types

/** Zero-cost timestamp wrapper over epoch microseconds.
  *
  * Stores timestamps as Long (microseconds since Unix epoch), matching Spark's internal
  * representation. Prevents accidental confusion with plain Long values.
  */
opaque type Timestamp = Long

/** Construction of Timestamps. */
object Timestamp {

  /** A timestamp from microseconds since the epoch. */
  inline def ofEpochMicro(micros: Long): Timestamp = micros

  extension (t: Timestamp) {

    /** Microseconds since the epoch. */
    inline def toEpochMicro: Long = t
  }

  given CanEqual[Timestamp, Timestamp] = CanEqual.derived
  given Ordering[Timestamp] = Ordering.Long
}
