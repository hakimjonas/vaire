package net.ghoula.vaire.types

/** Zero-cost timezone-free timestamp wrapper over epoch microseconds.
  *
  * Like Timestamp but without timezone context. Matches Spark's TimestampNTZType.
  */
opaque type TimestampNTZ = Long

/** Construction of zone-less Timestamps. */
object TimestampNTZ {

  /** A timestamp from microseconds since the epoch (local wall-clock semantics). */
  inline def ofEpochMicro(micros: Long): TimestampNTZ = micros

  extension (t: TimestampNTZ) {

    /** Microseconds since the epoch. */
    inline def toEpochMicro: Long = t
  }

  given CanEqual[TimestampNTZ, TimestampNTZ] = CanEqual.derived
  given Ordering[TimestampNTZ] = Ordering.Long
}
