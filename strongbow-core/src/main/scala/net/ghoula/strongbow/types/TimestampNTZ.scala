package net.ghoula.strongbow.types

/** Zero-cost timezone-free timestamp wrapper over epoch microseconds.
  *
  * Like Timestamp but without timezone context. Matches Spark's TimestampNTZType.
  */
opaque type TimestampNTZ = Long

object TimestampNTZ {
  inline def ofEpochMicro(micros: Long): TimestampNTZ = micros

  extension (t: TimestampNTZ) {
    inline def toEpochMicro: Long = t
  }

  given CanEqual[TimestampNTZ, TimestampNTZ] = CanEqual.derived
  given Ordering[TimestampNTZ] = Ordering.Long
}
