package net.ghoula.strongbow.types

/** Zero-cost binary data wrapper over Array[Byte].
  *
  * Distinguishes binary column values from individual Byte values (ByteColumn). Columnar storage
  * uses a flat layout — all binary values concatenated into one Array[Byte] with an offset array —
  * matching Arrow/Parquet variable-length binary representation.
  */
opaque type Binary = Array[Byte]

object Binary {
  inline def apply(bytes: Array[Byte]): Binary = bytes

  extension (b: Binary) {
    inline def toBytes: Array[Byte] = b
    inline def length: Int = b.length
  }

  given CanEqual[Binary, Binary] = CanEqual.derived
  given Ordering[Binary] = (a, b) => java.util.Arrays.compare(a, b)
}
