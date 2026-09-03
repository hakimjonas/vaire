package net.ghoula.strongbow.types

/** Zero-cost binary data wrapper over Array[Byte].
  *
  * Distinguishes binary column values from individual Byte values (ByteColumn). Columnar storage
  * uses a flat layout — all binary values concatenated into one Array[Byte] with an offset array —
  * matching Arrow/Parquet variable-length binary representation.
  */
opaque type Binary = Array[Byte]

/** Construction of Binary values. */
object Binary {

  /** A Binary over the given bytes (no copy). */
  inline def apply(bytes: Array[Byte]): Binary = bytes

  extension (b: Binary) {

    /** The underlying bytes. */
    inline def toBytes: Array[Byte] = b

    /** The byte length. */
    inline def length: Int = b.length
  }

  given CanEqual[Binary, Binary] = CanEqual.derived
  given Ordering[Binary] = (a, b) => java.util.Arrays.compare(a, b)
}
