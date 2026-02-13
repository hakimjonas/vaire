package net.ghoula.strongbow.types

/** Zero-cost column index wrapper.
  *
  * Opaque type compiles to raw Int at runtime—no allocation overhead.
  */
opaque type ColumnIndex = Int

object ColumnIndex {
  inline def apply(i: Int): ColumnIndex = i

  extension (idx: ColumnIndex) {
    inline def toInt: Int = idx
    inline def +(other: Int): ColumnIndex = idx + other
    inline def -(other: Int): ColumnIndex = idx - other
  }
}
