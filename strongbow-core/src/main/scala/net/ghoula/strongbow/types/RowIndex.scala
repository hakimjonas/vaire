package net.ghoula.strongbow.types

/** Zero-cost row index wrapper.
  *
  * Opaque type compiles to raw Int at runtime—no allocation overhead.
  */
opaque type RowIndex = Int

object RowIndex {
  inline def apply(i: Int): RowIndex = i

  extension (idx: RowIndex) {
    inline def toInt: Int = idx
    inline def +(other: Int): RowIndex = idx + other
    inline def -(other: Int): RowIndex = idx - other
  }
}
