package net.ghoula.strongbow.types

/** Zero-cost row index wrapper.
  *
  * Opaque type compiles to raw Int at runtime—no allocation overhead.
  */
opaque type RowIndex = Int

/** Construction and arithmetic for RowIndex. */
object RowIndex {

  /** A row index over the given 0-based position. */
  inline def apply(i: Int): RowIndex = i

  extension (idx: RowIndex) {

    /** The raw 0-based position. */
    inline def toInt: Int = idx

    /** The position shifted forward. */
    inline def +(other: Int): RowIndex = idx + other

    /** The position shifted backward. */
    inline def -(other: Int): RowIndex = idx - other
  }
}
