package net.ghoula.strongbow.types

/** Zero-cost column index wrapper.
  *
  * Opaque type compiles to raw Int at runtime—no allocation overhead.
  */
opaque type ColumnIndex = Int

/** Construction and arithmetic for ColumnIndex. */
object ColumnIndex {

  /** A column index over the given 0-based position. */
  inline def apply(i: Int): ColumnIndex = i

  extension (idx: ColumnIndex) {

    /** The raw 0-based position. */
    inline def toInt: Int = idx

    /** The position shifted forward. */
    inline def +(other: Int): ColumnIndex = idx + other

    /** The position shifted backward. */
    inline def -(other: Int): ColumnIndex = idx - other
  }
}
