package net.ghoula.vaire.types

/** Zero-cost decimal wrapper over unscaled Long value.
  *
  * Stores decimal values as their unscaled Long representation. The scale is column-level metadata
  * carried by DecimalColumn and ColumnType.DecimalType, not per-value. Covers precision up to 18
  * digits, matching Spark's internal Long optimization for DecimalType.
  */
opaque type Decimal = Long

/** Construction of Decimal values. */
object Decimal {

  /** A decimal from its unscaled Long value. */
  inline def ofUnscaled(unscaled: Long): Decimal = unscaled

  extension (d: Decimal) {

    /** The unscaled Long representation. */
    inline def toUnscaled: Long = d

    /** The value as a BigDecimal scaled by the column's scale. */
    def toBigDecimal(scale: Int): java.math.BigDecimal =
      java.math.BigDecimal.valueOf(d, scale)
  }

  /** A decimal from a BigDecimal at the given scale (must fit 18-digit precision). */
  def fromBigDecimal(bd: java.math.BigDecimal): Option[Decimal] =
    try Some(ofUnscaled(bd.unscaledValue().longValueExact()))
    catch { case _: ArithmeticException => None }

  given CanEqual[Decimal, Decimal] = CanEqual.derived
  given Ordering[Decimal] = Ordering.Long
}
