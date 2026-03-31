package net.ghoula.strongbow.types

/** Zero-cost decimal wrapper over unscaled Long value.
  *
  * Stores decimal values as their unscaled Long representation. The scale is column-level metadata
  * carried by DecimalColumn and ColumnType.DecimalType, not per-value. Covers precision up to 18
  * digits, matching Spark's internal Long optimization for DecimalType.
  */
opaque type Decimal = Long

object Decimal {
  inline def ofUnscaled(unscaled: Long): Decimal = unscaled

  extension (d: Decimal) {
    inline def toUnscaled: Long = d

    def toBigDecimal(scale: Int): java.math.BigDecimal =
      java.math.BigDecimal.valueOf(d, scale)
  }

  def fromBigDecimal(bd: java.math.BigDecimal): Option[Decimal] =
    try Some(ofUnscaled(bd.unscaledValue().longValueExact()))
    catch { case _: ArithmeticException => None }

  given CanEqual[Decimal, Decimal] = CanEqual.derived
  given Ordering[Decimal] = Ordering.Long
}
