package net.ghoula.vaire.types

/** Zero-cost date wrapper over java.time.LocalDate.
  *
  * Provides a vaire-native date type, so users never need to import java.time directly. Compiles to
  * java.time.LocalDate at runtime with no allocation overhead.
  */
opaque type Date = java.time.LocalDate

/** Construction and field access for Date. */
object Date {

  /** A date from calendar parts. */
  inline def apply(year: Int, month: Int, day: Int): Date = java.time.LocalDate.of(year, month, day)

  /** A date from days since the epoch. */
  inline def ofEpochDay(epochDay: Long): Date = java.time.LocalDate.ofEpochDay(epochDay)

  /** The current date. */
  inline def today: Date = java.time.LocalDate.now().nn

  /** Re-frames a java.time.LocalDate as a Date (no allocation). */
  inline def fromLocalDate(d: java.time.LocalDate): Date = d

  extension (d: Date) {

    /** The underlying java.time.LocalDate. */
    inline def toLocalDate: java.time.LocalDate = d

    /** Days since the epoch. */
    inline def toEpochDay: Long = d.toEpochDay

    /** The calendar year. */
    inline def getYear: Int = d.getYear

    /** The calendar month, 1-12. */
    inline def getMonthValue: Int = d.getMonthValue

    /** The day of month, 1-31. */
    inline def getDayOfMonth: Int = d.getDayOfMonth

    /** The day of week. */
    inline def getDayOfWeek: java.time.DayOfWeek = d.getDayOfWeek

    /** The 1-based day within the year. */
    inline def getDayOfYear: Int = d.getDayOfYear

    /** The number of days in the date's month. */
    inline def lengthOfMonth: Int = d.lengthOfMonth()

    /** Whether this date is strictly after `other`. */
    inline def isAfter(other: Date): Boolean = d.isAfter(other)

    /** Whether this date is strictly before `other`. */
    inline def isBefore(other: Date): Boolean = d.isBefore(other)

    /** The date shifted forward by the given days. */
    inline def plusDays(days: Long): Date = d.plusDays(days)

    /** The date shifted back by the given days. */
    inline def minusDays(days: Long): Date = d.minusDays(days)

    /** The date shifted by the given months, clamped to the month end. */
    inline def plusMonths(months: Long): Date = d.plusMonths(months)
  }

  given CanEqual[Date, Date] = CanEqual.derived
  given Ordering[Date] = Ordering.by(_.toEpochDay)
}
