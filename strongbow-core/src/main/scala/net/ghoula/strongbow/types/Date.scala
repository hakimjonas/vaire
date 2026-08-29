package net.ghoula.strongbow.types

/** Zero-cost date wrapper over java.time.LocalDate.
  *
  * Provides a strongbow-native date type, so users never need to import java.time directly.
  * Compiles to java.time.LocalDate at runtime with no allocation overhead.
  */
opaque type Date = java.time.LocalDate

object Date {
  inline def apply(year: Int, month: Int, day: Int): Date = java.time.LocalDate.of(year, month, day)
  inline def ofEpochDay(epochDay: Long): Date = java.time.LocalDate.ofEpochDay(epochDay)
  inline def today: Date = java.time.LocalDate.now().nn

  inline def fromLocalDate(d: java.time.LocalDate): Date = d

  extension (d: Date) {
    inline def toLocalDate: java.time.LocalDate = d
    inline def toEpochDay: Long = d.toEpochDay
    inline def getYear: Int = d.getYear
    inline def getMonthValue: Int = d.getMonthValue
    inline def getDayOfMonth: Int = d.getDayOfMonth
    inline def getDayOfWeek: java.time.DayOfWeek = d.getDayOfWeek
    inline def getDayOfYear: Int = d.getDayOfYear
    inline def lengthOfMonth: Int = d.lengthOfMonth()
    inline def isAfter(other: Date): Boolean = d.isAfter(other)
    inline def isBefore(other: Date): Boolean = d.isBefore(other)
    inline def plusDays(days: Long): Date = d.plusDays(days)
    inline def minusDays(days: Long): Date = d.minusDays(days)
    inline def plusMonths(months: Long): Date = d.plusMonths(months)
  }

  given CanEqual[Date, Date] = CanEqual.derived
  given Ordering[Date] = Ordering.by(_.toEpochDay)
}
