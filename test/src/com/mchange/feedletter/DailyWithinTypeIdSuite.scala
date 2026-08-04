package com.mchange.feedletter

import java.time.{LocalDate,Year,ZoneId}
import java.util.Locale

// Email.Daily writes a within-type-id with one function and reads it back with another, so
// the two have to agree. They did not: the formatter used 'YYYY', the week-based year, while
// isComplete reads the calendar year back via ChronoField.YEAR. See the comments on
// Daily.WtiFormatter and Daily.dayLocalDate.
class DailyWithinTypeIdSuite extends munit.FunSuite:

  private val Tz = ZoneId.of("America/New_York")

  private val daily = SubscriptionManager.Email.Daily(
    from                              = Destination.Email( "newsletter@example.com", None ),
    replyTo                           = None,
    composeUntemplateName             = "com.example.Compose",
    confirmUntemplateName             = "com.example.Confirm",
    statusChangeUntemplateName        = "com.example.StatusChange",
    removalNotificationUntemplateName = "com.example.RemovalNotification",
    timeZone                          = None,
    extraParams                       = Map.empty
  )

  private val years = List( 2024, 2025, 2026, 2027, 2028 ) // 2024 and 2028 are leap years

  private def daysOf( year : Int ) : Seq[LocalDate] =
    (1 to (if Year.isLeap(year) then 366 else 365)).map( LocalDate.ofYearDay( year, _ ) )

  // the invariant the YYYY bug violated: a key must name the day it was written for
  test("a within-type-id round trips back to the date that produced it"):
    for
      year <- years
      date <- daysOf( year )
    do
      val wti = daily.dayWithinTypeId( date.atStartOfDay( Tz ) )
      assertEquals( daily.dayLocalDate( wti ), date, s"round trip failed for ${date} via '${wti}'" )

  // week-based-year is defined by WeekFields, so 'YYYY' silently varied with the JVM's
  // locale. The calendar year does not.
  test("within-type-ids do not depend on the default locale"):
    val saved = Locale.getDefault( Locale.Category.FORMAT )
    try
      val boundaryDates = List( "2024-12-29", "2024-12-31", "2027-01-01", "2027-01-03" ).map( LocalDate.parse )
      val perLocale =
        List( Locale.US, Locale.UK, Locale.FRANCE, Locale.JAPAN ).map: loc =>
          Locale.setDefault( Locale.Category.FORMAT, loc )
          boundaryDates.map( d => daily.dayWithinTypeId( d.atStartOfDay( Tz ) ) )
      assertEquals( perLocale.distinct.size, 1, s"within-type-ids varied by locale: ${perLocale}" )
    finally
      Locale.setDefault( Locale.Category.FORMAT, saved )

  test("the year in a within-type-id is the calendar year, even in the week owning January 1"):
    // 2024-12-29 is a Sunday; under US week rules its week already belongs to 2025
    assertEquals( daily.dayWithinTypeId( LocalDate.parse("2024-12-29").atStartOfDay( Tz ) ), "2024-day364" )
    assertEquals( daily.dayWithinTypeId( LocalDate.parse("2024-12-31").atStartOfDay( Tz ) ), "2024-day366" )
    // 2027-01-01 is a Friday; under ISO week rules its week still belongs to 2026
    assertEquals( daily.dayWithinTypeId( LocalDate.parse("2027-01-01").atStartOfDay( Tz ) ), "2027-day01" )

  test("the sample within-type-id matches the format actually written"):
    assertEquals( daily.sampleWithinTypeId, daily.dayWithinTypeId( LocalDate.parse("2024-01-04").atStartOfDay( Tz ) ) )
    assertEquals( daily.dayLocalDate( daily.sampleWithinTypeId ), LocalDate.parse("2024-01-04") )

  // keys written before the fix can pair one year's number with another year's day-of-year
  test("a legacy within-type-id with an out-of-range day yields a date rather than throwing"):
    // written 2024-12-31 under the old YYYY formatter; 2025 is not a leap year
    assertEquals( daily.dayLocalDate( "2025-day366" ), LocalDate.parse("2025-12-31") )
    assertEquals( daily.dayFormattedIsoLocal( "2025-day366" ), "2025-12-31" )

  test("legacy keys are clamped at both ends, for any year"):
    assertEquals( daily.dayLocalDate( "2025-day999" ), LocalDate.parse("2025-12-31") )
    assertEquals( daily.dayLocalDate( "2024-day999" ), LocalDate.parse("2024-12-31") ) // leap, 366 days
    assertEquals( daily.dayLocalDate( "2025-day0" ),   LocalDate.parse("2025-01-01") )

  test("defaultSubject renders for every day of every year, and for legacy keys"):
    val name = SubscribableName("news")
    val url  = FeedUrl("https://example.com/feed")
    for
      year <- years
      date <- daysOf( year )
    do
      val wti = daily.dayWithinTypeId( date.atStartOfDay( Tz ) )
      assertEquals( daily.defaultSubject( name, wti, url, Seq.empty, Tz ), s"[news] All posts, ${date}" )
    // the key that used to throw inside route(), stalling completion for every subscribable
    assertEquals( daily.defaultSubject( name, "2025-day366", url, Seq.empty, Tz ), "[news] All posts, 2025-12-31" )

end DailyWithinTypeIdSuite
