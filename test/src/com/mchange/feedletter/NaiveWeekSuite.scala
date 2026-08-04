package com.mchange.feedletter

import java.time.{LocalDate,Year,ZoneId}

// NaiveWeek still backs the legacy Email.Weekly, so its two halves -- week(), which maps a
// date to a week number, and weekStartWeekEndLocalDate(), which maps a week number back to
// dates -- have to agree. They did not: the inverse ran a day late from week 2 onward, and
// threw outright for week 53. See the comment on weekStartWeekEndLocalDate.
class NaiveWeekSuite extends munit.FunSuite:

  private val Tz = ZoneId.of("America/New_York")

  private val years = List( 2024, 2025, 2026, 2027, 2028 ) // 2024 and 2028 are leap years

  private def daysOf( year : Int ) : Seq[LocalDate] =
    (1 to (if Year.isLeap(year) then 366 else 365)).map( LocalDate.ofYearDay( year, _ ) )

  private def span( year : Int, week : Int ) : (LocalDate,LocalDate) =
    NaiveWeek.weekStartWeekEndLocalDate( s"${year}-week${week}" )

  // the invariant that ties the two halves together, and that both original bugs violated
  test("every day falls within the span reported for its own week"):
    for
      year <- years
      date <- daysOf( year )
    do
      val week = NaiveWeek.week( date.atStartOfDay( Tz ) )
      val ( start, end ) = span( year, week )
      assert(
        !date.isBefore( start ) && !date.isAfter( end ),
        s"${date} is in week ${week}, but that week is reported as ${start}..${end}"
      )

  test("no week of any year throws, including the short week 53"):
    for
      year <- years
      date <- daysOf( year )
    do
      val week = NaiveWeek.week( date.atStartOfDay( Tz ) )
      span( year, week ) // would throw a DateTimeException for week 53 before the fix

  test("the year's first week is the short one, six days long"):
    assertEquals( span( 2027, 1 ), (LocalDate.parse("2027-01-01"), LocalDate.parse("2027-01-06")) )

  test("subsequent weeks are seven days, starting where the previous one left off"):
    assertEquals( span( 2027, 2 ), (LocalDate.parse("2027-01-07"), LocalDate.parse("2027-01-13")) )
    assertEquals( span( 2027, 3 ), (LocalDate.parse("2027-01-14"), LocalDate.parse("2027-01-20")) )

  test("week 53 is a stub, clamped to the end of the year"):
    assertEquals( span( 2027, 53 ), (LocalDate.parse("2027-12-30"), LocalDate.parse("2027-12-31")) ) // 365 days
    assertEquals( span( 2028, 53 ), (LocalDate.parse("2028-12-29"), LocalDate.parse("2028-12-31")) ) // 366 days

  test("consecutive weeks tile the year without gap or overlap"):
    for
      year <- years
      week <- 1 until 53
    do
      val ( _, end )       = span( year, week )
      val ( nextStart, _ ) = span( year, week + 1 )
      assertEquals( nextStart, end.plusDays(1), s"${year} week ${week} ends ${end}, but week ${week+1} starts ${nextStart}" )

  test("weeks 1 and 53 together account for exactly the days the other weeks do not"):
    years.foreach: year =>
      val ( firstStart, _ ) = span( year, 1 )
      val ( _, lastEnd )    = span( year, 53 )
      assertEquals( firstStart, LocalDate.ofYearDay( year, 1 ) )
      assertEquals( lastEnd, LocalDate.ofYearDay( year, if Year.isLeap(year) then 366 else 365 ) )

end NaiveWeekSuite
