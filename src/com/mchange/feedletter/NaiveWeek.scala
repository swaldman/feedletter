package com.mchange.feedletter

import java.time.*
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField.{DAY_OF_YEAR, YEAR}

// initially I tried to use java.time's WeekFields, but in subtle ways
// it didn't behave as I expect.
//
// this is naive, final weeks will be short, but it's clear and predictable!

object NaiveWeek:
  private val PartialFormatter = DateTimeFormatter.ofPattern("yyyy-'week'")

  def week( zdt : ZonedDateTime ) : Int =
    val dayOfYear = zdt.get( DAY_OF_YEAR )
    (dayOfYear / 7) + 1

  def format( zdt : ZonedDateTime ) : String = PartialFormatter.format(zdt) + f"${week(zdt)}%02d" // zero-padded week-of-year

  def yearWeekFromZonedDateTime( zdt : ZonedDateTime ) : ( Int, Int ) =
    val year = zdt.get( YEAR )
    val weeknum = week( zdt )
    ( year, weeknum )

  def yearWeekFromString( naiveWeekStr : String ) : ( Int, Int ) =
    val ( yearStr, restStr ) = naiveWeekStr.span( Character.isDigit )
    val year = yearStr.toInt
    val week = restStr.dropWhile( c => !Character.isDigit(c) ).toInt
    ( year, week )

  // This is the inverse of week(), and the two are easy to drift apart. They did.
  //
  // week() is (dayOfYear / 7) + 1 under integer division. Because day-of-year is
  // 1-based rather than 0-based, the year's first bucket comes out short: days 1-6 are
  // week 1, and the first full seven-day bucket is days 7-13. Inverting
  // floor(d/7) + 1 == w gives 7(w-1) <= d <= 7(w-1)+6, so week w runs from day 7w-7
  // through day 7w-1.
  //
  // Two of those endpoints fall outside the year and must be clamped rather than simply
  // computed: week 1 would begin on day 0, and week 53 would end on day 370. Neither
  // exists, and LocalDate.ofYearDay throws on both.
  //
  // The original version wrote the start as 7(w-1)+1. That clamps week 1's start
  // correctly -- which is presumably why it was written that way -- but it pushes every
  // other week a day late at both ends, and it left week 53's end at day 371, past the
  // end of every year, leap or not. That threw a DateTimeException, reachable from
  // Email.Weekly.defaultSubject in the last days of December whenever no item in a batch
  // carried a pubDate. Because assignables are completed inside a single transaction
  // spanning all of them, one such throw rolled back every completion in the pass, and
  // did so deterministically on each retry.
  def weekStartWeekEndLocalDate( yearWeekStr : String ) : (LocalDate,LocalDate) =
    val ( year, week ) = yearWeekFromString( yearWeekStr )
    val lastDayOfYear         = if Year.isLeap( year ) then 366 else 365
    val startDayOfYear        = ((week - 1) * 7).max( 1 ).min( lastDayOfYear )
    val endDayOfYearInclusive = ((week * 7) - 1).max( startDayOfYear ).min( lastDayOfYear )
    val weekStart = LocalDate.ofYearDay(year, startDayOfYear)
    val weekEnd = LocalDate.ofYearDay(year, endDayOfYearInclusive)
    (weekStart, weekEnd)

