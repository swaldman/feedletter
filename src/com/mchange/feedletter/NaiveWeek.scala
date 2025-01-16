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

  def weekStartWeekEndLocalDate( yearWeekStr : String ) : (LocalDate,LocalDate) =
    val ( year, week ) = yearWeekFromString( yearWeekStr )
    val startDayOfYear        = ((week - 1) * 7) + 1
    val endDayOfYearInclusive = startDayOfYear + 6
    val weekStart = LocalDate.ofYearDay(year, startDayOfYear)
    val weekEnd = LocalDate.ofYearDay(year, endDayOfYearInclusive)
    (weekStart, weekEnd)

