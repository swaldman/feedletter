package com.mchange.feedletter

import java.time.{DayOfWeek,LocalDateTime,LocalTime,ZonedDateTime}
import java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME
import java.time.temporal.TemporalAdjusters

/**
  * Week arithmetic for SubscriptionManager.Email.BetterWeekly.
  *
  * Unlike NaiveWeek, weeks here are not anchored to January 1. They are anchored to an
  * operator-chosen day-of-week and time-of-day, so every period is exactly one week of
  * wall-clock time and always ends on the same weekday, forever.
  *
  * Everything is computed in LocalDateTime and never resolved to an Instant. That is
  * deliberate: it makes a period an honest wall-clock week (167 or 169 real hours across
  * a daylight-savings transition, which is what "every Monday at 9am" means to a reader),
  * and it means a boundary time that does not exist on a spring-forward day -- 02:30 in
  * much of the US -- never has to be resolved and so never poses a problem.
  *
  * Periods are half-open, [start, end). The period *end* is what gets recorded as an
  * assignable's within-type-id, so completion is just "are we past the inscribed deadline",
  * with no dependence on the subscription manager's current settings. Editing a
  * subscribable's schedule therefore cannot strand or silently re-time an assignable that
  * is already open.
  */
object BetterWeek:

  /**
    * The smallest schedule boundary strictly after `ldt` -- that is, the end of the
    * half-open period containing `ldt`.
    *
    * Note that `nextOrSame` followed by `with(timeOfDay)` can land at or *before* `ldt`,
    * when `ldt` already falls on the boundary day but later in the day. Hence the guard.
    */
  def periodEnd( ldt : LocalDateTime, dayOfWeek : DayOfWeek, timeOfDay : LocalTime ) : LocalDateTime =
    val candidate = ldt.`with`( TemporalAdjusters.nextOrSame( dayOfWeek ) ).`with`( timeOfDay )
    if candidate.isAfter( ldt ) then candidate else candidate.plusWeeks( 1 )

  /** The start of the half-open period ending at `periodEnd`. */
  def periodStart( periodEnd : LocalDateTime ) : LocalDateTime = periodEnd.minusWeeks( 1 )

  def format( ldt : LocalDateTime ) : String = ISO_LOCAL_DATE_TIME.format( ldt )

  def parse( withinTypeId : String ) : LocalDateTime = LocalDateTime.parse( withinTypeId )

  /**
    * The day-of-week whose boundary most recently elapsed, used when a subscribable is
    * defined without an explicit day. It puts the first deadline a full week out rather
    * than hours away: if today's boundary time has already passed we anchor to today,
    * otherwise to yesterday.
    */
  def defaultDayOfWeek( now : ZonedDateTime, timeOfDay : LocalTime ) : DayOfWeek =
    if now.toLocalTime().isBefore( timeOfDay ) then now.minusDays(1).getDayOfWeek() else now.getDayOfWeek()

end BetterWeek
