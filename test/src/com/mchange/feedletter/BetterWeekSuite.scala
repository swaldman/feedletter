package com.mchange.feedletter

import java.time.{DayOfWeek,Duration,LocalDateTime,LocalTime,ZoneId}
import java.time.DayOfWeek.*

// BetterWeek fixes the boundaries of BetterWeekly's assignables, and its output is
// persisted as an assignable's within-type-id, so these are the invariants that keep
// newsletters landing on the day they are supposed to.
class BetterWeekSuite extends munit.FunSuite:

  private def ldt( s : String ) : LocalDateTime = LocalDateTime.parse( s )

  private def periodEnd( at : String, dow : DayOfWeek, time : String ) : LocalDateTime =
    BetterWeek.periodEnd( ldt( at ), dow, LocalTime.parse( time ) )

  // 2026-08-03 is a Monday
  test("periodEnd finds the next boundary from mid-period"):
    assertEquals( periodEnd( "2026-08-05T14:30", MONDAY, "09:00" ), ldt("2026-08-10T09:00") )

  test("periodEnd rolls forward a full week when the boundary already passed earlier today"):
    // it is Monday afternoon; Monday 09:00 is behind us, so the deadline is next Monday
    assertEquals( periodEnd( "2026-08-03T14:30", MONDAY, "09:00" ), ldt("2026-08-10T09:00") )

  test("periodEnd takes today's boundary when it is still ahead of us"):
    assertEquals( periodEnd( "2026-08-03T07:30", MONDAY, "09:00" ), ldt("2026-08-03T09:00") )

  // periods are half-open, [start, end) -- an item checked exactly on a boundary belongs
  // to the period that boundary opens, not the one it closes
  test("periodEnd is strictly after its argument, so boundaries open a new period"):
    assertEquals( periodEnd( "2026-08-03T09:00", MONDAY, "09:00" ), ldt("2026-08-10T09:00") )

  test("periodEnd is idempotent across a whole period"):
    val expected = ldt("2026-08-10T09:00")
    val instants =
      Iterator.iterate( ldt("2026-08-03T09:00") )( _.plusHours(1) ).takeWhile( _.isBefore(expected) ).toList
    assertEquals( instants.size, 168 ) // one week of hours
    instants.foreach( i => assertEquals( BetterWeek.periodEnd( i, MONDAY, LocalTime.of(9,0) ), expected ) )

  test("periodStart inverts periodEnd"):
    assertEquals( BetterWeek.periodStart( ldt("2026-08-10T09:00") ), ldt("2026-08-03T09:00") )

  test("consecutive periods tile without gap or overlap"):
    val first  = periodEnd( "2026-08-05T14:30", MONDAY, "09:00" )
    val second = BetterWeek.periodEnd( first, MONDAY, LocalTime.of(9,0) )
    assertEquals( BetterWeek.periodStart( second ), first )

  test("periodEnd works for every day of the week"):
    // Wednesday 2026-08-05; each boundary should land within the following seven days
    DayOfWeek.values.foreach: dow =>
      val end = periodEnd( "2026-08-05T14:30", dow, "09:00" )
      assertEquals( end.getDayOfWeek, dow )
      assert( end.isAfter( ldt("2026-08-05T14:30") ), s"$dow: $end should be after the reference time" )
      assert( !end.isAfter( ldt("2026-08-12T14:30") ), s"$dow: $end should be within a week of the reference time" )

  test("within-type-ids round trip through format and parse"):
    val end = periodEnd( "2026-08-05T14:30", MONDAY, "09:00" )
    // ISO_LOCAL_DATE_TIME always emits seconds, giving within-type-ids a fixed width
    assertEquals( BetterWeek.format( end ), "2026-08-10T09:00:00" )
    assertEquals( BetterWeek.parse( BetterWeek.format( end ) ), end )

  test("within-type-ids sort lexicographically in chronological order"):
    val ends = List( "2026-08-05T14:30", "2026-01-05T14:30", "2026-12-05T14:30", "2027-01-05T14:30" )
      .map( s => BetterWeek.format( periodEnd( s, MONDAY, "09:00" ) ) )
    assertEquals( ends.sorted, ends.map( BetterWeek.parse ).sorted.map( BetterWeek.format ) )

  // year boundaries are exactly where NaiveWeek gets into trouble; here they are unremarkable
  test("periods cross the new year without incident"):
    assertEquals( periodEnd( "2026-12-30T14:30", MONDAY, "09:00" ), ldt("2027-01-04T09:00") )
    assertEquals( BetterWeek.periodStart( ldt("2027-01-04T09:00") ), ldt("2026-12-28T09:00") )

  // -------------------------------------------------------------------------
  // daylight savings
  //
  // all arithmetic is on LocalDateTime, so a period is a wall-clock week: the reader
  // always gets mail at 09:00 local, and the real elapsed time flexes to 167 or 169 hours
  // -------------------------------------------------------------------------

  test("spring forward keeps the wall-clock boundary, shortening the real week to 167 hours"):
    // US DST begins Sunday 2026-03-08
    val start = ldt("2026-03-02T09:00")
    val end   = BetterWeek.periodEnd( start, MONDAY, LocalTime.of(9,0) )
    assertEquals( end, ldt("2026-03-09T09:00") )
    val tz = ZoneId.of("America/New_York")
    assertEquals( Duration.between( start.atZone(tz), end.atZone(tz) ).toHours, 167L )

  test("fall back keeps the wall-clock boundary, lengthening the real week to 169 hours"):
    // US DST ends Sunday 2026-11-01
    val start = ldt("2026-10-26T09:00")
    val end   = BetterWeek.periodEnd( start, MONDAY, LocalTime.of(9,0) )
    assertEquals( end, ldt("2026-11-02T09:00") )
    val tz = ZoneId.of("America/New_York")
    assertEquals( Duration.between( start.atZone(tz), end.atZone(tz) ).toHours, 169L )

  test("a boundary time that does not exist on a spring-forward day is still well defined"):
    // 02:30 never happens on 2026-03-08 in New York, but we never resolve it to an instant
    assertEquals( periodEnd( "2026-03-04T14:30", SUNDAY, "02:30" ), ldt("2026-03-08T02:30") )

  // -------------------------------------------------------------------------
  // default day-of-week
  //
  // chosen so the first deadline is a full week out rather than hours away
  // -------------------------------------------------------------------------

  private def defaultDayOfWeek( now : String, time : String ) : DayOfWeek =
    BetterWeek.defaultDayOfWeek( ldt( now ).atZone( ZoneId.of("America/New_York") ), LocalTime.parse( time ) )

  test("defaultDayOfWeek picks today once today's boundary time has elapsed"):
    assertEquals( defaultDayOfWeek( "2026-08-05T14:30", "00:00" ), WEDNESDAY )

  test("defaultDayOfWeek picks yesterday when today's boundary time has not yet elapsed"):
    assertEquals( defaultDayOfWeek( "2026-08-05T14:30", "20:00" ), TUESDAY )

  test("defaultDayOfWeek treats an exactly-now boundary as elapsed"):
    assertEquals( defaultDayOfWeek( "2026-08-05T14:30", "14:30" ), WEDNESDAY )

  test("defaultDayOfWeek wraps correctly across the week boundary"):
    // Sunday 2026-08-09, before the boundary time, so we anchor to Saturday
    assertEquals( defaultDayOfWeek( "2026-08-09T01:00", "06:00" ), SATURDAY )

  test("the default day-of-week puts the first deadline a full week out"):
    // for any time of day, defaulting the day and then asking for the period end should
    // yield a deadline more than six days away -- never a partial first week
    val now = ldt("2026-08-05T14:30")
    List( "00:00", "06:00", "14:30", "20:00", "23:59" ).foreach: t =>
      val time = LocalTime.parse( t )
      val dow  = BetterWeek.defaultDayOfWeek( now.atZone( ZoneId.of("America/New_York") ), time )
      val end  = BetterWeek.periodEnd( now, dow, time )
      val hoursOut = Duration.between( now, end ).toHours
      assert( hoursOut >= 144, s"time $t: deadline $end is only $hoursOut hours out" )
      assert( hoursOut <= 168, s"time $t: deadline $end is $hoursOut hours out, more than a week" )

  // -------------------------------------------------------------------------
  // subject lines
  //
  // when no item in a batch carries a pubDate, the subject falls back to dates derived
  // from the within-type-id. this is where legacy Weekly goes wrong -- its fallback is
  // off by a day on both ends, and throws outright in the last days of December
  // -------------------------------------------------------------------------

  private def betterWeekly( dow : DayOfWeek, time : String ) =
    SubscriptionManager.Email.BetterWeekly(
      from                              = Destination.Email( "newsletter@example.com", None ),
      replyTo                           = None,
      composeUntemplateName             = "com.example.Compose",
      confirmUntemplateName             = "com.example.Confirm",
      statusChangeUntemplateName        = "com.example.StatusChange",
      removalNotificationUntemplateName = "com.example.RemovalNotification",
      dayOfWeek                         = dow,
      timeOfDay                         = LocalTime.parse( time ),
      timeZone                          = None,
      extraParams                       = Map.empty
    )

  private def subjectWithoutPubDates( sm : SubscriptionManager.Email.BetterWeekly, withinTypeId : String ) : String =
    sm.defaultSubject( SubscribableName("news"), withinTypeId, FeedUrl("https://example.com/feed"), Seq.empty, ZoneId.of("America/New_York") )

  test("a midnight-boundary period reports the seven dates it actually covers"):
    assertEquals(
      subjectWithoutPubDates( betterWeekly( MONDAY, "00:00" ), "2026-08-10T00:00:00" ),
      "[news] Posts, 2026-08-03 to 2026-08-09"
    )

  test("a mid-day-boundary period reports both partial days it touches"):
    assertEquals(
      subjectWithoutPubDates( betterWeekly( MONDAY, "09:00" ), "2026-08-10T09:00:00" ),
      "[news] Posts, 2026-08-03 to 2026-08-10"
    )

  test("a period spanning the new year yields a subject rather than an exception"):
    assertEquals(
      subjectWithoutPubDates( betterWeekly( MONDAY, "00:00" ), "2027-01-04T00:00:00" ),
      "[news] Posts, 2026-12-28 to 2027-01-03"
    )

  test("the sample within-type-id is usable, as the style commands assume"):
    val sm = betterWeekly( MONDAY, "00:00" )
    assertEquals( BetterWeek.parse( sm.sampleWithinTypeId ), ldt("2024-12-30T00:00") )
    assertEquals( subjectWithoutPubDates( sm, sm.sampleWithinTypeId ), "[news] Posts, 2024-12-23 to 2024-12-29" )

end BetterWeekSuite
