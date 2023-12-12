package com.mchange.feedletter

import java.sql.Connection
import java.time.{LocalDate,Instant}
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoField,WeekFields}

import DateTimeFormatter.ISO_LOCAL_DATE

import scala.collection.immutable

import com.mchange.feedletter.db.{AssignableKey, AssignableWithinTypeStatus, ItemStatus}

import com.mchange.conveniences.www.*
import com.mchange.feedletter.db.PgDatabase
import com.mchange.mailutil.*
import com.mchange.sc.v1.log.*
import MLevel.*

object SubscriptionType:
  private lazy given logger : MLogger = mlogger(this)

  private val GeneralRegex = """^(\w+)\.(\w+)\:(.*)$""".r

  extension ( s : String )
    def asOptionNotBlank : Option[String] = if s.trim().nonEmpty then Some(s) else None
    def asOptionNotBlankTrimmed : Option[String] =
      val t = s.trim()
      if t.nonEmpty then Some(t) else None

  object Email:
    class Each( params : Seq[(String,String)] ) extends Email("Each", params):

      override def withinTypeId( feedUrl : FeedUrl, lastCompleted : Option[AssignableWithinTypeStatus], mostRecentOpen : Option[AssignableWithinTypeStatus], guid : Guid, content : ItemContent, status : ItemStatus ) : Option[String] =
        Some( guid.toString() )

      override def isComplete( conn : Connection, withinTypeId : String, currentCount : Int, lastAssigned : Instant ) : Boolean = true

      override def route( conn : Connection, assignableKey : AssignableKey, contents : Set[ItemContent], destinations : Set[Destination] ) : Unit =
        assert( contents.size == 1, s"Email.Each expects contents of exactly one item from a completed assignable, found ${contents.size}. assignableKey: ${assignableKey}" )
        val computedSubject = subject( assignableKey.subscribableName, assignableKey.withinTypeId, assignableKey.feedUrl, contents )
        val fullTemplate = composeSingleItemHtmlMailTemplate( assignableKey, this, contents.head )
          val tosWithTemplateParams =
            destinations.map: destination =>
              ( destination, templateParams( assignableKey.subscribableName, assignableKey.withinTypeId, assignableKey.feedUrl, destination, contents ) )
        PgDatabase.queueForMailing( conn, fullTemplate, from.mkString(","), replyTo.mkString(",").asOptionNotBlankTrimmed, tosWithTemplateParams, computedSubject)

      override def defaultSubject( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Set[ItemContent] ) : String =
        assert( contents.size == 1, s"Email.Each expects contents exactly one item, while generating default subject, we found ${contents.size}." )
        s"""[${subscribableName}] New Post: ${contents.head.title.getOrElse("(untitled post)")}"""

    class Weekly( params : Seq[(String,String)] ) extends Email( "Weekly", params ):
      private val WtiFormatter = DateTimeFormatter.ofPattern("YYYY-'week'ww")

      // this is only fixed on assignment, should be lastChecked, because week in which firstSeen might already have passed
      override def withinTypeId(
        feedUrl        : FeedUrl,
        lastCompleted  : Option[AssignableWithinTypeStatus],
        mostRecentOpen : Option[AssignableWithinTypeStatus],
        guid           : Guid,
        content        : ItemContent,
        status         : ItemStatus
      ) : Option[String] =
        Some( WtiFormatter.format( status.lastChecked ) ) 

      // Regular TemporalFields don't work on the formatter-parsed accessor. We need a WeekFields thang first 
      private def extractYearWeekAndWeekFields( withinTypeId : String ) : (Int, Int, WeekFields) =
        val ( yearStr, restStr ) = withinTypeId.span( Character.isDigit )
        val year = yearStr.toInt
        val baseDayOfWeek = LocalDate.of( year, 1, 1 ).getDayOfWeek()
        val weekNum = restStr.dropWhile( c => !Character.isDigit(c) ).toInt
        ( year, weekNum, WeekFields.of(baseDayOfWeek, 1) )

      override def isComplete( conn : Connection, withinTypeId : String, currentCount : Int, lastAssigned : Instant ) : Boolean =
        val ( year, woy, weekFields ) = extractYearWeekAndWeekFields( withinTypeId )
        val tz = PgDatabase.timeZone( conn ) // do we really need to hit this every time?
        val laZoned = lastAssigned.atZone(tz)
        val laYear = laZoned.get( ChronoField.YEAR )
        laYear > year || (laYear == year && laZoned.get( ChronoField.ALIGNED_WEEK_OF_YEAR ) > woy)

      override def route( conn : Connection, assignableKey : AssignableKey, contents : Set[ItemContent], destinations : Set[Destination] ) : Unit =
        if contents.nonEmpty then
          val computedSubject = subject( assignableKey.subscribableName, assignableKey.withinTypeId, assignableKey.feedUrl, contents )
          val fullTemplate = composeMultipleItemHtmlMailTemplate( assignableKey, this, contents )
          val tosWithTemplateParams =
            destinations.map: destination =>
              ( destination, templateParams( assignableKey.subscribableName, assignableKey.withinTypeId, assignableKey.feedUrl, destination, contents ) )
          PgDatabase.queueForMailing( conn, fullTemplate, from.mkString(","), replyTo.mkString(",").asOptionNotBlankTrimmed, tosWithTemplateParams, computedSubject)

      private def weekStartWeekEnd( withinTypeId : String ) : (String,String) =
        val ( year, woy, weekFields ) = extractYearWeekAndWeekFields( withinTypeId )
        val weekStart = LocalDate.of(year, 1, 1).`with`( weekFields.weekOfWeekBasedYear(), woy ).`with`(weekFields.dayOfWeek(), 1 )
        val weekEnd = LocalDate.of(year, 1, 1).`with`( weekFields.weekOfWeekBasedYear(), woy ).`with`(weekFields.dayOfWeek(), 7 )
        (ISO_LOCAL_DATE.format(weekStart),ISO_LOCAL_DATE.format(weekEnd))

      override def defaultSubject( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Set[ItemContent] ) : String =
        val (weekStart, weekEnd) = weekStartWeekEnd(withinTypeId)
        s"[${subscribableName}] All posts, ${weekStart} to ${weekEnd}"

      override def defaultTemplateParams( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, destination : Destination, contents : Set[ItemContent] ) : Map[String,String] =
        val mainDefaultTemplateParams = super.defaultTemplateParams(subscribableName, withinTypeId, feedUrl, destination, contents)
        val (weekStart, weekEnd) = weekStartWeekEnd(withinTypeId)
        mainDefaultTemplateParams ++ (("weekStart", weekStart)::("weekEnd", weekEnd)::Nil)

  abstract class Email(subtype : String, params : Seq[Tuple2[String,String]]) extends SubscriptionType("Email", subtype, params):
    val from    : Seq[String] = wwwFormFindAllValues("from", params)
    val replyTo : Seq[String] = wwwFormFindAllValues("replyTo", params)

    def subject( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Set[ItemContent] ) : String =
      val args = ( subscribableName, withinTypeId, feedUrl, contents )
      config.SubjectCustomizers.get( subscribableName ).fold( defaultSubject.tupled(args) ): customizer =>
        customizer.tupled( args )

    def defaultSubject( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Set[ItemContent] ) : String

    def templateParams( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, destination : Destination, contents : Set[ItemContent] ) : TemplateParams =
      val args = ( subscribableName, withinTypeId, feedUrl, destination, contents )
      TemplateParams( defaultTemplateParams.tupled( args ) ++ config.TemplateParamCustomizers.get( subscribableName ).fold(Nil)( _.tupled.apply(args) ) )

    def defaultTemplateParams( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, destination : Destination, contents : Set[ItemContent] ) : Map[String,String] =
      val toFull = destination.toString()
      val toAddress = Smtp.Address.parseSingle( toFull )
      val toNickname = toAddress.displayName
      val toEmail  = toAddress.email
      params.toMap ++ Map(
        "from"              -> from.mkString(", "),
        "replyTo"           -> replyTo.mkString(", "),
        "to"                -> toFull,
        "toFull"            -> toFull,
        "toNickname"        -> toNickname.getOrElse(""),
        "toEmail"           -> toEmail,
        "toNicknameOrEmail" -> toNickname.getOrElse( toEmail ),
        "numItems"          -> contents.size.toString()
      )

    override def validateDestination( conn : Connection, destination : Destination, feedUrl : FeedUrl, subscribableName : SubscribableName ) : Boolean =
      try
        Smtp.Address.parseSingle( destination.toString(), strict = true  )
        true
      catch
        case failure : SmtpAddressParseFailed =>
          WARNING.log( s"[${subscribableName}] Could not validate destination for email subscription '${destination}'. Rejecting." )
          false

    if from.isEmpty then
      throw new InvalidSubscriptionType(
        "An Email subscription type must include at least one 'from' param. Params given: " +
        params.map( (k,v) => k + " -> " + v ).mkString(", ")
      )
  end Email

  def dispatch( category : String, subtype : String, params : Seq[(String,String)] ) : Option[SubscriptionType] =
    ( category, subtype ) match
      case ("Email", "Each")   => Some( Email.Each(params) )
      case ("Email", "Weekly") => Some( Email.Weekly(params) )
      case _                   => None 

  private def preparse( s : String ) : Option[Tuple3[String,String,Seq[Tuple2[String,String]]]] =
    s match
      case GeneralRegex( category, subtype, params ) => Some( Tuple3( category, subtype, wwwFormDecodeUTF8( params ) ) )
      case _ => None

  def parse( str : String ) : SubscriptionType =
      preparse( str ).flatMap( dispatch.tupled ).getOrElse:
        throw new InvalidSubscriptionType(s"'${str}' could not be parsed into a valid subscription type.")

sealed abstract class SubscriptionType( val category : String, val subtype : String, val params : Seq[(String,String)] ):
  def withinTypeId( feedUrl : FeedUrl, lastCompleted : Option[AssignableWithinTypeStatus], mostRecentOpen : Option[AssignableWithinTypeStatus], guid : Guid, content : ItemContent, status : ItemStatus ) : Option[String]
  def isComplete( conn : Connection, withinTypeId : String, currentCount : Int, lastAssigned : Instant ) : Boolean
  def validateDestination( conn : Connection, destination : Destination, feedUrl : FeedUrl, subscribableName : SubscribableName ) : Boolean
  def route( conn : Connection, assignableKey : AssignableKey, contents : Set[ItemContent], destinations : Set[Destination] ) : Unit = ??? // XXX: temporary, make abstract when we stabilize
  override def toString() : String = s"${category}.${subtype}:${wwwFormEncodeUTF8( params.toSeq* )}"
  override def equals( other : Any ) : Boolean =
    other match
      case stype : SubscriptionType =>
        category == category && subtype == subtype && params == params
      case _ =>
        false
  override def hashCode(): Int =
    category.## ^ subtype.## ^ params.##





