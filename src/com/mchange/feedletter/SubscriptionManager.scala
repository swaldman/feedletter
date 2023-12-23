package com.mchange.feedletter

import java.sql.Connection
import java.time.{LocalDate,Instant}
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoField,WeekFields}

import DateTimeFormatter.ISO_LOCAL_DATE

import scala.collection.immutable

import com.mchange.feedletter.db.{AssignableKey, AssignableWithinTypeStatus, ItemStatus}

import scala.collection.immutable

import com.mchange.conveniences.string.*
import com.mchange.conveniences.collection.*
import com.mchange.feedletter.db.PgDatabase
import com.mchange.mailutil.*
import scala.util.control.NonFatal

import Jsoniter.{*, given}
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.{core as jsoniter}

import MLevel.*

object SubscriptionManager extends SelfLogging:

  object Tag:
    def apply( s : String ) : Tag = s
  opaque type Tag = String

  sealed trait Untemplated:
    def untemplateName : String
  sealed trait Templating:
    def templateParams( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, destination : Destination, contents : Set[ItemContent] ) : TemplateParams
  sealed trait Factory:
    def fromJson( json : String ) : SubscriptionManager
    def tag : Tag

  val Factories =
    val all = Email.Each :: Email.Weekly :: Nil
    all.map( f => (f.tag, f) ).toMap

  object Email:
    object Each extends Factory:
      override def fromJson( json : String ) : Each = jsoniter.readFromString[Each](json)
      override def tag : Tag = "Email.Each"
      given jsoniter.JsonValueCodec[Each] = JsonCodecMaker.make
    case class Each( from : Smtp.Address, replyTo : Option[Smtp.Address], untemplateName : String, extraParams : Map[String,String] ) extends Email:

      override val sampleWithinTypeId = "https://www.someblog.com/post/1111.html"

      override val factory = Each

      override lazy val json = jsoniter.writeToString(this)

      override def withinTypeId( conn : Connection, feedId : FeedId, guid : Guid, content : ItemContent, status : ItemStatus, lastCompleted : Option[AssignableWithinTypeStatus], mostRecentOpen : Option[AssignableWithinTypeStatus] ) : Option[String] =
        Some( guid.toString() )

      override def isComplete( conn : Connection, withinTypeId : String, currentCount : Int, lastAssigned : Instant ) : Boolean = true

      override def route( conn : Connection, assignableKey : AssignableKey, contents : Set[ItemContent], destinations : Set[Destination] ) : Unit =
        val uniqueContent = contents.uniqueOr: (c, nu) =>
          throw new WrongContentsMultiplicity(s"${this}: We expect exactly one item to render, found $nu: " + contents.map( ci => (ci.title orElse ci.link).getOrElse("<item>") ).mkString(", "))
        val ( feedId, feedUrl ) = PgDatabase.feedIdUrlForSubscribableName( conn, assignableKey.subscribableName )
        val computedSubject = subject( assignableKey.subscribableName, assignableKey.withinTypeId, feedUrl, contents )
        val fullTemplate =
          val info = ComposeInfo.Single( feedUrl.toString(), assignableKey.subscribableName.toString(), this, assignableKey.withinTypeId, contents.head )
          val compose = findComposeUntemplateSingle(untemplateName)
          compose( info ).text
        val tosWithTemplateParams =
          destinations.map: destination =>
            ( destination, templateParams( assignableKey.subscribableName, assignableKey.withinTypeId, feedUrl, destination, contents ) )
        PgDatabase.queueForMailing( conn, fullTemplate, from.toInternetAddress.toString(), replyTo.map(_.toInternetAddress.toString()), tosWithTemplateParams, computedSubject)

      override def defaultSubject( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Set[ItemContent] ) : String =
        assert( contents.size == 1, s"Email.Each expects contents exactly one item, while generating default subject, we found ${contents.size}." )
        s"[${subscribableName}] " + contents.head.title.fold("New Untitled Post")( title => s"New Post: ${title}" )

    object Weekly extends Factory:
      override def fromJson( json : String ) : Weekly = jsoniter.readFromString[Weekly](json)
      override def tag : Tag = "Email.Weekly"
      given jsoniter.JsonValueCodec[Weekly] = JsonCodecMaker.make
    final case class Weekly( from : Smtp.Address, replyTo : Option[Smtp.Address], untemplateName : String, extraParams : Map[String,String] ) extends Email:
      private val WtiFormatter = DateTimeFormatter.ofPattern("YYYY-'week'ww")

      override val sampleWithinTypeId = "2023-week50"

      override val factory = Weekly

      override lazy val json = jsoniter.writeToString(this)

      // this is only fixed on assignment, should be lastChecked, because week in which firstSeen might already have passed
      override def withinTypeId(
        conn           : Connection,
        feedId         : FeedId,
        guid           : Guid,
        content        : ItemContent,
        status         : ItemStatus,
        lastCompleted  : Option[AssignableWithinTypeStatus],
        mostRecentOpen : Option[AssignableWithinTypeStatus]
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
        val tz = PgDatabase.Config.timeZone( conn ) // do we really need to hit this every time?
        val laZoned = lastAssigned.atZone(tz)
        val laYear = laZoned.get( ChronoField.YEAR )
        laYear > year || (laYear == year && laZoned.get( ChronoField.ALIGNED_WEEK_OF_YEAR ) > woy)

      override def route( conn : Connection, assignableKey : AssignableKey, contents : Set[ItemContent], destinations : Set[Destination] ) : Unit =
        if contents.nonEmpty then
          val ( feedId, feedUrl ) = PgDatabase.feedIdUrlForSubscribableName( conn, assignableKey.subscribableName )
          val computedSubject = subject( assignableKey.subscribableName, assignableKey.withinTypeId, feedUrl, contents )
          val fullTemplate =
            val info = ComposeInfo.Multiple( feedUrl.toString(), assignableKey.subscribableName.toString(), this, assignableKey.withinTypeId, contents )
            val compose = findComposeUntemplateMultiple(untemplateName)
            compose( info ).text
          val tosWithTemplateParams =
            destinations.map: destination =>
              ( destination, templateParams( assignableKey.subscribableName, assignableKey.withinTypeId, feedUrl, destination, contents ) )
          PgDatabase.queueForMailing( conn, fullTemplate, from.toInternetAddress.toString(), replyTo.map(_.toInternetAddress.toString()), tosWithTemplateParams, computedSubject)

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

  trait Email extends SubscriptionManager, Untemplated, Templating:
    def from           : Smtp.Address
    def replyTo        : Option[Smtp.Address]
    def untemplateName : String
    def extraParams    : Map[String,String]

    override def sampleDestination : Destination = Destination("user@example.com")

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
      extraParams.toMap ++ Map(
        "from"              -> from.toInternetAddress.toString(),
        "replyTo"           -> replyTo.map( _.toInternetAddress.toString() ).getOrElse(""),
        "to"                -> toFull,
        "toFull"            -> toFull,
        "toNickname"        -> toNickname.getOrElse(""),
        "toEmail"           -> toEmail,
        "toNicknameOrEmail" -> toNickname.getOrElse( toEmail ),
        "numItems"          -> contents.size.toString()
      ).filter( _._2.nonEmpty )

    override def validateDestination( conn : Connection, destination : Destination, subscribableName : SubscribableName ) : Boolean =
      try
        Smtp.Address.parseSingle( destination.toString(), strict = true  )
        true
      catch
        case failure : SmtpAddressParseFailed =>
          WARNING.log( s"[${subscribableName}] Could not validate destination for email subscription '${destination}'. Rejecting." )
          false
  end Email

  def materialize( tag : Tag, json : String ) : SubscriptionManager =
    val factory = Factories.get(tag).getOrElse:
      throw new InvalidSubscriptionManager(s"Tag '${tag}' unknown for $json")
    factory.fromJson( json )  

sealed trait SubscriptionManager:
  def sampleWithinTypeId : String
  def sampleDestination  : Destination
  def withinTypeId( conn : Connection, feedId : FeedId, guid : Guid, content : ItemContent, status : ItemStatus, lastCompleted : Option[AssignableWithinTypeStatus], mostRecentOpen : Option[AssignableWithinTypeStatus] ) : Option[String]
  def isComplete( conn : Connection, withinTypeId : String, currentCount : Int, lastAssigned : Instant ) : Boolean
  def validateDestination( conn : Connection, destination : Destination, subscribableName : SubscribableName ) : Boolean
  def route( conn : Connection, assignableKey : AssignableKey, contents : Set[ItemContent], destinations : Set[Destination] ) : Unit = ??? // XXX: temporary, make abstract when we stabilize
  def factory : SubscriptionManager.Factory
  def tag : SubscriptionManager.Tag = factory.tag
  def json : String
