package com.mchange.feedletter

import java.sql.Connection
import java.time.{LocalDate,Instant}
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoField,WeekFields}

import DateTimeFormatter.ISO_LOCAL_DATE

import scala.collection.immutable

import com.mchange.feedletter.db.{AddressHeader, AssignableKey, AssignableWithinTypeStatus, From, ItemStatus, ReplyTo, To}

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
import com.mchange.feedletter.api.V0.ResponsePayload

object SubscriptionManager extends SelfLogging:
  val  Json = SubscriptionManagerJson
  type Json = SubscriptionManagerJson

  object Tag:
    def apply( s : String ) : Tag = s
  opaque type Tag = String

  sealed trait UntemplatedCompose:
    def composeUntemplateName : String
  sealed trait UntemplatedConfirm:
    def confirmUntemplateName : String
  sealed trait UntemplatedStatusChanged:
    def statusChangedUntemplateName : String
  sealed trait TemplatingCompose extends SubscriptionManager:
    def composeTemplateParams( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, destination : this.D, contents : Set[ItemContent] ) : TemplateParams
  sealed trait Factory:
    def fromJson( json : Json ) : SubscriptionManager
    def tag : Tag

  val Factories =
    val all = Email.Each :: Email.Weekly :: Nil
    all.map( f => (f.tag, f) ).toMap

  object Email:
    object Each extends Factory:
      override def fromJson( json : Json ) : Each = jsoniter.readFromString[Each](json.toString())
      override def tag : Tag = "Email.Each"
      given jsoniter.JsonValueCodec[Each] = JsonCodecMaker.make
    case class Each(
      from                        : Smtp.Address,
      replyTo                     : Option[Smtp.Address],
      composeUntemplateName       : String,
      confirmUntemplateName       : String,
      statusChangedUntemplateName : String,
      extraParams                 : Map[String,String]
    ) extends Email:

      override val sampleWithinTypeId = "https://www.someblog.com/post/1111.html"

      override val factory = Each

      override lazy val json = Json( jsoniter.writeToString(this) )

      override lazy val jsonPretty = Json( jsoniter.writeToString(this, jsoniter.WriterConfig.withIndentionStep(4)) )

      override def withinTypeId( conn : Connection, feedId : FeedId, guid : Guid, content : ItemContent, status : ItemStatus, lastCompleted : Option[AssignableWithinTypeStatus], mostRecentOpen : Option[AssignableWithinTypeStatus] ) : Option[String] =
        Some( guid.toString() )

      override def isComplete( conn : Connection, withinTypeId : String, currentCount : Int, lastAssigned : Instant ) : Boolean = true

      override def route( conn : Connection, assignableKey : AssignableKey, contents : Set[ItemContent], destinations : Set[D] ) : Unit =
        val uniqueContent = contents.uniqueOr: (c, nu) =>
          throw new WrongContentsMultiplicity(s"${this}: We expect exactly one item to render, found $nu: " + contents.map( ci => (ci.title orElse ci.link).getOrElse("<item>") ).mkString(", "))
        val ( feedId, feedUrl ) = PgDatabase.feedIdUrlForSubscribableName( conn, assignableKey.subscribableName )
        val computedSubject = subject( assignableKey.subscribableName, assignableKey.withinTypeId, feedUrl, contents )
        val fullTemplate =
          val info = ComposeInfo.Single( feedUrl.toString(), assignableKey.subscribableName.toString(), this, assignableKey.withinTypeId, contents.head )
          val compose = AllUntemplates.findComposeUntemplateSingle(composeUntemplateName)
          compose( info ).text
        val tosWithTemplateParams =
          destinations.map: destination =>
            ( AddressHeader[To](destination.rendered), composeTemplateParams( assignableKey.subscribableName, assignableKey.withinTypeId, feedUrl, destination, contents ) )
        PgDatabase.queueForMailing( conn, fullTemplate, AddressHeader[From](from), replyTo.map(AddressHeader.apply[ReplyTo]), tosWithTemplateParams, computedSubject)

      override def defaultSubject( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Set[ItemContent] ) : String =
        assert( contents.size == 1, s"Email.Each expects contents exactly one item, while generating default subject, we found ${contents.size}." )
        s"[${subscribableName}] " + contents.head.title.fold("New Untitled Post")( title => s"New Post: ${title}" )

    object Weekly extends Factory:
      override def fromJson( json : Json ) : Weekly = jsoniter.readFromString[Weekly](json.toString())
      override def tag : Tag = "Email.Weekly"
      given jsoniter.JsonValueCodec[Weekly] = JsonCodecMaker.make
    final case class Weekly(
      from                        : Smtp.Address,
      replyTo                     : Option[Smtp.Address],
      composeUntemplateName       : String,
      confirmUntemplateName       : String,
      statusChangedUntemplateName : String,
      extraParams                 : Map[String,String]
    ) extends Email:
      private val WtiFormatter = DateTimeFormatter.ofPattern("YYYY-'week'ww")

      override val sampleWithinTypeId = "2023-week50"

      override val factory = Weekly

      override lazy val json = Json( jsoniter.writeToString(this) )

      override lazy val jsonPretty = Json( jsoniter.writeToString(this, jsoniter.WriterConfig.withIndentionStep(4)) )

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

      override def route( conn : Connection, assignableKey : AssignableKey, contents : Set[ItemContent], destinations : Set[D] ) : Unit =
        if contents.nonEmpty then
          val ( feedId, feedUrl ) = PgDatabase.feedIdUrlForSubscribableName( conn, assignableKey.subscribableName )
          val computedSubject = subject( assignableKey.subscribableName, assignableKey.withinTypeId, feedUrl, contents )
          val fullTemplate =
            val info = ComposeInfo.Multiple( feedUrl.toString(), assignableKey.subscribableName.toString(), this, assignableKey.withinTypeId, contents )
            val compose = AllUntemplates.findComposeUntemplateMultiple(composeUntemplateName)
            compose( info ).text
          val tosWithTemplateParams =
            destinations.map: destination =>
              ( AddressHeader[To](destination.rendered), composeTemplateParams( assignableKey.subscribableName, assignableKey.withinTypeId, feedUrl, destination, contents ) )
          PgDatabase.queueForMailing( conn, fullTemplate, AddressHeader[From](from), replyTo.map(AddressHeader.apply[ReplyTo]), tosWithTemplateParams, computedSubject)

      private def weekStartWeekEnd( withinTypeId : String ) : (String,String) =
        val ( year, woy, weekFields ) = extractYearWeekAndWeekFields( withinTypeId )
        val weekStart = LocalDate.of(year, 1, 1).`with`( weekFields.weekOfWeekBasedYear(), woy ).`with`(weekFields.dayOfWeek(), 1 )
        val weekEnd = LocalDate.of(year, 1, 1).`with`( weekFields.weekOfWeekBasedYear(), woy ).`with`(weekFields.dayOfWeek(), 7 )
        (ISO_LOCAL_DATE.format(weekStart),ISO_LOCAL_DATE.format(weekEnd))

      override def defaultSubject( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Set[ItemContent] ) : String =
        val (weekStart, weekEnd) = weekStartWeekEnd(withinTypeId)
        s"[${subscribableName}] All posts, ${weekStart} to ${weekEnd}"

      override def defaultComposeTemplateParams( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, destination : D, contents : Set[ItemContent] ) : Map[String,String] =
        val mainDefaultTemplateParams = super.defaultComposeTemplateParams(subscribableName, withinTypeId, feedUrl, destination, contents)
        val (weekStart, weekEnd) = weekStartWeekEnd(withinTypeId)
        mainDefaultTemplateParams ++ (("weekStart", weekStart)::("weekEnd", weekEnd)::Nil)

  trait Email extends SubscriptionManager, UntemplatedCompose, UntemplatedConfirm, UntemplatedStatusChanged, TemplatingCompose:
    def from                        : Smtp.Address
    def replyTo                     : Option[Smtp.Address]
    def composeUntemplateName       : String
    def confirmUntemplateName       : String
    def statusChangedUntemplateName : String
    def extraParams                 : Map[String,String]

    type D = Destination.Email

    override val destinationFactory = Destination.Email

    override def sampleDestination : Destination.Email = Destination.Email("user@example.com", Some("Some User"))

    def subject( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Set[ItemContent] ) : String =
      val args = ( subscribableName, withinTypeId, feedUrl, contents )
      config.SubjectCustomizers.get( subscribableName ).fold( defaultSubject.tupled(args) ): customizer =>
        customizer.tupled( args )

    def defaultSubject( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Set[ItemContent] ) : String

    def composeTemplateParams( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, destination : D, contents : Set[ItemContent] ) : TemplateParams =
      val localArgs = ( subscribableName, withinTypeId, feedUrl, destination, contents )
      val customizerArgs = ( subscribableName, withinTypeId, feedUrl, destination : Destination, contents )
      TemplateParams( defaultComposeTemplateParams.tupled( localArgs ) ++ config.ComposeTemplateParamCustomizers.get( subscribableName ).fold(Nil)( _.tupled.apply(customizerArgs) ) )

    def defaultComposeTemplateParams( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, destination : D, contents : Set[ItemContent] ) : Map[String,String] =
      val toAddress = destination.toAddress
      val toFull = toAddress.rendered
      val toNickname = toAddress.displayName
      val toEmail  = toAddress.email
      extraParams.toMap ++ Map(
        "from"              -> from.rendered,
        "replyTo"           -> replyTo.map( _.rendered ).getOrElse(""),
        "to"                -> toFull,
        "toFull"            -> toFull,
        "toNickname"        -> toNickname.getOrElse(""),
        "toEmail"           -> toEmail,
        "toNicknameOrEmail" -> toNickname.getOrElse( toEmail ),
        "numItems"          -> contents.size.toString()
      ).filter( _._2.nonEmpty )

    // the destination should already be validated before we get to this point.
    // we won't revalidate
    override def maybeConfirmSubscription( conn : Connection, destination : D, subscribableName : SubscribableName, subscriptionId : SubscriptionId, secretSalt : String ) : Boolean =
      val subject = s"[${subscribableName}] Please confirm your new subscription" // XXX: Hardcoded subject, revisit someday
      val mailText =
        val confirmUntemplate = AllUntemplates.findConfirmUntemplate( confirmUntemplateName )
        val confirmInfo = ConfirmInfo( destination, subscribableName, this, subscriptionId, secretSalt )
        confirmUntemplate( confirmInfo ).text
      PgDatabase.queueForMailing( conn, mailText, AddressHeader[From](from), replyTo.map(AddressHeader.apply[ReplyTo]), AddressHeader[To](destination.toAddress),TemplateParams.empty,subject)
      true

    override def validateDestinationOrThrow( conn : Connection, destination : Destination, subscribableName : SubscribableName ) : Unit =
      destination match
        case emailDestination : Destination.Email => Some( emailDestination )
        case _ =>
          throw new InvalidDestination( s"[${subscribableName}] Email subscription requires email destination. Destination '${destination}' is not. Rejecting." )

    override def htmlForStatusChanged( statusChangedInfo : StatusChangedInfo ) : String =
      val untemplate = AllUntemplates.findStatusChangedUntemplate(statusChangedUntemplateName)
      untemplate( statusChangedInfo ).text

  end Email

  def materialize( tag : Tag, json : Json ) : SubscriptionManager =
    val factory = Factories.get(tag).getOrElse:
      throw new InvalidSubscriptionManager(s"Tag '${tag}' unknown for $json")
    factory.fromJson( json )

sealed trait SubscriptionManager extends Jsonable:
  type D <: Destination

  def destinationFactory : Destination.Factory[D]

  def sampleWithinTypeId : String
  def sampleDestination  : D
  def withinTypeId( conn : Connection, feedId : FeedId, guid : Guid, content : ItemContent, status : ItemStatus, lastCompleted : Option[AssignableWithinTypeStatus], mostRecentOpen : Option[AssignableWithinTypeStatus] ) : Option[String]
  def isComplete( conn : Connection, withinTypeId : String, currentCount : Int, lastAssigned : Instant ) : Boolean
  def validateDestinationOrThrow( conn : Connection, destination : Destination, subscribableName : SubscribableName ) : Unit
  def maybeConfirmSubscription( conn : Connection, destination : D, subscribableName : SubscribableName, subscriptionId : SubscriptionId, secretSalt : String ) : Boolean // return false iff confirmation is unnecessary for this subscription
  def route( conn : Connection, assignableKey : AssignableKey, contents : Set[ItemContent], destinations : Set[D] ) : Unit
  def factory : SubscriptionManager.Factory
  def tag : SubscriptionManager.Tag = factory.tag
  def json : SubscriptionManager.Json
  def jsonPretty : SubscriptionManager.Json

  def htmlForStatusChanged( statusChangedInfo : StatusChangedInfo ) : String

  def materializeDestination( destinationJson : Destination.Json ) : D =
    Destination.materialize( destinationFactory.tag, destinationJson ).asInstanceOf[D]

  def narrowDestinationOrThrow( destination : Destination ) : D =
    try destination.asInstanceOf[D]
    catch
      case cce : ClassCastException => throw new InvalidDestination(s"Destination '$destination' is not valid for SubscriptionManager '$this'.", cce)

  def narrowDestination( destination : Destination ) : Option[D] =
    try Some(destination.asInstanceOf[D])
    catch
      case cce : ClassCastException => None


