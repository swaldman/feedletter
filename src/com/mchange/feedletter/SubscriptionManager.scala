package com.mchange.feedletter

import java.sql.Connection
import java.time.{LocalDate,Instant}
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoField,WeekFields}

import DateTimeFormatter.ISO_LOCAL_DATE

import scala.collection.immutable

import com.mchange.feedletter.db.{AddressHeader, AssignableKey, AssignableWithinTypeStatus, From, ItemStatus, ReplyTo, To}
import com.mchange.feedletter.api.ApiLinkGenerator

import scala.collection.immutable

import com.mchange.conveniences.string.*
import com.mchange.conveniences.collection.*
import com.mchange.feedletter.db.PgDatabase
import scala.util.control.NonFatal

import MLevel.*

import upickle.default.*

object SubscriptionManager extends SelfLogging:
  val  Json = SubscriptionManagerJson
  type Json = SubscriptionManagerJson

  sealed trait UntemplatedCompose:
    def composeUntemplateName : String
  sealed trait UntemplatedConfirm:
    def confirmUntemplateName : String
  sealed trait UntemplatedStatusChange:
    def statusChangeUntemplateName : String
  sealed trait TemplatingCompose extends SubscriptionManager:
    def composeTemplateParams( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, destination : this.D, subscriptionId : SubscriptionId, removeLink : String ) : TemplateParams

  object Email:
    type Companion = Each.type | Weekly.type

    case class Each(
      from                       : Destination.Email,
      replyTo                    : Option[Destination.Email],
      composeUntemplateName      : String,
      confirmUntemplateName      : String,
      statusChangeUntemplateName : String,
      extraParams                : Map[String,String]
    ) extends Email:

      override val sampleWithinTypeId = "https://www.someblog.com/post/1111.html"

      override def withinTypeId( conn : Connection, feedId : FeedId, guid : Guid, content : ItemContent, status : ItemStatus, lastCompleted : Option[AssignableWithinTypeStatus], mostRecentOpen : Option[AssignableWithinTypeStatus] ) : Option[String] =
        Some( guid.toString() )

      override def isComplete( conn : Connection, withinTypeId : String, currentCount : Int, lastAssigned : Instant ) : Boolean = true

      override def route( conn : Connection, assignableKey : AssignableKey, contents : Set[ItemContent], idestinations : Set[IdentifiedDestination[D]], apiLinkGenerator : ApiLinkGenerator ) : Unit =
        val uniqueContent = contents.uniqueOr: (c, nu) =>
          throw new WrongContentsMultiplicity(s"${this}: We expect exactly one item to render, found $nu: " + contents.map( ci => (ci.title orElse ci.link).getOrElse("<item>") ).mkString(", "))
        val ( feedId, feedUrl ) = PgDatabase.feedIdUrlForSubscribableName( conn, assignableKey.subscribableName )
        val computedSubject = subject( assignableKey.subscribableName, assignableKey.withinTypeId, feedUrl, contents )
        val fullTemplate =
          val info = ComposeInfo.Single( feedUrl.toString(), assignableKey.subscribableName.toString(), this, assignableKey.withinTypeId, contents.head )
          val compose = AllUntemplates.findComposeUntemplateSingle(composeUntemplateName)
          compose( info ).text
        val tosWithTemplateParams = findTosWithTemplateParams( assignableKey, feedUrl, idestinations, apiLinkGenerator )
        PgDatabase.queueForMailing( conn, fullTemplate, AddressHeader[From](from), replyTo.map(AddressHeader.apply[ReplyTo]), tosWithTemplateParams, computedSubject)

      override def defaultSubject( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Set[ItemContent] ) : String =
        assert( contents.size == 1, s"Email.Each expects contents exactly one item, while generating default subject, we found ${contents.size}." )
        s"[${subscribableName}] " + contents.head.title.fold("New Untitled Post")( title => s"New Post: ${title}" )

    final case class Weekly(
      from                       : Destination.Email,
      replyTo                    : Option[Destination.Email],
      composeUntemplateName      : String,
      confirmUntemplateName      : String,
      statusChangeUntemplateName : String,
      extraParams                : Map[String,String]
    ) extends Email:
      private val WtiFormatter = DateTimeFormatter.ofPattern("YYYY-'week'ww")

      override val sampleWithinTypeId = "2023-week50"

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

      override def route( conn : Connection, assignableKey : AssignableKey, contents : Set[ItemContent], idestinations : Set[IdentifiedDestination[D]], apiLinkGenerator : ApiLinkGenerator ) : Unit =
        if contents.nonEmpty then
          val ( feedId, feedUrl ) = PgDatabase.feedIdUrlForSubscribableName( conn, assignableKey.subscribableName )
          val computedSubject = subject( assignableKey.subscribableName, assignableKey.withinTypeId, feedUrl, contents )
          val fullTemplate =
            val info = ComposeInfo.Multiple( feedUrl.toString(), assignableKey.subscribableName.toString(), this, assignableKey.withinTypeId, contents )
            val compose = AllUntemplates.findComposeUntemplateMultiple(composeUntemplateName)
            compose( info ).text
          val tosWithTemplateParams = findTosWithTemplateParams( assignableKey, feedUrl, idestinations, apiLinkGenerator )
          PgDatabase.queueForMailing( conn, fullTemplate, AddressHeader[From](from), replyTo.map(AddressHeader.apply[ReplyTo]), tosWithTemplateParams, computedSubject)

      private def weekStartWeekEnd( withinTypeId : String ) : (String,String) =
        val ( year, woy, weekFields ) = extractYearWeekAndWeekFields( withinTypeId )
        val weekStart = LocalDate.of(year, 1, 1).`with`( weekFields.weekOfWeekBasedYear(), woy ).`with`(weekFields.dayOfWeek(), 1 )
        val weekEnd = LocalDate.of(year, 1, 1).`with`( weekFields.weekOfWeekBasedYear(), woy ).`with`(weekFields.dayOfWeek(), 7 )
        (ISO_LOCAL_DATE.format(weekStart),ISO_LOCAL_DATE.format(weekEnd))

      override def defaultSubject( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Set[ItemContent] ) : String =
        val (weekStart, weekEnd) = weekStartWeekEnd(withinTypeId)
        s"[${subscribableName}] All posts, ${weekStart} to ${weekEnd}"

  sealed trait Email extends SubscriptionManager, UntemplatedCompose, UntemplatedConfirm, UntemplatedStatusChange, TemplatingCompose:
    def from                       : Destination.Email
    def replyTo                    : Option[Destination.Email]
    def composeUntemplateName      : String
    def confirmUntemplateName      : String
    def statusChangeUntemplateName : String
    def extraParams                : Map[String,String]

    type D = Destination.Email

    override def sampleDestination : Destination.Email = Destination.Email("user@example.com", Some("Some User"))

    protected def findTosWithTemplateParams( assignableKey : AssignableKey, feedUrl : FeedUrl, idestinations : Set[IdentifiedDestination[D]], apiLinkGenerator : ApiLinkGenerator ) : Set[(AddressHeader[To],TemplateParams)] =
      idestinations.map: idestination =>
        val to = idestination.destination.rendered
        val sid = idestination.subscriptionId
        val templateParams = composeTemplateParams( assignableKey.subscribableName, assignableKey.withinTypeId, feedUrl, idestination.destination, sid, apiLinkGenerator.removeGetLink(sid) )
        ( AddressHeader[To](to), templateParams )

    def subject( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Set[ItemContent] ) : String =
      val args = ( subscribableName, withinTypeId, feedUrl, contents )
      config.SubjectCustomizers.get( subscribableName ).fold( defaultSubject.tupled(args) ): customizer =>
        customizer.tupled( args )

    def defaultSubject( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Set[ItemContent] ) : String

    def composeTemplateParams( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, destination : D, subscriptionId : SubscriptionId, removeLink : String ) : TemplateParams =
      val localArgs = ( subscribableName, withinTypeId, feedUrl, destination, subscriptionId, removeLink )
      val customizerArgs = ( subscribableName, withinTypeId, feedUrl, destination : Destination, subscriptionId, removeLink )
      TemplateParams( defaultComposeTemplateParams.tupled( localArgs ) ++ config.ComposeTemplateParamCustomizers.get( subscribableName ).fold(Nil)( _.tupled.apply(customizerArgs) ) )

    def defaultComposeTemplateParams( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, destination : D, subscriptionId : SubscriptionId, removeLink : String ) : Map[String,String] =
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
        "removeLink"        -> removeLink
      ).filter( _._2.nonEmpty )

    // the destination should already be validated before we get to this point.
    // we won't revalidate
    override def maybePromptConfirmation( conn : Connection, subscriptionId : SubscriptionId, subscribableName : SubscribableName, destination : D, confirmGetLink : String ) : Boolean =
      val subject = s"[${subscribableName}] Please confirm your new subscription" // XXX: Hardcoded subject, revisit someday
      val mailText =
        val confirmUntemplate = AllUntemplates.findConfirmUntemplate( confirmUntemplateName )
        val confirmInfo = ConfirmInfo( destination, subscribableName, this, confirmGetLink )
        confirmUntemplate( confirmInfo ).text
      PgDatabase.queueForMailing( conn, mailText, AddressHeader[From](from), replyTo.map(AddressHeader.apply[ReplyTo]), AddressHeader[To](destination.toAddress),TemplateParams.empty,subject)
      true

    override def validateDestinationOrThrow( conn : Connection, destination : Destination, subscribableName : SubscribableName ) : Unit =
      destination match
        case emailDestination : Destination.Email => Some( emailDestination )
        case _ =>
          throw new InvalidDestination( s"[${subscribableName}] Email subscription requires email destination. Destination '${destination}' is not. Rejecting." )

    override def htmlForStatusChange( statusChangeInfo : StatusChangeInfo ) : String =
      val untemplate = AllUntemplates.findStatusChangeUntemplate(statusChangeUntemplateName)
      untemplate( statusChangeInfo ).text

    override def displayShort( destination : D ) : String = destination.displayNamePart.getOrElse( destination.addressPart )
    override def displayFull( destination : D ) : String = destination.toAddress.rendered

  end Email

  def materialize( json : Json ) : SubscriptionManager = read[SubscriptionManager]( json.toString() )

  object Tag:
    def forJsonVal( jsonVal : String ) : Option[Tag] = Tag.values.find( _.jsonVal == jsonVal )
  enum Tag(val jsonVal : String):
    case Email_Each   extends Tag("Email.Each")
    case Email_Weekly extends Tag("Email.Weekly")

  private def toUJsonV1( subscriptionManager : SubscriptionManager ) : ujson.Value =
    def esf( emailsub : Email.Each | Email.Weekly ) : ujson.Obj =
      val values = Seq(
        "from" -> writeJs[Destination]( emailsub.from ),
        "composeUntemplateName" -> ujson.Str( emailsub.composeUntemplateName ),
        "confirmUntemplateName" -> ujson.Str( emailsub.confirmUntemplateName ),
        "statusChangeUntemplateName" -> ujson.Str( emailsub.statusChangeUntemplateName ),
        "extraParams" -> writeJs( emailsub.extraParams ) 
      ) ++ emailsub.replyTo.map( rt => ("replyTo" -> writeJs[Destination](rt) ) )
      ujson.Obj.from( values )
    def eef( each : Email.Each ) : ujson.Obj = esf( each )
    def ewf( weekly : Email.Weekly ) : ujson.Obj = esf( weekly )
    val (fields, tpe) =
      subscriptionManager match
        case each   : Email.Each   => (eef(each), Tag.Email_Each)
        case weekly : Email.Weekly => (ewf(weekly),  Tag.Email_Weekly)
    val headerFields =
       ujson.Obj(
         "version" -> 1,
         "type" -> tpe.jsonVal
       )
    ujson.Obj.from(headerFields.obj ++ fields.obj)

  private def fromUJsonV1( jsonValue : ujson.Value ) : SubscriptionManager =
    val obj = jsonValue.obj
    val version = obj.get("version").map( _.num.toInt )
    if version.nonEmpty && version != Some(1) then
      throw new InvalidSubscriptionManager(s"Unpickling SubscriptionManager, found version ${version.get}, expected version 1: " + obj.mkString(", "))
    else
      val tpe = obj("type").str
      val tag = Tag.forJsonVal(tpe).getOrElse:
        throw new InvalidSubscriptionManager(s"While unpickling a subscription manager, found unknown tag '$tpe': " + jsonValue)
      tag match
        case Tag.Email_Each => Email.Each(
          from = read[Destination](obj("from")).asInstanceOf[Destination.Email],
          replyTo = obj.get("replyTo").map( rtv => read[Destination](rtv).asInstanceOf[Destination.Email] ),
          composeUntemplateName      = obj("composeUntemplateName").str,
          confirmUntemplateName      = obj("confirmUntemplateName").str,
          statusChangeUntemplateName = obj("statusChangeUntemplateName").str,
          extraParams                = read[Map[String,String]](obj("extraParams"))
        )
        case Tag.Email_Weekly => Email.Weekly(
          from = read[Destination](obj("from")).asInstanceOf[Destination.Email],
          replyTo = obj.get("replyTo").map( rtv => read[Destination](rtv).asInstanceOf[Destination.Email] ),
          composeUntemplateName      = obj("composeUntemplateName").str,
          confirmUntemplateName      = obj("confirmUntemplateName").str,
          statusChangeUntemplateName = obj("statusChangeUntemplateName").str,
          extraParams                = read[Map[String,String]](obj("extraParams"))
        )

  given ReadWriter[SubscriptionManager] = readwriter[ujson.Value].bimap[SubscriptionManager]( toUJsonV1, fromUJsonV1 )

sealed trait SubscriptionManager extends Jsonable:
  type D <: Destination

  def sampleWithinTypeId : String
  def sampleDestination  : D // used for styling, but also to check at runtime that Destinations are of the expected class. See narrowXXX methods below
  def withinTypeId( conn : Connection, feedId : FeedId, guid : Guid, content : ItemContent, status : ItemStatus, lastCompleted : Option[AssignableWithinTypeStatus], mostRecentOpen : Option[AssignableWithinTypeStatus] ) : Option[String]
  def isComplete( conn : Connection, withinTypeId : String, currentCount : Int, lastAssigned : Instant ) : Boolean
  def validateDestinationOrThrow( conn : Connection, destination : Destination, subscribableName : SubscribableName ) : Unit

  /**
    * This method must either:
    *
    * * Send a notification that will lead to a future confirmation by the user, and return `true`
    *
    * OR
    *
    * * Update the the confirmed field of the subscription to already `true`, and then return false
    *
    * @param conn
    * @param destination
    * @param subscribableName
    * @param confirmGetLink
    *
    * @return whether a subscriber has been prompted for a future confirmation
    */
  def maybePromptConfirmation( conn : Connection, subscriptionId : SubscriptionId, subscribableName : SubscribableName, destination : D, confirmGetLink : String ) : Boolean

  def route( conn : Connection, assignableKey : AssignableKey, contents : Set[ItemContent], destinations : Set[IdentifiedDestination[D]], apiLinkGenerator : ApiLinkGenerator ) : Unit

  def json       : SubscriptionManager.Json = SubscriptionManager.Json( write[SubscriptionManager](this) )
  def jsonPretty : SubscriptionManager.Json = SubscriptionManager.Json( write[SubscriptionManager](this, indent=4) )

  def htmlForStatusChange( statusChangeInfo : StatusChangeInfo ) : String

  def materializeDestination( destinationJson : Destination.Json ) : D = Destination.materialize( destinationJson ).asInstanceOf[D]

  def narrowDestinationOrThrow( destination : Destination ) : D =
    if destination.getClass == sampleDestination.getClass then
      destination.asInstanceOf[D]
    else
      throw new InvalidDestination(s"Destination '$destination' is not valid for SubscriptionManager '$this'.")

  def narrowDestination( destination : Destination ) : Either[Destination,D] =
    if destination.getClass == sampleDestination.getClass then
      Right(destination.asInstanceOf[D])
    else
      Left(destination)

  def narrowIdentifiedDestinationOrThrow( idestination : IdentifiedDestination[Destination] ) : IdentifiedDestination[D] =
    if idestination.destination.getClass == sampleDestination.getClass then
      idestination.asInstanceOf[IdentifiedDestination[D]]
    else
      throw new InvalidDestination(s"Identified destination '$idestination' is not valid for SubscriptionManager '$this'.")

  def narrowIdentifiedDestination( idestination : IdentifiedDestination[Destination] ) : Either[IdentifiedDestination[Destination],IdentifiedDestination[D]] =
    if idestination.destination.getClass == sampleDestination.getClass then
      Right( idestination.asInstanceOf[IdentifiedDestination[D]] )
    else
      Left( idestination )

  def displayShort( destination : D ) : String
  def displayFull( destination : D ) : String

