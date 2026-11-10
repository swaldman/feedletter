package com.mchange.feedletter

import com.mchange.feedletter.style.*

import java.sql.Connection
import java.time.{DayOfWeek,LocalDate,LocalTime,Instant,Year,ZoneId,ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField

import DateTimeFormatter.ISO_LOCAL_DATE

import com.mchange.conveniences.string.*
import com.mchange.conveniences.collection.*

import com.mchange.feedletter.api.ApiLinkGenerator
import com.mchange.feedletter.db.PgDatabase

import audiofluidity.rss.Element

import upickle.default.*

import LoggingApi.*

/**
  * The root of the subscription-manager hierarchy.
  *
  * This trait and everything that extends it are `sealed`, so the whole hierarchy is
  * necessarily declared in this one file -- Scala 3 requires a sealed type's direct
  * children to share its source file. That is what keeps the exhaustive match in
  * SubscriptionManagerCodec.toUJsonV1 compiler-checked.
  */
sealed trait SubscriptionManager extends Jsonable:
  import SubscriptionManager.logAdapter

  type D  <: Destination

  def extraParams : Map[String,String]
  def withExtraParams( extraParams : Map[String,String] ) : SubscriptionManager

  def sampleWithinTypeId : String
  def sampleDestination  : D // used for styling, but also to check at runtime that Destinations are of the expected class. See narrowXXX methods below

  /*
   *  We used to let Iffy.HintAnnounce.Policy.Never prevent assignment, as well as explicit filters.
   *  We've removed that, so that content customizers potentially can override Iffy.HintAnnounce.Policy,
   *  or do whatever they like with the full content except as explicitly filtered via a customizer
   */
  final def withinTypeId( conn : Connection, subscribableName : SubscribableName, feedId : FeedId, guid : Guid, content : ItemContent, status : ItemStatus ) : Option[String] =
    if checkFilter( subscribableName, content ) then
      assignWithinTypeId( conn, subscribableName, feedId, guid, content, status )
    else
      INFO.log( s"Not assigning item due to a user-provided filter returning false. [subscribableName: ${subscribableName}, guid: ${guid}, content: ${content}]" )
      None

  protected def assignWithinTypeId( conn : Connection, subscribableName : SubscribableName, feedId : FeedId, guid : Guid, content : ItemContent, status : ItemStatus ) : Option[String]

  def isComplete( conn : Connection, feedId : FeedId, subscribableName : SubscribableName, withinTypeId : String, currentCount : Int, feedLastAssigned : Instant ) : Boolean

  def validateSubscriptionOrThrow( conn : Connection, fromExternalApi : Boolean, destination : Destination, subscribableName : SubscribableName ) : Unit =
    if fromExternalApi && !supportsExternalSubscriptionApi then
      throw new ExternalApiForibidden( s"[${subscribableName}]: Subscriptions must be made by an administrator, rather than via the public API." )
    else
      narrowDestination( destination ) match
        case Right( d ) => d.assertValid
        case Left ( _ ) =>
          throw new InvalidDestination( s"[${subscribableName}] Incorrect destination type. We expect a ${sampleDestination.getClass.getName()}. Destination '${destination}' is not. Rejecting." )

  def composeTemplateParams( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, destination : D, subscriptionId : SubscriptionId, removeLink : String ) : TemplateParams =
    TemplateParams(
      defaultComposeTemplateParams(subscribableName, withinTypeId, feedUrl, destination, subscriptionId, removeLink ) ++
      Customizer.TemplateParams.retrieve( subscribableName ).fold(Nil)( customizer => customizer( subscribableName, this, feedUrl, destination, subscriptionId, removeLink ) )
    )

  def defaultComposeTemplateParams( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, destination : D, subscriptionId : SubscriptionId, removeLink : String ) : Map[String,String]

  def customizeContents( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Seq[ItemContent], tz : ZoneId ) : Seq[ItemContent] =
    val customizer = Customizer.Contents.retrieve(subscribableName)
    customizer match
      case Some( c ) => c( subscribableName, this, feedUrl, contents, tz )
      case None => contents

  def checkFilter( subscribableName : SubscribableName, content : ItemContent ) : Boolean =
    val filter = Customizer.Filter.retrieve(subscribableName)
    filter match
      case Some( f ) => f( subscribableName, this, content )
      case None      => true

  final def route( conn : Connection, assignableKey : AssignableKey, contents : Seq[ItemContent], destinations : Set[IdentifiedDestination[D]], apiLinkGenerator : ApiLinkGenerator ) : Unit =
    val ( feedId, feedUrl ) = PgDatabase.feedIdUrlForSubscribableName( conn, assignableKey.subscribableName )
    val timeZone = bestTimeZone( conn )
    val transformedContents = transformContentsForRoute( assignableKey, feedUrl, contents, timeZone )
    if transformedContents.nonEmpty then doRoute(conn, assignableKey, feedId, feedUrl, transformedContents, destinations, timeZone, apiLinkGenerator)

  def hintAnnouncePolicy( subscribableName : SubscribableName, content : ItemContent ) : Element.Iffy.HintAnnounce.Policy =
    val restrictionFinder = Customizer.HintAnnounceRestriction.retrieve( subscribableName ).getOrElse( ( _, _, _ ) => None )
    restrictionFinder( subscribableName, this, content ).getOrElse( content.iffyHintAnnounceUnrestrictedPolicy )

  // XXX: this can filter or reorder, but if we ever add, we'll have to be careful about SubscriptionManagers that expect single-item contents
  def transformContentsForRoute( assignableKey : AssignableKey, feedUrl : FeedUrl, contents : Seq[ItemContent], timeZone : ZoneId ) : Seq[ItemContent] =
    val withCustomizedContents =
      customizeContents( assignableKey.subscribableName, assignableKey.withinTypeId, feedUrl, contents, timeZone )
    val withHintAnnounce =
      if this.respectHintAnnounce then
        applyHintAnnounceForRoute( assignableKey.subscribableName, assignableKey.withinTypeId, withCustomizedContents )
      else
        withCustomizedContents
    withHintAnnounce

  def respectHintAnnounce : Boolean = true

  def applyHintAnnounceForRoute( subscribableName : SubscribableName, withinTypeId : String, contents : Seq[ItemContent] ) : Seq[ItemContent] =
    def policy( content : ItemContent ) : Element.Iffy.HintAnnounce.Policy = hintAnnouncePolicy( subscribableName, content )
    val (never, notNever) = contents.partition( c => policy(c) == Element.Iffy.HintAnnounce.Policy.Never )
    // we now never even assign nevers, so never should always be empty
    def handleNotNevers =
      if notNever.forall( c => policy(c) == Element.Iffy.HintAnnounce.Policy.Piggyback) then
        if notNever.nonEmpty then INFO.log( s"""Skipping announcement entirely, due to hint-announce all Piggyback [subscribableName: ${subscribableName}, withinTypeId: ${withinTypeId}, skipped: ${notNever.mkString(", ")}]""" )
        Nil
      else
        notNever
    (never.nonEmpty, notNever.nonEmpty) match
      case ( true, true   ) =>
        INFO.log( s"""Skipping announcement of some items, due to hint-announce policy 'Never' [subscribableName: ${subscribableName}, withinTypeId: ${withinTypeId}, skipped: ${never.mkString(", ")}]""" )
        handleNotNevers
      case ( false, true  ) =>
        handleNotNevers
      case ( true, false  ) =>
        INFO.log( s"""Skipping announcement of all items, due to hint-announce policy 'Never' [subscribableName: ${subscribableName}, withinTypeId: ${withinTypeId}, skipped: ${never.mkString(", ")}]""" )
        Nil
      case ( false, false ) =>
        Nil

  protected def doRoute( conn : Connection, assignableKey : AssignableKey, feedId : FeedId, feedUrl : FeedUrl, contents : Seq[ItemContent], destinations : Set[IdentifiedDestination[D]], timeZone : ZoneId, apiLinkGenerator : ApiLinkGenerator ) : Unit

  def json       : SubscriptionManager.Json = SubscriptionManager.Json( write[SubscriptionManager](this) )
  def jsonPretty : SubscriptionManager.Json = SubscriptionManager.Json( write[SubscriptionManager](this, indent=4) )

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

  final def descShort( destination : D ) : String = destination.shortDesc
  final def descFull( destination : D ) : String  = destination.fullDesc

  // these we override as convenient
  def displayShort( destination : D ) : String = destination.shortDesc
  def displayFull( destination : D ) : String  = destination.fullDesc

  // this we defer until D is concrete
  def destinationCsvRowHeaders : Seq[String]
  final def destinationCsvRow( destination : D ) : Seq[String] = destination.toCsvRow

  def supportsExternalSubscriptionApi : Boolean = this.isInstanceOf[SubscriptionManager.SupportsExternalSubscriptionApi]

  def bestTimeZone( conn : Connection ) : ZoneId = PgDatabase.Config.timeZone( conn ) // SubscriptionManagers that allow locally-specified time zones should override

end SubscriptionManager

object SubscriptionManager extends SelfLogging:

  val  Json = SubscriptionManagerJson
  type Json = SubscriptionManagerJson

  def materialize( json : Json ) : SubscriptionManager = read[SubscriptionManager]( json.toString() )

  // ---------------------------------------------------------------------------
  // capability traits
  // ---------------------------------------------------------------------------

  sealed trait UntemplatedCompose extends SubscriptionManager:
    def composeUntemplateName : String
    def withComposeUntemplateName( name : String ) : UntemplatedCompose
    def isComposeMultiple : Boolean
  sealed trait UntemplatedConfirm extends SubscriptionManager:
    def confirmUntemplateName : String
    def withConfirmUntemplateName( name : String ) : UntemplatedConfirm
  sealed trait UntemplatedStatusChange extends SubscriptionManager:
    def statusChangeUntemplateName : String
    def withStatusChangeUntemplateName( name : String ) : UntemplatedStatusChange
  sealed trait UntemplatedRemovalNotification extends SubscriptionManager:
    def removalNotificationUntemplateName : String
    def withRemovalNotificationUntemplateName( name : String ) : UntemplatedRemovalNotification
  sealed trait PeriodBased extends SubscriptionManager:
    def timeZone : Option[ZoneId]
    override def bestTimeZone( conn : Connection ) : ZoneId = timeZone.getOrElse( PgDatabase.Config.timeZone( conn ) )

  sealed trait SupportsExternalSubscriptionApi extends SubscriptionManager:
    /**
      * This method must either:
      *
      * * Send a notification that will lead to a future confirmation by the user, and return `true`
      *
      * OR
      *
      * * Update the confirmed field of the subscription to already `true`, and then return `false`
      *
      * @param conn
      * @param destination
      * @param subscribableName
      * @param confirmGetLink
      *
      * @return whether a subscriber has been prompted for a future confirmation
      */
    def maybePromptConfirmation( conn : Connection, as : AppSetup, subscriptionId : SubscriptionId, subscribableName : SubscribableName, destination : this.D, confirmGetLink : String, removeGetLink : String ) : Boolean
    def maybeSendRemovalNotification( conn : Connection, as : AppSetup, subscriptionId : SubscriptionId, subscribableName : SubscribableName, destination : this.D, createGetLink : String ) : Boolean
    def htmlForStatusChange( statusChangeInfo : StatusChangeInfo ) : String

  sealed trait AbstractAnnounce extends SubscriptionManager:
      override def assignWithinTypeId( conn : Connection, subscribableName : SubscribableName, feedId : FeedId, guid : Guid, content : ItemContent, status : ItemStatus ) : Option[String] =
        Some( guid.toString() )

      override def isComplete( conn : Connection, feedId : FeedId, subscribableName : SubscribableName, withinTypeId : String, currentCount : Int, feedLastAssigned : Instant ) : Boolean = true

      def defaultFormatTemplate( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, content : ItemContent, tz : ZoneId ) : Option[String] = // ADD EXTRA-PARAMS AND GUIDs
        //assert( contents.size == 1, s"Mastodon.Announce expects contents exactly one item, while generating default subject, we found ${contents.size}." )
        ( content.title, content.author, content.link) match
          case (Some(title), Some(author), Some(link)) => Some( s"[${subscribableName}] New Post: ${title}, by ${author} ${link}" )
          case (Some(title), None,         Some(link)) => Some( s"[${subscribableName}] New Post: ${title} ${link}" )
          case (None,        Some(author), Some(link)) => Some( s"[${subscribableName}] New Untitled Post, by ${author} ${link}" )
          case (None,        None,         Some(link)) => Some( s"[${subscribableName}] New Untitled Post at ${link}" )
          case (_,           _,            None      ) =>
            WARNING.log( s"No link found, skipping masto announcement. withinTypeId: ${withinTypeId}" )
            None

  // ---------------------------------------------------------------------------
  // e-mail
  // ---------------------------------------------------------------------------

  sealed trait Email extends SubscriptionManager, UntemplatedCompose, UntemplatedConfirm, UntemplatedStatusChange, UntemplatedRemovalNotification, SupportsExternalSubscriptionApi:
    def from                              : Destination.Email
    def replyTo                           : Option[Destination.Email]
    def composeUntemplateName             : String
    def confirmUntemplateName             : String
    def statusChangeUntemplateName        : String
    def removalNotificationUntemplateName : String
    def extraParams                       : Map[String,String]

    type D = Destination.Email

    override def sampleDestination : Destination.Email = Destination.Email("user@example.com", Some("Some User"))

    protected def findTosWithTemplateParams( assignableKey : AssignableKey, feedUrl : FeedUrl, idestinations : Set[IdentifiedDestination[D]], apiLinkGenerator : ApiLinkGenerator ) : Set[(AddressHeader[To],TemplateParams)] =
      idestinations.map: idestination =>
        val to = idestination.destination.rendered
        val sid = idestination.subscriptionId
        val templateParams = composeTemplateParams( assignableKey.subscribableName, assignableKey.withinTypeId, feedUrl, idestination.destination, sid, apiLinkGenerator.removeGetLink(sid) )
        ( AddressHeader[To](to), templateParams )

    protected def doRouteSingle( conn : Connection, assignableKey : AssignableKey, feedId : FeedId, feedUrl : FeedUrl, contents : Seq[ItemContent], idestinations : Set[IdentifiedDestination[D]], timeZone : ZoneId, apiLinkGenerator : ApiLinkGenerator ) : Unit =
      val uniqueContent = contents.uniqueOr: (c, nu) =>
        throw new WrongContentsMultiplicity(s"${this}: We expect exactly one item to render, found $nu: " + contents.map( ci => (ci.title orElse ci.link).getOrElse("<item>") ).mkString(", "))
      val computedSubject = subject( assignableKey.subscribableName, assignableKey.withinTypeId, feedUrl, contents, timeZone )
      val fullTemplate =
        val info = ComposeInfo.Single( feedUrl, assignableKey.subscribableName, this, assignableKey.withinTypeId, timeZone, uniqueContent )
        val compose = AllUntemplates.findComposeUntemplateSingle(composeUntemplateName)
        compose( info ).text
      val tosWithTemplateParams = findTosWithTemplateParams( assignableKey, feedUrl, idestinations, apiLinkGenerator )
      PgDatabase.queueForMailing( conn, fullTemplate, AddressHeader[From](from), replyTo.map(AddressHeader.apply[ReplyTo]), tosWithTemplateParams, computedSubject)

    protected def doRouteMultiple( conn : Connection, assignableKey : AssignableKey, feedId : FeedId, feedUrl : FeedUrl, contents : Seq[ItemContent], idestinations : Set[IdentifiedDestination[D]], timeZone : ZoneId, apiLinkGenerator : ApiLinkGenerator ) : Unit =
      val computedSubject = subject( assignableKey.subscribableName, assignableKey.withinTypeId, feedUrl, contents, timeZone )
      val fullTemplate =
        val info = ComposeInfo.Multiple( feedUrl, assignableKey.subscribableName, this, assignableKey.withinTypeId, timeZone, contents )
        val compose = AllUntemplates.findComposeUntemplateMultiple(composeUntemplateName)
        compose( info ).text
      val tosWithTemplateParams = findTosWithTemplateParams( assignableKey, feedUrl, idestinations, apiLinkGenerator )
      PgDatabase.queueForMailing( conn, fullTemplate, AddressHeader[From](from), replyTo.map(AddressHeader.apply[ReplyTo]), tosWithTemplateParams, computedSubject)

    def subject( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Seq[ItemContent], tz : ZoneId ) : String =
      Customizer.Subject.retrieve( subscribableName ).fold( defaultSubject( subscribableName, withinTypeId, feedUrl, contents, tz : ZoneId ) ): customizer =>
        customizer( subscribableName, this, feedUrl, contents, tz )

    def defaultSubject( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Seq[ItemContent], tz : ZoneId ) : String

    override def defaultComposeTemplateParams( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, destination : D, subscriptionId : SubscriptionId, removeLink : String ) : Map[String,String] =
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
    override def maybePromptConfirmation( conn : Connection, as : AppSetup, subscriptionId : SubscriptionId, subscribableName : SubscribableName, destination : D, confirmGetLink : String, removeGetLink : String ) : Boolean =
      val subject = s"[${subscribableName}] Please confirm your new subscription" // XXX: Hardcoded subject, revisit someday
      val confirmHours = PgDatabase.Config.confirmHours( conn )
      val mailText =
        val confirmUntemplate = AllUntemplates.findConfirmUntemplate( confirmUntemplateName )
        val confirmInfo = ConfirmInfo( destination, subscribableName, this, confirmGetLink, removeGetLink, confirmHours )
        confirmUntemplate( confirmInfo ).text
      PgDatabase.mailImmediately( conn, as, mailText, AddressHeader[From](from), replyTo.map(AddressHeader.apply[ReplyTo]), AddressHeader[To](destination.toAddress),TemplateParams.empty,subject)
      true

    override def maybeSendRemovalNotification( conn : Connection, as : AppSetup, subscriptionId : SubscriptionId, subscribableName : SubscribableName, destination : this.D, createGetLink : String ) : Boolean =
      val subject = s"[${subscribableName}] Unsubscribed! We are sorry to see you go." // XXX: Hardcoded subject, revisit someday
      val mailText =
        val removalNotificationUntemplate = AllUntemplates.findRemovalNotificationUntemplate( removalNotificationUntemplateName )
        val removalNotificationInfo = RemovalNotificationInfo( subscribableName, this, destination, createGetLink )
        removalNotificationUntemplate( removalNotificationInfo ).text
      PgDatabase.mailImmediately( conn, as, mailText, AddressHeader[From](from), replyTo.map(AddressHeader.apply[ReplyTo]), AddressHeader[To](destination.toAddress),TemplateParams.empty,subject)
      true

    override def htmlForStatusChange( statusChangeInfo : StatusChangeInfo ) : String =
      val untemplate = AllUntemplates.findStatusChangeUntemplate(statusChangeUntemplateName)
      untemplate( statusChangeInfo ).text

    override def displayShort( destination : D ) : String = destination.displayNamePart.getOrElse( destination.addressPart )

    override def destinationCsvRowHeaders : Seq[String] = Destination.csvRowHeaders[D]

  end Email

  object Email:
    type Instance = Each | Daily | Weekly | BetterWeekly | Fixed

    case class Each(
      from                              : Destination.Email,
      replyTo                           : Option[Destination.Email],
      composeUntemplateName             : String,
      confirmUntemplateName             : String,
      statusChangeUntemplateName        : String,
      removalNotificationUntemplateName : String,
      extraParams                       : Map[String,String]
    ) extends Email:

      override val sampleWithinTypeId = "https://www.someblog.com/post/1111.html"

      override def withExtraParams( extraParams : Map[String,String] ) : Each = this.copy( extraParams = extraParams )

      override def withComposeUntemplateName( name : String )             : Each = this.copy( composeUntemplateName             = name )
      override def withConfirmUntemplateName( name : String )             : Each = this.copy( confirmUntemplateName             = name )
      override def withStatusChangeUntemplateName( name : String )        : Each = this.copy( statusChangeUntemplateName        = name )
      override def withRemovalNotificationUntemplateName( name : String ) : Each = this.copy( removalNotificationUntemplateName = name )

      override def isComposeMultiple : Boolean = false

      override def assignWithinTypeId( conn : Connection, subscribableName : SubscribableName, feedId : FeedId, guid : Guid, content : ItemContent, status : ItemStatus ) : Option[String] =
        Some( guid.toString() )

      override def isComplete( conn : Connection, feedId : FeedId, subscribableName : SubscribableName, withinTypeId : String, currentCount : Int, feedLastAssigned : Instant ) : Boolean = true

      override def doRoute( conn : Connection, assignableKey : AssignableKey, feedId : FeedId, feedUrl : FeedUrl, contents : Seq[ItemContent], idestinations : Set[IdentifiedDestination[D]], timeZone : ZoneId, apiLinkGenerator : ApiLinkGenerator ) : Unit =
        doRouteSingle( conn, assignableKey, feedId, feedUrl, contents, idestinations, timeZone, apiLinkGenerator )

      override def defaultSubject( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Seq[ItemContent], tz : ZoneId ) : String =
        assert( contents.size == 1, s"Email.Each expects contents exactly one item, while generating default subject, we found ${contents.size}." )
        s"[${subscribableName}] " + contents.head.title.fold("New Untitled Post")( title => s"New Post: ${title}" )

    // initially I tried to use java.time's WeekFields, but in subtle ways
    // it didn't behave as I expect.
    //
    // so i now use a much simpler NaiveWeek utility
    //
    // Weekly is retained only so that already-defined subscribables keep working. Its
    // weeks are anchored to January 1, so the weekday on which mail goes out drifts from
    // year to year, and the year's last week is a 1-2 day stub. New weekly subscribables
    // are backed by BetterWeekly, just below.

    final case class Weekly(
      from                              : Destination.Email,
      replyTo                           : Option[Destination.Email],
      composeUntemplateName             : String,
      confirmUntemplateName             : String,
      statusChangeUntemplateName        : String,
      removalNotificationUntemplateName : String,
      timeZone                          : Option[ZoneId],
      extraParams                       : Map[String,String]
    ) extends Email, PeriodBased:

      override val sampleWithinTypeId = "2023-week50"

      override def withExtraParams( extraParams : Map[String,String] ) : Weekly = this.copy( extraParams = extraParams )

      override def withComposeUntemplateName( name : String )             : Weekly = this.copy( composeUntemplateName             = name )
      override def withConfirmUntemplateName( name : String )             : Weekly = this.copy( confirmUntemplateName             = name )
      override def withStatusChangeUntemplateName( name : String )        : Weekly = this.copy( statusChangeUntemplateName        = name )
      override def withRemovalNotificationUntemplateName( name : String ) : Weekly = this.copy( removalNotificationUntemplateName = name )

      override def isComposeMultiple : Boolean = true

      // this is only fixed on assignment, should be lastChecked, because week in which firstSeen might already have passed
      override def assignWithinTypeId(
        conn             : Connection,
        subscribableName : SubscribableName,
        feedId           : FeedId,
        guid             : Guid,
        content          : ItemContent,
        status           : ItemStatus
      ) : Option[String] =
        val tz = bestTimeZone( conn )
        Some( NaiveWeek.format( status.lastChecked.atZone(tz) ) )

      override def isComplete( conn : Connection, feedId : FeedId, subscribableName : SubscribableName, withinTypeId : String, currentCount : Int, feedLastAssigned : Instant ) : Boolean =
        val ( year, woy ) = NaiveWeek.yearWeekFromString( withinTypeId )
        val tz = bestTimeZone( conn )
        val laZoned = feedLastAssigned.atZone(tz)
        val laYear = laZoned.get( ChronoField.YEAR )
        laYear > year || (laYear == year && NaiveWeek.week(laZoned) > woy)

      override def doRoute( conn : Connection, assignableKey : AssignableKey, feedId : FeedId, feedUrl : FeedUrl, contents : Seq[ItemContent], idestinations : Set[IdentifiedDestination[D]], timeZone : ZoneId, apiLinkGenerator : ApiLinkGenerator ) : Unit =
        doRouteMultiple( conn, assignableKey, feedId, feedUrl, contents, idestinations, timeZone, apiLinkGenerator )

      // these dates used to come out a day later on both ends than the actual dates in the
      // subscribable, and to throw outright for week 53. See the comment on
      // NaiveWeek.weekStartWeekEndLocalDate for what was wrong and why.
      def weekStartWeekEndFormattedIsoLocal( withinTypeId : String ) : (String,String) =
        val (weekStart, weekEnd) = NaiveWeek.weekStartWeekEndLocalDate(withinTypeId)
        (ISO_LOCAL_DATE.format(weekStart),ISO_LOCAL_DATE.format(weekEnd))

      override def defaultSubject( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Seq[ItemContent], tz : ZoneId ) : String =
        val allDates = contents.map( _.pubDate ).flatten.sorted.map( _.atZone(tz) )
        val (minDate, maxDate) =
          if allDates.isEmpty then
            weekStartWeekEndFormattedIsoLocal(withinTypeId) // backstop, when nothing in the batch carries a pubDate
          else
            (ISO_LOCAL_DATE.format(allDates.head), ISO_LOCAL_DATE.format(allDates.last))
        if minDate != maxDate then
          s"[${subscribableName}] Posts, ${minDate} to ${maxDate}"
        else
          s"[${subscribableName}] Posts from ${minDate}"

    /**
      * A weekly subscribable whose weeks are anchored to an explicit day-of-week and
      * time-of-day rather than to January 1. See BetterWeek for the arithmetic and for
      * why within-type-ids record the *end* of a period.
      */
    final case class BetterWeekly(
      from                              : Destination.Email,
      replyTo                           : Option[Destination.Email],
      composeUntemplateName             : String,
      confirmUntemplateName             : String,
      statusChangeUntemplateName        : String,
      removalNotificationUntemplateName : String,
      dayOfWeek                         : DayOfWeek,
      timeOfDay                         : LocalTime,
      timeZone                          : Option[ZoneId],
      extraParams                       : Map[String,String]
    ) extends Email, PeriodBased:

      override val sampleWithinTypeId = "2024-12-30T00:00:00" // a Monday midnight

      override def withExtraParams( extraParams : Map[String,String] ) : BetterWeekly = this.copy( extraParams = extraParams )

      override def withComposeUntemplateName( name : String )             : BetterWeekly = this.copy( composeUntemplateName             = name )
      override def withConfirmUntemplateName( name : String )             : BetterWeekly = this.copy( confirmUntemplateName             = name )
      override def withStatusChangeUntemplateName( name : String )        : BetterWeekly = this.copy( statusChangeUntemplateName        = name )
      override def withRemovalNotificationUntemplateName( name : String ) : BetterWeekly = this.copy( removalNotificationUntemplateName = name )

      override def isComposeMultiple : Boolean = true

      // keyed on lastChecked rather than firstSeen, for the same reason as Weekly above:
      // the period in which an item was first seen may already have closed
      override def assignWithinTypeId(
        conn             : Connection,
        subscribableName : SubscribableName,
        feedId           : FeedId,
        guid             : Guid,
        content          : ItemContent,
        status           : ItemStatus
      ) : Option[String] =
        val tz = bestTimeZone( conn )
        val local = status.lastChecked.atZone(tz).toLocalDateTime()
        Some( BetterWeek.format( BetterWeek.periodEnd( local, dayOfWeek, timeOfDay ) ) )

      override def isComplete( conn : Connection, feedId : FeedId, subscribableName : SubscribableName, withinTypeId : String, currentCount : Int, feedLastAssigned : Instant ) : Boolean =
        val deadline = BetterWeek.parse( withinTypeId )
        val tz = bestTimeZone( conn )
        !feedLastAssigned.atZone(tz).toLocalDateTime().isBefore( deadline )

      override def doRoute( conn : Connection, assignableKey : AssignableKey, feedId : FeedId, feedUrl : FeedUrl, contents : Seq[ItemContent], idestinations : Set[IdentifiedDestination[D]], timeZone : ZoneId, apiLinkGenerator : ApiLinkGenerator ) : Unit =
        doRouteMultiple( conn, assignableKey, feedId, feedUrl, contents, idestinations, timeZone, apiLinkGenerator )

      // the inclusive first and last dates covered by the period, derived from the
      // deadline the within-type-id records. periods are half-open, so the last date
      // covered is the day before the deadline unless the deadline falls at midnight
      def periodStartEndFormattedIsoLocal( withinTypeId : String ) : (String,String) =
        val periodEnd = BetterWeek.parse( withinTypeId )
        val firstDate = BetterWeek.periodStart( periodEnd ).toLocalDate()
        val lastDate  = periodEnd.toLocalDate().minusDays( if periodEnd.toLocalTime() == LocalTime.MIDNIGHT then 1 else 0 )
        (ISO_LOCAL_DATE.format(firstDate),ISO_LOCAL_DATE.format(lastDate))

      override def defaultSubject( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Seq[ItemContent], tz : ZoneId ) : String =
        val allDates = contents.map( _.pubDate ).flatten.sorted.map( _.atZone(tz) )
        val (minDate, maxDate) =
          if allDates.isEmpty then
            periodStartEndFormattedIsoLocal(withinTypeId)
          else
            (ISO_LOCAL_DATE.format(allDates.head), ISO_LOCAL_DATE.format(allDates.last))
        if minDate != maxDate then
          s"[${subscribableName}] Posts, ${minDate} to ${maxDate}"
        else
          s"[${subscribableName}] Posts from ${minDate}"


    final case class Daily(
      from                              : Destination.Email,
      replyTo                           : Option[Destination.Email],
      composeUntemplateName             : String,
      confirmUntemplateName             : String,
      statusChangeUntemplateName        : String,
      removalNotificationUntemplateName : String,
      timeZone                          : Option[ZoneId],
      extraParams                       : Map[String,String]
    ) extends Email, PeriodBased:

      // 'yyyy' is the calendar year; 'YYYY' would be the *week-based* year, the year owning
      // the week a date falls in. This used to say YYYY, which meant that for the last few
      // days of December -- the days whose week already belongs to the next year -- the
      // stored year ran ahead of (or, under ISO week rules, behind) the calendar year that
      // isComplete below reads back via ChronoField.YEAR. Assignables opened in that window
      // either stalled for roughly a year or completed the moment they opened, depending on
      // the JVM's locale, since week-based-year is defined by WeekFields and so varies with
      // it. Every other day of the year the two patterns agree exactly.
      private val WtiFormatter = DateTimeFormatter.ofPattern("yyyy-'day'DD")

      // exposed so the round trip against dayLocalDate can be tested; 'DD' pads to a minimum
      // of two digits, which is why the sample below is day04 rather than day4
      def dayWithinTypeId( zdt : ZonedDateTime ) : String = WtiFormatter.format( zdt )

      override val sampleWithinTypeId = "2024-day04"

      override def withExtraParams( extraParams : Map[String,String] ) : Daily = this.copy( extraParams = extraParams )

      override def withComposeUntemplateName( name : String )             : Daily = this.copy( composeUntemplateName             = name )
      override def withConfirmUntemplateName( name : String )             : Daily = this.copy( confirmUntemplateName             = name )
      override def withStatusChangeUntemplateName( name : String )        : Daily = this.copy( statusChangeUntemplateName        = name )
      override def withRemovalNotificationUntemplateName( name : String ) : Daily = this.copy( removalNotificationUntemplateName = name )

      override def isComposeMultiple : Boolean = true

      // this is only fixed on assignment, should be lastChecked, because week in which firstSeen might already have passed
      override def assignWithinTypeId(
        conn             : Connection,
        subscribableName : SubscribableName,
        feedId           : FeedId,
        guid             : Guid,
        content          : ItemContent,
        status           : ItemStatus
      ) : Option[String] =
        val tz = bestTimeZone( conn )
        Some( dayWithinTypeId( status.lastChecked.atZone(tz) ) )

      // parsed by hand rather than through the formatter, because the two numbers are read
      // back as independent quantities rather than resolved together as a date
      private def extractYearAndDay( withinTypeId : String ) : (Int, Int) =
        val ( yearStr, restStr ) = withinTypeId.span( Character.isDigit )
        val dayStr = restStr.dropWhile( c => !Character.isDigit(c) )
        ( yearStr.toInt, dayStr.toInt )

      override def isComplete( conn : Connection, feedId : FeedId, subscribableName : SubscribableName, withinTypeId : String, currentCount : Int, feedLastAssigned : Instant ) : Boolean =
        val ( year, day ) = extractYearAndDay( withinTypeId )
        val tz = bestTimeZone( conn )
        val laZoned = feedLastAssigned.atZone(tz)
        val laYear = laZoned.get( ChronoField.YEAR )
        laYear > year || (laYear == year && laZoned.get( ChronoField.DAY_OF_YEAR ) > day)

      override def doRoute( conn : Connection, assignableKey : AssignableKey, feedId : FeedId, feedUrl : FeedUrl, contents : Seq[ItemContent], idestinations : Set[IdentifiedDestination[D]], timeZone : ZoneId, apiLinkGenerator : ApiLinkGenerator ) : Unit =
        doRouteMultiple( conn, assignableKey, feedId, feedUrl, contents, idestinations, timeZone, apiLinkGenerator )

      // The day is clamped into the stored year because within-type-ids written before the
      // YYYY -> yyyy fix above can pair one year's number with another year's day-of-year.
      // '2025-day366' is the reachable case -- written on 2024-12-31, and invalid because
      // 2025 is not a leap year. Since defaultSubject calls this unconditionally, an
      // unclamped throw here would propagate out of route() and roll back the single
      // transaction in which completeAssignables routes *every* assignable, deterministically
      // on each retry. Clamping keeps such a legacy key merely wrong, rather than poisonous.
      def dayLocalDate( withinTypeId : String ) : LocalDate =
        val ( year, day ) = extractYearAndDay( withinTypeId )
        val lastDayOfYear = if Year.isLeap( year ) then 366 else 365
        LocalDate.ofYearDay( year, day.max( 1 ).min( lastDayOfYear ) )

      def dayFormattedIsoLocal( withinTypeId : String ) : String = ISO_LOCAL_DATE.format( dayLocalDate( withinTypeId ) )

      override def defaultSubject( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Seq[ItemContent], tz : ZoneId ) : String =
        s"[${subscribableName}] All posts, ${dayFormattedIsoLocal(withinTypeId)}"


    final case class Fixed(
      from                              : Destination.Email,
      replyTo                           : Option[Destination.Email],
      composeUntemplateName             : String,
      confirmUntemplateName             : String,
      statusChangeUntemplateName        : String,
      removalNotificationUntemplateName : String,
      numItemsPerLetter                 : Int,
      extraParams                       : Map[String,String]
    ) extends Email:
      override val sampleWithinTypeId = "1"

      override def withExtraParams( extraParams : Map[String,String] ) : Fixed = this.copy( extraParams = extraParams )

      override def withComposeUntemplateName( name : String )             : Fixed = this.copy( composeUntemplateName             = name )
      override def withConfirmUntemplateName( name : String )             : Fixed = this.copy( confirmUntemplateName             = name )
      override def withStatusChangeUntemplateName( name : String )        : Fixed = this.copy( statusChangeUntemplateName        = name )
      override def withRemovalNotificationUntemplateName( name : String ) : Fixed = this.copy( removalNotificationUntemplateName = name )

      override def isComposeMultiple : Boolean = true

      // this is only fixed on assignment, should be lastChecked, because week in which firstSeen might already have passed
      override def assignWithinTypeId(
        conn             : Connection,
        subscribableName : SubscribableName,
        feedId           : FeedId,
        guid             : Guid,
        content          : ItemContent,
        status           : ItemStatus
      ) : Option[String] =
        def nextAfter( wti : String ) : String = (wti.toLong + 1).toString
        PgDatabase.mostRecentlyOpenedAssignableWithinTypeStatus( conn, subscribableName ) match
          case Some( AssignableWithinTypeStatus( withinTypeId, count ) ) =>
            if count < numItemsPerLetter then Some( withinTypeId ) else Some( nextAfter(withinTypeId) )
          case None =>
            PgDatabase.lastCompletedWithinTypeId( conn, subscribableName ) match
              case Some(wti) => Some(nextAfter(wti))
              case None => // first series!
                Some("1")

      override def isComplete( conn : Connection, feedId : FeedId, subscribableName : SubscribableName, withinTypeId : String, currentCount : Int, feedLastAssigned : Instant ) : Boolean =
        currentCount == numItemsPerLetter

      override def doRoute( conn : Connection, assignableKey : AssignableKey, feedId : FeedId, feedUrl : FeedUrl, contents : Seq[ItemContent], idestinations : Set[IdentifiedDestination[D]], timeZone : ZoneId, apiLinkGenerator : ApiLinkGenerator ) : Unit =
        doRouteMultiple( conn, assignableKey, feedId, feedUrl, contents, idestinations, timeZone, apiLinkGenerator )

      override def defaultSubject( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Seq[ItemContent], tz : ZoneId ) : String =
        s"[${subscribableName}] ${numItemsPerLetter} new items"

  end Email

  // ---------------------------------------------------------------------------
  // mastodon
  // ---------------------------------------------------------------------------

  sealed trait Mastodon extends SubscriptionManager:
    override type D = Destination.Mastodon

    override val sampleDestination = Destination.Mastodon( name = "mothership", instanceUrl = "https://mastodon.social/" )

    override def destinationCsvRowHeaders : Seq[String] = Destination.csvRowHeaders[D]
  end Mastodon

  object Mastodon:
    final case class Announce( extraParams : Map[String,String] ) extends SubscriptionManager.Mastodon, SubscriptionManager.AbstractAnnounce:

      override val sampleWithinTypeId = "https://www.someblog.com/post/1111.html"

      override def withExtraParams( extraParams : Map[String,String] ) : Announce = this.copy( extraParams = extraParams )

      def formatTemplate( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, content : ItemContent, tz : ZoneId ) : Option[String] =
        Customizer.MastoAnnouncement.retrieve( subscribableName ).fold( defaultFormatTemplate( subscribableName, withinTypeId, feedUrl, content, tz ) ): customizer =>
          customizer( subscribableName, this, feedUrl, content, tz )

      override def doRoute( conn : Connection, assignableKey : AssignableKey, feedId : FeedId, feedUrl : FeedUrl, contents : Seq[ItemContent], idestinations : Set[IdentifiedDestination[D]], timeZone : ZoneId, apiLinkGenerator : ApiLinkGenerator ) : Unit =
        val uniqueContent = contents.uniqueOr: (c, nu) =>
          throw new WrongContentsMultiplicity(s"${this}: We expect exactly one item to render, found $nu: " + contents.map( ci => (ci.title orElse ci.link).getOrElse("<item>") ).mkString(", "))
        val mbTemplate = formatTemplate( assignableKey.subscribableName, assignableKey.withinTypeId, feedUrl, uniqueContent, timeZone )
        mbTemplate.foreach: template =>
          val mastoDestinationsWithTemplateParams =
            idestinations.map: idestination =>
              val destination = idestination.destination
              val sid = idestination.subscriptionId
              val templateParams = composeTemplateParams( assignableKey.subscribableName, assignableKey.withinTypeId, feedUrl, destination, sid, apiLinkGenerator.removeGetLink(sid) )
              ( destination, templateParams )
          mastoDestinationsWithTemplateParams.foreach: ( destination, templateParams ) =>
            val fullContent = templateParams.fill( template )
            PgDatabase.queueForMastoPost( conn, fullContent, MastoInstanceUrl( destination.instanceUrl ), MastoName( destination.name ), uniqueContent.media )

      override def defaultComposeTemplateParams( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, destination : D, subscriptionId : SubscriptionId, removeLink : String ) : Map[String,String] =
        Map(
          "instanceUrl" -> destination.instanceUrl,
          "destinationName" -> destination.name
        )

  end Mastodon

  // ---------------------------------------------------------------------------
  // bluesky
  // ---------------------------------------------------------------------------

  sealed trait BlueSky extends SubscriptionManager:
    override type D = Destination.BlueSky

    override val sampleDestination = Destination.BlueSky( entrywayUrl = "https://bsky.social/", identifier = "interfluidity.com" )

    override def destinationCsvRowHeaders : Seq[String] = Destination.csvRowHeaders[D]
  end BlueSky

  object BlueSky:
    final case class Announce( extraParams : Map[String,String] ) extends SubscriptionManager.BlueSky, SubscriptionManager.AbstractAnnounce:

      override val sampleWithinTypeId = "https://www.someblog.com/post/1111.html"

      override def withExtraParams( extraParams : Map[String,String] ) : Announce = this.copy( extraParams = extraParams )

      def formatTemplate( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, content : ItemContent, tz : ZoneId ) : Option[String] =
        Customizer.BskyAnnouncement.retrieve( subscribableName ).fold( defaultFormatTemplate( subscribableName, withinTypeId, feedUrl, content, tz ) ): customizer =>
          customizer( subscribableName, this, feedUrl, content, tz )

      override def doRoute( conn : Connection, assignableKey : AssignableKey, feedId : FeedId, feedUrl : FeedUrl, contents : Seq[ItemContent], idestinations : Set[IdentifiedDestination[D]], timeZone : ZoneId, apiLinkGenerator : ApiLinkGenerator ) : Unit =
        val uniqueContent = contents.uniqueOr: (c, nu) =>
          throw new WrongContentsMultiplicity(s"${this}: We expect exactly one item to render, found $nu: " + contents.map( ci => (ci.title orElse ci.link).getOrElse("<item>") ).mkString(", "))
        val mbTemplate = formatTemplate( assignableKey.subscribableName, assignableKey.withinTypeId, feedUrl, uniqueContent, timeZone )
        mbTemplate.foreach: template =>
          val bskyDestinationsWithTemplateParams =
            idestinations.map: idestination =>
              val destination = idestination.destination
              val sid = idestination.subscriptionId
              val templateParams = composeTemplateParams( assignableKey.subscribableName, assignableKey.withinTypeId, feedUrl, destination, sid, apiLinkGenerator.removeGetLink(sid) )
              ( destination, templateParams )
          bskyDestinationsWithTemplateParams.foreach: ( destination, templateParams ) =>
            val fullContent = templateParams.fill( template )
            PgDatabase.queueForBskyPost( conn, fullContent, BskyEntrywayUrl( destination.entrywayUrl ), BskyIdentifier( destination.identifier ), uniqueContent.media )

      override def defaultComposeTemplateParams( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, destination : D, subscriptionId : SubscriptionId, removeLink : String ) : Map[String,String] =
        Map(
          "entrywayUrl" -> destination.entrywayUrl,
          "identifier" -> destination.identifier
        )

  end BlueSky

  // ---------------------------------------------------------------------------
  // wire format
  //
  // Tag.jsonVal strings are a persisted contract -- they appear in the
  // subscription_manager_json column of the subscribable table. The codec
  // mechanics live in SubscriptionManagerCodec; the given must stay here, where
  // upickle's implicit resolution looks for it.
  // ---------------------------------------------------------------------------

  object Tag:
    def forJsonVal( jsonVal : String ) : Option[Tag] = Tag.values.find( _.jsonVal == jsonVal )
  enum Tag(val jsonVal : String):
    case Email_Each         extends Tag("Email.Each")
    case Email_Daily        extends Tag("Email.Daily")
    case Email_Weekly       extends Tag("Email.Weekly")
    case Email_BetterWeekly extends Tag("Email.BetterWeekly")
    case Email_Fixed        extends Tag("Email.Fixed")
    case Masto_Announce     extends Tag("Mastodon.Announce")
    case Bsky_Announce      extends Tag("BlueSky.Announce")

  given ReadWriter[SubscriptionManager] =
    readwriter[ujson.Value].bimap[SubscriptionManager]( SubscriptionManagerCodec.toUJsonV1, SubscriptionManagerCodec.fromUJsonV1 )

end SubscriptionManager
