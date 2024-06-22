package com.mchange.feedletter

import com.mchange.feedletter.style.*

import java.sql.Connection
import java.time.{LocalDate,Instant}
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoField,WeekFields}

import DateTimeFormatter.ISO_LOCAL_DATE

import scala.collection.immutable

import com.mchange.feedletter.api.ApiLinkGenerator

import scala.collection.immutable

import com.mchange.conveniences.string.*
import com.mchange.conveniences.collection.*
import com.mchange.feedletter.db.PgDatabase
import scala.util.control.NonFatal

import MLevel.*

import upickle.default.*
import java.time.ZoneId
import com.mchange.feedletter.Destination.Key
import com.mchange.feedletter.db.PgDatabase.Config.timeZone

object SubscriptionManager:
  private lazy given logger : MLogger = MLogger(this)

  val  Json = SubscriptionManagerJson
  type Json = SubscriptionManagerJson

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
      * * Update the the confirmed field of the subscription to already `true`, and then return false
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

  object Mastodon:
    final case class Announce( extraParams : Map[String,String] ) extends SubscriptionManager.Mastodon:

      override val sampleWithinTypeId = "https://www.someblog.com/post/1111.html"

      override def withExtraParams( extraParams : Map[String,String] ) : Announce = this.copy( extraParams = extraParams )

      override def assignWithinTypeId( conn : Connection, subscribableName : SubscribableName, feedId : FeedId, guid : Guid, content : ItemContent, status : ItemStatus ) : Option[String] =
        Some( guid.toString() )

      override def isComplete( conn : Connection, feedId : FeedId, subscribableName : SubscribableName, withinTypeId : String, currentCount : Int, feedLastAssigned : Instant ) : Boolean = true

      def formatTemplate( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, content : ItemContent, tz : ZoneId ) : Option[String] =
        Customizer.MastoAnnouncement.retrieve( subscribableName ).fold( defaultFormatTemplate( subscribableName, withinTypeId, feedUrl, content, tz ) ): customizer =>
          customizer( subscribableName, this, feedUrl, content, tz )

      def defaultFormatTemplate( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, content : ItemContent, tz : ZoneId ) : Option[String] = // ADD EXTRA-PARAMS AND GUIDs
        //assert( contents.size == 1, s"Mastodon.Announce expects contents exactly one item, while generating default subject, we found ${contents.size}." )
        ( content.title, content.author, content.link) match
          case (Some(title), Some(author), Some(link)) => Some( s"[${subscribableName}] New Post: ${title}, by ${author} ${link}" )
          case (Some(title), None,         Some(link)) => Some( s"[${subscribableName}] New Post: ${title} ${link}" )
          case (None,        Some(author), Some(link)) => Some( s"[${subscribableName}] New Untitled Post, by ${author} ${link}" )
          case (None,        None,         Some(link)) => Some( s"[${subscribableName}] New Untitled Post at ${link}" )
          case (_,           _,            None      ) =>
            WARNING.log( s"No link found. withinTypeId: ${withinTypeId}" ) 
            None

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

  sealed trait Mastodon extends SubscriptionManager:
    override type D = Destination.Mastodon

    override val sampleDestination = Destination.Mastodon( name = "mothership", instanceUrl = "https://mastodon.social/" )

    override def destinationCsvRowHeaders : Seq[String] = Destination.csvRowHeaders[D]
  end Mastodon

  object Email:
    type Companion = Each.type | Daily.type | Weekly.type | Fixed.type
    type Instance  = Each      | Daily      | Weekly      | Fixed

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

      private val WtiFormatter = DateTimeFormatter.ofPattern("YYYY-'week'ww")

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
        Some( WtiFormatter.format( status.lastChecked.atZone(tz) ) )

      // Regular TemporalFields don't work on the formatter-parsed accessor. We need a WeekFields thang first 
      private def extractYearWeekAndWeekFields( withinTypeId : String ) : (Int, Int, WeekFields) =
        val ( yearStr, restStr ) = withinTypeId.span( Character.isDigit )
        val year = yearStr.toInt
        val baseDayOfWeek = LocalDate.of( year, 1, 1 ).getDayOfWeek()
        val weekNum = restStr.dropWhile( c => !Character.isDigit(c) ).toInt
        ( year, weekNum, WeekFields.of(baseDayOfWeek, 1) )

      override def isComplete( conn : Connection, feedId : FeedId, subscribableName : SubscribableName, withinTypeId : String, currentCount : Int, feedLastAssigned : Instant ) : Boolean =
        val ( year, woy, weekFields ) = extractYearWeekAndWeekFields( withinTypeId )
        val tz = bestTimeZone( conn )
        val laZoned = feedLastAssigned.atZone(tz)
        val laYear = laZoned.get( ChronoField.YEAR )
        laYear > year || (laYear == year && laZoned.get( ChronoField.ALIGNED_WEEK_OF_YEAR ) > woy)

      override def doRoute( conn : Connection, assignableKey : AssignableKey, feedId : FeedId, feedUrl : FeedUrl, contents : Seq[ItemContent], idestinations : Set[IdentifiedDestination[D]], timeZone : ZoneId, apiLinkGenerator : ApiLinkGenerator ) : Unit =
        doRouteMultiple( conn, assignableKey, feedId, feedUrl, contents, idestinations, timeZone, apiLinkGenerator )

      // not sure why, but there's an off-by-on issue. these dates tend to be a day later on both ends
      // than the actual dates in the subscribable.
      // maybe use LocalDateTime set to noon to avoid timezone issues?
      // Or use ZonedDateTime at bestTimeZone (but we don't have conn)
      def weekStartWeekEndLocalDate( withinTypeId : String ) : (LocalDate,LocalDate) =
        val ( year, woy, weekFields ) = extractYearWeekAndWeekFields( withinTypeId )
        val weekStart = LocalDate.of(year, 1, 1).`with`( weekFields.weekOfWeekBasedYear(), woy ).`with`(weekFields.dayOfWeek(), 1 )
        val weekEnd = LocalDate.of(year, 1, 1).`with`( weekFields.weekOfWeekBasedYear(), woy ).`with`(weekFields.dayOfWeek(), 7 )
        (weekStart, weekEnd)

      // not sure why, but there's an off-by-on issue. these dates tend to be a day later on both ends
      // than the actual dates in the subscribable. See comment above
      def weekStartWeekEndFormattedIsoLocal( withinTypeId : String ) : (String,String) =
        val (weekStart, weekEnd) = weekStartWeekEndLocalDate(withinTypeId)
        (ISO_LOCAL_DATE.format(weekStart),ISO_LOCAL_DATE.format(weekEnd))

      override def defaultSubject( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Seq[ItemContent], tz : ZoneId ) : String =
        val allDates = contents.map( _.pubDate ).flatten.sorted.map( _.atZone(tz) )
        val (minDate, maxDate) =
          if allDates.isEmpty then
            weekStartWeekEndFormattedIsoLocal(withinTypeId) // backstop, but see comments above!
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

      private val WtiFormatter = DateTimeFormatter.ofPattern("YYYY-'day'DD")

      override val sampleWithinTypeId = "2024-day4"

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
        Some( WtiFormatter.format( status.lastChecked.atZone(tz) ) )

      // Regular TemporalFields don't work on the formatter-parsed accessor. We need a WeekFields thang first 
      private def extractYearAndDay( withinTypeId : String ) : (Int, Int) =
        val ( yearStr, restStr ) = withinTypeId.span( Character.isDigit )
        val dayStr = restStr.dropWhile( c => !Character.isDigit(c) ).toInt
        ( yearStr.toInt, dayStr.toInt )

      override def isComplete( conn : Connection, feedId : FeedId, subscribableName : SubscribableName, withinTypeId : String, currentCount : Int, feedLastAssigned : Instant ) : Boolean =
        val ( year, day ) = extractYearAndDay( withinTypeId )
        val tz = bestTimeZone( conn )
        val laZoned = feedLastAssigned.atZone(tz)
        val laYear = laZoned.get( ChronoField.YEAR )
        laYear > year || (laYear == year && laZoned.get( ChronoField.DAY_OF_YEAR ) > day)

      override def doRoute( conn : Connection, assignableKey : AssignableKey, feedId : FeedId, feedUrl : FeedUrl, contents : Seq[ItemContent], idestinations : Set[IdentifiedDestination[D]], timeZone : ZoneId, apiLinkGenerator : ApiLinkGenerator ) : Unit =
        doRouteMultiple( conn, assignableKey, feedId, feedUrl, contents, idestinations, timeZone, apiLinkGenerator )

      def dayLocalDate( withinTypeId : String ) : LocalDate =
        val ( year, day ) = extractYearAndDay( withinTypeId )
        LocalDate.ofYearDay( year, day )

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
      private val WtiFormatter = DateTimeFormatter.ofPattern("YYYY-'day'DD")

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

  def materialize( json : Json ) : SubscriptionManager = read[SubscriptionManager]( json.toString() )

  object Tag:
    def forJsonVal( jsonVal : String ) : Option[Tag] = Tag.values.find( _.jsonVal == jsonVal )
  enum Tag(val jsonVal : String):
    case Email_Each     extends Tag("Email.Each")
    case Email_Daily    extends Tag("Email.Daily")
    case Email_Weekly   extends Tag("Email.Weekly")
    case Email_Fixed    extends Tag("Email.Fixed")
    case Masto_Announce extends Tag("Mastodon.Announce")

  private def toUJsonV1( subscriptionManager : SubscriptionManager ) : ujson.Value =
    def esf( emailsub : Email.Instance ) : ujson.Obj = // "email shared fields"
      val values = Seq(
        "from" -> writeJs[Destination]( emailsub.from ),
        "composeUntemplateName" -> ujson.Str( emailsub.composeUntemplateName ),
        "confirmUntemplateName" -> ujson.Str( emailsub.confirmUntemplateName ),
        "statusChangeUntemplateName" -> ujson.Str( emailsub.statusChangeUntemplateName ),
        "removalNotificationUntemplateName" -> ujson.Str( emailsub.removalNotificationUntemplateName ),
        "extraParams" -> writeJs( emailsub.extraParams ) 
      ) ++ emailsub.replyTo.map( rt => ("replyTo" -> writeJs[Destination](rt) ) )
      ujson.Obj.from( values )
    def epbsf( pbsm : Email.Instance & PeriodBased ) : ujson.Obj = // "email period-based shared fields"
      pbsm.timeZone match
        case Some( tz ) => ujson.Obj.from( esf( pbsm ).obj addOne( ("timeZone" -> tz.getId()) ) )
        case None       => esf( pbsm )
    def eef( each : Email.Each ) : ujson.Obj = esf( each )
    def edf( daily : Email.Daily ) : ujson.Obj = epbsf( daily )
    def ewf( weekly : Email.Weekly ) : ujson.Obj = epbsf( weekly )
    def eff( fixed : Email.Fixed ) : ujson.Obj = ujson.Obj.from( esf( fixed ).obj addOne( ("numItemsPerLetter" -> fixed.numItemsPerLetter) ) )
    def maf( announce : Mastodon.Announce ) : ujson.Obj = ujson.Obj( // "mastodon announce fields"
      "extraParams" -> writeJs( announce.extraParams ) 
    )
    val (fields, tpe) =
      subscriptionManager match
        case each     : Email.Each        => ( eef(each),     Tag.Email_Each     )
        case daily    : Email.Daily       => ( edf(daily),    Tag.Email_Daily    )
        case weekly   : Email.Weekly      => ( ewf(weekly),   Tag.Email_Weekly   )
        case fixed    : Email.Fixed       => ( eff(fixed),    Tag.Email_Fixed    )
        case announce : Mastodon.Announce => ( maf(announce), Tag.Masto_Announce )
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
          composeUntemplateName             = obj("composeUntemplateName").str,
          confirmUntemplateName             = obj("confirmUntemplateName").str,
          statusChangeUntemplateName        = obj("statusChangeUntemplateName").str,
          removalNotificationUntemplateName = obj("removalNotificationUntemplateName").str,
          extraParams                       = read[Map[String,String]](obj("extraParams"))
        )
        case Tag.Email_Daily => Email.Daily(
          from = read[Destination](obj("from")).asInstanceOf[Destination.Email],
          replyTo = obj.get("replyTo").map( rtv => read[Destination](rtv).asInstanceOf[Destination.Email] ),
          composeUntemplateName             = obj("composeUntemplateName").str,
          confirmUntemplateName             = obj("confirmUntemplateName").str,
          statusChangeUntemplateName        = obj("statusChangeUntemplateName").str,
          removalNotificationUntemplateName = obj("removalNotificationUntemplateName").str,
          timeZone                          = obj.get("timeZone").map( _.str ).map( ZoneId.of ),
          extraParams                       = read[Map[String,String]](obj("extraParams"))
        )
        case Tag.Email_Weekly => Email.Weekly(
          from = read[Destination](obj("from")).asInstanceOf[Destination.Email],
          replyTo = obj.get("replyTo").map( rtv => read[Destination](rtv).asInstanceOf[Destination.Email] ),
          composeUntemplateName             = obj("composeUntemplateName").str,
          confirmUntemplateName             = obj("confirmUntemplateName").str,
          statusChangeUntemplateName        = obj("statusChangeUntemplateName").str,
          removalNotificationUntemplateName = obj("removalNotificationUntemplateName").str,
          timeZone                          = obj.get("timeZone").map( _.str ).map( ZoneId.of ),
          extraParams                       = read[Map[String,String]](obj("extraParams"))
        )
        case Tag.Email_Fixed => Email.Fixed(
          from = read[Destination](obj("from")).asInstanceOf[Destination.Email],
          replyTo = obj.get("replyTo").map( rtv => read[Destination](rtv).asInstanceOf[Destination.Email] ),
          composeUntemplateName             = obj("composeUntemplateName").str,
          confirmUntemplateName             = obj("confirmUntemplateName").str,
          statusChangeUntemplateName        = obj("statusChangeUntemplateName").str,
          removalNotificationUntemplateName = obj("removalNotificationUntemplateName").str,
          numItemsPerLetter                 = obj("numItemsPerLetter").num.toInt,
          extraParams                       = read[Map[String,String]](obj("extraParams"))
        )
        case Tag.Masto_Announce => Mastodon.Announce(
          extraParams = read[Map[String,String]](obj("extraParams"))
        )

  given ReadWriter[SubscriptionManager] = readwriter[ujson.Value].bimap[SubscriptionManager]( toUJsonV1, fromUJsonV1 )

sealed trait SubscriptionManager extends Jsonable:
  import SubscriptionManager.logger

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
        case Right( _ ) => ()
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

  def hintAnnouncePolicy( subscribableName : SubscribableName, content : ItemContent ) : Iffy.HintAnnounce.Policy =
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
    def policy( content : ItemContent ) : Iffy.HintAnnounce.Policy = hintAnnouncePolicy( subscribableName, content )
    val (never, notNever) = contents.partition( c => policy(c) == Iffy.HintAnnounce.Policy.Never )
    // we now never even assign nevers, so never should always be empty
    def handleNotNevers =
      if notNever.forall( c => policy(c) == Iffy.HintAnnounce.Policy.Piggyback) then
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
