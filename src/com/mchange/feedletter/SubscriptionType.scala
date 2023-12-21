package com.mchange.feedletter

import java.sql.Connection
import java.time.{LocalDate,Instant}
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoField,WeekFields}

import DateTimeFormatter.ISO_LOCAL_DATE

import scala.collection.immutable

import com.mchange.feedletter.db.{AssignableKey, AssignableWithinTypeStatus, ItemStatus}

import scala.collection.immutable

import com.mchange.conveniences.www.*
import com.mchange.conveniences.string.*
import com.mchange.conveniences.collection.*
import com.mchange.feedletter.db.PgDatabase
import com.mchange.mailutil.*
import com.mchange.sc.v1.log.*
import MLevel.*
import scala.util.control.NonFatal

object SubscriptionType:
  private lazy given logger : MLogger = mlogger(this)

  private val GeneralRegex = """^(\w+)\.(\w+)\:(.*)$""".r

  sealed trait Untemplated:
    def untemplateName : String
  sealed trait Templating:
    def templateParams( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, destination : Destination, contents : Set[ItemContent] ) : TemplateParams
  sealed trait Factory:
    def apply( params : Seq[(String,String)] ) : SubscriptionType

  object Email:
    object Each extends Factory:
      override def apply( params : Seq[(String,String)] ) : Each = new Each(params)
    class Each( params : Seq[(String,String)] ) extends Email("Each", params):

      override val sampleWithinTypeId = "https://www.someblog.com/post/1111.html"
      
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
        PgDatabase.queueForMailing( conn, fullTemplate, from.mkString(","), replyTo.mkString(",").toOptionNotBlank, tosWithTemplateParams, computedSubject)

      override def defaultSubject( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Set[ItemContent] ) : String =
        assert( contents.size == 1, s"Email.Each expects contents exactly one item, while generating default subject, we found ${contents.size}." )
        s"[${subscribableName}] " + contents.head.title.fold("New Untitled Post")( title => s"New Post: ${title}" )

    object Weekly extends Factory:
      override def apply( params : Seq[(String,String)] ) : Weekly = new Weekly(params)
    class Weekly( params : Seq[(String,String)] ) extends Email( "Weekly", params ):
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
          PgDatabase.queueForMailing( conn, fullTemplate, from.mkString(","), replyTo.mkString(",").toOptionNotBlank, tosWithTemplateParams, computedSubject)

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

  abstract class Email(subcategory : String, params : Seq[Tuple2[String,String]]) extends SubscriptionType("Email", subcategory, params),Untemplated,Templating:
    val from    : Seq[String] = paramsAllValues("from")
    val replyTo : Seq[String] = paramsAllValues("replyTo")

    override val untemplateName : String = paramsAllValues("untemplateName").uniqueOr: (c, nu) =>
      throw new InvalidSubscriptionType(s"Each email subscription should define a unique untemplate name, found ${nu}: $c")

    if from.isEmpty then
      throw new InvalidSubscriptionType("All e-mail subscriptions must have a from address defined. Params: " + params.mkString(" "))
    else
      try
        from.foreach( Smtp.Address.parseCommaSeparated(_, strict=true) )
        replyTo.foreach( Smtp.Address.parseCommaSeparated(_, strict=true) )
      catch
        case NonFatal(t) =>
          throw new InvalidSubscriptionType(
            "Failed to parse as e-mail addresses, to and/or replyTo elements: to -> " +
            from.mkString(", ") + "; replyTo -> " + replyTo.mkString(", "),
            t
          )

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
      params.toMap ++ Map(
        "from"              -> from.mkString(", "),
        "replyTo"           -> replyTo.mkString(", "),
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

    if from.isEmpty then
      throw new InvalidSubscriptionType(
        "An Email subscription type must include at least one 'from' param. Params given: " +
        params.map( (k,v) => k + " -> " + v ).mkString(", ")
      )
  end Email

  def dispatch( category : String, subcategory : String, params : Seq[(String,String)] ) : Option[SubscriptionType] =
    ( category, subcategory ) match
      case ("Email", "Each")   => Some( Email.Each(params) )
      case ("Email", "Weekly") => Some( Email.Weekly(params) )
      case _                   => None

  private def preparse( s : String ) : Option[Tuple3[String,String,Seq[Tuple2[String,String]]]] =
    s match
      case GeneralRegex( category, subcategory, params ) => Some( Tuple3( category, subcategory, wwwFormDecodeUTF8( params ) ) )
      case _ => None

  def parse( str : String ) : SubscriptionType =
      preparse( str ).flatMap( dispatch.tupled ).getOrElse:
        throw new InvalidSubscriptionType(s"'${str}' could not be parsed into a valid subscription type.")

  private def semisort( unsortedParams : Seq[(String,String)] ) : Seq[(String,String)] =
    val sortedKeys = immutable.SortedSet.from( unsortedParams.map( _(0) ) )
    sortedKeys.toSeq.flatMap( key => unsortedParams.filter( (k,_) => k == key ) )

sealed abstract class SubscriptionType( val category : String, val subcategory : String, unsortedParams : Seq[(String,String)] ):
  final val params = SubscriptionType.semisort( unsortedParams )
  def sampleWithinTypeId : String
  def sampleDestination  : Destination
  def paramsFirstValue( key : String ) : Option[String] = wwwFormFindFirstValue( key, params )
  def paramsAllValues( key : String ) : Seq[String] = wwwFormFindAllValues( key, params )
  def withinTypeId( conn : Connection, feedId : FeedId, guid : Guid, content : ItemContent, status : ItemStatus, lastCompleted : Option[AssignableWithinTypeStatus], mostRecentOpen : Option[AssignableWithinTypeStatus] ) : Option[String]
  def isComplete( conn : Connection, withinTypeId : String, currentCount : Int, lastAssigned : Instant ) : Boolean
  def validateDestination( conn : Connection, destination : Destination, subscribableName : SubscribableName ) : Boolean
  def route( conn : Connection, assignableKey : AssignableKey, contents : Set[ItemContent], destinations : Set[Destination] ) : Unit = ??? // XXX: temporary, make abstract when we stabilize
  override def toString() : String = s"${category}.${subcategory}:${wwwFormEncodeUTF8( params.toSeq* )}"
  override def equals( other : Any ) : Boolean =
    other match
      case otherStype : SubscriptionType =>
        this.category == otherStype.category && this.subcategory == otherStype.subcategory && this.params == otherStype.params
      case _ =>
        false
  override def hashCode(): Int =
    category.## ^ subcategory.## ^ params.##





