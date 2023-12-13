package com.mchange.feedletter

import zio.*

import java.nio.file.{Path as JPath}
import java.time.Instant

import javax.sql.DataSource

import scala.util.Using

import db.AssignableKey

import scala.collection.immutable

import com.mchange.conveniences.www.*
import trivialtemplate.TrivialTemplate

type ZCommand = ZIO[AppSetup & DataSource, Throwable, Any]

enum ConfigKey:
  case DumpDbDir
  case MailNextBatchTime
  case MailBatchSize
  case MailBatchDelaySecs
  case MailMaxRetries
  case TimeZone

type SubjectCustomizer = ( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Set[ItemContent] ) => String
type TemplateParamCustomizer = ( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, destination : Destination, contents : Set[ItemContent] ) => Map[String,String]

object FeedInfo:
  def forNewFeed( feedUrl : FeedUrl, minDelayMinutes : Int, awaitStabilizationMinutes : Int, maxDelayMinutes : Int ): FeedInfo =
    val startTime = Instant.now()
    FeedInfo( feedUrl, minDelayMinutes, awaitStabilizationMinutes, maxDelayMinutes, startTime, startTime )
final case class FeedInfo( feedUrl : FeedUrl, minDelayMinutes : Int, awaitStabilizationMinutes : Int, maxDelayMinutes : Int, added : Instant, lastAssigned : Instant )

final case class ExcludedItem( feedUrl : FeedUrl, guid : String, title : Option[String], author : Option[String], publicationDate : Option[Instant], link : Option[String] )

final case class AdminSubscribeOptions( feedUrl : FeedUrl, subscribableName : SubscribableName, destination : Destination )

object Destination:
  def apply( s : String ) : Destination = s
opaque type Destination = String

object FeedUrl:
  def apply( s : String ) : FeedUrl = s
opaque type FeedUrl = String

object Guid:
  def apply( s : String ) : Guid = s
opaque type Guid = String

object SubscribableName:
  def apply( s : String ) : SubscribableName = s
opaque type SubscribableName = String

object TemplateParams:
  def apply( s : String ) : TemplateParams = TemplateParams( wwwFormDecodeUTF8( s ).toMap )
  //val defaults : String => String = key => s"<i>&lt;oops! could not insert param '$key'&gt;</i>" 
case class TemplateParams( toMap : Map[String,String] ):
  override def toString(): String = wwwFormEncodeUTF8( toMap.toSeq* )
  def fill( template : String ) = TrivialTemplate( template ).resolve(this.toMap, TrivialTemplate.Defaults.AsIs)

def composeMultipleItemHtmlMailTemplate( assignableKey : AssignableKey, stype : SubscriptionType, contents : Set[ItemContent] ) : String = ???

def composeSingleItemHtmlMailTemplate( assignableKey : AssignableKey, stype : SubscriptionType, contents : ItemContent ) : String = ???

def digestFeed( feedUrl : String ) : Task[FeedDigest] =
  ZIO.attemptBlocking( FeedDigest(feedUrl) )

private def assertHomeDirStr( hdContainingPath : JPath ) : String =
  sys.props.get("user.home").getOrElse:
    throw new FeedletterException(s"Trying to resolve ~ from '${hdContainingPath}', but System property 'user.home' is not set.")

def checkExpandTildeHomeDirPath( path : JPath ) : JPath =
  val fs = path.getFileSystem()
  val sep = fs.getSeparator()
  val pathStr = path.toString.trim
  val homePrefix = "~" + sep
  if pathStr.startsWith( homePrefix ) then
    val homeDirStr = assertHomeDirStr(path)
    val homeDir = JPath.of( homeDirStr )
    homeDir.resolve( JPath.of( pathStr.substring( homePrefix.length ) ) )
  else
    val homeSegment = sep + "~" + sep
    val i = pathStr.indexOf( homeSegment )
    if i >= 0 then
      val homeDirStr = assertHomeDirStr(path)
      val homeDir = JPath.of( homeDirStr )
      homeDir.resolve( JPath.of( pathStr.substring( i + homeSegment.length ) ) )
    else
      path







