package com.mchange.feedletter

import zio.*

import java.nio.file.{Path as JPath}
import java.time.Instant

import javax.sql.DataSource

import scala.util.Using

import db.AssignableKey

import scala.collection.immutable

type ZCommand = ZIO[AppSetup & DataSource, Throwable, Any]

enum ConfigKey:
  case DumpDbDir
  case MailNextBatchTime
  case MailBatchSize
  case MailBatchDelaySecs
  case MailMaxRetries
  case TimeZone

type SubjectCustomizer = ( subscriptionTypeName : String, withinTypeId : String, feedUrl : String, contents : Set[ItemContent] ) => String

object FeedInfo:
  def forNewFeed( feedUrl : String, minDelayMinutes : Int, awaitStabilizationMinutes : Int, maxDelayMinutes : Int ): FeedInfo =
    val startTime = Instant.now()
    FeedInfo( feedUrl, minDelayMinutes, awaitStabilizationMinutes, maxDelayMinutes, startTime, startTime )
final case class FeedInfo( feedUrl : String, minDelayMinutes : Int, awaitStabilizationMinutes : Int, maxDelayMinutes : Int, subscribed : Instant, lastAssigned : Instant )

final case class ExcludedItem( feedUrl : String, guid : String, title : Option[String], author : Option[String], publicationDate : Option[Instant], link : Option[String] )

final case class AdminSubscribeOptions( stypeName : String, destination : String, feedUrl : String )

def composeMultipleItemHtmlMailContent( assignableKey : AssignableKey, stype : SubscriptionType, contents : Set[ItemContent] ) : String = ???

def composeSingleItemHtmlMailContent( assignableKey : AssignableKey, stype : SubscriptionType, contents : ItemContent ) : String = ???

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







