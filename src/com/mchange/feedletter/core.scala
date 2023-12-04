package com.mchange.feedletter

import zio.*

import java.io.InputStream
import java.nio.file.{Path as JPath}
import java.time.Instant
import javax.sql.DataSource

import scala.util.Using
import scala.xml.{Elem,XML}

import scala.collection.immutable

type ZCommand = ZIO[AppSetup & DataSource, Throwable, Any]

enum ConfigKey:
  case MailNextBatchTime
  case MailBatchSize
  case MailBatchDelaySecs
  case DumpDbDir

final case class FeedDigest( guidToItemContent : immutable.Map[String,ItemContent], timestamp : Instant )

final case class FeedInfo( feedUrl : String, minDelayMinutes : Int, awaitStabilizationMinutes : Int, maxDelayMinutes : Int, paused : Boolean, subscribed : Instant )

final case class ExcludedItem( feedUrl : String, guid : String, title : Option[String], author : Option[String], publicationDate : Option[Instant], link : Option[String] )

final case class AdminSubscribeOptions( stype : SubscriptionType, destination : String, feedUrl : String )

def composeMultipleItemHtmlMailContent( contents : Set[ItemContent] ) : String = ???

def composeSingleItemHtmlMailContent( content : ItemContent ) : String = ???

def doDigestFeed( is : InputStream ) : FeedDigest =
  val rootElem = XML.load( is )
  val rssElem =
    rootElem.label match
      case "rss" => rootElem
      case other => throw new UnsupportedFeedType(s"'${other}' cannot be the root element of a supported feed type.")
  val items : Seq[Elem] = (rssElem \\ "item").map( _.asInstanceOf[Elem] )
  val guidToItemContent =
    val guids = items.map( _ \ "guid" ).map( _.text.trim )
    val itemContents = items.map( ItemContent.fromItemElem )
    guids.zip( itemContents ).toMap
  FeedDigest( guidToItemContent, Instant.now() )  

def doDigestFeed( feedUrl : String ) : FeedDigest =
  requests.get.stream( feedUrl ).readBytesThrough( doDigestFeed )

def digestFeed( feedUrl : String ) : Task[FeedDigest] =
  ZIO.attemptBlocking( doDigestFeed(feedUrl) )

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







