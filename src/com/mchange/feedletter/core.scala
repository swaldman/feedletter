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

final case class FeedInfo( feedUrl : String, minDelaySeconds : Int, awaitStabilizationSeconds : Int, paused : Boolean )

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


import com.mchange.sc.v1.texttable
import zio.*

val FeedInfoColumns = Seq( texttable.Column("Feed URL"), texttable.Column("Min Delay Secs"), texttable.Column("Await Stabilization Secs"), texttable.Column("Paused") )

def printFeedInfoTable( fis: Set[FeedInfo] ) : Task[Unit] =
  ZIO.attempt( texttable.printProductTable( FeedInfoColumns )( fis.toList.map( texttable.Row.apply ) ) ) // preserve the order if the set is sorted

val ConfigKeyColumns = Seq( texttable.Column("Configuration Key"), texttable.Column("Value") )

def printConfigurationTuplesTable( tups : Set[Tuple2[ConfigKey,String]] ) : Task[Unit] =
  ZIO.attempt( texttable.printProductTable( ConfigKeyColumns )( tups.toList.map( texttable.Row.apply ) ) ) // preserve the order if the set is sorted






