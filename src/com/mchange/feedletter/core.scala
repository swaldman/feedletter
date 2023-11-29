package com.mchange.feedletter

import zio.*

import java.io.InputStream
import java.time.Instant
import javax.sql.DataSource

import scala.util.Using
import scala.xml.{Elem,XML}

import scala.collection.immutable

type ZCommand = ZIO[AppSetup & DataSource, Throwable, Any]

case class FeedDigest( guidToItemContent : immutable.Map[String,ItemContent], timestamp : Instant )

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










