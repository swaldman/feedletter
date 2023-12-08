package com.mchange.feedletter

import java.io.InputStream
import java.time.Instant

import scala.collection.immutable
import scala.xml.{Elem,XML}


object FeedDigest:
  def apply( is : InputStream ) : FeedDigest =
    // we err on the side of the timestamp being early, so there's no risk
    // interval-dependent subscriptions see full assignment through a timestamp
    // then an item appearing at the very end of the interval, unassigned.
    val asOf = Instant.now() 
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
    FeedDigest( guidToItemContent, asOf )  

  def apply( feedUrl : String ) : FeedDigest =
    requests.get.stream( feedUrl ).readBytesThrough( this.apply )

final case class FeedDigest( guidToItemContent : immutable.Map[String,ItemContent], timestamp : Instant )
