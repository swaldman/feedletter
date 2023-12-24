package com.mchange.feedletter

import java.io.InputStream
import java.time.Instant

import scala.collection.immutable
import scala.xml.{Elem,XML}


object FeedDigest:
  // we should err on the side of the timestamp being slightly early, so there's no risk
  // interval-dependent subscriptions see full assignment through a timestamp
  // then an item appearing at the very end of the interval, unassigned.
  def apply( is : InputStream, asOf : Instant ) : FeedDigest =
    val rootElem = XML.load( is )
    val rssElem =
      rootElem.label match
        case "rss" => rootElem
        case other => throw new UnsupportedFeedType(s"'${other}' cannot be the root element of a supported feed type.")
    val items : Seq[Elem] = (rssElem \\ "item").map( _.asInstanceOf[Elem] )
    val orderedGuids = items.map( _ \ "guid" ).map( _.text.trim ).map( Guid.apply )
    val itemContents = orderedGuids.map( g => ItemContent.fromRssGuid(rssElem,g.toString()) ) 
    val guidToItemContent = orderedGuids.zip( itemContents ).toMap
    FeedDigest( orderedGuids, guidToItemContent, asOf )

  def apply( is : InputStream) : FeedDigest = apply( is, Instant.now() )

  def apply( feedUrl : FeedUrl, asOf : Instant = Instant.now() ) : FeedDigest =
    requests.get.stream( feedUrl.toString() ).readBytesThrough( is => this.apply(is, asOf) )

final case class FeedDigest( orderedGuids : Seq[Guid], guidToItemContent : immutable.Map[Guid,ItemContent], timestamp : Instant ):
  def isEmpty  : Boolean = orderedGuids.isEmpty
  def nonEmpty : Boolean = orderedGuids.nonEmpty
