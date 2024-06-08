package com.mchange.feedletter

import java.io.InputStream
import java.time.Instant

import scala.collection.immutable
import scala.xml.{Elem,Node,NodeSeq,XML}

import audiofluidity.rss.atom.rssElemFromAtomFeedElem
import audiofluidity.rss.{Element,Namespace}

import com.mchange.conveniences.string.*

import MLevel.*

object FeedDigest extends SelfLogging:
  // we should err on the side of the timestamp being slightly early, so there's no risk
  // interval-dependent subscriptions see full assignment through a timestamp
  // then an item appearing at the very end of the interval, unassigned.
  def apply( is : InputStream, asOf : Instant ) : FeedDigest =
    val rootElem = XML.load( is )
    val rssElem =
      rootElem.label match
        case "rss" => rootElem
        case "feed" => rssElemFromAtomFeedElem( rootElem )
        case other => throw new UnsupportedFeedType(s"'${other}' cannot be the root element of a supported feed type.")
    val items : Seq[Elem] =
      val raw : Seq[Elem] = (rssElem \\ "item").map( _.asInstanceOf[Elem] )
      def ensureGuid( elem : Elem ) : Seq[Elem] =
        if (elem \ "guid").isEmpty then
          val link = (elem \ "link").filter(_.prefix == null).text.trim // avoid any atom:link elements
          if link.isEmpty then
            val title = (elem \ "title").text
            val post = if title.isEmpty then "An untitled post" else s"Post '${title}'"
            WARNING.log(s"${post} contains neither link nor guid element. It will be skipped and never notified.")
            Seq.empty
          else
            Seq( elem.copy( child = elem.child :+ Element.Guid(false, "feedletter-synthetic:" + link).toElem ) )
        else
          Seq(elem)
      raw.flatMap(ensureGuid)
    val fileOrderedGuids = items.map( _ \ "guid" ).map( _.text.trim ).map( Guid.apply )
    val updatedRssElem = replaceItemsUnderRssElem( rssElem, items )
    val itemContents = fileOrderedGuids.map( g => ItemContent.fromRssGuid(updatedRssElem,g.str) )
    val guidToItemContent = fileOrderedGuids.zip( itemContents ).toMap
    if fileOrderedGuids.length != guidToItemContent.size then
      WARNING.log(s"While parsing a feed, found ${fileOrderedGuids.length-guidToItemContent.size} duplicated guids. Only one item will be notified per GUID!")
    FeedDigest( fileOrderedGuids, guidToItemContent, asOf )

  def apply( is : InputStream ) : FeedDigest = apply( is, Instant.now() )

  def apply( feedUrl : FeedUrl, asOf : Instant = Instant.now() ) : FeedDigest =
    requests.get.stream( feedUrl.str, keepAlive = false ).readBytesThrough( is => this.apply(is, asOf) )

  private def replaceItemsUnderRssElem( rssElem : Elem, items : Seq[Elem] ) : Elem =
    val channelElems = rssElem \ "channel"
    if channelElems.size != 1 then
      throw new FeedletterException(s"An rss element should have precisely one channel, found ${channelElems.size}")
    else
      def noItemElems( n : Node ) = n match
        case elem if elem.label == "item" && elem.prefix.nullOrBlank => false
        case _                                                       => true
      def noChannelElems( n : Node ) = n match
        case elem if elem.label == "channel" && elem.prefix.nullOrBlank => false
        case _                                                          => true

      val ce = channelElems.head.asInstanceOf[Elem]
      val newChannelElem = ce.copy( child=(ce.child.filter( noItemElems ) ++ items ) )
      rssElem.copy( child=(rssElem.child.filter( noChannelElems ) :+ newChannelElem) )

final case class FeedDigest( fileOrderedGuids : Seq[Guid], guidToItemContent : immutable.Map[Guid,ItemContent], timestamp : Instant ):
  def isEmpty  : Boolean = fileOrderedGuids.isEmpty
  def nonEmpty : Boolean = fileOrderedGuids.nonEmpty
