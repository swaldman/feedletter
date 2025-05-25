package com.mchange.feedletter

import java.io.InputStream
import java.time.Instant

import scala.collection.immutable
import scala.xml.{Elem,Node,NodeSeq,Null,XML}

import audiofluidity.rss.atom.rssElemFromAtomFeedElem
import audiofluidity.rss.{Element,Namespace}

import com.mchange.conveniences.string.*

import scala.xml.UnprefixedAttribute
import scala.xml.TopScope

import LoggingApi.*

object FeedDigest extends SelfLogging:
  // we should err on the side of the timestamp being slightly early, so there's no risk
  // interval-dependent subscriptions see full assignment through a timestamp
  // then an item appearing at the very end of the interval, unassigned.
  def apply( is : InputStream, asOf : Instant, selfUrl : Option[String] ) : FeedDigest =
    val rootElem = XML.load( is )
    val rssElem =
      val converted =
        rootElem.label match
          case "rss" => rootElem
          case "feed" => rssElemFromAtomFeedElem( rootElem )
          case other => throw new UnsupportedFeedType(s"'${other}' cannot be the root element of a supported feed type.")
      maybeAugmentSelfLink(converted,selfUrl)    

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

  def apply( is : InputStream ) : FeedDigest = apply( is, Instant.now(), None )

  def apply( feedUrl : FeedUrl, asOf : Instant = Instant.now() ) : FeedDigest =
    requests.get.stream( feedUrl.str, keepAlive = false ).readBytesThrough( is => this.apply(is, asOf, Some(feedUrl.str)) )

  private def noItemElems( n : Node ) = n match
    case elem : Elem if elem.label == "item" && elem.prefix.nullOrBlank => false
    case _                                                       => true

  private def noChannelElems( n : Node ) = n match
    case elem : Elem if elem.label == "channel" && elem.prefix.nullOrBlank => false
    case _                                                          => true

  private def replaceItemsUnderRssElem( rssElem : Elem, items : Seq[Elem] ) : Elem =
    val channelElems = rssElem \ "channel"
    if channelElems.size != 1 then
      throw new FeedletterException(s"An rss element should have precisely one channel, found ${channelElems.size}")
    else
      val ce = channelElems.head.asInstanceOf[Elem]
      val newChannelElem = ce.copy( child=(ce.child.filter( noItemElems ) ++ items ) )
      rssElem.copy( child=(rssElem.child.filter( noChannelElems ) :+ newChannelElem) )

  private def maybeAugmentSelfLink( maybeNoSelfRss : Elem, selfUrl : Option[String] ) =
    val origChannelElemNodeSeq = maybeNoSelfRss \ "channel"
    if origChannelElemNodeSeq.isEmpty then
      maybeNoSelfRss
    else
      val origChannelElem = origChannelElemNodeSeq.collect { case e : Elem => e }.head // we know it's nonempty and contains an Elem, should be fine
      def hasSelfLinkAlready =
        (origChannelElemNodeSeq \ "link").collect { case e : scala.xml.Elem if e.prefix == "atom" && e \@ "type" == "application/rss+xml" && e \@ "rel" == "self" => e }.nonEmpty
      if hasSelfLinkAlready || selfUrl.isEmpty then
        maybeNoSelfRss
      else
        val newSelfLink =
          val attributes =
            new UnprefixedAttribute("rel","self",
              new UnprefixedAttribute("type","application/rss+xml",
                new UnprefixedAttribute("href",selfUrl.get,
                  Null
                )
              )
            )
          Elem(prefix="atom",label="link",attributes=attributes,scope=TopScope,minimizeEmpty=true)
        val newChannelElem = origChannelElem.copy(child = newSelfLink +: origChannelElem.child)
        maybeNoSelfRss.copy( child=(maybeNoSelfRss.child.filter( noChannelElems ) :+ newChannelElem) )

final case class FeedDigest( fileOrderedGuids : Seq[Guid], guidToItemContent : immutable.Map[Guid,ItemContent], timestamp : Instant ):
  def isEmpty  : Boolean = fileOrderedGuids.isEmpty
  def nonEmpty : Boolean = fileOrderedGuids.nonEmpty
