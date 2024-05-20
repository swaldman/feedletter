package com.mchange.feedletter

import java.io.InputStream
import java.time.Instant

import scala.collection.immutable
import scala.xml.{Elem,Node,NodeSeq,XML}

import audiofluidity.rss.atom.rssElemFromAtomFeedElem
import audiofluidity.rss.util.{colorablyInNamespace,zeroOrOneChildElem}
import audiofluidity.rss.{Element,Namespace}

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
      lazy val channelLevelWhenUpdated = extractWhenUpdated( rssElem \ "channel" )
      def handleUpdated( item : Elem ) : Seq[Elem] =
        val mbUpdated = extractUpdated(item)
        mbUpdated match
          case None              => Seq(item)
          case Some( timestamp ) =>
            val itemLevelWhenUpdated = extractWhenUpdated(item)
            val effectiveWhenUpdated = (itemLevelWhenUpdated orElse channelLevelWhenUpdated).getOrElse( Element.Iffy.WhenUpdated.Value.Ignore )
            val mbOriginalGuid = extractOriginalGuid(item)
            (effectiveWhenUpdated, mbOriginalGuid) match
              case ( Element.Iffy.WhenUpdated.Value.Ignore, None )               => Seq(item)                                                  // nothing to do...
              case ( Element.Iffy.WhenUpdated.Value.Ignore, Some(origGuid) )     => Seq(replaceGuid(item, origGuid))                           // just update cache of original item, suppress announcement
              case ( Element.Iffy.WhenUpdated.Value.Resurface, None )            => Seq(item)                                                  // no new GUID, shouldn't announce, should update cached contents
              case ( Element.Iffy.WhenUpdated.Value.Resurface, Some(origGuid) )  => Seq(replaceGuid(item, origGuid))                           // just update cache of original item, suppress announcement
              case ( Element.Iffy.WhenUpdated.Value.Reannounce, None )           => Seq(item) ++ transformTitleGuidToAnnounce(item, timestamp) // keep orig item to update cache, synthesize new one to announce
              case ( Element.Iffy.WhenUpdated.Value.Reannounce, Some(origGuid) ) => Seq(replaceGuid( item, origGuid ), item)                   // keep new item to announce, but also provide item with old guid to update cache
      raw.flatMap(ensureGuid).flatMap(handleUpdated)
    val fileOrderedGuids = items.map( _ \ "guid" ).map( _.text.trim ).map( Guid.apply )
    val updatedRssElem = replaceItems( rssElem, items )
    val itemContents = fileOrderedGuids.map( g => ItemContent.fromRssGuid(updatedRssElem,g.str) )
    val guidToItemContent = fileOrderedGuids.zip( itemContents ).toMap
    if fileOrderedGuids.length != guidToItemContent.size then
      WARNING.log(s"While parsing a feed, found ${fileOrderedGuids.length-guidToItemContent.size} duplicated guids. Only one item will be notified per GUID!")
    FeedDigest( fileOrderedGuids, guidToItemContent, asOf )

  def apply( is : InputStream ) : FeedDigest = apply( is, Instant.now() )

  def apply( feedUrl : FeedUrl, asOf : Instant = Instant.now() ) : FeedDigest =
    requests.get.stream( feedUrl.str, keepAlive = false ).readBytesThrough( is => this.apply(is, asOf) )

  private def extractWhenUpdated( parentElems : NodeSeq ) : Option[Element.Iffy.WhenUpdated.Value] =
    val whenUpdatedElems = parentElems \ "when-updated"
    val values =
      whenUpdatedElems
        .filter( _.asInstanceOf[Elem].colorablyInNamespace(Namespace.Iffy) )
        .map( _.text.trim ).map( Element.Iffy.WhenUpdated.Value.lenientParse )
        .flatten
        .toSet
    values.size match
      case 0 => None
      case 1 => Some( values.head )
      case _ =>
        WARNING.log(s"""Found multiple values for iffy:when-updated [${values.mkString(", ")}], treating as invalid and ignoring them all.""")
        None

  private def extractOriginalGuid( parentItem : Elem ) : Option[String] =
    zeroOrOneChildElem( parentItem, "original-guid" ) match
      case Right( mbElem ) if mbElem.forall( _.colorablyInNamespace(Namespace.Iffy) ) => mbElem.map ( _.text.trim ) // this case succeeds if mbElem is empty, yielding None
      case Right( mbElem) =>
        WARNING.log(s"Found 'original-guid' tag, but not in the expected 'iffy' namespace: ${mbElem}")
        None
      case Left( seq ) =>
        WARNING.log(s"""Found multiple values for iffy:original-guid [${seq.map(_.text.trim).mkString(", ")}], treating as invalid and ignoring them all.""")
        None

  private def extractUpdated( parentItem : Elem ) : Option[String] =
    zeroOrOneChildElem( parentItem, "updated" ) match
      case Right( mbElem ) if mbElem.forall( _.colorablyInNamespace(Namespace.Atom)) => mbElem.map ( _.text.trim ) // this case succeeds if mbElem is empty, yielding None
      case Right( mbElem) =>
        WARNING.log(s"Found 'updated' tag, but not in the expected 'atom' namespace: ${mbElem}")
        None
      case Left( seq ) =>
        WARNING.log(s"""Found multiple values for 'atom:updated' [${seq.map(_.text.trim).mkString(", ")}], treating as invalid and ignoring them all.""")
        None

  private def replaceGuid( item : Elem, newGuid : String ) : Elem =
    item.copy( child = item.child.filter( _.label != "guid" ) :+ Element.Guid( false, newGuid ).toElem )

  private def replaceTitle( item : Elem, newTitle : String ) : Elem =
    item.copy( child = Element.Title(newTitle).toElem +: item.child.filter( _.label != "title" ) )

  private def replaceItems( rssElem : Elem, items : Seq[Elem] ) : Elem =
    val channelElems = rssElem \ "channel"
    if channelElems.size != 1 then
      throw new FeedletterException(s"An rss element should have precisely one channel, found ${channelElems.size}")
    else
      def noItemElems( n : Node ) = n match
        case elem if elem.label == "item" => false
        case _                            => true
      def noChannelElems( n : Node ) = n match
        case elem if elem.label == "channel" => false
        case _                               => true

      val ce = channelElems.head.asInstanceOf[Elem]
      val newChannelElem = ce.copy( child=(ce.child.filter( noItemElems ) ++ items ) )
      rssElem.copy( child=(rssElem.child.filter( noChannelElems ) :+ newChannelElem) )

  private def transformTitleGuidToAnnounce( itemElem : Elem, updatedTimestamp : String ) : Option[Elem] =
    val oldGuid = (itemElem \ "guid").text.trim
    if oldGuid.isEmpty then
      WARNING.log("While trying to announce an updated item, failed to find guid. This shouldn't happen, we ought to have ensured that one exists.")
      None
    else
      val oldTitle = (itemElem \ "title").text.trim
      val newTitle = "Updated: ${oldTitle} (${updatedTimestamp})"
      val newGuid = s"${oldGuid}#updated-${updatedTimestamp}" // we won't reannounce multiple times for one updated, because as long as the timestamp doesn't change, we'll just update cache
      val withNewGuid = replaceGuid(itemElem, newGuid)
      val withNewTitle = replaceTitle( withNewGuid, newTitle )
      Some( withNewTitle )

final case class FeedDigest( fileOrderedGuids : Seq[Guid], guidToItemContent : immutable.Map[Guid,ItemContent], timestamp : Instant ):
  def isEmpty  : Boolean = fileOrderedGuids.isEmpty
  def nonEmpty : Boolean = fileOrderedGuids.nonEmpty
