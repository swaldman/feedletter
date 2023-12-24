package com.mchange.feedletter

import java.time.Instant
import scala.xml.{Elem,Node,NodeSeq,XML}
import scala.util.{Success,Failure}
import audiofluidity.rss.util.*
import com.mchange.conveniences.collection.*
import com.mchange.sc.v1.log.*
import MLevel.*

object ItemContent:
  private lazy given logger : MLogger = MLogger(this)

  private def whitespaceSignificant( elem : Elem ) : Boolean =
    elem.label match
      case "rss"|"channel"|"item"|"image" => false
      case _                              => true

  def fromRssGuid( rawRss : Elem, guid : String ) : ItemContent =
    val singleItemRss =
      audiofluidity.rss.util.singleItemRss( rawRss, guid, SkipUnstableChannelElements ) match
        case Left( s )     => throw new FeedletterException( s"Could not create single-item RSS: $s" )
        case Right( elem ) => elem
    val normalizedSingleItemRss = stripInsignificantWhitespaceRecursive( singleItemRss, whitespaceSignificant )
    new ItemContent( normalizedSingleItemRss )

  def unsafeUnpickle( s : String ) : ItemContent = new ItemContent( XML.loadString(s) )

case class ItemContent private ( val rssElem : Elem ):
  given MLogger = ItemContent.logger

  val itemElem : Elem = (rssElem \ "channel" \ "item").uniqueOr { (ns : NodeSeq, nu : NotUnique) =>
    throw new AssertionError( s"ItemContent should only be initialized with single-item RSS, we found ${nu}." )
  }.asInstanceOf[Elem]

  lazy val title   : Option[String]  = extractTitle
  lazy val author  : Option[String]  = extractCreatorAuthor
  lazy val article : Option[String]  = extractContent
  lazy val pubDate : Option[Instant] = extractPubDate
  lazy val link    : Option[String]  = extractLink

  def contentHash : Int = this.## // XXX: should I use a better, more guaranteed-stable hash?

  private def extractCreatorAuthor : Option[String] =
    def mbCreator = (itemElem \ "creator").filter(lenientDublinCoreNamespace).headOption.map( _.text.trim )
    def mbAuthor  = (itemElem \ "author").headOption.map( parseAuthorFromAuthorElem )
    mbCreator orElse mbAuthor

  private def extractContent = ((itemElem \ "encoded").filter(lenientRdfContentNamespace).headOption orElse (itemElem \ "description").headOption).map( _.text.trim )

  private def extractTitle = (itemElem \ "title").headOption.map( _.text.trim )

  private def extractPubDate =
    (itemElem \ "pubDate").headOption.map( _.text.trim ).flatMap: dateStr =>
      attemptLenientParsePubDateToInstant( dateStr ) match
        case Success( instant ) => Some( instant )
        case Failure( t ) =>
          val title = extractTitle
          WARNING.log(s"""From '${title.getOrElse("(untitled)")}, could not parse pubDate '${dateStr}'.""", t )
          None

  private def extractLink =
    val guidIsPermalink = (itemElem \ "guid" \@ "isPermalink") == "true"
    def guidLink = if guidIsPermalink then (itemElem \ "guid").headOption else None
    def origLink = (itemElem \ "origLink").headOption // feedburner BS, but better than the indirection it otherwise embeds
    def straightLink = (itemElem \ "link").headOption
    (guidLink orElse origLink orElse straightLink).map( _.text.trim )

  private def parseAuthorFromAuthorElem( authorElem : Node ) : String = authorElem.text.trim // could do better!

  private def lenientRdfContentNamespace(node : Node ) : Boolean =
    node.namespace.contains("://purl.org/rss/1.0/modules/content")

  private def lenientDublinCoreNamespace(node : Node ) : Boolean =
    node.namespace.contains("://purl.org/dc/elements/1.1")
