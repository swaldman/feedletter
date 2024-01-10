package com.mchange.feedletter

import java.time.Instant
import scala.xml.{Elem,Node,NodeSeq,XML}
import scala.util.{Success,Failure}
import audiofluidity.rss.util.*
import com.mchange.conveniences.collection.*
import com.mchange.conveniences.string.*
import com.mchange.sc.v1.log.*
import MLevel.*
import com.mchange.mailutil.Smtp
import cats.instances.try_
import scala.util.control.NonFatal

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
    new ItemContent( guid, normalizedSingleItemRss )

  def fromPrenormalizedSingleItemRss( guid : String, prenormalizedSingleItemRssStr : String ) : ItemContent =
    new ItemContent( guid, XML.loadString(prenormalizedSingleItemRssStr) )

  final case class Media( url : String, mimeType : Option[String], length : Option[Long], alt : Option[String] )

case class ItemContent private ( val guid : String, val rssElem : Elem ):
  import ItemContent.{Media,logger}

  val itemElem : Elem = (rssElem \ "channel" \ "item").uniqueOr { (ns : NodeSeq, nu : NotUnique) =>
    throw new AssertionError( s"ItemContent should only be initialized with single-item RSS, we found ${nu}." )
  }.asInstanceOf[Elem]

  lazy val title   : Option[String]  = extractTitle
  lazy val author  : Option[String]  = extractCreatorAuthor
  lazy val article : Option[String]  = extractContent
  lazy val pubDate : Option[Instant] = extractPubDate
  lazy val link    : Option[String]  = extractLink
  lazy val media   : Seq[Media]      = extractMedia

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

  private def extractMedia = mediaFromMrssContent ++ mediaFromEnclosure

  private def mediaFromMrssContent =
    val mediaContentElems = (itemElem \ "content").filter( _.namespace.contains("://search.yahoo.com/mrss") )
    val maybes =
      mediaContentElems.map: elem =>
        val url = (elem \@ "url").toOptionNotBlank
        val mimeType = (elem \@ "type").toOptionNotBlank
        val length = (elem \@ "fileSize").toOptionNotBlank.map( _.toLong )
        val alt = (elem \ "description").headOption.map( _.text.trim )
        url.map( u => Media( u, mimeType, length, alt ) )
    maybes.actuals

  private def mediaFromEnclosure =
    val enclosureElems = (itemElem \ "enclosure")
    val maybes =
      enclosureElems.map: elem =>
        val url = (elem \@ "url").toOptionNotBlank
        val mimeType = (elem \@ "type").toOptionNotBlank
        val length = (elem \@ "length").toOptionNotBlank.map( _.toLong )
        url.map( u => Media( u, mimeType, length, None ) )
    maybes.actuals

  private def parseAuthorFromAuthorElem( authorElem : Node ) : String =
    def fromAddress( a : Smtp.Address ) : String = a.displayName.getOrElse( a.email )
    val text = authorElem.text.trim
    try
      val addresses = Smtp.Address.parseCommaSeparated(text)
      val names = addresses.map( fromAddress )
      names.size match
        case 1 => names.head
        case n => (names.init :+ "and ${names.last}").mkString(", ")
    catch
      case NonFatal( t ) => text

  private def lenientRdfContentNamespace(node : Node ) : Boolean =
    node.namespace.contains("://purl.org/rss/1.0/modules/content")

  private def lenientDublinCoreNamespace(node : Node ) : Boolean =
    node.namespace.contains("://purl.org/dc/elements/1.1")
