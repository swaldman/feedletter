package com.mchange.feedletter

import java.time.Instant
import scala.xml.{Elem,Node,NodeSeq,XML}
import scala.util.{Success,Failure}
import audiofluidity.rss.{Element,Namespace}
import audiofluidity.rss.util.*
import com.mchange.conveniences.collection.*
import com.mchange.conveniences.string.*
import com.mchange.mailutil.Smtp
import scala.util.control.NonFatal

import LoggingApi.*

given itemContentLogAdapter : LogAdapter = logAdapterFor( "com.mchange.feedletter.ItemContent")


object ItemContent:

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



import ItemContent.Media

case class ItemContent private (
  val guid                     : String,
  val rssElemBeforeOverrides   : Elem,
  overrideTitle                : Option[String]                         = None,
  overrideAuthor               : Option[String]                         = None,
  overrideArticle              : Option[String]                         = None,
  overridePubDate              : Option[Instant]                        = None,
  overrideLink                 : Option[String]                         = None,
  overrideMedia                : Option[Seq[Media]]                     = None,
  overrideHintAnnounceParsings : Option[Seq[Element.Iffy.HintAnnounce]] = None
):

  def withTitle( title : String )      : ItemContent = this.copy( overrideTitle = Some( title ) )
  def withAuthor( author : String )    : ItemContent = this.copy( overrideAuthor = Some( author ) )
  def withArticle( article : String )  : ItemContent = this.copy( overrideArticle = Some( article ) )
  def withPubDate( pubDate : Instant ) : ItemContent = this.copy( overridePubDate = Some( pubDate ) )
  def withLink( link : String )        : ItemContent = this.copy( overrideLink = Some( link ) )
  def withMedia( media : Seq[Media] )  : ItemContent = this.copy( overrideMedia = Some( media ) )

  def withHintAnnounceParsings( parsings : Seq[Element.Iffy.HintAnnounce] ) : ItemContent = this.copy( overrideHintAnnounceParsings = Some(parsings) )

  def withHintAnnouncePolicy( policy : Element.Iffy.HintAnnounce.Policy ) : ItemContent = withHintAnnounceParsings(Seq(Element.Iffy.HintAnnounce(policy, None)))

  lazy val itemElem : Elem = (rssElemBeforeOverrides \ "channel" \ "item").uniqueOr { (ns : NodeSeq, nu : NotUnique) =>
    throw new AssertionError( s"ItemContent should only be initialized with single-item RSS, we found ${nu}." )
  }.asInstanceOf[Elem]

  lazy val title   : Option[String]  = overrideTitle   orElse extractTitle
  lazy val author  : Option[String]  = overrideAuthor  orElse extractCreatorAuthor
  lazy val article : Option[String]  = overrideArticle orElse extractContent
  lazy val pubDate : Option[Instant] = overridePubDate orElse extractPubDate
  lazy val link    : Option[String]  = overrideLink    orElse extractLink
  lazy val media   : Seq[Media]      = overrideMedia.getOrElse( extractMedia )

  lazy val iffyHintAnnounceParsings : Seq[Element.Iffy.HintAnnounce] =
    overrideHintAnnounceParsings.getOrElse:
      val (warnings, parsings) = Element.Iffy.HintAnnounce.extractFromChildren( itemElem )
      warnings.foreach( w => WARNING.log(w) )
      parsings.map( _(1) )

  lazy val iffyHintAnnounceUnrestrictedPolicy : Element.Iffy.HintAnnounce.Policy =
    iffyHintAnnounceParsings
      .collect {
        case iha if iha.restriction.isEmpty =>
          val rawPolicy = iha.policy.value
          Element.Iffy.HintAnnounce.Policy.lenientParse(rawPolicy) match
            case Some( policy ) => policy
            case None =>
              val out = Element.Iffy.HintAnnounce.Policy.Always
              WARNING.log("Found bad iffy:hint-announce policy '${rawPolicy}'. Defaulting to '${out}'.")
              DEBUG.log(s"item with bad iffy:hint-announce policy '${rawPolicy}':\n${itemElem}")
              out
      }
      .headOption
      .getOrElse( Element.Iffy.HintAnnounce.Policy.Always )

  def contentHash : Int = this.## // XXX: should I use a better, more guaranteed-stable hash?

  private def extractCreatorAuthor : Option[String] =
    def mbCreator = (itemElem \ "creator").filter(lenientDublinCoreNamespace).headOption.map( _.text.trim )
    def mbAuthor  = (itemElem \ "author").headOption.map( parseAuthorFromAuthorElem )
    mbCreator orElse mbAuthor

  private def extractContent =
    val contentElem =
      (itemElem \ "encoded").filter(lenientRdfContentNamespace).headOption orElse
      (itemElem \ "content").filter(lenientAtomNamespace).headOption orElse
      (itemElem \ "description").headOption
    contentElem.map( _.text.trim )

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
    val mediaContentElems = (itemElem \ "content").filter( lenientMediaNamespace )
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

  private def lenientAtomNamespace(node : Node ) : Boolean =
    node.namespace.contains("://www.w3.org/2005/Atom")

  private def lenientMediaNamespace(node : Node ) : Boolean =
    node.namespace.contains("://search.yahoo.com/mrss")

