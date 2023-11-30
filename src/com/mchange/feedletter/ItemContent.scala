package com.mchange.feedletter

import java.time.Instant
import scala.xml.{Elem,Node}
import scala.util.{Success,Failure}
import audiofluidity.rss.util.attemptLenientParsePubDateToInstant

import com.mchange.sc.v1.log.*
import MLevel.*

object ItemContent:
  private lazy given logger : MLogger = mlogger( this )

  private def parseAuthorFromAuthorElem( authorElem : Node ) : String = authorElem.text.trim // could do better!

  private def lenientRdfContentNamespace(node : Node ) : Boolean =
    node.namespace.contains("://purl.org/rss/1.0/modules/content")

  private def lenientDublinCoreNamespace(node : Node ) : Boolean =
    node.namespace.contains("://purl.org/dc/elements/1.1")

  def fromItemElem( itemElem : Elem ) : ItemContent =
    def extractCreatorAuthor : Option[String] =
      def mbCreator = (itemElem \ "creator").filter(lenientDublinCoreNamespace).headOption.map( _.text.trim )
      def mbAuthor  = (itemElem \ "author").headOption.map( parseAuthorFromAuthorElem )
      mbCreator orElse mbAuthor
    def extractContent = ((itemElem \ "encoded").filter(lenientRdfContentNamespace).headOption orElse (itemElem \ "description").headOption).map( _.text.trim )
    def extractTitle = (itemElem \ "title").headOption.map( _.text.trim )
    def extractPubDate =
      (itemElem \ "pubDate").headOption.map( _.text.trim ).flatMap: dateStr =>
        attemptLenientParsePubDateToInstant( dateStr ) match
          case Success( instant ) => Some( instant )
          case Failure( t ) =>
            val title = extractTitle
            WARNING.log(s"""From '${title.getOrElse("(untitled)")}, could not parse pubDate '${dateStr}'.""", t )
            None
    def extractLink =
      val guidIsPermalink = (itemElem \ "guid" \@ "isPermalink") == "true"
      def guidLink = if guidIsPermalink then (itemElem \ "guid").headOption else None
      def origLink = (itemElem \ "origLink").headOption // feedburner BS, but better than the indirection it otherwise embeds
      def straightLink = (itemElem \ "link").headOption
      (guidLink orElse origLink orElse straightLink).map( _.text.trim )
    ItemContent( extractTitle, extractCreatorAuthor, extractContent, extractPubDate, extractLink )

  val Empty = ItemContent( None, None, None, None, None )
  val EmptyHashCode = Empty.##

case class ItemContent( title : Option[String], author : Option[String], article : Option[String], pubDate : Option[Instant], link : Option[String] )

