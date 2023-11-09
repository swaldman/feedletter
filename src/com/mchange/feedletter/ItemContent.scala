package com.mchange.feedletter

import java.time.Instant
import scala.util.hashing.MurmurHash3
import scala.xml.{Elem,Node}
import scala.util.{Success,Failure}
import audiofluidity.rss.util.attemptLenientParsePubDateToInstant

import com.mchange.sc.v1.log.*
import MLevel.*

object ItemContent:
  private lazy given logger : MLogger = mlogger( this )

  private def parseAuthorFromAuthorElem( authorElem : Node ) : String = authorElem.text // could do better!
  
  def fromItemElem( itemElem : Elem ) =
    def extractCreatorAuthor : Option[String] =
      def mbCreator = (itemElem \ "creator").headOption.map( _.text )
      def mbAuthor  = (itemElem \ "author").headOption.map( parseAuthorFromAuthorElem )
      mbCreator orElse mbAuthor
    def extractContent = ((itemElem \ "encoded").headOption orElse (itemElem \ "description").headOption).map( _.text )
    def extractTitle = (itemElem \ "title").headOption.map( _.text )
    def extractPubDate =
      (itemElem \ "pubDate").headOption.map( _.text ).flatMap: dateStr =>
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
      (guidLink orElse origLink orElse straightLink).map( _.text )
    ItemContent( extractTitle, extractCreatorAuthor, extractContent, extractPubDate, extractLink )
    
case class ItemContent( title : Option[String], author : Option[String], article : Option[String], pubDate : Option[Instant], link : Option[String] )

