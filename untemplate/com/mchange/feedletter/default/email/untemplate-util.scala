def ifLink( link : Option[String], text : String ) : String =
  link.fold(text)(l => s"""<a href="$l">$text</a>""")
end ifLink


def isEmbeddableMedia( media : com.mchange.feedletter.ItemContent.Media ) : Boolean =
  media.isImage || media.isAnimatedImageAsVideo
end isEmbeddableMedia

// Animated images transcoded to video (e.g. Mastodon "gifv") play only in mail clients that support <video>
// (Apple Mail, iOS Mail); Gmail and Outlook strip it. So we always follow it with a visible caption linking
// to the post (or the media itself), which carries the alt text for everyone else.
def embedMedia( media : com.mchange.feedletter.ItemContent.Media, link : Option[String] ) : String =
  val escapedAlt = media.alt.map( scala.xml.Utility.escape )
  if media.isAnimatedImageAsVideo then
    val titleAttr = escapedAlt.fold("")(a => s""" title="$a"""")
    val caption = escapedAlt.fold("Animated image")(a => s"Animated image: $a")
    s"""<video src="${media.url}" autoplay loop muted playsinline${titleAttr}></video>
       |<div class="media-animated-caption"><a href="${link.getOrElse(media.url)}">&#9654; ${caption}</a></div>""".stripMargin
  else
    s"""<img src="${media.url}"${escapedAlt.fold("")(a => s""" alt="$a"""")}>"""
end embedMedia
