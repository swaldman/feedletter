package com.mchange.feedletter

import com.mchange.conveniences.string.*
import com.mchange.feedletter.Destination.Key
import scala.util.control.NonFatal

import LoggingApi.*

object Masto extends SelfLogging:

  private val CrlfsRegex = """[\r\n]+""".r

  private def oneLineForLog(s : String) = CrlfsRegex.replaceAllIn(s," / ");

  private def postMedia( accessToken : String, instanceUrl : String, media : ItemContent.Media, skipFailures : Boolean ) : Option[String] =
    try
      val mediaEndpoint = pathJoin( instanceUrl, "api/v2/media" )
      val mbFilename =
        val lastSlash = media.url.lastIndexOf("/")
        if lastSlash >= 0 then
          media.url.substring(lastSlash+1).toOptionNotBlank
        else
          None
      val data = requests.get(media.url).data.array
      val fileMultiItem =
        mbFilename match
          case Some(fn) => requests.MultiItem("file", data=data, filename=fn)
          case None     => requests.MultiItem("file", data=data)
      val multipart =
        media.alt match
          case Some( text ) => requests.MultiPart( fileMultiItem, requests.MultiItem("description", data=text) )
          case None         => requests.MultiPart( fileMultiItem )
      val headers = Map (
        "Authorization" -> s"Bearer ${accessToken}",
      )
      val response = requests.post( mediaEndpoint, data=multipart, headers=headers )
      val jsonOut = ujson.read(response.text())
      Some( jsonOut.obj("id").str ) // the id comes back as a JSON *String*, not a number
    catch
      case NonFatal(t) =>
        if skipFailures then
          WARNING.log(s"Failed to post media item, skipping: ${media}", t)
          None
        else
          throw t

  private val MaxImages = 4 // Mastodon's per-post limit

  // Mastodon allows either up to 4 images, or exactly one video or audio item, but never a mix.
  // If we have both images and video/audio, we strip the video/audio.
  // Otherwise we cap images at MaxImages, and keep only the first video/audio item.
  private def postableMedia(mpostable : MastoPostable) : Seq[ItemContent.Media] =
    val media = mpostable.media
    if media.size <= 1 then
      media
    else
      def mimeStarts( m : ItemContent.Media, prefix : String ) : Boolean = m.mimeType.fold(false)( _.trim.toLowerCase.startsWith(prefix) )
      // animated-image-as-video ("gifv") items are treated as images, not video
      def isImageLike( m : ItemContent.Media ) : Boolean = m.isImage || m.isAnimatedImageAsVideo
      def isVideoOrAudio( m : ItemContent.Media ) : Boolean = !isImageLike(m) && (mimeStarts(m, "video/") || mimeStarts(m, "audio/"))
      val images = media.filter( isImageLike )
      val avs    = media.filter( isVideoOrAudio )
      val out =
        if images.nonEmpty then
          if avs.nonEmpty then
            WARNING.log(s"""Stripping video/audio media items from MastoPostable with content "${oneLineForLog(mpostable.finalContent)}", whose media ${oneLineForLog(media.mkString("[",",","]"))} mixes images and video/audio, which Mastodon does not support.""")
          images.take( MaxImages )
        else if avs.nonEmpty then
          avs.take(1)
        else
          media // unrecognized types; leave alone
      if out.size < media.size && images.isEmpty then
        WARNING.log(s"""Mastodon allows only one video/audio item per post; trimming media for MastoPostable with content "${oneLineForLog(mpostable.finalContent)}" to the first.""")
      else if images.size > MaxImages then
        WARNING.log(s"""Mastodon allows at most ${MaxImages} images per post; trimming ${images.size} images for MastoPostable with content "${oneLineForLog(mpostable.finalContent)}".""")
      out

  def post( as : AppSetup, mastoPostable : MastoPostable, skipMediaOnMediaFail : Boolean = false ) : Unit =
    val accessTokenKey = s"feedletter.masto.access.token.${mastoPostable.name}"
    val accessToken = as.secrets.get( accessTokenKey ).getOrElse:
      throw new NoAccessToken( s"No access token found in application secrets under key '${accessTokenKey}'." )
    val mediaIds =
      val filteredMedia = postableMedia( mastoPostable )
      filteredMedia.map( media => postMedia(accessToken, mastoPostable.instanceUrl.str, media, skipMediaOnMediaFail) ).flatten
    val statusEndpoint = pathJoin( mastoPostable.instanceUrl.str, "api/v1/statuses/" )
    val headers = Map (
      "Authorization" -> s"Bearer ${accessToken}",
      "Content-Type"  ->  "application/json",
    )
    val jsonData =
      val obj = ujson.Obj(
        "status" -> ujson.Str(mastoPostable.finalContent),
        "media_ids" -> ujson.Arr( mediaIds.map( ujson.Str.apply )* ),
      )
      ujson.write(obj)
    try
      requests.post( statusEndpoint, data=jsonData, headers=headers )
    catch
      case rfe : requests.RequestFailedException =>
        FINE.log( s"Initial attempt to post MastoPostable failed! MastoPostable: ${oneLineForLog(mastoPostable.finalContent)}", rfe )
        val statusCode = rfe.response.statusCode
        if statusCode == 422 then // often this means the server requires a delay to finish processing the media
          FINE.log( "Pausing then retrying masto-post in case medie needs to be processed." )
          Thread.sleep(1000) // ick -- rework all this as ZIO at some point
          try
            requests.post( statusEndpoint, data=jsonData, headers=headers )
          catch
            case rfe2 : requests.RequestFailedException =>
              FINE.log( "Delayed repost attempt failed.", rfe2 )
              if skipMediaOnMediaFail then
                FINE.log( "Retrying masto-post without media." )
                requests.post( statusEndpoint, data=ujson.Obj("status" -> ujson.Str(mastoPostable.finalContent)), headers=headers )
              else
                throw rfe2
        else
          throw rfe


