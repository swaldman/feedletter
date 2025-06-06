package com.mchange.feedletter

import com.mchange.conveniences.string.*
import com.mchange.feedletter.Destination.Key
import scala.util.control.NonFatal

import LoggingApi.*

object Masto extends SelfLogging:

  /*
  private def filenameForMimeType( mimeType : String ) : String =
    val simpleMimeType =
      val semi = mimeType.indexOf(';') 
      if semi >= 0 then mimeType.substring(0,semi).trim else mimeType.trim
    simpleMimeType match
      case ...

    val base = scala.util.Random.nextLong()
  */

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
          WARNING.log("Failed to post media item, skipping: ${media}", t)
          None
        else
          throw t

  def post( as : AppSetup, mastoPostable : MastoPostable, skipMediaOnMediaFail : Boolean = false ) : Unit =
    val accessTokenKey = s"feedletter.masto.access.token.${mastoPostable.name}"
    val accessToken = as.secrets.get( accessTokenKey ).getOrElse:
      throw new NoAccessToken( s"No access token found in application secrets under key '${accessTokenKey}'." )
    val mediaIds = mastoPostable.media.map( media => postMedia(accessToken, mastoPostable.instanceUrl.str, media, skipMediaOnMediaFail) ).flatten
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
        FINE.log( s"Initial attempt to post MastoPostable failed! MastoPostable: ${mastoPostable}", rfe )
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


