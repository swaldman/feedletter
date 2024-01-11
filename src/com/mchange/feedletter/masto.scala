package com.mchange.feedletter

import scala.util.Using
import com.mchange.conveniences.string.*
import com.mchange.feedletter.Destination.Key

/*
private def filenameForMimeType( mimeType : String ) : String =
  val simpleMimeType =
    val semi = mimeType.indexOf(';') 
    if semi >= 0 then mimeType.substring(0,semi).trim else mimeType.trim
  simpleMimeType match
    case ...

  val base = scala.util.Random.nextLong()
*/

def postMedia( accessToken : String, instanceUrl : String, media : ItemContent.Media ) : String =
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
  jsonOut.obj("id").str // the id comes back as a JSON *String*, not a number

def mastoPost( as : AppSetup, mastoPostable : MastoPostable ) =
  val accessTokenKey = s"feedletter.masto.access.token.${mastoPostable.name}"
  val accessToken = as.secrets.get( accessTokenKey ).getOrElse:
    throw new NoAccessToken( s"No access token found in application secrets under key '${accessTokenKey}'." )
  val mediaIds = mastoPostable.media.map( media => postMedia(accessToken, mastoPostable.instanceUrl.toString(), media) )
  val statusEndpoint = pathJoin( mastoPostable.instanceUrl.toString(), "api/v1/statuses/" )
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
  requests.post( statusEndpoint, data=jsonData, headers=headers )
  
    

