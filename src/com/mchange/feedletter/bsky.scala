package com.mchange.feedletter

import java.time.Instant
import java.time.format.DateTimeFormatter.ISO_INSTANT

import scala.collection.mutable

import scala.util.control.NonFatal

import com.mchange.conveniences.string.*

import LoggingApi.*

object Bsky extends SelfLogging:
  // Bluesky permits at most 4 images per post, and rejects blob uploads larger than 1,000,000 bytes.
  private val MaxImagesPerPost = 4
  private val MaxBlobBytes     = 1000000

  private def simpleMimeType( mimeType : String ) : String =
    val semi = mimeType.indexOf(';')
    if semi >= 0 then mimeType.substring(0,semi).trim else mimeType.trim

  // Uploads an image as a blob and returns its blob ref (the JSON to embed), or None if it should be skipped.
  private def uploadImageBlob( accessJwt : String, entrywayUrl : String, media : ItemContent.Media ) : Option[ujson.Value] =
    try
      val data = requests.get( media.url ).data.array
      if data.length > MaxBlobBytes then
        WARNING.log( s"Image at '${media.url}' is ${data.length} bytes, exceeding Bluesky's ${MaxBlobBytes}-byte blob limit. Skipping." )
        None
      else
        val mimeType = media.mimeType.map( simpleMimeType ).getOrElse("application/octet-stream")
        val uploadBlobEndpoint = pathJoin( entrywayUrl, "/xrpc/com.atproto.repo.uploadBlob" )
        val headers = Map(
          "Authorization" -> s"Bearer ${accessJwt}",
          "Content-Type"  -> mimeType,
        )
        val response = requests.post( uploadBlobEndpoint, data=data, headers=headers )
        Some( ujson.read( response.text() ).obj("blob") )
    catch
      case NonFatal(t) =>
        WARNING.log( s"Failed to upload image blob to Bluesky from '${media.url}'. Skipping.", t )
        None

  object Regex:
    val Url = "(?i)\\b(?:(?:https?|ftp)://)(?:\\S+(?::\\S*)?@)?(?:(?!(?:10|127)(?:\\.\\d{1,3}){3})(?!(?:169\\.254|192\\.168)(?:\\.\\d{1,3}){2})(?!172\\.(?:1[6-9]|2\\d|3[0-1])(?:\\.\\d{1,3}){2})(?:[1-9]\\d?|1\\d\\d|2[01]\\d|22[0-3])(?:\\.(?:1?\\d{1,2}|2[0-4]\\d|25[0-5])){2}(?:\\.(?:[1-9]\\d?|1\\d\\d|2[0-4]\\d|25[0-4]))|(?:(?:[a-z\\u00a1-\\uffff0-9]-*)*[a-z\\u00a1-\\uffff0-9]+)(?:\\.(?:[a-z\\u00a1-\\uffff0-9]-*)*[a-z\\u00a1-\\uffff0-9]+)*(?:\\.(?:[a-z\\u00a1-\\uffff]{2,}))\\.?)(?::\\d{2,5})?(?:[/?#]\\S*)?\\b".r // taken from https://stackoverflow.com/questions/31440758/perfect-url-validation-regex-in-java
    val Mention = """(?i)(?:$|\W)(@([a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)\b""".r // modified from https://docs.bsky.app/docs/advanced-guides/posts

  def post( as : AppSetup, bskyPostable : BskyPostable ) : Unit =
    val appPasswordKey = s"feedletter.bsky.identifier.${bskyPostable.identifier.str}"
    val appPassword = as.secrets.get( appPasswordKey ).getOrElse:
      throw new NoAppPassword( s"No app-password found in application secrets under key '${appPasswordKey}'." )

    val session : ujson.Obj =
      val req = ujson.Obj("identifier" -> bskyPostable.identifier.str, "password" -> appPassword)
      val url = pathJoin( bskyPostable.entrywayUrl.str, "/xrpc/com.atproto.server.createSession" )
      val response = requests.post( url, data=req, headers=Map("Content-Type"->"application/json") )
      ujson.read( response.text() ).obj

    val accessJwt : String = session("accessJwt").str

    val imageEmbed : Option[ujson.Value] =
      val imageMedia = bskyPostable.media.filter( _.mimeType.map( _.startsWith("image") ).getOrElse(false) )
      val capped =
        if imageMedia.size > MaxImagesPerPost then
          WARNING.log( s"Bluesky post has ${imageMedia.size} images, exceeding the limit of ${MaxImagesPerPost}. Only the first ${MaxImagesPerPost} will be posted." )
          imageMedia.take( MaxImagesPerPost )
        else
          imageMedia
      val entries =
        capped.flatMap: media =>
          uploadImageBlob( accessJwt, bskyPostable.entrywayUrl.str, media ).map: blob =>
            ujson.Obj( "alt" -> ujson.Str( media.alt.getOrElse("") ), "image" -> blob )
      if entries.nonEmpty then
        Some( ujson.Obj( "$type" -> ujson.Str("app.bsky.embed.images"), "images" -> ujson.Arr( entries* ) ) )
      else
        None

    def facets( text : String ) : ujson.Arr =
      def utf8IndexFor( s : String, si : Int ) : Int = s.substring(0,si).getBytes(java.nio.charset.StandardCharsets.UTF_8).length
      def mentions : mutable.ArrayBuffer[ujson.Value] =
        val buff = mutable.ArrayBuffer.empty[ujson.Value]
        val matches = Bsky.Regex.Mention.findAllMatchIn( text )
        matches.foreach: m =>
          val handle = m.group(1).substring(1)
          try
            val facet =
              val index = ujson.Obj( "byteStart" -> utf8IndexFor(text, m.start(1)), "byteEnd" -> utf8IndexFor(text, m.end(1)) )
              val features =
                val resolveHandleEndpoint = pathJoin( bskyPostable.entrywayUrl.str, "/xrpc/com.atproto.identity.resolveHandle" )
                val response = requests.get( resolveHandleEndpoint, params=Map("handle" -> handle), headers=Map("Content-Type"->"application/json") )
                val responseMap = ujson.read( response.text() ).obj
                val did = responseMap("did").str
                ujson.Arr( ujson.Obj( "$type" -> "app.bsky.richtext.facet#mention", "did" -> did ) )
              ujson.Obj( "index" -> index, "features" -> features )
            buff += facet
          catch
            case rfe : requests.RequestFailedException =>
              WARNING.log( s"Failed to lookup handle '${handle}'. Skipping. Error response: ${rfe.response}" )
            case NonFatal(t) =>
              WARNING.log( s"Something went wrong while looking up handle '${handle}'.", t )
        buff
      def urls : mutable.ArrayBuffer[ujson.Value] =
        val buff = mutable.ArrayBuffer.empty[ujson.Value]
        val matches = Bsky.Regex.Url.findAllMatchIn( text )
        matches.foreach: m =>
          val facet =
            val index = ujson.Obj( "byteStart" -> utf8IndexFor(text, m.start(0)), "byteEnd" -> utf8IndexFor(text, m.end(0)) )
            val features = ujson.Arr( ujson.Obj( "$type" -> "app.bsky.richtext.facet#link", "uri" -> m.group(0) ) )
            ujson.Obj( "index" -> index, "features" -> features )
          buff += facet
        buff
      urls ++ mentions

    val createRecordEndpoint = pathJoin( bskyPostable.entrywayUrl.str, "/xrpc/com.atproto.repo.createRecord" )
    val text = bskyPostable.finalContent
    val fs   = facets( text ) // resolves handles over the network, so compute only once

    def buildRecord( embed : Option[ujson.Value] ) : ujson.Obj =
      val lhm = upickle.core.LinkedHashMap.apply[String,ujson.Value](
        "$type" -> ujson.Str("app.bsky.feed.post") :: "text" -> ujson.Str(text) :: "createdAt" -> ujson.Str(ISO_INSTANT.format(Instant.now())) :: Nil
      )
      if fs.value.nonEmpty then lhm += ( "facets" -> fs )
      embed.foreach( e => lhm += ( "embed" -> e ) )
      ujson.Obj(lhm)

    val headers = Map(
      "Authorization" -> s"""Bearer ${accessJwt}""",
      "Content-Type"  ->  "application/json",
    )

    def doPost( embed : Option[ujson.Value] ) =
      val reqData = ujson.Obj(
        "repo" -> session("did"),
        "collection" -> "app.bsky.feed.post",
        "record" -> buildRecord( embed ),
      )
      requests.post( createRecordEndpoint, data=reqData, headers=headers )

    try
      doPost( imageEmbed )
    catch
      case rfe : requests.RequestFailedException if imageEmbed.nonEmpty =>
        FINE.log( s"Initial attempt to post BskyPostable with media failed! Retrying without media. BskyPostable: ${bskyPostable}", rfe )
        doPost( None )
