package com.mchange.feedletter

import java.time.Instant
import java.time.format.DateTimeFormatter.ISO_INSTANT

import scala.collection.mutable

import scala.util.control.NonFatal

import com.mchange.conveniences.string.*

import LoggingApi.*

object Bsky extends SelfLogging:
  object Regex:
    val Url = "(?i)\\b(?:(?:https?|ftp)://)(?:\\S+(?::\\S*)?@)?(?:(?!(?:10|127)(?:\\.\\d{1,3}){3})(?!(?:169\\.254|192\\.168)(?:\\.\\d{1,3}){2})(?!172\\.(?:1[6-9]|2\\d|3[0-1])(?:\\.\\d{1,3}){2})(?:[1-9]\\d?|1\\d\\d|2[01]\\d|22[0-3])(?:\\.(?:1?\\d{1,2}|2[0-4]\\d|25[0-5])){2}(?:\\.(?:[1-9]\\d?|1\\d\\d|2[0-4]\\d|25[0-4]))|(?:(?:[a-z\\u00a1-\\uffff0-9]-*)*[a-z\\u00a1-\\uffff0-9]+)(?:\\.(?:[a-z\\u00a1-\\uffff0-9]-*)*[a-z\\u00a1-\\uffff0-9]+)*(?:\\.(?:[a-z\\u00a1-\\uffff]{2,}))\\.?)(?::\\d{2,5})?(?:[/?#]\\S*)?\\b".r // taken from https://stackoverflow.com/questions/31440758/perfect-url-validation-regex-in-java
    val Mention = """(?i)(?:$|\W)(@([a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)\b""".r // modified from https://docs.bsky.app/docs/advanced-guides/posts

  // Note: Posting of media is not currently supported
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
    val record =
      val lhm = upickle.core.LinkedHashMap.apply[String,ujson.Value](
        "$type" -> ujson.Str("app.bsky.feed.post") :: "text" -> ujson.Str(text) :: "createdAt" -> ujson.Str(ISO_INSTANT.format(Instant.now())) :: Nil
      )
      val fs  = facets( text )
      if fs.value.nonEmpty then lhm += ( "facets" -> fs )
      ujson.Obj(lhm)
    val headers = Map(
      "Authorization" -> s"""Bearer ${accessJwt}""",
      "Content-Type"  ->  "application/json",
    )
    val reqData = ujson.Obj(
      "repo" -> session("did"),
      "collection" -> "app.bsky.feed.post",
      "record" -> record,
    )
    requests.post( createRecordEndpoint, data=reqData, headers=headers )
