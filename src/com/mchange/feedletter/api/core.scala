package com.mchange.feedletter.api

import com.mchange.feedletter.Destination

import com.mchange.cryptoutil.*

import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import sttp.tapir.Schema


object V0:
  given JsonValueCodec[RequestPayload]                      = JsonCodecMaker.make
  given JsonValueCodec[RequestPayload.Subscription.Request] = JsonCodecMaker.make
  given JsonValueCodec[RequestPayload.Subscription.Confirm] = JsonCodecMaker.make
  given JsonValueCodec[RequestPayload.Subscription.Remove]  = JsonCodecMaker.make

  given JsonValueCodec[ResponsePayload.Yes] = JsonCodecMaker.make
  given JsonValueCodec[ResponsePayload.No]  = JsonCodecMaker.make

  given Schema[Destination]         = Schema.derived[Destination]
  given Schema[RequestPayload]      = Schema.derived[RequestPayload]
  given Schema[ResponsePayload.Yes] = Schema.derived[ResponsePayload.Yes]
  given Schema[ResponsePayload.No]  = Schema.derived[ResponsePayload.No]

  object RequestPayload:
    object Subscription:
      case class Request( subscribableName : String, destination : Destination ) extends RequestPayload
      case class Confirm( subscriptionId : Long, invitation : Option[String] ) extends RequestPayload.Invited:
        def unsaltedBytes : Seq[Byte] = subscriptionId.toByteSeqBigEndian
      case class Remove( subscriptionId : Long, invitation : Option[String] ) extends RequestPayload.Invited:
        def unsaltedBytes : Seq[Byte] = subscriptionId.toByteSeqBigEndian
    sealed trait Invited extends RequestPayload:
      def invitation    : Option[String]
      def unsaltedBytes : Seq[Byte]
  sealed trait RequestPayload

  object ResponsePayload:
    case class Yes( message : String ) extends ResponsePayload
    case class No( error : String ) extends ResponsePayload
  sealed trait ResponsePayload

  object TapirEndpoint:
    import sttp.tapir.ztapir.*
    import sttp.tapir.json.jsoniter.*

    val Base = endpoint
                 .post
                 .in("v0")
                 .in("subscription")
                 .in( jsonBody[RequestPayload] )
                 .out( jsonBody[ResponsePayload.Yes] )
                 .errorOut( jsonBody[ResponsePayload.No] )
    val Request = Base.in( "request" )
    val Confirm = Base.in( "confirm" )
    val Remove  = Base.in( "remove" )
  end TapirEndpoint
