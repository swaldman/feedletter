package com.mchange.feedletter.api

import com.mchange.feedletter.{Destination,SubscribableName}
import com.mchange.feedletter.db.PgDatabase

import com.mchange.cryptoutil.*

import com.mchange.conveniences.throwable.*

import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import sttp.tapir.Schema

import zio.*
import java.time.Instant
import javax.sql.DataSource


object V0:
  given JsonValueCodec[RequestPayload]                      = JsonCodecMaker.make
  given JsonValueCodec[RequestPayload.Subscription.Create]  = JsonCodecMaker.make
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
      case class Create( subscribableName : String, destination : Destination ) extends RequestPayload
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

    val PostBase = endpoint
                     .post
                     .in("v0")
                     .in("subscription")
                     .in( jsonBody[RequestPayload] )
                     .out( jsonBody[ResponsePayload.Yes] )
                     .errorOut( jsonBody[ResponsePayload.No] )
    val Create = PostBase.in( "create" )
    // val Confirm = Base.in( "confirm" )
    // val Remove  = Base.in( "remove" )
  end TapirEndpoint

  object ServerEndpoint:
    type ZOut = ZIO[Any,ResponsePayload.No,ResponsePayload.Yes]

    def mapError( task : Task[ResponsePayload.Yes] ) : ZOut =
      task.mapError: t =>
        val error = s"""|The following error occurred:
                        |
                        |${t.fullStackTrace}""".stripMargin
        ResponsePayload.No( error )

    def subscriptionCreateLogic( ds : DataSource, now : Instant )( sreq : RequestPayload.Subscription.Create ) : ZOut =
      val mainTask =
        for
          _ <- PgDatabase.addSubscription( ds, SubscribableName(sreq.subscribableName), sreq.destination, false, now )
        yield
          ResponsePayload.Yes("Subscription created, but unconfirmed. Please respond to the confirmation request, coming soon.")
      mapError( mainTask )
