package com.mchange.feedletter.api

import com.mchange.feedletter.{Destination,SubscribableName,SubscriptionId,toLong}
import com.mchange.feedletter.db.PgDatabase

import com.mchange.cryptoutil.{*,given}

import com.mchange.conveniences.throwable.*

import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import sttp.tapir.Schema

import zio.*

import java.time.Instant
import javax.sql.DataSource

import scala.collection.immutable

import com.mchange.feedletter.db.withConnectionTransactional
import com.mchange.feedletter.FeedInfo.forNewFeed
import com.mchange.feedletter.AppSetup
import com.mchange.feedletter.api.V0.RequestPayload.Subscription


object V0:
  given JsonValueCodec[RequestPayload]                      = JsonCodecMaker.make
  given JsonValueCodec[RequestPayload.Subscription.Create]  = JsonCodecMaker.make
  given JsonValueCodec[RequestPayload.Subscription.Confirm] = JsonCodecMaker.make
  given JsonValueCodec[RequestPayload.Subscription.Remove]  = JsonCodecMaker.make

  given JsonValueCodec[ResponsePayload.Subscription.Created]   = JsonCodecMaker.make
  given JsonValueCodec[ResponsePayload.Subscription.Confirmed] = JsonCodecMaker.make
  given JsonValueCodec[ResponsePayload.Subscription.Removed]   = JsonCodecMaker.make
  given JsonValueCodec[ResponsePayload.Failure]                = JsonCodecMaker.make

  given Schema[Destination]                         = Schema.derived[Destination]
  given Schema[RequestPayload.Subscription.Create]  = Schema.derived[RequestPayload.Subscription.Create]
  given Schema[RequestPayload.Subscription.Confirm] = Schema.derived[RequestPayload.Subscription.Confirm]
  given Schema[RequestPayload.Subscription.Remove]  = Schema.derived[RequestPayload.Subscription.Remove]

  given Schema[ResponsePayload.Subscription.Created]   = Schema.derived[ResponsePayload.Subscription.Created]
  given Schema[ResponsePayload.Subscription.Confirmed] = Schema.derived[ResponsePayload.Subscription.Confirmed]
  given Schema[ResponsePayload.Subscription.Removed]   = Schema.derived[ResponsePayload.Subscription.Removed]
  given Schema[ResponsePayload.Failure]                = Schema.derived[ResponsePayload.Failure]

  private def bytesUtf8( s : String ) : Array[Byte] = s.getBytes(scala.io.Codec.UTF8.charSet)

  object RequestPayload:
    object Subscription:
      case class Create( subscribableName : String, destination : Destination ) extends RequestPayload
      object Confirm extends Bouncer[Confirm]("ConfirmSuffix"):
        def noninvitationContentsBytes( req : Confirm ) = req.subscriptionId.toByteSeqBigEndian
        def invite( subscriptionId : Long, secretSalt : String ) : Confirm =
          val nascent = Confirm( subscriptionId, "" )
          nascent.copy( invitation = regenInvitation( nascent, secretSalt ) )
      case class Confirm( subscriptionId : Long, invitation : String ) extends RequestPayload.Invited
      object Remove extends Bouncer[Remove]("RemoveSuffix"):
        def noninvitationContentsBytes( req : Remove ) = req.subscriptionId.toByteSeqBigEndian
        def invite( subscriptionId : Long, secretSalt : String ) : Remove =
          val nascent = Remove( subscriptionId, "" )
          nascent.copy( invitation = regenInvitation( nascent, secretSalt ) )
      case class Remove( subscriptionId : Long, invitation : String ) extends RequestPayload.Invited
    sealed trait Invited extends RequestPayload:
      def invitation    : String
    sealed trait Bouncer[T <: Invited]( suffix : String ):
      def noninvitationContentsBytes( req : T ) : immutable.Seq[Byte]
      def genInvitationBytes( req : T, secretSalt : String ) : immutable.Seq[Byte] =
        Hash.SHA3_256.hash(noninvitationContentsBytes(req) ++ bytesUtf8(secretSalt) ++ bytesUtf8(suffix)).toSeq
      def regenInvitation( req : T, secretSalt : String ) : String = genInvitationBytes(req,secretSalt).hex
      def checkInvitation( req : T, secretSalt : String ) : Boolean =
        val received = req.invitation.decodeHexToSeq
        val expected = genInvitationBytes(req, secretSalt)
        received == expected
      def assertInvitation( req : T, secretSalt : String ) : Unit =
        if !checkInvitation(req, secretSalt) then
          throw new InvalidInvitation( s"'${req.invitation}' is not a valid invitation for request ${req}. Cannot process." )
    end Bouncer
  sealed trait RequestPayload

  object ResponsePayload:
    object Subscription:
      case class Created( message : String, id : Long , confirmationRequired : Boolean, `type` : String = "Subscription.Created", success : Boolean = true ) extends ResponsePayload
      case class Confirmed( message : String, id : Long , `type` : String = "Subscription.Confirmed", success : Boolean = true ) extends ResponsePayload
      case class Removed( message : String, id : Long , `type` : String = "Subscription.Removed", success : Boolean = true ) extends ResponsePayload
    case class Failure( message : String, throwableClassName : Option[String], fullStackTrace : Option[String], `type` : String = "Failure", success : Boolean = false ) extends ResponsePayload
  sealed trait ResponsePayload:
    def `type`  : String
    def success : Boolean
    def message : String

  object TapirEndpoint:
    import sttp.tapir.ztapir.*
    import sttp.tapir.json.jsoniter.*

    object Post:
      val Base = endpoint
                   .post
                   .in("v0")
                   .in("subscription")
                   .errorOut( jsonBody[ResponsePayload.Failure] )
      val Create  = Base.in( "create" ).in( jsonBody[RequestPayload.Subscription.Create] ).out( jsonBody[ResponsePayload.Subscription.Created] )
      val Confirm = Base.in( "confirm" ).in( jsonBody[RequestPayload.Subscription.Confirm] ).out( jsonBody[ResponsePayload.Subscription.Confirmed] )
      val Remove  = Base.in( "remove" ).in( jsonBody[RequestPayload.Subscription.Remove] ).out( jsonBody[ResponsePayload.Subscription.Removed] )
    object Get:
      val Base = endpoint
                   .get
                   .in("v0")
                   .in("subscription")
                   .errorOut( stringBody )
      val Confirm = Base.in( "confirm" ).in( queryParams ).out( htmlBodyUtf8 )
      val Remove  = Base.in( "remove" ).in( jsonBody[RequestPayload.Subscription.Remove] ).out( htmlBodyUtf8 )
  end TapirEndpoint

  object ServerEndpoint:
    type ZOut[T <: ResponsePayload] = ZIO[Any,ResponsePayload.Failure,T]

    def mapError[T <: ResponsePayload]( task : Task[T] ) : ZOut[T] =
      task.mapError: t =>
        ResponsePayload.Failure(
          message = t.getMessage(),
          throwableClassName = Some( t.getClass.getName ),
          fullStackTrace = Some( t.fullStackTrace )
        )

    def subscriptionCreateLogic( ds : DataSource, as : AppSetup, now : Instant )( screate : RequestPayload.Subscription.Create ) : ZOut[ResponsePayload.Subscription.Created] =
      val mainTask =
        withConnectionTransactional( ds ): conn =>
          val sname = SubscribableName(screate.subscribableName)
          val (sman, sid) = PgDatabase.addSubscription( conn, sname, screate.destination, false, now ) // validates the destination!
          val confirming = sman.maybeConfirmSubscription( conn, sman.narrowDestinationOrThrow(screate.destination), sname, sid, as.secretSalt )
          val confirmedMessage =
            if confirming then ", but unconfirmed. Please respond to the confirmation request, coming soon." else ". No confirmation necessary."
          ResponsePayload.Subscription.Created(s"Subscription ${sid} successfully created${confirmedMessage}", sid.toLong, confirming)
      mapError( mainTask )

    def subscriptionConfirmLogic( ds : DataSource, as : AppSetup )( sconfirm : RequestPayload.Subscription.Confirm ) : ZOut[ResponsePayload.Subscription.Confirmed] =
      val mainTask =
        withConnectionTransactional( ds ): conn =>
          val sid = SubscriptionId(sconfirm.subscriptionId)
          RequestPayload.Subscription.Confirm.assertInvitation( sconfirm, as.secretSalt )
          PgDatabase.updateConfirmed( conn, sid, true )
          ResponsePayload.Subscription.Confirmed(s"Subscription ${sid} successfully confirmed.", sid.toLong)
      mapError( mainTask )

