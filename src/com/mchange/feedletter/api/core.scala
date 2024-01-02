package com.mchange.feedletter.api

import com.mchange.feedletter.*
import com.mchange.feedletter.db.PgDatabase

import com.mchange.cryptoutil.{*,given}

import com.mchange.conveniences.string.*
import com.mchange.conveniences.throwable.*
import com.mchange.conveniences.www.*

import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*

import sttp.model.QueryParams
import sttp.tapir.Schema

import zio.*

import java.time.Instant
import javax.sql.DataSource

import scala.collection.immutable

import com.mchange.feedletter.db.withConnectionTransactional
import com.mchange.feedletter.FeedInfo.forNewFeed
import com.mchange.feedletter.AppSetup
import com.mchange.feedletter.api.V0.RequestPayload.Subscription
import com.mchange.feedletter.SubscriptionManager
import sttp.tapir.EndpointIO.annotations.endpointInput

object ApiLinkGenerator:
  val Dummy = new ApiLinkGenerator:
    def confirmGetLink( sid : SubscriptionId ) : String = "http://localhost:8024/v0/subscription/confirm?subscriptionId=0&invitation=fake"
    def removeGetLink( sid : SubscriptionId ) : String  = "http://localhost:8024/v0/subscription/remove?subscriptionId=0&invitation=fake"
trait ApiLinkGenerator:
  def confirmGetLink( sid : SubscriptionId ) : String
  def removeGetLink( sid : SubscriptionId ) : String

object V0 extends SelfLogging:
  import MLevel.*

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
    extension ( queryParams : QueryParams )
      def assertParam( key : String ) : String =
        queryParams.get(key).getOrElse:
          throw new InvalidRequest( s"Expected query param '$key' required, not found." )
    object Subscription:
      case class Create( subscribableName : String, destination : Destination ) extends RequestPayload
      object Confirm extends Bouncer[Confirm]("ConfirmSuffix"):
        def fromQueryParams( queryParams : QueryParams ) : Confirm =
          val subscriptionId : Long   = queryParams.assertParam( "subscriptionId" ).toLong
          val invitation     : String = queryParams.assertParam( "invitation" )
          Confirm( subscriptionId, invitation )
        def noninvitationContentsBytes( req : Confirm ) = req.subscriptionId.toByteSeqBigEndian
        def invite( subscriptionId : Long, secretSalt : String ) : Confirm =
          val nascent = Confirm( subscriptionId, "" )
          nascent.copy( invitation = regenInvitation( nascent, secretSalt ) )
      case class Confirm( subscriptionId : Long, invitation : String ) extends RequestPayload.Invited
      object Remove extends Bouncer[Remove]("RemoveSuffix"):
        def fromQueryParams( queryParams : QueryParams ) : Remove =
          val subscriptionId : Long   = queryParams.assertParam( "subscriptionId" ).toLong
          val invitation     : String = queryParams.assertParam( "invitation" )
          Remove( subscriptionId, invitation )
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
  sealed trait RequestPayload extends Product:
    lazy val toMap : Map[String,String] = (0 until productArity).map( i => (productElementName(i), productElement(i).toString) ).toMap
    lazy val toGetParams : String = wwwFormEncodeUTF8( toMap.toSeq* )

  object ResponsePayload:
    object Subscription:
      case class Created( message : String, id : Long , confirmationRequired : Boolean, `type` : String = "Subscription.Created", success : Boolean = true ) extends ResponsePayload.Success( SubscriptionStatusChanged.Created )
      case class Confirmed( message : String, id : Long , `type` : String = "Subscription.Confirmed", success : Boolean = true ) extends ResponsePayload.Success( SubscriptionStatusChanged.Confirmed )
      case class Removed( message : String, id : Long , `type` : String = "Subscription.Removed", success : Boolean = true ) extends ResponsePayload.Success( SubscriptionStatusChanged.Removed )
    sealed trait Success( val statusChanged : SubscriptionStatusChanged ) extends ResponsePayload 
    case class Failure( message : String, throwableClassName : Option[String], fullStackTrace : Option[String], `type` : String = "Failure", success : Boolean = false ) extends ResponsePayload
  sealed trait ResponsePayload:
    def `type`  : String
    def success : Boolean
    def message : String

  class TapirApi(val serverUrl : String, val locationPathElements : List[String], val secretSalt : String) extends ApiLinkGenerator:

    val basePathElements = locationPathElements ::: "v0" :: "subscription" :: Nil

    val createPathElements  = basePathElements ::: "create"  :: Nil
    val confirmPathElements = basePathElements ::: "confirm" :: Nil
    val removePathElements  = basePathElements ::: "remove"  :: Nil

    val createFullPath  = createPathElements.mkString("/","/","")
    val confirmFullPath = createPathElements.mkString("/","/","")
    val removeFullPath  = createPathElements.mkString("/","/","")

    val confirmEndpointUrl = pathJoin( serverUrl, confirmFullPath )
    val removeEndpointUrl = pathJoin( serverUrl, removeFullPath )

    def confirmGetLink( sid : SubscriptionId ) : String =
      val confirmRequest = RequestPayload.Subscription.Confirm.invite(sid.toLong, secretSalt)
      confirmEndpointUrl + "?" + confirmRequest.toGetParams

    def removeGetLink( sid : SubscriptionId ) : String =
      val removeRequest = RequestPayload.Subscription.Remove.invite(sid.toLong, secretSalt)
      removeEndpointUrl + "?" + removeRequest.toGetParams

    object BasicEndpoint:
      import sttp.tapir.ztapir.*
      import sttp.tapir.json.jsoniter.*
      import sttp.tapir.PublicEndpoint

      def addElements[IN,ERROUT,OUT,R]( baseEndpoint : PublicEndpoint[IN,ERROUT,OUT,R], elems : List[String] ) =
        elems.foldLeft( baseEndpoint )( (accum,next) => accum.in(next) )

      object Post:
        val Base = endpoint
                     .post
                     .errorOut( jsonBody[ResponsePayload.Failure] )
        val Create  = addElements(Base,createPathElements).in( jsonBody[RequestPayload.Subscription.Create] ).out( jsonBody[ResponsePayload.Subscription.Created] )
        val Confirm = addElements(Base,confirmPathElements).in( jsonBody[RequestPayload.Subscription.Confirm] ).out( jsonBody[ResponsePayload.Subscription.Confirmed] )
        val Remove  = addElements(Base,removePathElements).in( jsonBody[RequestPayload.Subscription.Remove] ).out( jsonBody[ResponsePayload.Subscription.Removed] )
      object Get:
        val Base = endpoint
                     .get
                     .errorOut( stringBody )
                     .in( queryParams )
                     .out( htmlBodyUtf8 )
        val Confirm = addElements(Base,confirmPathElements)
        val Remove  = addElements(Base,removePathElements)
    end BasicEndpoint

    object ServerEndpoint:
      import BasicEndpoint.*
      import sttp.tapir.ztapir.*

      def allEndpoints( ds : DataSource, as : AppSetup ) : List[ZServerEndpoint[Any,Any]] =
        List(
          Post.Create.zServerLogic( subscriptionCreateLogicPost( ds, as ) ),
          Post.Confirm.zServerLogic( subscriptionConfirmLogicPost( ds, as ) ),
          Post.Remove.zServerLogic( subscriptionRemoveLogicPost( ds, as ) ),
          Get.Confirm.zServerLogic( subscriptionConfirmLogicGet( ds, as ) ),
          Get.Remove.zServerLogic( subscriptionRemoveLogicGet( ds, as ) ),
        )

      type ZSharedOut[T <: ResponsePayload.Success] = ZIO[Any,ResponsePayload.Failure,(SubscriptionInfo,T)]
      type ZPostOut[T <: ResponsePayload.Success]   = ZIO[Any,ResponsePayload.Failure,T]
      type ZGetOut                                  = ZIO[Any,String,String]

      def mapError[T]( task : Task[T] ) : ZIO[Any,ResponsePayload.Failure,T] =
        task.mapError: t =>
          WARNING.log("An error occurred while processing an API request.", t)
          ResponsePayload.Failure(
            message = t.getMessage(),
            throwableClassName = Some( t.getClass.getName ),
            fullStackTrace = Some( t.fullStackTrace )
          )

      def sharedToGet[T <: ResponsePayload.Success]( zso : ZSharedOut[T] ) : ZGetOut =
        def failureToPlainText( f : ResponsePayload.Failure ) =
          val base = 
            s"""|The following failure has occurred:
                |
                |  ${f.message}
                |""".stripMargin
          def throwablePart(fst : String) : String =
            s"""|
                |It was associated with the following exception:
                |
                |$fst
                |""".stripMargin
          f.fullStackTrace.fold(base)(fst => base + throwablePart(fst))
        def successToHtmlText( sharedSuccess : (SubscriptionInfo,ResponsePayload.Success) ) : String =
          val (sinfo, rp) = sharedSuccess
          val sman = sinfo.manager
          sman.htmlForStatusChanged( StatusChangedInfo( sinfo, rp.statusChanged ) )
        zso.mapError( failureToPlainText ).map( successToHtmlText )

      def subscriptionCreateLogicPost( ds : DataSource, as : AppSetup )( screate : RequestPayload.Subscription.Create ) : ZPostOut[ResponsePayload.Subscription.Created] =
        WARNING.log( s"subscriptionCreateLogicPost( $screate )" )
        try
          val mainTask =
            withConnectionTransactional( ds ): conn =>
              val sname = SubscribableName(screate.subscribableName)
              val (sman, sid) = PgDatabase.addSubscription( conn, sname, screate.destination, false, Instant.now ) // validates the destination!
              val cgl = confirmGetLink( sid )
              val confirming = sman.maybeConfirmSubscription( conn, sman.narrowDestinationOrThrow(screate.destination), sname, cgl )
              val confirmedMessage =
                if confirming then ", but unconfirmed. Please respond to the confirmation request, coming soon." else ". No confirmation necessary."
              ResponsePayload.Subscription.Created(s"Subscription ${sid} successfully created${confirmedMessage}", sid.toLong, confirming)
          mapError( mainTask )
        catch
          case t : Throwable =>
            t.printStackTrace()
            throw t

      def subscriptionConfirmLogicShared( ds : DataSource, as : AppSetup )( sconfirm : RequestPayload.Subscription.Confirm ) : ZSharedOut[ResponsePayload.Subscription.Confirmed] =
        val mainTask =
          withConnectionTransactional( ds ): conn =>
            val sid = SubscriptionId(sconfirm.subscriptionId)
            RequestPayload.Subscription.Confirm.assertInvitation( sconfirm, as.secretSalt )
            PgDatabase.updateConfirmed( conn, sid, true )
            val sinfo = PgDatabase.subscriptionInfoForSubscriptionId( conn, sid )
            ( sinfo, ResponsePayload.Subscription.Confirmed(s"Subscription ${sid} of '${sinfo.destination.unique}' successfully confirmed.", sid.toLong) )
        mapError( mainTask )

      def subscriptionConfirmLogicPost( ds : DataSource, as : AppSetup )( sconfirm : RequestPayload.Subscription.Confirm ) : ZPostOut[ResponsePayload.Subscription.Confirmed] =
        subscriptionConfirmLogicShared( ds, as )( sconfirm ).map( _(1) )

      def subscriptionConfirmLogicGet( ds : DataSource, as : AppSetup )( qps : QueryParams ) : ZGetOut =
        try
          val sconfirm = RequestPayload.Subscription.Confirm.fromQueryParams(qps)
          val sharedOut = subscriptionConfirmLogicShared( ds, as )( sconfirm )
          sharedToGet( sharedOut )
        catch
          case t : Throwable =>
            ZIO.fail( t.fullStackTrace )

      def subscriptionRemoveLogicShared( ds : DataSource, as : AppSetup )( sremove : RequestPayload.Subscription.Remove ) : ZSharedOut[ResponsePayload.Subscription.Removed] =
        val mainTask =
          withConnectionTransactional( ds ): conn =>
            val sid   = SubscriptionId(sremove.subscriptionId)
            val sinfo = PgDatabase.unsubscribe( conn, sid )
            ( sinfo, ResponsePayload.Subscription.Removed(s"Unsubscribed. Subscription ${sid} of '${sinfo.destination.unique}' successfully removed.", sid.toLong) )
        mapError( mainTask )

      def subscriptionRemoveLogicPost( ds : DataSource, as : AppSetup )( sremove : RequestPayload.Subscription.Remove ) : ZPostOut[ResponsePayload.Subscription.Removed] =
        subscriptionRemoveLogicShared( ds, as )( sremove ).map( _(1) )

      def subscriptionRemoveLogicGet( ds : DataSource, as : AppSetup )( qps : QueryParams ) : ZGetOut =
        try
          val sremove = RequestPayload.Subscription.Remove.fromQueryParams(qps)
          val sharedOut = subscriptionRemoveLogicShared( ds, as )( sremove )
          sharedToGet( sharedOut )
        catch
          case t : Throwable =>
            ZIO.fail( t.fullStackTrace )
  end TapirApi


