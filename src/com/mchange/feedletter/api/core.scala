package com.mchange.feedletter.api

import com.mchange.feedletter.*
import com.mchange.feedletter.db.PgDatabase

import com.mchange.cryptoutil.{*,given}

import com.mchange.mailutil.Smtp

import com.mchange.conveniences.string.*
import com.mchange.conveniences.throwable.*
import com.mchange.conveniences.www.*

import sttp.model.QueryParams
import sttp.tapir.Schema

import zio.*

import java.time.Instant
import javax.sql.DataSource

import scala.io.Codec
import scala.collection.immutable

import com.mchange.feedletter.db.withConnectionTransactional
import com.mchange.feedletter.AppSetup
import com.mchange.feedletter.SubscriptionManager

import upickle.default.*
import sttp.tapir.json.upickle.*

trait ApiLinkGenerator:
  def createGetLink( subscribableName : SubscribableName, destination : Destination ) : String
  def confirmGetLink( sid : SubscriptionId ) : String
  def removeGetLink( sid : SubscriptionId ) : String

object V0 extends SelfLogging:
  import MLevel.*

  given ReadWriter[RequestPayload.Subscription.Create]  = ReadWriter.derived
  given ReadWriter[RequestPayload.Subscription.Confirm] = ReadWriter.derived
  given ReadWriter[RequestPayload.Subscription.Remove]  = ReadWriter.derived

  given ReadWriter[SubscriptionStatusChanged.Info]         = ReadWriter.derived
  given ReadWriter[SubscriptionStatusChanged]              = ReadWriter.derived
  given ReadWriter[ResponsePayload.Subscription.Created]   = ReadWriter.derived
  given ReadWriter[ResponsePayload.Subscription.Confirmed] = ReadWriter.derived
  given ReadWriter[ResponsePayload.Subscription.Removed]   = ReadWriter.derived
  given ReadWriter[ResponsePayload.Failure]                = ReadWriter.derived

  // given Schema[Destination]                         = Schema.derived[Destination]
  // given Schema[SubscriptionStatusChanged.Info]      = Schema.derived[SubscriptionStatusChanged.Info]
  // given Schema[SubscriptionStatusChanged]           = Schema.derived[SubscriptionStatusChanged]
  // given Schema[RequestPayload.Subscription.Create]  = Schema.derived[RequestPayload.Subscription.Create]
  // given Schema[RequestPayload.Subscription.Confirm] = Schema.derived[RequestPayload.Subscription.Confirm]
  // given Schema[RequestPayload.Subscription.Remove]  = Schema.derived[RequestPayload.Subscription.Remove]

  // given Schema[ResponsePayload]                        = Schema.derived[ResponsePayload]
  // given Schema[ResponsePayload.Subscription.Created]   = Schema.derived[ResponsePayload.Subscription.Created]
  // given Schema[ResponsePayload.Subscription.Confirmed] = Schema.derived[ResponsePayload.Subscription.Confirmed]
  // given Schema[ResponsePayload.Subscription.Removed]   = Schema.derived[ResponsePayload.Subscription.Removed]
  // given Schema[ResponsePayload.Success]                = Schema.derived[ResponsePayload.Success]
  // given Schema[ResponsePayload.Failure]                = Schema.derived[ResponsePayload.Failure]

  private def bytesUtf8( s : String ) : Array[Byte] = s.getBytes(scala.io.Codec.UTF8.charSet)

  object RequestPayload:
    extension ( queryParams : QueryParams )
      def assertParam( key : String ) : String =
        queryParams.get(key).getOrElse:
          throw new InvalidRequest( s"Expected query param '$key' required, not found." )
    object Subscription:
      object Create:
        def fromQueryParams( queryParams : QueryParams ) : Create =
          val subscribableName : String = queryParams.assertParam( "subscribableName" )
          val destination : Destination = Destination.fromFields( queryParams.toSeq ).getOrElse:
            throw new InvalidRequest( "Could not decode a Destination for subscription create request. Fields: " + queryParams.toSeq.mkString(", ") )
          Create( subscribableName, destination )
      case class Create( subscribableName : String, destination : Destination ) extends RequestPayload:
        override lazy val toMap : Map[String,String] = Map( "subscribableName" -> subscribableName ) ++ destination.toFields
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

  object SubscriptionStatusChanged:
    case class Info( subscriptionName : String, destination : Destination ) // does NOT extend SubscriptionStatusChanged
    case class Created( info : Info )         extends SubscriptionStatusChanged( SubscriptionStatusChange.Created )
    case class Confirmed( info : Info )       extends SubscriptionStatusChanged( SubscriptionStatusChange.Confirmed )
    case class Removed( info : Option[Info] ) extends SubscriptionStatusChanged( SubscriptionStatusChange.Removed )
  sealed trait SubscriptionStatusChanged( val statusChange : SubscriptionStatusChange )

  extension ( sinfo : SubscriptionInfo )
    def thin : SubscriptionStatusChanged.Info = SubscriptionStatusChanged.Info( sinfo.name.toString, sinfo.destination )

  object ResponsePayload:
    object Subscription:
      case class Created( message : String, id : Long, confirmationRequired : Boolean, statusChanged : SubscriptionStatusChanged.Created, success : Boolean = true ) extends ResponsePayload.Success
      case class Confirmed( message : String, id : Long, statusChanged : SubscriptionStatusChanged.Confirmed, success : Boolean = true ) extends ResponsePayload.Success
      case class Removed( message : String, id : Long, statusChanged : SubscriptionStatusChanged.Removed, success : Boolean = true ) extends ResponsePayload.Success
    sealed trait Success extends ResponsePayload:
      def statusChanged : SubscriptionStatusChanged
    case class Failure( message : String, throwableClassName : Option[String], fullStackTrace : Option[String], success : Boolean = false ) extends ResponsePayload
  sealed trait ResponsePayload:
    def success : Boolean
    def message : String

  class TapirApi(val serverUrl : String, val locationPathElements : List[String], val secretSalt : String) extends ApiLinkGenerator:

    val basePathElements = locationPathElements ::: "v0" :: "subscription" :: Nil

    val createPathElements  = basePathElements ::: "create"  :: Nil
    val confirmPathElements = basePathElements ::: "confirm" :: Nil
    val removePathElements  = basePathElements ::: "remove"  :: Nil

    val createFullPath  = createPathElements.mkString("/","/","")
    val confirmFullPath = confirmPathElements.mkString("/","/","")
    val removeFullPath  = removePathElements.mkString("/","/","")

    val createEndpointUrl = pathJoin( serverUrl, createFullPath )
    val confirmEndpointUrl = pathJoin( serverUrl, confirmFullPath )
    val removeEndpointUrl = pathJoin( serverUrl, removeFullPath )

    def createGetLink( subscribableName : SubscribableName, destination : Destination ) : String =
      val createRequest = RequestPayload.Subscription.Create( subscribableName.toString, destination )
      createEndpointUrl + "?" + createRequest.toGetParams

    def confirmGetLink( sid : SubscriptionId ) : String =
      val confirmRequest = RequestPayload.Subscription.Confirm.invite(sid.toLong, secretSalt)
      confirmEndpointUrl + "?" + confirmRequest.toGetParams

    def removeGetLink( sid : SubscriptionId ) : String =
      val removeRequest = RequestPayload.Subscription.Remove.invite(sid.toLong, secretSalt)
      removeEndpointUrl + "?" + removeRequest.toGetParams

    object BasicEndpoint:
      import sttp.tapir.ztapir.*
      import sttp.tapir.json.upickle.*
      import sttp.tapir.PublicEndpoint

      def addElements[IN,ERROUT,OUT,R]( baseEndpoint : PublicEndpoint[IN,ERROUT,OUT,R], elems : List[String] ) =
        elems.foldLeft( baseEndpoint )( (accum,next) => accum.in(next) )

      object Post:
        import com.mchange.feedletter.api.V0.given
      /*
        Too much trouble deriving Tapir Schemas... we'll handle the JSON in our own logic

        val Base = endpoint
                     .post
                     .errorOut( jsonBody[ResponsePayload.Failure] )
        val Create  = addElements(Base,createPathElements).in( jsonBody[RequestPayload.Subscription.Create] ).out( jsonBody[ResponsePayload.Subscription.Created] )
        val Confirm = addElements(Base,confirmPathElements).in( jsonBody[RequestPayload.Subscription.Confirm] ).out( jsonBody[ResponsePayload.Subscription.Confirmed] )
        val Remove  = addElements(Base,removePathElements).in( jsonBody[RequestPayload.Subscription.Remove] ).out( jsonBody[ResponsePayload.Subscription.Removed] )
      */

        val Base = endpoint
                     .post
                     .in( stringJsonBody )
                     .out( stringJsonBody )
                     .errorOut( stringJsonBody )
        val Create  = addElements(Base,createPathElements)
        val Confirm = addElements(Base,confirmPathElements)
        val Remove  = addElements(Base,removePathElements)

      object Get:
        val Base = endpoint
                     .get
                     .errorOut( stringBody )
                     .in( queryParams )
                     .out( htmlBodyUtf8 )
        val Create  = addElements(Base,createPathElements)
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
          Get.Create.zServerLogic( subscriptionCreateLogicGet( ds, as ) ),
          Get.Confirm.zServerLogic( subscriptionConfirmLogicGet( ds, as ) ),
          Get.Remove.zServerLogic( subscriptionRemoveLogicGet( ds, as ) ),
        )

      type ZSharedOut[T <: ResponsePayload.Success] = ZIO[Any,ResponsePayload.Failure,(Option[SubscriptionInfo],T)]
      type ZPostOut                                 = ZIO[Any,String,String]
      type ZGetOut                                  = ZIO[Any,String,String]

      // type ZPostOut[T <: ResponsePayload.Success]   = ZIO[Any,ResponsePayload.Failure,T]

      def mapError[T]( task : Task[T] ) : ZIO[Any,ResponsePayload.Failure,T] =
        task.mapError: t =>
          WARNING.log("An error occurred while processing an API request.", t)
          ResponsePayload.Failure(
            message = t.getMessage(),
            throwableClassName = Some( t.getClass.getName ),
            fullStackTrace = Some( t.fullStackTrace )
          )

      def sharedToGet[T <: ResponsePayload.Success]( zpo : ZSharedOut[T] ) : ZGetOut =
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
        def successToHtmlText( tup : (Option[SubscriptionInfo],ResponsePayload.Success) ) : String =
          val (mbSinfo, rp) = tup
          mbSinfo match
            case Some( sinfo ) =>
              val sman = sinfo.manager
              sman.htmlForStatusChange( new StatusChangeInfo( rp.statusChanged.statusChange, sinfo.name.toString, sman, sinfo.destination, !sinfo.confirmed, removeGetLink(sinfo.id), createGetLink(sinfo.name, sinfo.destination) ) )
            case None =>
              """|<html>
                 |  <head><title>Subscription Re-removed</title></head>
                 |  <body>
                 |    <h1>Subscription Re-removed</h1>
                 |    <p>The subscription you are trying to remove has already been unsubscribed. Have a nice day!</p>
                 |  </body>
                 |</html>""".stripMargin
        zpo.mapError( failureToPlainText ).map( successToHtmlText )

      def subscriptionCreateLogicShared( ds : DataSource, as : AppSetup )( screate : RequestPayload.Subscription.Create ) : ZSharedOut[ResponsePayload.Subscription.Created] =
        TRACE.log( s"subscriptionCreateLogicShared( $screate )" )
        val mainTask =
          withConnectionTransactional( ds ): conn =>
            val sname = SubscribableName(screate.subscribableName)
            val destination = screate.destination
            val (sman, sid) = PgDatabase.addSubscription( conn, true, sname, destination, false, Instant.now ) // validates the destination!
            val cgl = confirmGetLink( sid )
            val confirming = sman.maybePromptConfirmation( conn, sid, sname, sman.narrowDestinationOrThrow(destination), cgl )
            val confirmedMessage =
              if confirming then ", but unconfirmed. Please respond to the confirmation request, coming soon." else ". No confirmation necessary."
            val sinfo = SubscriptionInfo( sid, sname, sman, destination, !confirming )
            ( Some(sinfo), ResponsePayload.Subscription.Created(s"Subscription ${sid} successfully created${confirmedMessage}", sid.toLong, confirming, SubscriptionStatusChanged.Created(sinfo.thin)) )
        mapError( mainTask )

/*
      def subscriptionCreateLogicPost( ds : DataSource, as : AppSetup )( screate : RequestPayload.Subscription.Create ) : ZPostOut[ResponsePayload.Subscription.Created] =
        subscriptionCreateLogicShared( ds, as )( screate ).map( _(1) )
*/

      def subscriptionCreateLogicPost( ds : DataSource, as : AppSetup )( screateJson : String ) : ZPostOut =
        val screate = read[RequestPayload.Subscription.Create]( screateJson )
        subscriptionCreateLogicShared( ds, as )( screate ).map( tup => write(tup(1)) ).mapError( failure => write(failure) )

      def subscriptionCreateLogicGet( ds : DataSource, as : AppSetup )( qps : QueryParams ) : ZGetOut =
        try
          val screate = RequestPayload.Subscription.Create.fromQueryParams(qps)
          val sharedOut = subscriptionCreateLogicShared( ds, as )( screate )
          sharedToGet( sharedOut )
        catch
          case t : Throwable =>
            ZIO.fail( t.fullStackTrace )

      def subscriptionConfirmLogicShared( ds : DataSource, as : AppSetup )( sconfirm : RequestPayload.Subscription.Confirm ) : ZSharedOut[ResponsePayload.Subscription.Confirmed] =
        val mainTask =
          withConnectionTransactional( ds ): conn =>
            val sid = SubscriptionId(sconfirm.subscriptionId)
            RequestPayload.Subscription.Confirm.assertInvitation( sconfirm, as.secretSalt )
            PgDatabase.updateConfirmed( conn, sid, true )
            val mbSinfo = PgDatabase.subscriptionInfoForSubscriptionId( conn, sid )
            val sinfo = mbSinfo.getOrElse:
              throw new AssertionError( s"If a subscription successfully confirmed, it ought to be available to select from the database!")
            ( Some(sinfo), ResponsePayload.Subscription.Confirmed(s"Subscription ${sid} of '${sinfo.destination.unique}' successfully confirmed.", sid.toLong, SubscriptionStatusChanged.Confirmed(sinfo.thin) ) )
        mapError( mainTask )

/*
      def subscriptionConfirmLogicPost( ds : DataSource, as : AppSetup )( sconfirm : RequestPayload.Subscription.Confirm ) : ZPostOut[ResponsePayload.Subscription.Confirmed] =
        subscriptionConfirmLogicShared( ds, as )( sconfirm ).map( _(1) )
*/

      def subscriptionConfirmLogicPost( ds : DataSource, as : AppSetup )( sconfirmJson : String ) : ZPostOut =
        val sconfirm = read[RequestPayload.Subscription.Confirm]( sconfirmJson )
        subscriptionConfirmLogicShared( ds, as )( sconfirm ).map( tup => write(tup(1)) ).mapError( failure => write(failure) )

      def subscriptionConfirmLogicGet( ds : DataSource, as : AppSetup )( qps : QueryParams ) : ZGetOut =
        try
          val sconfirm = RequestPayload.Subscription.Confirm.fromQueryParams(qps)
          val sharedOut = subscriptionConfirmLogicShared( ds, as )( sconfirm )
          sharedToGet( sharedOut )
        catch
          case t : Throwable =>
            ZIO.fail( t.fullStackTrace )

      def subscriptionRemoveLogicShared( ds : DataSource, as : AppSetup )( sremove : RequestPayload.Subscription.Remove ) : ZSharedOut[ResponsePayload.Subscription.Removed] =
        TRACE.log( s"subscriptionRemoveLogicShared( $sremove )" )
        val mainTask =
          withConnectionTransactional( ds ): conn =>
            val sid   = SubscriptionId(sremove.subscriptionId)
            val mbSinfo = PgDatabase.unsubscribe( conn, sid )
            val message =
              mbSinfo.fold("Subscription with ID ${sid} has already removed."): sinfo =>
                s"Unsubscribed. Subscription ${sid} of '${sinfo.destination.unique}' successfully removed."
            ( mbSinfo, ResponsePayload.Subscription.Removed(message, sid.toLong,SubscriptionStatusChanged.Removed(mbSinfo.map(_.thin))) )
        mapError( mainTask )

      def subscriptionRemoveLogicPost( ds : DataSource, as : AppSetup )( sremoveJson : String ) : ZPostOut =
        val sremove = read[RequestPayload.Subscription.Remove]( sremoveJson )
        subscriptionRemoveLogicShared( ds, as )( sremove ).map( tup => write(tup(1)) ).mapError( failure => write(failure) )

/*
      def subscriptionRemoveLogicPost( ds : DataSource, as : AppSetup )( sremove : RequestPayload.Subscription.Remove ) : ZPostOut[ResponsePayload.Subscription.Removed] =
        subscriptionRemoveLogicShared( ds, as )( sremove ).map( _(1) )
*/

      def subscriptionRemoveLogicGet( ds : DataSource, as : AppSetup )( qps : QueryParams ) : ZGetOut =
        try
          val sremove = RequestPayload.Subscription.Remove.fromQueryParams(qps)
          val sharedOut = subscriptionRemoveLogicShared( ds, as )( sremove )
          sharedToGet( sharedOut )
        catch
          case t : Throwable =>
            ZIO.fail( t.fullStackTrace )
  end TapirApi


