package com.mchange.feedletter.api

import com.mchange.feedletter.*
import com.mchange.feedletter.db.PgDatabase
import com.mchange.feedletter.style.StatusChangeInfo

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

import com.mchange.feedletter.AppSetup
import com.mchange.feedletter.SubscriptionManager

import com.mchange.sc.zsqlutil.withConnectionTransactional

import upickle.default.*
import sttp.tapir.json.upickle.*

import LoggingApi.*

trait ApiLinkGenerator:
  def createGetLink( subscribableName : SubscribableName, destination : Destination ) : String
  def confirmGetLink( sid : SubscriptionId ) : String
  def removeGetLink( sid : SubscriptionId ) : String

object V0 extends SelfLogging:

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
      def assertParamTrimmed( key : String ) : String =
        queryParams.get(key).map( _.trim ).getOrElse:
          throw new InvalidRequest( s"Expected query param '$key' required, not found." )
      def toSeqTrimmedValues : Seq[(String,String)] =
        queryParams.toSeq.map( (k,v) => (k,v.trim) )

    object Subscription:
      object Create:
        def fromQueryParams( queryParams : QueryParams ) : Create =
          val subscribableName : String = queryParams.assertParamTrimmed( "subscribableName" )
          val destination : Destination = Destination.fromFields( queryParams.toSeqTrimmedValues ).getOrElse:
            throw new InvalidRequest( "Could not decode a Destination for subscription create request. Fields: " + queryParams.toSeq.mkString(", ") )
          Create( subscribableName, destination )
        def fromBodyFormSeq( formData : Seq[(String,String)] ) : Create =
          fromQueryParams( QueryParams.fromSeq( formData ) )
      case class Create( subscribableName : String, destination : Destination ) extends RequestPayload:
        override lazy val toMap : Map[String,String] = Map( "subscribableName" -> subscribableName ) ++ destination.toFields
      object Confirm extends Bouncer[Confirm]("ConfirmSuffix"):
        def fromQueryParams( queryParams : QueryParams ) : Confirm =
          val subscriptionId : Long   = queryParams.assertParamTrimmed( "subscriptionId" ).toLong
          val invitation     : String = queryParams.assertParamTrimmed( "invitation" )
          Confirm( subscriptionId, invitation )
        def noninvitationContentsBytes( req : Confirm ) = req.subscriptionId.toByteSeqBigEndian
        def invite( subscriptionId : Long, secretSalt : String ) : Confirm =
          val nascent = Confirm( subscriptionId, "" )
          nascent.copy( invitation = regenInvitation( nascent, secretSalt ) )
      case class Confirm( subscriptionId : Long, invitation : String ) extends RequestPayload.Invited
      object Remove extends Bouncer[Remove]("RemoveSuffix"):
        def fromQueryParams( queryParams : QueryParams ) : Remove =
          val subscriptionId : Long   = queryParams.assertParamTrimmed( "subscriptionId" ).toLong
          val invitation     : String = queryParams.assertParamTrimmed( "invitation" )
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
    case class Info( subscribableName : String, destination : Destination ) // does NOT extend SubscriptionStatusChanged
    case class Created( info : Info )         extends SubscriptionStatusChanged( SubscriptionStatusChange.Created )
    case class Confirmed( info : Info )       extends SubscriptionStatusChanged( SubscriptionStatusChange.Confirmed )
    case class Removed( info : Option[Info] ) extends SubscriptionStatusChanged( SubscriptionStatusChange.Removed )
  sealed trait SubscriptionStatusChanged( val statusChange : SubscriptionStatusChange )

  extension ( sinfo : SubscriptionInfo )
    def thin : SubscriptionStatusChanged.Info = SubscriptionStatusChanged.Info( sinfo.name.str, sinfo.destination )

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
      val createRequest = RequestPayload.Subscription.Create( subscribableName.str, destination )
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
        val Base = endpoint
                     .post
                     .errorOut( stringBody )
                     .in( formBody[Seq[(String, String)]] )
                     .out( htmlBodyUtf8 )
        val Create  = addElements(Base,createPathElements)

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
          Get.Create.zServerLogic( subscriptionCreateLogicGet( ds, as ) ),
          Get.Confirm.zServerLogic( subscriptionConfirmLogicGet( ds, as ) ),
          Get.Remove.zServerLogic( subscriptionRemoveLogicGet( ds, as ) ),
        )

      type ZRawOut[T <: ResponsePayload.Success] = ZIO[Any,ResponsePayload.Failure,(Option[SubscriptionInfo],T)]
      type ZOut                                  = ZIO[Any,String,String]

      def mapError[T]( task : Task[T] ) : ZIO[Any,ResponsePayload.Failure,T] =
        task.mapError: t =>
          WARNING.log("An error occurred while processing an API request.", t)
          ResponsePayload.Failure(
            message = t.getMessage(),
            throwableClassName = Some( t.getClass.getName ),
            fullStackTrace = Some( t.fullStackTrace )
          )

      val AlreadySubscribedClassName = classOf[AlreadySubscribed].getName

      def rawToGet[T <: ResponsePayload.Success]( zpo : ZRawOut[T] ) : ZOut =
        def failureToPlainText( f : ResponsePayload.Failure ) =
          f.throwableClassName match
            case Some( AlreadySubscribedClassName ) =>
               s"""|${HoorayFiglet}
                   |
                   |   You have already successfully subscribed to this mailing list.
                   |
                   |   Details:
                   |     ${f.message}
                   |""".stripMargin
            case _ =>
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
              sman match
                case vsman : SubscriptionManager.SupportsExternalSubscriptionApi =>
                  vsman.htmlForStatusChange( new StatusChangeInfo( rp.statusChanged.statusChange, sinfo.name, sman, sinfo.destination, !sinfo.confirmed, removeGetLink(sinfo.id), createGetLink(sinfo.name, sinfo.destination) ) )
                case _ => // we should never see this, raw logic should have checked already
                  throw new InvalidSubscribable(s"Subscribable '${sinfo.name}' does not support the external subscription API. (Manager '${sinfo.manager}' does not support.)")
            case None =>
              """|<html>
                 |  <head><title>Subscription Re-removed</title></head>
                 |  <body>
                 |    <h1>Subscription Re-removed</h1>
                 |    <p>The subscription you are trying to remove has already been unsubscribed. Have a nice day!</p>
                 |  </body>
                 |</html>""".stripMargin
        zpo.mapError( failureToPlainText ).map( successToHtmlText )

      def subscriptionCreateLogicRaw( ds : DataSource, as : AppSetup )( screate : RequestPayload.Subscription.Create ) : ZRawOut[ResponsePayload.Subscription.Created] =
        TRACE.log( s"subscriptionCreateLogicRaw( $screate )" )
        val mainTask =
          withConnectionTransactional( ds ): conn =>
            val sname = SubscribableName(screate.subscribableName)
            val destination = screate.destination
            if PgDatabase.destinationAlreadySubscribed( conn, destination, sname ) then
              throw new AlreadySubscribed( s"${destination.shortDesc} is already subscribed to '$sname'." )
            val (sman, sid) = PgDatabase.addSubscription( conn, true, sname, destination, false, Instant.now ) // validates the destination!
            sman match
              case vsman : SubscriptionManager.SupportsExternalSubscriptionApi =>
                val cgl = confirmGetLink( sid )
                val rgl = removeGetLink( sid )
                val confirming = vsman.maybePromptConfirmation( conn, as, sid, sname, vsman.narrowDestinationOrThrow(destination), cgl, rgl )
                val confirmedMessage =
                  if confirming then ", but unconfirmed. Please respond to the confirmation request, coming soon." else ". No confirmation necessary."
                val sinfo = SubscriptionInfo( sid, sname, vsman, destination, !confirming )
                val outMsg = s"Subscription ${sid} successfully created${confirmedMessage}"
                INFO.log(outMsg)
                ( Some(sinfo), ResponsePayload.Subscription.Created(outMsg, sid.toLong, confirming, SubscriptionStatusChanged.Created(sinfo.thin)) )
              case _ =>
                throw new InvalidSubscribable(s"Can't subscribe. Subscribable '${sname}' does not support the external subscription API. (Manager '$sman' does not support.)")
        mapError( mainTask )

      def subscriptionCreateLogicPost( ds : DataSource, as : AppSetup )( bodyFormParams : Seq[(String,String)] ) : ZOut =
        try
          val screate = RequestPayload.Subscription.Create.fromBodyFormSeq( bodyFormParams )
          val rawOut = subscriptionCreateLogicRaw( ds, as )( screate )
          rawToGet(rawOut)
        catch
          case t : Throwable =>
            ZIO.fail( t.fullStackTrace )

      def subscriptionCreateLogicGet( ds : DataSource, as : AppSetup )( qps : QueryParams ) : ZOut =
        try
          val screate = RequestPayload.Subscription.Create.fromQueryParams(qps)
          val rawOut = subscriptionCreateLogicRaw( ds, as )( screate )
          rawToGet( rawOut )
        catch
          case t : Throwable =>
            ZIO.fail( t.fullStackTrace )

      def subscriptionConfirmLogicRaw( ds : DataSource, as : AppSetup )( sconfirm : RequestPayload.Subscription.Confirm ) : ZRawOut[ResponsePayload.Subscription.Confirmed] =
        val mainTask =
          withConnectionTransactional( ds ): conn =>
            val sid = SubscriptionId(sconfirm.subscriptionId)
            RequestPayload.Subscription.Confirm.assertInvitation( sconfirm, as.secretSalt )
            val mbSinfo = PgDatabase.subscriptionInfoForSubscriptionId( conn, sid )
            val sinfo = mbSinfo.getOrElse:
              throw new UnknownSubscriptionId( s"Received an invited attempt to conform subscription ${sid}, but that subscription is now unknown. Perhaps it has already been removed?" )
            sinfo.manager match
              case vsman : SubscriptionManager.SupportsExternalSubscriptionApi =>
                PgDatabase.updateConfirmed( conn, sid, true )
                val outMsg = s"Subscription ${sid} of '${sinfo.destination.unique}' successfully confirmed."
                INFO.log(outMsg)
                ( Some(sinfo), ResponsePayload.Subscription.Confirmed(outMsg, sid.toLong, SubscriptionStatusChanged.Confirmed(sinfo.thin) ) )
              case _ =>
                throw new InvalidSubscribable(s"Can't comfirm. Subscribable '${sinfo.name}' does not support the external subscription API. (Manager '${sinfo.manager}' does not support.)")
        mapError( mainTask )

      def subscriptionConfirmLogicGet( ds : DataSource, as : AppSetup )( qps : QueryParams ) : ZOut =
        try
          val sconfirm = RequestPayload.Subscription.Confirm.fromQueryParams(qps)
          val rawOut = subscriptionConfirmLogicRaw( ds, as )( sconfirm )
          rawToGet( rawOut )
        catch
          case t : Throwable =>
            ZIO.fail( t.fullStackTrace )

      def subscriptionRemoveLogicRaw( ds : DataSource, as : AppSetup )( sremove : RequestPayload.Subscription.Remove ) : ZRawOut[ResponsePayload.Subscription.Removed] =
        TRACE.log( s"subscriptionRemoveLogicRaw( $sremove )" )
        val mainTask =
          withConnectionTransactional( ds ): conn =>
            val sid   = SubscriptionId(sremove.subscriptionId)
            val mbSinfo = PgDatabase.unsubscribe( conn, sid )
            val message =
              mbSinfo match
                case Some(sinfo) =>
                  sinfo.manager match
                    case vsman : SubscriptionManager.SupportsExternalSubscriptionApi =>
                      val d = vsman.narrowDestinationOrThrow(sinfo.destination)
                      vsman.maybeSendRemovalNotification(conn,as,sid,sinfo.name,d,createGetLink(sinfo.name,d))
                      s"Unsubscribed. Subscription ${sid} of '${sinfo.destination.unique}' successfully removed."
                    case _ =>  
                      // aborts the transaction, rolls back the unsubscribe
                      throw new InvalidSubscribable(s"Can't remove. Subscribable '${sinfo.name}' does not support the external subscription API. (Manager '${sinfo.manager}' does not support.)")
                case None =>
                  s"Subscription with ID ${sid} does not exist or has already been removed."
            INFO.log(message)
            ( mbSinfo, ResponsePayload.Subscription.Removed(message, sid.toLong,SubscriptionStatusChanged.Removed(mbSinfo.map(_.thin))) )
        mapError( mainTask )

      def subscriptionRemoveLogicGet( ds : DataSource, as : AppSetup )( qps : QueryParams ) : ZOut =
        try
          val sremove = RequestPayload.Subscription.Remove.fromQueryParams(qps)
          val rawOut = subscriptionRemoveLogicRaw( ds, as )( sremove )
          rawToGet( rawOut )
        catch
          case t : Throwable =>
            ZIO.fail( t.fullStackTrace )

    // generated by https://www.askapache.com/online-tools/figlet-ascii/
    val HoorayFiglet =
      """|hhhhhhh                                                                                                         
         |h:::::h                                                                                                         
         |h:::::h                                                                                                         
         |h:::::h                                                                                                         
         |h::::h hhhhh          ooooooooooo      ooooooooooo   rrrrr   rrrrrrrrr   aaaaaaaaaaaaayyyyyyy           yyyyyyy
         |h::::hh:::::hhh     oo:::::::::::oo  oo:::::::::::oo r::::rrr:::::::::r  a::::::::::::ay:::::y         y:::::y 
         |h::::::::::::::hh  o:::::::::::::::oo:::::::::::::::or:::::::::::::::::r aaaaaaaaa:::::ay:::::y       y:::::y  
         |h:::::::hhh::::::h o:::::ooooo:::::oo:::::ooooo:::::orr::::::rrrrr::::::r         a::::a y:::::y     y:::::y   
         |h::::::h   h::::::ho::::o     o::::oo::::o     o::::o r:::::r     r:::::r  aaaaaaa:::::a  y:::::y   y:::::y    
         |h:::::h     h:::::ho::::o     o::::oo::::o     o::::o r:::::r     rrrrrrraa::::::::::::a   y:::::y y:::::y     
         |h:::::h     h:::::ho::::o     o::::oo::::o     o::::o r:::::r           a::::aaaa::::::a    y:::::y:::::y      
         |h:::::h     h:::::ho::::o     o::::oo::::o     o::::o r:::::r          a::::a    a:::::a     y:::::::::y       
         |h:::::h     h:::::ho:::::ooooo:::::oo:::::ooooo:::::o r:::::r          a::::a    a:::::a      y:::::::y        
         |h:::::h     h:::::ho:::::::::::::::oo:::::::::::::::o r:::::r          a:::::aaaa::::::a       y:::::y         
         |h:::::h     h:::::h oo:::::::::::oo  oo:::::::::::oo  r:::::r           a::::::::::aa:::a     y:::::y          
         |hhhhhhh     hhhhhhh   ooooooooooo      ooooooooooo    rrrrrrr            aaaaaaaaaa  aaaa    y:::::y           
         |                                                                                            y:::::y            
         |                                                                                           y:::::y             
         |                                                                                          y:::::y              
         |                                                                                        y:::::y                
         |                                                                                       yyyyyyy                 """.stripMargin
  end TapirApi


