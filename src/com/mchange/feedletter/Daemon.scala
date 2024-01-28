package com.mchange.feedletter

import zio.*
import javax.sql.DataSource
import com.mchange.feedletter.db.PgDatabase
import com.mchange.feedletter.api.ApiLinkGenerator
import com.mchange.mailutil.*

import MLevel.*
import com.mchange.feedletter.db.withConnectionTransactional

import com.mchange.feedletter.BuildInfo

object Daemon extends SelfLogging:

  private object RetrySchedule:
    val updateAssignComplete = Schedule.exponential( 10.seconds, 1.25f ) && Schedule.upTo( 5.minutes ) // XXX: hard-coded for now
    val mailNextGroup = updateAssignComplete // XXX: for now!
    val mastoNotify = updateAssignComplete     // XXX: for now!
    val expireUnconfirmedSubscriptions = updateAssignComplete
    val mainDaemon = Schedule.exponential( 10.seconds, 1.25f ) || Schedule.fixed( 1.minute ) // XXX: hard-coded for now, retries forever
    val checkFlags = Schedule.exponential( 5.seconds, 1.25f ) || Schedule.fixed( 10.seconds ) // XXX: hard-coded for now, retries forever

  private object CyclingSchedule:
    val updateAssignComplete = Schedule.spaced( 1.minute ).jittered(0.0, 0.5) // XXX: hard-coded for now
    def mailNextGroup( mailBatchDelaySeconds : Int ) =
      val initDelay = scala.util.Random.nextInt(mailBatchDelaySeconds).seconds
      Schedule.delayed( Schedule.once.map( _ => initDelay ) ) andThen Schedule.spaced( mailBatchDelaySeconds.seconds )
    val mastoNotify = Schedule.spaced( 2.minutes ).jittered(0.0, 0.5)  
    val checkFlags = Schedule.spaced( 5.seconds ).jittered(0.0, 0.25)  
    val expireUnconfirmedSubscriptions = Schedule.fixed( 1.hours )

  // updateAssign and complete are distinct transactions,
  // and everything is idempotent.
  //
  // but completion work logically follows updateAssign, so for now at
  // least we are combining them.
  object UpdateAssignComplete:
    private def updateAssignComplete( ds : DataSource, apiLinkGenerator : ApiLinkGenerator ) : Task[Unit] =
      PgDatabase.updateAssignItems( ds ) *> PgDatabase.completeAssignables( ds, apiLinkGenerator )

    private def retrying( ds : DataSource, apiLinkGenerator : ApiLinkGenerator ) : Task[Unit] =
      updateAssignComplete( ds, apiLinkGenerator )
        .zlogErrorDefect( WARNING, what = "UpdateAssignComplete" )
        .retry( RetrySchedule.updateAssignComplete )

    def cyclingRetrying( ds : DataSource, apiLinkGenerator : ApiLinkGenerator ) : Task[Unit] =
      retrying( ds, apiLinkGenerator )
        .catchAll( t => WARNING.zlog( "Retry cycle for UpdateAssignComplete failed...", t ) )
        .schedule( CyclingSchedule.updateAssignComplete )
        .unit
        .onInterrupt( DEBUG.zlog( "UpdateAsignComplete.cyclingRetrying fiber interrupted." ) )

  object MailNextGroup:
    private def retrying( ds : DataSource, smtpContext : Smtp.Context ) : Task[Unit] =
      PgDatabase.mailNextGroup( ds, smtpContext )
        .zlogErrorDefect( WARNING, what = "MailNextGroup" )
        .retry( RetrySchedule.mailNextGroup )

    def cyclingRetrying( ds : DataSource, smtpContext : Smtp.Context, mailBatchDelaySeconds : Int ) : Task[Unit] =
      retrying( ds, smtpContext )
        .catchAll( t => WARNING.zlog( "Retry cycle for MailNextGroup failed...", t ) )
        .schedule( CyclingSchedule.mailNextGroup( mailBatchDelaySeconds ) )
        .unit
        .onInterrupt( DEBUG.zlog( "MailNextGroup.cyclingRetrying fiber interrupted." ) )

  object MastoNotify:
    private def retrying( ds : DataSource, appSetup : AppSetup ) : Task[Unit] =
      PgDatabase.notifyAllMastoPosts( ds, appSetup )
        .zlogErrorDefect( WARNING, what = "MastoNotify" )
        .retry( RetrySchedule.mastoNotify )

    def cyclingRetrying( ds : DataSource, appSetup : AppSetup ) : Task[Unit] =
      retrying( ds, appSetup )
        .catchAll( t => WARNING.zlog( "Retry cycle for MastoNotify failed...", t ) )
        .schedule( CyclingSchedule.mastoNotify )
        .unit
        .onInterrupt( DEBUG.zlog( "MastoNotify.cyclingRetrying fiber interrupted." ) )

  object ExpireUnconfirmedSubscriptions:
    private def retrying( ds : DataSource ) : Task[Unit] =
      PgDatabase.expireUnconfirmed( ds )
        .zlogErrorDefect( WARNING, what = "ExpireUnconfirmedSubscriptions" )
        .retry( RetrySchedule.expireUnconfirmedSubscriptions )

    def cyclingRetrying( ds : DataSource ) : Task[Unit] =
      retrying( ds )
        .catchAll( t => WARNING.zlog( "Retry cycle for ExpireUnconfirmedSubscriptions failed...", t ) )
        .schedule( CyclingSchedule.expireUnconfirmedSubscriptions )
        .unit
        .onInterrupt( DEBUG.zlog( "ExpireUnconfirmedSubscriptions fiber interrupted." ) )

  object CheckFlags:
    private def clearNextImmediatelyMailable( ds : DataSource, as : AppSetup ) : Task[Boolean] =
      for
        _      <- PgDatabase.clearFlag( ds, Flag.ImmediateMailQueued )
        mbNext <- PgDatabase.sendNextImmediatelyMailable( ds, as )
      yield
        mbNext

    // there is a potential race conditions here, Flag.ImmediateMailQueued could be set and more mail
    // queued before ref is set to false. the worst this could do, though, is have us miss a cycle,
    // delay handling the mail very briefly. (in practice, given the times involved, this should be
    // vanishingly rare unless the "immediately mailable" load is very high.
    private def clearImmediatelyMailable( ds : DataSource, as : AppSetup, alreadyClearingRef : Ref[Boolean] ) : Task[Unit] =
      for
        alreadyClearing <- alreadyClearingRef.get
        _               <- if alreadyClearing then throw new AssertionError("alreadyClearing already set!") else ZIO.unit
        _               <- alreadyClearingRef.set( true )
        _               <- clearNextImmediatelyMailable( ds, as ).repeatWhile(identity)
        _               <- alreadyClearingRef.set( false )
      yield ()

    //alreadyClearingImmediateMailRef
    private def checkFlags( ds : DataSource, as : AppSetup, reloadWebApi : Promise[Throwable,Unit], acimRef : Ref[Boolean] ) : Task[Unit] =
      def checkShouldClear : Task[Boolean] =
        acimRef.get.flatMap: acim =>
          if acim then ZIO.succeed(false)
          else PgDatabase.checkFlag( ds, Flag.ImmediateMailQueued )
      end checkShouldClear
      for
        mustReload  <- PgDatabase.checkFlag( ds, Flag.MustReloadDaemon )
        _           <- if mustReload then reloadWebApi.succeed( () ) else ZIO.unit
        shouldClear <- checkShouldClear
        _           <- if shouldClear then clearImmediatelyMailable( ds, as, acimRef ).fork else ZIO.unit
      yield ()

    //alreadyClearingImmediateMailRef
    private def retrying( ds : DataSource, as : AppSetup, reloadWebApi : Promise[Throwable,Unit], acimRef : Ref[Boolean] ) : Task[Unit] =
      checkFlags( ds, as, reloadWebApi, acimRef )
        .zlogErrorDefect( WARNING, what = "CheckFlags" )
        .retry( RetrySchedule.checkFlags )

    //alreadyClearingImmediateMailRef
    def cyclingRetrying( ds : DataSource, as : AppSetup, reloadWebApi : Promise[Throwable,Unit], acimRef : Ref[Boolean] ) : Task[Unit] =
      retrying( ds, as, reloadWebApi, acimRef )
        .catchAll( t => WARNING.zlog( "Retry cycle for CheckFlags failed...", t ) )
        .schedule( CyclingSchedule.checkFlags )
        .unit
        .onInterrupt( DEBUG.zlog( "CheckFlags.cyclingRetrying fiber interrupted." ) )


  def tapirApi( ds : DataSource, as : AppSetup ) : Task[api.V0.TapirApi] =
    for
      tup0         <- PgDatabase.webApiUrlBasePath( ds )
      (server, bp) =  tup0
      out          <- ZIO.attempt( api.V0.TapirApi( server, bp, as.secretSalt ) )
    yield out

  private def webDaemon( ds : DataSource, as : AppSetup, tapirApi : api.V0.TapirApi ) : Task[Unit] =
    import zio.http.Server
    import sttp.tapir.ztapir.*
    import sttp.tapir.server.ziohttp.* //ZioHttpInterpreter
    import sttp.tapir.server.interceptor.log.DefaultServerLog

    for
      tup          <- PgDatabase.webDaemonBinding( ds )
      (host, port) =  tup
      httpApp      = ZioHttpInterpreter().toHttp( tapirApi.ServerEndpoint.allEndpoints( ds, as ) )
      _            <- INFO.zlog( s"Starting web API service on interface '$host', port $port." )
      _            <- Server
                        .serve(httpApp)
                        .provide( ZLayer.succeed( Server.Config.default.binding(host,port) ), Server.live )
                        .onInterrupt( DEBUG.zlog( "webDaemon fiber interrupted." ) )
    yield ()

  def startup( ds : DataSource, as : AppSetup ) : Task[Unit] =
    val singleLoad =
      for
        _        <- INFO.zlog( s"feedletter-${BuildInfo.version} daemon (re)starting." )
        _        <- PgDatabase.clearFlag(ds, Flag.MustReloadDaemon)
        mbds     <- withConnectionTransactional( ds )( conn => PgDatabase.Config.mailBatchDelaySeconds(conn) )
        tapirApi <- tapirApi(ds,as)
        _        <- INFO.zlog( s"Spawning daemon fibers." )
        fuac     <- UpdateAssignComplete.cyclingRetrying( ds, tapirApi ).fork
        fmng     <- MailNextGroup.cyclingRetrying( ds, as.smtpContext, mbds ).fork
        fch      <- ExpireUnconfirmedSubscriptions.cyclingRetrying( ds ).fork
        fmn      <- MastoNotify.cyclingRetrying( ds, as ).fork
        fwd      <- webDaemon( ds, as, tapirApi ).fork
        rwap     <- Promise.make[Throwable,Unit]
        acimRef  <- Ref.make( false )
        fcf      <- CheckFlags.cyclingRetrying(ds, as, rwap, acimRef).fork
        _        <- rwap.await
        _        <- INFO.zlog( s"Flag ${Flag.MustReloadDaemon} found. Shutting down daemon and restarting." )
        _        <- fuac.interrupt
        _        <- fmng.interrupt
        _        <- fch.interrupt
        _        <- fmn.interrupt
        _        <- fwd.interrupt
        _        <- fcf.interrupt
        _        <- DEBUG.zlog("All daemon fibers interrupted.")
      yield ()
    singleLoad.zlogErrorDefect(WARNING).resurrect.retry( RetrySchedule.mainDaemon ) // if we have database problems, keep trying to reconnect
      .schedule( Schedule.forever ) // a successful completion signals a reload request. so we restart
      .zlogErrorDefect(SEVERE, what="Main daemon loop")
      .unit 
