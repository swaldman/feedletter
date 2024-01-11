package com.mchange.feedletter

import zio.*
import javax.sql.DataSource
import com.mchange.feedletter.db.PgDatabase
import com.mchange.feedletter.api.ApiLinkGenerator
import com.mchange.mailutil.*

import MLevel.*
import com.mchange.feedletter.db.withConnectionTransactional

object Daemon extends SelfLogging:

  private object RetrySchedule:
    val updateAssignComplete = Schedule.exponential( 10.seconds, 1.25f ) && Schedule.upTo( 5.minutes ) // XXX: hard-coded for now
    val mailNextGroup = updateAssignComplete // XXX: for now!
    val expireUnconfirmedSubscriptions = updateAssignComplete
    val mainDaemon = Schedule.exponential( 10.seconds, 1.25f ) || Schedule.fixed( 1.minute ) // XXX: hard-coded for now, retries forever

  private object CyclingSchedule:
    val updateAssignComplete = Schedule.spaced( 1.minute ).jittered(0.0, 0.5) // XXX: hard-coded for now
    def mailNextGroup( mailBatchDelaySeconds : Int ) =
      val initDelay = scala.util.Random.nextInt(mailBatchDelaySeconds).seconds
      Schedule.delayed( Schedule.once.map( _ => initDelay ) ) andThen Schedule.spaced( mailBatchDelaySeconds.seconds )
    val checkReloadWebDaemon = Schedule.spaced( 30.seconds )
    val expireUnconfirmedSubscriptions = Schedule.fixed( 1.hours )

  // updateAssign and complete are distinct transactions,
  // and everything is idempotent.
  //
  // but completion work logically follows updateAssign, so for now at
  // least we are combining them.
  private def updateAssignComplete( ds : DataSource, apiLinkGenerator : ApiLinkGenerator ) : Task[Unit] =
    PgDatabase.updateAssignItems( ds ) *> PgDatabase.completeAssignables( ds, apiLinkGenerator )

  private def retryingUpdateAssignComplete( ds : DataSource, apiLinkGenerator : ApiLinkGenerator ) : Task[Unit] =
    updateAssignComplete( ds, apiLinkGenerator )
      .zlogErrorDefect( WARNING, what = "updateAssignComplete" )
      .retry( RetrySchedule.updateAssignComplete )

  private def cyclingRetryingUpdateAssignComplete( ds : DataSource, apiLinkGenerator : ApiLinkGenerator ) : Task[Unit] =
    retryingUpdateAssignComplete( ds, apiLinkGenerator )
      .catchAll( t => WARNING.zlog( "Retry cycle for updateAssignComplete failed...", t ) )
      .schedule( CyclingSchedule.updateAssignComplete )
      .unit
      .onInterrupt( DEBUG.zlog( "cyclingRetryingUpdateAsignComplete fiber interrupted." ) )

  private def retryingMailNextGroup( ds : DataSource, smtpContext : Smtp.Context ) : Task[Unit] =
    PgDatabase.mailNextGroup( ds, smtpContext )
      .zlogErrorDefect( WARNING, what = "mailNextGroup" )
      .retry( RetrySchedule.mailNextGroup )

  private def cyclingRetryingMailNextGroup( ds : DataSource, smtpContext : Smtp.Context, mailBatchDelaySeconds : Int ) : Task[Unit] =
    retryingMailNextGroup( ds, smtpContext )
      .catchAll( t => WARNING.zlog( "Retry cycle for mailNextGroupIfDue failed...", t ) )
      .schedule( CyclingSchedule.mailNextGroup( mailBatchDelaySeconds ) )
      .unit
      .onInterrupt( DEBUG.zlog( "cyclingRetryingMailNextGroup fiber interrupted." ) )

  private def retryingExpireUnconfirmedSubscriptions( ds : DataSource ) : Task[Unit] =
    PgDatabase.expireUnconfirmed( ds )
      .zlogErrorDefect( WARNING, what = "expireUnconfirmedSubscriptions" )
      .retry( RetrySchedule.expireUnconfirmedSubscriptions )

  private def cyclingRetryingExpireUnconfirmedSubscriptions( ds : DataSource ) : Task[Unit] =
    retryingExpireUnconfirmedSubscriptions( ds )
      .catchAll( t => WARNING.zlog( "Retry cycle for expireUnconfirmedSubscriptions failed...", t ) )
      .schedule( CyclingSchedule.expireUnconfirmedSubscriptions )
      .unit
      .onInterrupt( DEBUG.zlog( "expireUnconfirmedSubscriptions fiber interrupted." ) )

  def tapirApi( ds : DataSource, as : AppSetup ) : Task[api.V0.TapirApi] =
    for
      tup0         <- PgDatabase.webApiUrlBasePath( ds )
      (server, bp) =  tup0
    yield
      api.V0.TapirApi( server, bp, as.secretSalt )

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

  val ReloadCheckPeriod = 30.seconds // XXX: hard-coded for now

  def startup( ds : DataSource, as : AppSetup ) : Task[Unit] =
    def mustReloadCheck(ds : DataSource) =
      for
        _          <- ZIO.sleep(ReloadCheckPeriod)
        mustReload <- PgDatabase.checkMustReloadDaemon( ds )
      yield
        mustReload
    val singleLoad =
      for
        _        <- PgDatabase.clearMustReloadDaemon(ds)
        mbds     <- withConnectionTransactional( ds )( conn => PgDatabase.Config.mailBatchDelaySeconds(conn) )
        tapirApi <- tapirApi(ds,as)
        _        <- INFO.zlog( s"Spawning daemon fibers." )
        fuac     <- com.mchange.feedletter.Daemon.cyclingRetryingUpdateAssignComplete( ds, tapirApi ).fork
        fmng     <- com.mchange.feedletter.Daemon.cyclingRetryingMailNextGroup( ds, as.smtpContext, mbds ).fork
        fch      <- com.mchange.feedletter.Daemon.cyclingRetryingExpireUnconfirmedSubscriptions( ds ).fork
        fwd      <- com.mchange.feedletter.Daemon.webDaemon( ds, as, tapirApi ).fork
        _        <- ZIO.unit.schedule( Schedule.recurUntilZIO( _ =>  mustReloadCheck(ds).orDie ) ) // NOTE: should an error or defect occur, the fibers created are automatically interrupted
        _        <- INFO.zlog( s"Flag ${Flag.MustReloadDaemon} found. Shutting down daemon and restarting." )
        _        <- fuac.interrupt
        _        <- fmng.interrupt
        _        <- fch.interrupt
        _        <- fwd.interrupt
        _        <- DEBUG.zlog("All daemon fibers interrupted.")
      yield ()
    singleLoad.resurrect.retry( RetrySchedule.mainDaemon ) // if we have database problems, keep trying to reconnect
      .schedule( Schedule.forever ) // a successful completion signals a reload request. so we restart
      .unit 
