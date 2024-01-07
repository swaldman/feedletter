package com.mchange.feedletter

import zio.*
import javax.sql.DataSource
import com.mchange.feedletter.db.PgDatabase
import com.mchange.feedletter.api.ApiLinkGenerator
import com.mchange.mailutil.*

import MLevel.*
import com.mchange.feedletter.db.withConnection
import com.mchange.feedletter.db.withConnectionTransactional
import com.mchange.feedletter.SubscriptionManager.Tag.forJsonVal

object Daemon extends SelfLogging:

  private object RetrySchedule:
    val updateAssignComplete = Schedule.exponential( 10.seconds, 1.25f ) && Schedule.upTo( 5.minutes ) // XXX: hard-coded for now
    val mailNextGroupIfDue = updateAssignComplete // XXX: for now!

  private object CyclingSchedule:
    val updateAssignComplete = Schedule.fixed( 1.minute ).jittered(0.0, 0.5) // XXX: hard-coded for now
    val mailNextGroupIfDue = updateAssignComplete // XXX: for now!
    val checkReloadWebDaemon = Schedule.fixed( 30.seconds )

  // updateAssign and complete are distinct transactions,
  // and everything is idempotent.
  //
  // but completion work logically follows updateAssign, so for now at
  // least we are combining them.
  def updateAssignComplete( ds : DataSource, apiLinkGenerator : ApiLinkGenerator ) : Task[Unit] =
    PgDatabase.updateAssignItems( ds ) *> PgDatabase.completeAssignables( ds, apiLinkGenerator )

  def retryingUpdateAssignComplete( ds : DataSource, apiLinkGenerator : ApiLinkGenerator ) : Task[Unit] =
    updateAssignComplete( ds, apiLinkGenerator )
      .zlogErrorDefect( WARNING, what = "updateAssignComplete" )
      .retry( RetrySchedule.updateAssignComplete )

  def cyclingRetryingUpdateAssignComplete( ds : DataSource, apiLinkGenerator : ApiLinkGenerator ) : Task[Unit] =
    retryingUpdateAssignComplete( ds, apiLinkGenerator )
      .catchAll( t => WARNING.zlog( "Retry cycle for updateAssignComplete failed...", t ) )
      .schedule( CyclingSchedule.updateAssignComplete )
      .map( _ => () )
      .onInterrupt( DEBUG.zlog( "cyclingRetryingUpdateAsignComplete fiber interrupted." ) )

  def retryingMailNextGroupIfDue( ds : DataSource, smtpContext : Smtp.Context ) : Task[Unit] =
    PgDatabase.mailNextGroupIfDue( ds, smtpContext )
      .zlogErrorDefect( WARNING, what = "mailNextGroupIfDue" )
      .retry( RetrySchedule.mailNextGroupIfDue )

  def cyclingRetryingMailNextGroupIfDue( ds : DataSource, smtpContext : Smtp.Context ) : Task[Unit] =
    retryingMailNextGroupIfDue( ds, smtpContext )
      .catchAll( t => WARNING.zlog( "Retry cycle for mailNextGroupIfDue failed...", t ) )
      .schedule( CyclingSchedule.mailNextGroupIfDue )
      .map( _ => () )
      .onInterrupt( DEBUG.zlog( "cyclingRetryingMailNextGroupIfDue fiber interrupted." ) )

  def tapirApi( ds : DataSource, as : AppSetup ) : Task[api.V0.TapirApi] =
    for
      tup0         <- PgDatabase.webApiUrlBasePath( ds )
      (server, bp) =  tup0
    yield
      api.V0.TapirApi( server, bp, as.secretSalt )

  def webDaemon( ds : DataSource, as : AppSetup, tapirApi : api.V0.TapirApi ) : Task[Unit] =
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

