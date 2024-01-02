package com.mchange.feedletter

import zio.*
import javax.sql.DataSource
import com.mchange.feedletter.db.PgDatabase
import com.mchange.feedletter.api.ApiLinkGenerator
import com.mchange.mailutil.*

import MLevel.*

object Daemon extends SelfLogging:

  private object RetrySchedule:
    val updateAssignComplete = Schedule.exponential( 10.seconds, 1.25f ) && Schedule.upTo( 5.minutes ) // XXX: hard-coded for now
    val mailNextGroupIfDue = updateAssignComplete // XXX: for now!

  private object CyclingSchedule:
    val updateAssignComplete = Schedule.fixed( 1.minute ).jittered(0.0, 0.5) // XXX: hard-coded for now
    val mailNextGroupIfDue = updateAssignComplete // XXX: for now!

  // updateAssign and complete are distinct transactions,
  // and everything is idempotent.
  //
  // but completion work logically follows updateAssign, so for now at
  // least we are combining them.
  def updateAssignComplete( ds : DataSource, apiLinkGenerator : ApiLinkGenerator ) : Task[Unit] =
    PgDatabase.updateAssignItems( ds ) *> PgDatabase.completeAssignables( ds, apiLinkGenerator )

  def retryingUpdateAssignComplete( ds : DataSource, apiLinkGenerator : ApiLinkGenerator ) : Task[Unit] =
    updateAssignComplete( ds, apiLinkGenerator )
      .tapError( t => WARNING.zlog("updateAssignComplete failed. May retry.", t ) )
      .retry( RetrySchedule.updateAssignComplete )

  def cyclingRetryingUpdateAssignComplete( ds : DataSource, apiLinkGenerator : ApiLinkGenerator ) : Task[Unit] =
    retryingUpdateAssignComplete( ds, apiLinkGenerator )
      .catchAll( t => WARNING.zlog( "Retry cycle for updateAssignComplete failed...", t ) )
      .schedule( CyclingSchedule.updateAssignComplete ) *> ZIO.unit

  def retryingMailNextGroupIfDue( ds : DataSource, smtpContext : Smtp.Context ) : Task[Unit] =
    PgDatabase.mailNextGroupIfDue( ds, smtpContext )
      .tapError( t => WARNING.zlog("mailNextGroupIfDue failed. May retry.", t ) )
      .retry( RetrySchedule.mailNextGroupIfDue )

  def cyclingRetryingMailNextGroupIfDue( ds : DataSource, smtpContext : Smtp.Context ) : Task[Unit] =
    retryingMailNextGroupIfDue( ds, smtpContext )
      .catchAll( t => WARNING.zlog( "Retry cycle for mailNextGroupIfDue failed...", t ) )
      .schedule( CyclingSchedule.mailNextGroupIfDue ) *> ZIO.unit

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

    val VerboseServerInterpreterOptions : ZioHttpServerOptions[Any] =
      // modified from https://github.com/longliveenduro/zio-geolocation-tapir-tapir-starter/blob/b79c88b9b1c44a60d7c547d04ca22f12f420d21d/src/main/scala/com/tsystems/toil/Main.scala
      ZioHttpServerOptions
        .customiseInterceptors
        .serverLog(
          DefaultServerLog[Task](
            doLogWhenReceived = msg => ZIO.succeed(println(msg)),
            doLogWhenHandled = (msg, error) => ZIO.succeed(error.fold(println(msg))(err => println(s"msg: ${msg}, err: ${err}"))),
            doLogAllDecodeFailures = (msg, error) => ZIO.succeed(error.fold(println(msg))(err => println(s"msg: ${msg}, err: ${err}"))),
            doLogExceptions = (msg: String, exc: Throwable) => ZIO.succeed(println(s"msg: ${msg}, exc: ${exc}")),
            noLog = ZIO.unit
          )
        )
        .options
    ZIO.logLevel( LogLevel.Trace ): 
      for
        tup          <- PgDatabase.webDaemonBinding( ds )
        (host, port) =  tup
        httpApp      = ZioHttpInterpreter( /* VerboseServerInterpreterOptions */ ).toHttp( tapirApi.ServerEndpoint.allEndpoints( ds, as ) )
        _            <- Server.serve(httpApp).provide( ZLayer.succeed( Server.Config.default.binding(host,port) ), Server.live )
      yield ()
