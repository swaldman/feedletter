package com.mchange.feedletter

import zio.*
import javax.sql.DataSource
import com.mchange.feedletter.db.PgDatabase
import com.mchange.mailutil.*

import MLevel.*
import com.mchange.feedletter.FeedInfo.forNewFeed

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
  def updateAssignComplete( ds : DataSource ) : Task[Unit] =
    PgDatabase.updateAssignItems( ds ) *> PgDatabase.completeAssignables( ds )

  def retryingUpdateAssignComplete( ds : DataSource ) : Task[Unit] =
    updateAssignComplete( ds )
      .tapError( t => WARNING.zlog("updateAssignComplete failed. May retry.", t ) )
      .retry( RetrySchedule.updateAssignComplete )

  def cyclingRetryingUpdateAssignComplete( ds : DataSource ) : Task[Unit] =
    retryingUpdateAssignComplete( ds )
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

  def webDaemon( ds : DataSource, as : AppSetup ) : Task[Unit] =
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
    val httpApp = ZioHttpInterpreter( VerboseServerInterpreterOptions ).toHttp( api.V0.ServerEndpoint.allEndpoints( ds, as ) )
    for
      tup          <- PgDatabase.webDaemonBinding( ds )
      (host, port) =  tup
      _            <- Server.serve(httpApp).provide( ZLayer.succeed( Server.Config.default.binding(host,port) ), Server.live )
    yield ()
