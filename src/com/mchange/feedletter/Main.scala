package com.mchange.feedletter

import zio.*
import zio.cli.{CliApp,Command,Options,ZIOCliDefault}
import zio.cli.HelpDoc.Span.text
import com.mchange.feedletter.db.PgDatabase
import com.mchange.feedletter.Config

import com.mchange.sc.v1.log.*
import MLevel.*
import javax.sql.DataSource
import com.mchange.feedletter.db.DbVersionStatus

object Main extends ZIOCliDefault:
  private lazy given logger : MLogger = mlogger( this )

  type ZCommand = ZIO[AppSetup & Config & DataSource, Throwable, Any]

  object CommandConfig:
    case object DbDump extends CommandConfig:
      override def zcommand : ZCommand =
        for
          config <- ZIO.service[Config]
          ds     <- ZIO.service[DataSource]
          out    <- PgDatabase.dump(config, ds)
        yield
          INFO.log(s"The database was successfully dumped to '${out}'.")
      end zcommand  
    case object DbInit extends CommandConfig:
      override def zcommand : ZCommand =
        def doInit( config : Config, ds : DataSource, status : DbVersionStatus ) : Task[Unit] =
          if status == DbVersionStatus.SchemaMetadataNotFound then PgDatabase.migrate(config, ds)
          else
            status match
              case DbVersionStatus.Current(_) => ZIO.succeed( INFO.log("The database is already initialized and up-to-date." ) )
              case DbVersionStatus.OutOfDate( _, _) => ZIO.succeed( INFO.log("The database is already initialized, but out-of-date. Please migrate.") )
              case other => throw new FeedletterException(s"""${other}: ${other.errMessage.getOrElse("<no message available>")}""")
        for
          config <- ZIO.service[Config]
          ds     <- ZIO.service[DataSource]
          status <- PgDatabase.dbVersionStatus(config, ds)
          _      <- doInit( config, ds, status )
        yield ()
      end zcommand  
    case class DbMigrate( force : Boolean ) extends CommandConfig:
      override def zcommand: ZCommand =
        def doMigrate( config : Config, ds : DataSource ) = if force then PgDatabase.migrate(config, ds) else PgDatabase.cautiousMigrate(config, ds)
        for
          config <- ZIO.service[Config]
          ds <- ZIO.service[DataSource]
          _ <- doMigrate( config, ds )
        yield ()
      end zcommand  
  sealed trait CommandConfig:
    def zcommand : ZCommand = ZIO.unit

  val LayerConfig : ZLayer[AppSetup, Throwable, Config] = ZLayer.fromZIO( ZIO.attempt( config.config ) )

  val LayerDataSource : ZLayer[AppSetup, Throwable, DataSource] = ZLayer.fromZIO( ZIO.attempt( config.dataSource ) )

  val forceOption = Options.boolean("force").alias("f")

  val dbDumpCommand = Command("dump").map( _ => CommandConfig.DbDump )
  val dbInitCommand = Command("init").map( _ => CommandConfig.DbInit )
  val dbMigrateCommand = Command("migrate", forceOption).map( force => CommandConfig.DbMigrate(force) )

  val dbCommand = Command("db").subcommands(dbInitCommand, dbMigrateCommand, dbDumpCommand)
  
  val mainCommand = Command("feedletter").subcommands(dbCommand)

  val cliApp = CliApp.make(
    name = "feedletter",
    version = com.mchange.feedletter.BuildInfo.version,
    summary = text("Manage e-mail subscriptions to RSS feeds."),
    command = mainCommand
  ){
    case cc : CommandConfig =>
      cc.zcommand.provide( AppSetup.live, LayerDataSource, LayerConfig )
  }
