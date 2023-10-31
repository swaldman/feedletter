package com.mchange.feedletter

import zio.*
import zio.cli.{CliApp,Command,Options,ZIOCliDefault}
import zio.cli.HelpDoc.Span.text
import com.mchange.feedletter.db.PgDatabase
import com.mchange.feedletter.Config

import com.mchange.sc.v1.log.*
import MLevel.*
import javax.sql.DataSource

object Main extends ZIOCliDefault:
  private lazy given logger : MLogger = mlogger( this )

  type ZCommand = ZIO[AppSetup & Config & DataSource, Throwable, Any]

  object CommandConfig:
    case object DbDump extends CommandConfig
    case object DbInit extends CommandConfig
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
