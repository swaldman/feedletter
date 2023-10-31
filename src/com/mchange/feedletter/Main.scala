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

  val LayerConfig : ZLayer[AppSetup, Throwable, Config] = ZLayer.fromZIO( ZIO.attempt( config.config ) )

  val LayerDataSource : ZLayer[AppSetup, Throwable, DataSource] = ZLayer.fromZIO( ZIO.attempt( config.dataSource ) )

  val forceOption = Options.boolean("force").alias("f")

  val dbDumpCommand = Command("dump").map( _ => CommandConfig.DbDump )
  val dbInitCommand = Command("init").map( _ => CommandConfig.DbInit )
  val dbMigrateCommand = Command("migrate", forceOption).map( force => CommandConfig.DbMigrate(force) )

  val dbCommand = Command("db").subcommands(dbInitCommand, dbMigrateCommand, dbDumpCommand)

  val updateCommand = Command("update").map( _ => CommandConfig.Update )

  val sendmailCommand = Command("sendmail").map( _ => CommandConfig.Sendmail )

  val daemonCommand = Command("daemon").map( _ => CommandConfig.Daemon )
  
  val mainCommand = Command("feedletter").subcommands( daemonCommand, dbCommand, sendmailCommand, updateCommand )

  val cliApp = CliApp.make(
    name = "feedletter",
    version = com.mchange.feedletter.BuildInfo.version,
    summary = text("Manage e-mail subscriptions to RSS feeds."),
    command = mainCommand
  ){
    case cc : CommandConfig =>
      cc.zcommand.provide( AppSetup.live, LayerDataSource, LayerConfig )
  }
