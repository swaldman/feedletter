package com.mchange.feedletter

import zio.*
import zio.cli.{CliApp,Command,Options,ZIOCliDefault}
import zio.cli.HelpDoc.Span.text

object Main extends ZIOCliDefault:

  object CommandConfig:
    case object DbDump extends CommandConfig
    case object DbInit extends CommandConfig
    case class DbMigrate( force : Boolean ) extends CommandConfig
  sealed trait CommandConfig

  val config = com.mchange.feedletter.UserConfig

  val forceOption = Options.boolean("force").alias("f")

  val dbDumpCommand = Command("dump").map( _ => CommandConfig.DbDump )
  val dbInitCommand = Command("init").map( _ => CommandConfig.DbInit )
  val dbMigrateCommand = Command("migrate", forceOption).map( force => CommandConfig.DbMigrate(force) )

  val dbCommand = Command("db").subcommands(dbInitCommand, dbMigrateCommand)
  
  val mainCommand = Command("feedletter").subcommands(dbCommand)

  val cliApp = CliApp.make(
    name = "feedletter",
    version = com.mchange.feedletter.BuildInfo.version,
    summary = text("Manage e-mail subscriptions to RSS feeds."),
    command = mainCommand
  )(
    thing => Console.printLine(s"Hello: ${thing}")
  )
