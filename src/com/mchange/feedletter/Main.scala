package com.mchange.feedletter

import zio.*
import zio.cli.{CliApp,Command,Options,Args,ZIOCliDefault}
import zio.cli.HelpDoc.Span.text
import com.mchange.feedletter.db.PgDatabase

import com.mchange.sc.v1.log.*
import MLevel.*
import javax.sql.DataSource
import com.mchange.feedletter.db.DbVersionStatus

object Main extends ZIOCliDefault:
  private lazy given logger : MLogger = mlogger( this )

  val LayerDataSource : ZLayer[AppSetup, Throwable, DataSource] = ZLayer.fromZIO( ZIO.attempt( config.dataSource ) )

  val forceOption = Options.boolean("force").alias("f")

  val dbDumpCommand = Command("dump").map( _ => CommandConfig.DbDump )
  val dbInitCommand = Command("init").map( _ => CommandConfig.DbInit )
  val dbMigrateCommand = Command("migrate", forceOption).map( force => CommandConfig.DbMigrate(force) )

  val crankAssign = Command("assign").map( _ => CommandConfig.CrankAssign )
  val crankComplete = Command("complete").map( _ => CommandConfig.CrankComplete )

  val dbCommand = Command("db").subcommands(dbInitCommand, dbMigrateCommand, dbDumpCommand)
  val crankCommand = Command("crank").subcommands(crankAssign, crankComplete)

  val configSetOptions =
    val dumpDbDirOption = Options.directory("dump-db-dir").map( checkExpandTildeHomeDirPath ).map( _.toAbsolutePath ).optional.map( opt => opt.map( v => Tuple2(ConfigKey.DumpDbDir, v.toString ) ) )
    val mailBatchSizeOption =  Options.integer("mail-batch-size").optional.map( opt => opt.map( v => Tuple2(ConfigKey.MailBatchSize, v.toString ) ) )
    val mailBatchDelaySecs = Options.integer("mail-batch-delay-secs").optional.map( opt => opt.map( v => Tuple2(ConfigKey.MailBatchDelaySecs, v.toString ) ) )
    (dumpDbDirOption ++ mailBatchSizeOption ++ mailBatchDelaySecs).map( _.toList.collect { case Some(tup) => tup }.toMap )
  val configSetCommand = Command("set", configSetOptions).map( settings => CommandConfig.ConfigSet(settings) )

  val configListCommand = Command("list").map( _ => CommandConfig.ConfigList )

  val configAddFeedOptions =
    val minDelaySecsOption = Options.integer("min-delay-secs").map( _.toInt).withDefault(1800)
    val awaitStabilizationSecsOption = Options.integer("await-stabilization-secs").map( _.toInt ).withDefault(900)
    val pausedOption = Options.boolean("paused")
    (minDelaySecsOption ++ awaitStabilizationSecsOption ++ pausedOption)

  val configAddFeedArgs = Args.text("feed-url")

  val configAddFeedCommand = Command("add-feed", configAddFeedOptions, configAddFeedArgs).map { case ((minDelaySecs, awaitStabilizationSecs, paused), feedUrl) =>
    val fi = FeedInfo(feedUrl, minDelaySecs, awaitStabilizationSecs, paused )
    CommandConfig.ConfigAddFeed( fi )
  }

  val configListFeedsCommand = Command("list-feeds").map( _ => CommandConfig.ConfigListFeeds )

  val configCommand = Command("config").subcommands( configListCommand, configListFeedsCommand, configAddFeedCommand, configSetCommand )

  val sendmailCommand = Command("sendmail").map( _ => CommandConfig.Sendmail )

  val daemonCommand = Command("daemon").map( _ => CommandConfig.Daemon )

  val mainCommand = Command("feedletter").subcommands( dbCommand, configCommand, crankCommand, sendmailCommand, daemonCommand )

  val cliApp = CliApp.make(
    name = "feedletter",
    version = com.mchange.feedletter.BuildInfo.version,
    summary = text("Manage e-mail subscriptions to RSS feeds."),
    command = mainCommand
  ){
    case cc : CommandConfig =>
      cc.zcommand.provide( AppSetup.live, LayerDataSource )
  }
