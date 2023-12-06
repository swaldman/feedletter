package com.mchange.feedletter

import zio.*
import zio.cli.{CliApp,Command,Options,Args,ZIOCliDefault}
import zio.cli.HelpDoc.Span.text

import com.mchange.sc.v1.log.*
import MLevel.*

import java.time.Instant
import javax.sql.DataSource

import com.mchange.feedletter.db.DbVersionStatus

object Main extends ZIOCliDefault:
  private lazy given logger : MLogger = mlogger( this )

  val LayerDataSource : ZLayer[AppSetup, Throwable, DataSource] = ZLayer.fromZIO( ZIO.attempt( config.dataSource ) )

  val admin =
    val addFeed =
      val options =
        val minDelayMinutes = Options.integer("min-delay-mins").map( _.toInt).withDefault(30)
        val awaitStabilizationMinutes = Options.integer("await-stabilization-minutes").map( _.toInt ).withDefault(15)
        val maxDelayMinutes = Options.integer("max-delay-minutes").map( _.toInt).withDefault(180)
        val paused = Options.boolean("paused")
        (minDelayMinutes ++ awaitStabilizationMinutes ++ maxDelayMinutes ++ paused)
      val args = Args.text("feed-url")
      Command("add-feed", options, args).map { case ((minDelayMinutes, awaitStabilizationMinutes, maxDelayMinutes, paused), feedUrl) =>
        val fi = FeedInfo(feedUrl, minDelayMinutes, awaitStabilizationMinutes, maxDelayMinutes, paused, Instant.now )
        CommandConfig.Admin.AddFeed( fi )
      }
    val createSubscriptionType =
      val email =
        val options =
          val from = Options.text("from")
          val replyTo = Options.text("reply-to").optional
          val tpe =
            Options.enumeration("type")(
              "immediate" -> "Immediate",
              "weekly" -> "Weekly"
            ).withDefault("Immediate")
          val name = Options.text("name").withDefault("default")
          val extra = Options.keyValueMap("extra-params").withDefault(Map.empty)
          from ++ replyTo ++ tpe ++ name ++ extra
        Command("email", options).map( CommandConfig.Admin.CreateSubscriptionTypeEmail.apply )
      Command("create-subscription-type").subcommands(email)
    val listConfig = Command("list-config").map( _ => CommandConfig.Admin.ListConfig )
    val listExcluded = Command("list-excluded-items").map( _ => CommandConfig.Admin.ListExcludedItems )
    val listFeeds = Command("list-feeds").map( _ => CommandConfig.Admin.ListFeeds )
    val setConfig =
      val options =
        val dumpDbDir = Options.directory("dump-db-dir").map( checkExpandTildeHomeDirPath ).map( _.toAbsolutePath ).optional.map( opt => opt.map( v => Tuple2(ConfigKey.DumpDbDir, v.toString ) ) )
        val mailBatchSize =  Options.integer("mail-batch-size").optional.map( opt => opt.map( v => Tuple2(ConfigKey.MailBatchSize, v.toString ) ) )
        val mailBatchDelaySecs = Options.integer("mail-batch-delay-secs").optional.map( opt => opt.map( v => Tuple2(ConfigKey.MailBatchDelaySecs, v.toString ) ) )
        (dumpDbDir ++ mailBatchSize ++ mailBatchDelaySecs).map( _.toList.collect { case Some(tup) => tup }.toMap )
      Command("set-config", options).map( settings => CommandConfig.Admin.SetConfig(settings) )
    Command("admin").subcommands( addFeed, createSubscriptionType, listConfig, listExcluded, listFeeds, setConfig )

  val crank =
    val assign = Command("assign").map( _ => CommandConfig.Crank.Assign )
    val complete = Command("complete").map( _ => CommandConfig.Crank.Complete )
    Command("crank").subcommands(assign, complete)

  val daemon = Command("daemon").map( _ => CommandConfig.Daemon )

  val db =
    val dump = Command("dump").map( _ => CommandConfig.Db.Dump )
    val init = Command("init").map( _ => CommandConfig.Db.Init )
    val migrate =
      val forceOption = Options.boolean("force").alias("f")
      Command("migrate", forceOption).map( force => CommandConfig.Db.Migrate(force) )
    Command("db").subcommands(dump, init, migrate)

  val sendmail = Command("sendmail").map( _ => CommandConfig.Sendmail )

  val cliApp = CliApp.make(
    name = "feedletter",
    version = com.mchange.feedletter.BuildInfo.version,
    summary = text("Manage e-mail subscriptions to RSS feeds."),
    command = Command("feedletter").subcommands( admin, crank, daemon, db, sendmail )
  ){
    case cc : CommandConfig =>
      cc.zcommand.provide( AppSetup.live, LayerDataSource )
  }

