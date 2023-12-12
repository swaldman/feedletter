package com.mchange.feedletter

import zio.*
import java.lang.System

import com.monovore.decline.*
import cats.implicits.* // for mapN
import cats.data.{NonEmptyList,Validated,ValidatedNel}

import com.mchange.sc.v1.log.*
import MLevel.*

import java.time.Instant
import javax.sql.DataSource

import com.mchange.v2.c3p0.ComboPooledDataSource

import com.mchange.feedletter.db.DbVersionStatus
import java.nio.file.Path

object Main:
  private lazy given logger : MLogger = mlogger( this )

  val LayerDataSource : ZLayer[AppSetup, Throwable, DataSource] = ZLayer.fromZIO( ZIO.attempt( new ComboPooledDataSource() ) )

  object Admin:
    val addFeed =
      val header = "Add a new feed from which mail or notifications may be generated."
      val opts =
        val minDelayMinutes =
          val help = "Minimum wait (in miunutes) before a newly encountered item can be notified."
          Opts.option[Int]("min-delay-minutes", help=help, metavar="minutes").withDefault(30)
        val awaitStabilizationMinutes =
          val help = "Period (in minutes) over which an item should not have changed before it is considered stable and can be notified."
          Opts.option[Int]("await-stabilization-minutes", help=help, metavar="minutes").withDefault(15)
        val maxDelayMinutes =
          val help = "Notwithstanding other settings, maximum period past which an item should be notified, regardless of its stability."
          Opts.option[Int]("max-delay-minutes", help=help, metavar="minutes").withDefault(180)
        val feedUrl = Opts.argument[String](metavar="feed-url")  
        (minDelayMinutes, awaitStabilizationMinutes, maxDelayMinutes, feedUrl) mapN: (mindm, asm, maxdm, fu) =>
          val fi = FeedInfo.forNewFeed(FeedUrl(fu), mindm, asm, maxdm )
          CommandConfig.Admin.AddFeed( fi )
      Command("add-feed",header=header)( opts )
    val defineEmailSubscription =
      val header = "Define a kind of e-mail subscription."
      val opts =
        val feedUrl =
          val help = "The URL of the RSS feed to be subscribed."
          Opts.option[String]("feed-url", help=help, metavar="url").map( FeedUrl.apply )
        val name =
          val help = "A name for the new kind of email subscription."
          Opts.option[String]("name",help=help,metavar="name").map( SubscribableName.apply )
        val from =
          val help = "The email address from which emails should be sent."
          Opts.option[String]("from",help=help,metavar="e-mail address")
        val replyTo =
          val help = "E-mail address to which recipients should reply (if different from the 'from' address)."
          Opts.option[String]("reply-to",help=help,metavar="e-mail address").orNone
         // modified from decline's docs
        val kind =
          val each = Opts.flag("each",help="E-mail each item").map( _ => "Each" )
          val weekly = Opts.flag("weekly",help="E-mail a compilation, once a week.").map( _ => "Weekly")
          (each orElse weekly).withDefault("Each")
        val extraParams =
          def validate( strings : List[String] ) : ValidatedNel[String,List[Tuple2[String,String]]] =
            strings.map{ s =>
              s.split(":", 2) match
                case Array(key, value) => Validated.valid(Tuple2(key, value))
                case _ => Validated.invalidNel(s"Invalid key:value pair: ${s}")
            }.sequence
          Opts.options[String]("extra-params", "Extra params your subscription type might support, or that renderers might use.", metavar = "key:value")
            .map( _.toList)
            .withDefault(Nil)
            .mapValidated( validate )
            .map( Map.from )
        end extraParams
        ( feedUrl, name, from, replyTo, kind, extraParams ) mapN: ( fu, n, f, rt, k, ep ) =>
          CommandConfig.Admin.DefineEmailSubscription( fu, n, f, rt, k, ep )
      Command("define-email-subscription",header=header)( opts )
    val listConfig =
      val header = "List all configuration parameters."
      val opts = Opts( CommandConfig.Admin.ListConfig )
      Command("list-config",header=header)( opts )
    val listExcludedItems =
      val header = "List items excluded from generating notifications."
      val opts = Opts( CommandConfig.Admin.ListExcludedItems )
      Command("list-excluded-items",header=header)( opts )
    val listFeeds =
      val header = "List all feeds the application is watching."
      val opts = Opts( CommandConfig.Admin.ListFeeds )
      Command("list-feeds",header=header)( opts )
    val setConfig =
      val header = "Set configuration parameters."
      val opts =
        val dumpDbDir =
          val help = "Directory in which to create dump files prior to db migrations."
          Opts.option[Path]("dump-db-dir", help=help, metavar="directory")
            .map( checkExpandTildeHomeDirPath )
            .map( _.toAbsolutePath )
            .map( p => (ConfigKey.DumpDbDir, p.toString()) )
            .orNone
        val mailBatchSize =
          val help = "Number of e-mails to send in each 'batch' (to avoid overwhelming the SMTP server)."
          Opts.option[Int]("mail-batch-size", help=help, metavar="size")
            .map( i => (ConfigKey.MailBatchSize, i.toString()) )
            .orNone
        val mailBatchDelaySecs =
          val help = "Time between batches of e-mails are to be sent."
          Opts.option[Int]("mail-batch-delay-seconds", help=help, metavar="seconds")
            .map( i => (ConfigKey.MailBatchDelaySecs, i.toString()) )
            .orNone
        ( dumpDbDir, mailBatchSize, mailBatchDelaySecs ) mapN: (ddr, mbs, mbds) =>
          val settings = (Vector.empty ++ ddr ++ mbs ++ mbds).toMap
          CommandConfig.Admin.SetConfig( settings )
      Command("set-config", header=header)( opts )

  object Crank:
    val assign =
      val header = "Assign items to the groups (sometimes of just one) in which they will be notified."
      val opts = Opts( CommandConfig.Crank.Assign )
      Command("assign", header=header )( opts )
    val complete =
      val header = "Mark groups of assigned items as complete, and queue them for notification."
      val opts = Opts( CommandConfig.Crank.Complete )
      Command("complete", header=header )( opts )
    val sendMailGroup =
      val header = "Send one batch of queued mail."
      val opts = Opts( CommandConfig.Crank.SendMailGroup )
      Command("send-mail-group", header=header )( opts )

  object Db:
    val init =
      val header = "Initialize the database schema."
      val opts = Opts( CommandConfig.Db.Init )
      Command("init", header=header )( opts )
    val migrate =
      val header = "Initialize the database schema."
      val opts =
        val help = "Force migration even if the application can find no recent database dump."
        Opts.flag("force",help=help,short="f").orFalse.map( force => CommandConfig.Db.Migrate(force) )
      Command("migrate", header=header )( opts )

  val feedletter =
    val admin =
      val header = "Administer and configure an installation."
      val opts =
        import Admin.*
        Opts.subcommands(addFeed, defineEmailSubscription, listConfig, listExcludedItems, setConfig)
      Command( name="admin", header=header )( opts )
    val crank =
      val header = "Run a usually recurring operation a single time."
      val opts =
        import Crank.*
        Opts.subcommands(assign, complete, sendMailGroup)
      Command( name="crank", header=header )( opts )
    val db =
      val header = "Manage the database and database schema."
      val opts =
        import Db.*
        Opts.subcommands( init, migrate )
      Command( name="db", header=header )( opts )

    val subcommands = Opts.subcommands(admin,crank,db)
    Command(name="feedletter", header="Manage e-mail subscriptions to and other notifications from RSS feeds.")( subcommands )

  def main( args : Array[String] ) : Unit =
    feedletter.parse(args.toIndexedSeq, sys.env) match
      case Left(help) =>
        println(help)
        System.exit(1)
      case Right( cc : CommandConfig ) =>
        val task = cc.zcommand.provide( AppSetup.live, LayerDataSource )
        Unsafe.unsafely:
          Runtime.default.unsafe.run(task).getOrThrow()
