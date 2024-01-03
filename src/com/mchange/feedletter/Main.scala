package com.mchange.feedletter

import zio.*
import java.lang.System

import com.monovore.decline.*
import cats.implicits.* // for mapN
import cats.data.{NonEmptyList,Validated,ValidatedNel}

import MLevel.*

import java.nio.file.{Path as JPath}
import java.time.{Instant,ZoneId}
import java.util.{Properties, Map as JMap}
import javax.sql.DataSource

import com.mchange.conveniences.javanio.*

import com.mchange.feedletter.db.DbVersionStatus

object Main extends AbstractMain, SelfLogging:

  object Admin:
    val addFeed =
      val header = "Add a new feed from which mail or notifications may be generated."
      val opts =
        val minDelayMinutes =
          val help = "Minimum wait (in miunutes) before a newly encountered item can be notified."
          Opts.option[Int]("min-delay-minutes", help=help, metavar="minutes").withDefault(Default.Feed.MinDelayMinutes)
        val awaitStabilizationMinutes =
          val help = "Period (in minutes) over which an item should not have changed before it is considered stable and can be notified."
          Opts.option[Int]("await-stabilization-minutes", help=help, metavar="minutes").withDefault(Default.Feed.AwaitStabilizationMinutes)
        val maxDelayMinutes =
          val help = "Notwithstanding other settings, maximum period past which an item should be notified, regardless of its stability."
          Opts.option[Int]("max-delay-minutes", help=help, metavar="minutes").withDefault(Default.Feed.MaxDelayMinutes)
        val recheckEveryMinutes =
          val help = "Delay between refreshes of feeds, and redetermining items' availability for notification."
          Opts.option[Int]("recheck-every-minutes", help=help, metavar="minutes").withDefault(Default.Feed.RecheckEveryMinutes)
        val setTimings =
          (minDelayMinutes, awaitStabilizationMinutes, maxDelayMinutes, recheckEveryMinutes).mapN( (mindm, asm, maxdm, rem) => (mindm, asm, maxdm, rem) )
        val ping =
          val help = "Check feed as often as possible, notify as soon as possible, regardless of (in)stability."
          Opts.flag("ping",help=help).map( _ => (0,0,0,0) )
        val timings = ping orElse setTimings
        val feedUrl = Opts.argument[String](metavar="feed-url")
        (timings, feedUrl) mapN: (t, fu) =>
          val fi = FeedInfo.forNewFeed(FeedUrl(fu), t(0), t(1), t(2), t(3) )
          CommandConfig.Admin.AddFeed( fi )
      Command("add-feed",header=header)( opts )
    val defineEmailSubscription =
      val header = "Define a kind of e-mail subscription."
      val opts =
        val feedId =
          val help = "The ID of the RSS feed to be subscribed."
          Opts.option[Int]("feed-id", help=help, metavar="feed-id").map( FeedId.apply )
        val name =
          val help = "A name for the new kind of email subscription."
          Opts.option[String]("name",help=help,metavar="name").map( SubscribableName.apply )
        val from =
          val help = "The email address from which emails should be sent."
          Opts.option[String]("from",help=help,metavar="e-mail address")
        val replyTo =
          val help = "E-mail address to which recipients should reply (if different from the 'from' address)."
          Opts.option[String]("reply-to",help=help,metavar="e-mail address").orNone
        val composeUntemplateName =
          val help = "Fully qualified name of untemplate that will render notifications."
          Opts.option[String]("compose-untemplate",help=help,metavar="fully-qualified-name").orNone
        val confirmUntemplateName =
          val help = "Fully qualified name of untemplate that will ask for e-mail confirmations."
          Opts.option[String]("confirm-untemplate",help=help,metavar="fully-qualified-name").orNone
        val statusChangedUntemplateName =
          val help = "Fully qualified name of untemplate that will render results of GET request to the API."
          Opts.option[String]("status-changed-untemplate",help=help,metavar="fully-qualified-name").orNone
         // modified from decline's docs
        val kind =
          val each = Opts.flag("each",help="E-mail each item").map( _ => SubscriptionManager.Email.Each )
          val weekly = Opts.flag("weekly",help="E-mail a compilation, once a week.").map( _ => SubscriptionManager.Email.Weekly )
          (each orElse weekly).withDefault[SubscriptionManager.Email.Companion]( SubscriptionManager.Email.Each )
        val extraParams =
          def validate( strings : List[String] ) : ValidatedNel[String,List[Tuple2[String,String]]] =
            strings.map{ s =>
              s.split(":", 2) match
                case Array(key, value) => Validated.valid(Tuple2(key, value))
                case _ => Validated.invalidNel(s"Invalid key:value pair: ${s}")
            }.sequence
          Opts.options[String]("extra-param", "An extra params your notification renderers might use.", metavar = "key:value")
            .map( _.toList)
            .withDefault(Nil)
            .mapValidated( validate )
            .map( Map.from )
        end extraParams
        ( feedId, name, from, replyTo, composeUntemplateName, confirmUntemplateName, statusChangedUntemplateName, kind, extraParams ) mapN: ( fi, n, f, rt, comun, conun, scun, k, ep ) =>
          CommandConfig.Admin.DefineEmailSubscription( fi, n, f, rt, comun, conun, scun, k, ep )
      Command("define-email-subscription",header=header)( opts )
    val editSubscriptionDefinition =
      val header = "Edit a subscription definition."
      val opts =
        val name =
          val help = "Name of an existing email subscription."
          Opts.option[String]("name",help=help,metavar="name").map( SubscribableName.apply )
        name.map( n => CommandConfig.Admin.EditSubscriptionDefinition(n) )
      Command("edit-subscription-definition",header=header)( opts )
    val listComposeUntemplates =
      val header = "List available templates for composing notifications."
      val opts = Opts( CommandConfig.Admin.ListComposeUntemplates )
      Command("list-compose-untemplates",header=header)( opts )
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
    val listSubscriptionDefinitions =
      val header = "List all subscription definitions."
      val opts = Opts( CommandConfig.Admin.ListSubscribables )
      Command("list-subscription-definitions",header=header)( opts )
    val setConfig =
      val header = "Set configuration parameters."
      val opts =
        val dumpDbDir =
          val help = "Directory in which to create dump files prior to db migrations."
          Opts.option[JPath]("dump-db-dir", help=help, metavar="directory")
            .map( _.resolveTildeAsHome )
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
            .map( i => (ConfigKey.MailBatchDelaySeconds, i.toString()) )
            .orNone
        val mailMaxRetries =
          val help = "Number of times e-mail sends (defined as successful submission to an SMTP service) will be attempted before giving up."
          Opts.option[Int]("mail-max-retries", help=help, metavar="times")
            .map( i => (ConfigKey.MailMaxRetries, i.toString()) )
            .orNone
        val timeZone =
          val help = "ID of the time zone which subscriptions based on time periods should use."
          Opts.option[String]("time-zone", help=help, metavar="zone").map( ZoneId.of )
            .map( i => (ConfigKey.TimeZone, i.toString()) )
            .orNone
        ( dumpDbDir, mailBatchSize, mailBatchDelaySecs, mailMaxRetries, timeZone ) mapN: (ddr, mbs, mbds, mmr, tz) =>
          val settings = (Vector.empty ++ ddr ++ mbs ++ mbds ++ mmr ++ tz).toMap
          CommandConfig.Admin.SetConfig( settings )
      Command("set-config", header=header)( opts )
    val sendTestEmail =
      val header = "Send a brief email to test your SMTP configuration."
      val opts =
        val from =
          val help = "The email address from which the test mail should be sent."
          Opts.option[String]("from",help=help,metavar="e-mail address")
        val to =
          val help = "The email address to which the test mail should be sent."
          Opts.option[String]("to",help=help,metavar="e-mail address")
        ( from, to ) mapN ( (f,t) => CommandConfig.Admin.SendTestEmail(f,t) )
      Command("send-test-email", header=header)( opts )
    val subscribe =
      val header = "Subscribe to a defined subscription."
      val opts =
        val subscriptionName =
          val help = "The name of an already-defined subscription."
          Opts.option[String]("subscription-name",help=help,metavar="name").map( SubscribableName.apply )
        val destination = CommonOpts.AnyDestination
        val unconfirmed = Opts.flag("unconfirmed", help="Mark the subscription unconfirmed.").orFalse
        ( subscriptionName, destination, unconfirmed ) mapN: (sn, d, uc) =>
          CommandConfig.Admin.Subscribe( AdminSubscribeOptions(sn, d, !uc, Instant.now) ) // ! our flag is unconfirmed, our field is confirmed
      Command("subscribe", header=header)( opts )

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

  val daemon =
    val header = "Run daemon that watches feeds and sends notifications."
    val opts = Opts( CommandConfig.Daemon )
    Command("daemon", header=header )( opts )

  val feedletter =
    val opts : Opts[(Option[JPath], CommandConfig)] =
      val secrets = CommonOpts.Secrets
      val subcommands =
        val admin =
          val header = "Administer and configure an installation."
          val opts =
            import Admin.*
            Opts.subcommands(addFeed, defineEmailSubscription, editSubscriptionDefinition, listComposeUntemplates, listConfig, listExcludedItems, listFeeds, listSubscriptionDefinitions, sendTestEmail, setConfig, subscribe)
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
        Opts.subcommands(admin,crank,daemon,db)
      ( secrets, subcommands ) mapN( (sec,sub) => (sec,sub) )
    Command(name="feedletter", header="Manage e-mail subscriptions to and other notifications from RSS feeds.")( opts )

  override val baseCommand = feedletter
