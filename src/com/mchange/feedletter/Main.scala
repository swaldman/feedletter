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

  object SupportedProtocol:
    import cats.Applicative
    import cats.data.ValidatedNel
    import cats.syntax.validated._

    given Argument[SupportedProtocol] with
      def read(s: String): ValidatedNel[String, SupportedProtocol] =
        try
          Validated.valid( SupportedProtocol.valueOf(s) )
        catch
          case t : IllegalArgumentException => s"Unsupported Web API protocol: $s".invalidNel[SupportedProtocol]
      def defaultMetavar: String = SupportedProtocol.http.toString

  enum SupportedProtocol:
    case http, https

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
        val nf = NascentFeed(FeedUrl(fu), t(0), t(1), t(2), t(3) )
        CommandConfig.AddFeed( nf )
    Command("add-feed",header=header)( opts )
  val daemon =
    val header = "Run daemon that watches feeds and sends notifications."
    val opts = Opts( CommandConfig.Daemon )
    Command("daemon", header=header )( opts )
  val dbDump =
    val header = "Dump a backup of the database into a configured directory."
    val opts = Opts( CommandConfig.DbDump )
    Command("db-dump", header=header )( opts )
  val dbInit =
    val header = "Initialize the database schema."
    val opts = Opts( CommandConfig.DbInit )
    Command("db-init", header=header )( opts )
  val dbMigrate =
    val header = "Migrate to the latest version of the database schema."
    val opts =
      val help = "Force migration even if the application can find no recent database dump."
      Opts.flag("force",help=help,short="f").orFalse.map( force => CommandConfig.DbMigrate(force) )
    Command("db-migrate", header=header )( opts )
  val defineEmailSubscribable =
    val header = "Define a new email subscribable, a mailing lost to which users can subscribe."
    val opts =
      val feedId =
        val help = "The ID of the RSS feed to be watched."
        Opts.option[Int]("feed-id", help=help, metavar="feed-id").map( FeedId.apply )
      val name =
        val help = "A name for the new subscribable."
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
      val removalNotificationUntemplateName =
        val help = "Fully qualified name of untemplate that be mailed to users upon unsubscription."
        Opts.option[String]("removal-notification-untemplate",help=help,metavar="fully-qualified-name").orNone
      val statusChangeUntemplateName =
        val help = "Fully qualified name of untemplate that will render results of GET request to the API."
        Opts.option[String]("status-change-untemplate",help=help,metavar="fully-qualified-name").orNone
       // modified from decline's docs
      val kind = // we sometimes see type inference errors without the explicit type attributions, i'm not sure why
        type K = Tuple2[SubscriptionManager.Email.Companion,Option[ZoneId | Int]]
        val each = Opts.flag("each",help="E-mail each item.").map( _ => (SubscriptionManager.Email.Each, None) : K ) 
        val daily =
          val flag = Opts.flag("daily",help="E-mail a compilation, once a day.").map( _ => SubscriptionManager.Email.Daily )
          val zone = CommonOpts.TimeZone.orNone
          ( flag, zone ) mapN( (f, z) => ( f, z ) )
        val weekly =
          val flag = Opts.flag("weekly",help="E-mail a compilation, once a week.").map( _ => SubscriptionManager.Email.Weekly )
          val zone = CommonOpts.TimeZone.orNone
          ( flag, zone ) mapN( (f, z) => ( f, z ) )
        val fixed = Opts.option[Int]("num-items-per-letter",help="E-mail every fixed number of posts.",metavar="num").map( n => (SubscriptionManager.Email.Fixed, Some(n)) : K )
        (each orElse daily orElse weekly orElse fixed).withDefault[K]( (SubscriptionManager.Email.Each, None) : K )
      val extraParams = CommonOpts.ExtraParams
      ( feedId, name, from, replyTo, composeUntemplateName, confirmUntemplateName, removalNotificationUntemplateName, statusChangeUntemplateName, kind, extraParams ) mapN: ( fi, n, f, rt, comun, conun, rnun, scun, k, ep ) =>
        CommandConfig.DefineEmailSubscribable( fi, n, f, rt, comun, conun, rnun, scun, k, ep )
    Command("define-email-subscribable",header=header)( opts )
  val defineMastodonSubscribable =
    val header = "Define a Mastodon subscribable, a source from which Mastodon feeds can receive automatic posts.."
    val opts =
      val feedId =
        val help = "The ID of the RSS feed to be watched."
        Opts.option[Int]("feed-id", help=help, metavar="feed-id").map( FeedId.apply )
      val name =
        val help = "A name for the new subscribable."
        Opts.option[String]("name",help=help,metavar="name").map( SubscribableName.apply )
      val extraParams = CommonOpts.ExtraParams
      ( feedId, name, extraParams ) mapN: ( fi, n, ep ) =>
        CommandConfig.DefineMastodonSubscribable( fi, n, ep )
    Command("define-mastodon-subscribable",header=header)( opts )
  val editSubscribable =
    val header = "Edit an already-defined subscribable."
    val opts =
      val name =
        val help = "Name of an existing subscribable."
        Opts.option[String]("name",help=help,metavar="name").map( SubscribableName.apply )
      name.map( n => CommandConfig.EditSubscribable(n) )
    Command("edit-subscribable",header=header)( opts )
  val listComposeUntemplates =
    val header = "List available templates for composing notifications."
    val opts = Opts( CommandConfig.ListComposeUntemplates )
    Command("list-compose-untemplates",header=header)( opts )
  val listConfig =
    val header = "List all configuration parameters."
    val opts = Opts( CommandConfig.ListConfig )
    Command("list-config",header=header)( opts )
  val listExcludedItems =
    val header = "List items excluded from generating notifications."
    val opts = Opts( CommandConfig.ListExcludedItems )
    Command("list-excluded-items",header=header)( opts )
  val listFeeds =
    val header = "List all feeds the application is watching."
    val opts = Opts( CommandConfig.ListFeeds )
    Command("list-feeds",header=header)( opts )
  val listSubscribables =
    val header = "List all subscribables."
    val opts = Opts( CommandConfig.ListSubscribables )
    Command("list-subscribables",header=header)( opts )
  val setConfig =
    val header = "Set configuration parameters."
    def simpleConfigOpt[T]( key : ConfigKey )( name : String, help : String, metavar : String )(using Argument[T]) : Opts[Option[(ConfigKey,String)]] =
      Opts.option[T](name, help=help, metavar=metavar)
        .map( v => (key, v.toString()) )
        .orNone
    val opts =
      val dumpDbDir =
        val help = "Directory in which to create dump files prior to db migrations."
        Opts.option[JPath]("dump-db-dir", help=help, metavar="directory")
          .map( _.resolveTildeAsHome )
          .map( _.toAbsolutePath )
          .map( p => (ConfigKey.DumpDbDir, p.toString()) )
          .orNone
      val mailBatchSize = simpleConfigOpt[Int]( ConfigKey.MailBatchSize )(
        name    = "mail-batch-size",
        help    = "Number of e-mails to send in each 'batch' (to avoid overwhelming the SMTP server).",
        metavar = "size"
      )
      val mailBatchDelaySecs = simpleConfigOpt[Int]( ConfigKey.MailBatchDelaySeconds )(
        name    = "mail-batch-delay-seconds",
        help    = "Time between batches of e-mails are to be sent.",
        metavar = "seconds"
      )
      val mailMaxRetries = simpleConfigOpt[Int]( ConfigKey.MailMaxRetries )(
        name    = "mail-max-retries",
        help    = "Number of times e-mail sends (defined as successful submission to an SMTP service) will be attempted before giving up.",
        metavar = "times"
      )
      val timeZone = simpleConfigOpt[String]( ConfigKey.TimeZone )(
        name    = "time-zone",
        help    = "ID of the time zone which subscriptions based on time periods should use.",
        metavar = "zone"
      )
      val webDaemonInterface = simpleConfigOpt[String]( ConfigKey.WebDaemonInterface )(
        name    = "web-daemon-interface",
        help    = "The local interface to which the web-api daemon should bind.",
        metavar = "interface"
      )
      val webDaemonPort = simpleConfigOpt[Int]( ConfigKey.WebDaemonPort )(
        name    = "web-daemon-port",
        help    = "The local port to which the web-api daemon should bind.",
        metavar = "port"
      )
      val webApiProtocol = simpleConfigOpt[SupportedProtocol]( ConfigKey.WebApiProtocol )(
        name    = "web-api-protocol",
        help    = "The protocol (http or https) by which the web api is served.",
        metavar = "http|https"
      )
      val webApiHostName = simpleConfigOpt[String]( ConfigKey.WebApiHostName )(
        name    = "web-api-host-name",
        help    = "The host from which the web api is served.",
        metavar = "hostname"
      )
      val webApiBasePath = simpleConfigOpt[String]( ConfigKey.WebApiBasePath )(
        name    = "web-api-base-path",
        help    = "The URL base location upon which the web api is served (usually just '/').",
        metavar = "path"
      )
      val webApiPort = simpleConfigOpt[Int]( ConfigKey.WebApiPort )(
        name    = "web-api-port",
        help    = "The port from which the web api is served (usually blank, protocol determined).",
        metavar = "port"
      )
      ( dumpDbDir, mailBatchSize, mailBatchDelaySecs, mailMaxRetries, timeZone, webDaemonInterface, webDaemonPort, webApiProtocol, webApiHostName, webApiBasePath, webApiPort ) mapN: (ddr, mbs, mbds, mmr, tz, wdi, wdp, wapro, wahn, wabp, wapo) =>
        val settings : Map[ConfigKey,String] = (Vector.empty ++ ddr ++ mbs ++ mbds ++ mmr ++ tz ++ wdi ++ wdp ++ wapro ++ wahn ++ wabp ++ wapo).toMap
        CommandConfig.SetConfig( settings )
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
      ( from, to ) mapN ( (f,t) => CommandConfig.SendTestEmail(f,t) )
    Command("send-test-email", header=header)( opts )
  val subscribe =
    val header = "Subscribe to a subscribable."
    val opts =
      val subscribableName =
        val help = "The name of an already-defined subscribable."
        Opts.option[String]("subscribable-name",help=help,metavar="name").map( SubscribableName.apply )
      val destination = CommonOpts.AnyDestination
      val unconfirmed = Opts.flag("unconfirmed", help="Mark the subscription unconfirmed.").orFalse
      ( subscribableName, destination, unconfirmed ) mapN: (sn, d, uc) =>
        CommandConfig.Subscribe( AdminSubscribeOptions(sn, d, !uc, Instant.now) ) // ! our flag is unconfirmed, our field is confirmed
    Command("subscribe", header=header)( opts )

  val feedletter =
    val opts : Opts[(Option[JPath], CommandConfig)] =
      val secrets = CommonOpts.Secrets
      val subcommands =
        Opts.subcommands(
          addFeed,
          daemon,
          dbDump,
          dbInit,
          dbMigrate,
          defineEmailSubscribable,
          defineMastodonSubscribable,
          editSubscribable,
          listComposeUntemplates,
          listConfig,
          listExcludedItems,
          listFeeds,
          listSubscribables,
          sendTestEmail,
          setConfig,
          subscribe
        )
      ( secrets, subcommands ) mapN( (sec,sub) => (sec,sub) )
    Command(name="feedletter", header="Manage e-mail subscriptions to and notifications from RSS feeds.")( opts )

  override val baseCommand = feedletter
