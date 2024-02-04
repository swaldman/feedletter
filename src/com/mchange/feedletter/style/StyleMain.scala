package com.mchange.feedletter.style

import com.mchange.feedletter.*
import com.monovore.decline.*
import cats.implicits.* // for mapN
import cats.data.{NonEmptyList,Validated,ValidatedNel}

import com.mchange.mailutil.Smtp

import java.nio.file.{Path as JPath}

object StyleMain extends AbstractMain:

  object CommonStyleOpts:
    val SubscribableName =
      val help = "The name of an already defined subscribable that will use this template."
      Opts.option[String]("subscribable-name",help=help,metavar="name").map( com.mchange.feedletter.SubscribableName.apply )
    val DestinationOrDefault = CommonOpts.AnyDestination.orNone
    val Interface =
      val help = "The interface on which to bind an HTTP server, which will serve the rendered untemplate."
      Opts.option[String]("interface",help=help,metavar="interface").withDefault( Default.Style.StyleInterface )
    val Port =
      val help = "The port on which to run a HTTP server, which will serve the rendered untemplate."
      Opts.option[Int]("port",help=help,metavar="num").withDefault( Default.Style.StylePort )
    val UntemplateName =
      val help = "Fully name of an untemplate to style."
      Opts.option[String]("untemplate-name",help=help,metavar="fully-qualified-name").orNone
    val StyleDestServe =
      (Interface, Port) mapN: (i, p) =>
        StyleDest.Serve( i, p )
    val StyleDestMail =
      val from = Opts.option[String]("from",help="Address from which to send the example.",metavar="e-mail").map( em => Smtp.Address.parseSingle(em) )
      val to   = Opts.option[String]("from",help="Address to which to send the example.",metavar="e-mail").map( em => Smtp.Address.parseSingle(em) )
      (from, to) mapN: (f, t) =>
        StyleDest.Mail( f, t )
    val StyleDestAny =
      StyleDestMail orElse StyleDestServe

  val composeSingle =
    val header = "Style a template that composes a single item."
    val opts =
      val subscribableName = CommonStyleOpts.SubscribableName
      val untemplateName = CommonStyleOpts.UntemplateName
      val selection =
        val first  = Opts.flag("first",help="Display first item in feed.").map( _ => ComposeSelection.Single.First )
        val random = Opts.flag("random",help="Choose random item from feed to display").map( _ => ComposeSelection.Single.Random )
        val guid   = Opts.option[String]("guid",help="Choose guid of item to display.").map( s => ComposeSelection.Single.Guid(Guid(s)) )
        ( first orElse random orElse guid ).withDefault( ComposeSelection.Single.First )
      val destination = CommonStyleOpts.DestinationOrDefault
      val withinTypeId =
        val help = "A subscription-type specific sample within-type-id for the notification."
        Opts.option[String]("within-type-id",help=help,metavar="string").orNone
      val interface = CommonStyleOpts.Interface
      val port = CommonStyleOpts.Port
      ( subscribableName, untemplateName, selection, destination, withinTypeId, interface, port ) mapN: ( sn, un, s, d, wti, i, p ) =>
        CommandConfig.Style.ComposeUntemplateSingle( sn, un, s, d, wti, i, p )
    Command("compose-single",header=header)( opts )

  val composeMultiple =
    val header = "Style a template that composes a multiple items."
    val opts =
      val subscribableName = CommonStyleOpts.SubscribableName
      val untemplateName = CommonStyleOpts.UntemplateName
      val selection =
        val first  = Opts.option[Int]("first",help="Display first n items in feed.",metavar="n").map( n => ComposeSelection.Multiple.First(n) )
        val random = Opts.option[Int]("random",help="Choose n random items from feed to display", metavar="n").map( n => ComposeSelection.Multiple.Random(n) )
        val guids   = Opts.options[String]("guid",help="Explicitly choose guids of items to display.").map( _.toList.map(Guid.apply) ).map( v => ComposeSelection.Multiple.Guids(v) )
        ( first orElse random orElse guids ).withDefault( ComposeSelection.Multiple.First(Int.MaxValue) ) // just render everything
      val destination = CommonStyleOpts.DestinationOrDefault
      val withinTypeId =
        val help = "A subscription-type specific sample within-type-id for the notification."
        Opts.option[String]("within-type-id",help=help,metavar="string").orNone
      val interface = CommonStyleOpts.Interface
      val port = CommonStyleOpts.Port
      ( subscribableName, untemplateName, selection, destination, withinTypeId, interface, port ) mapN: ( sn, un, s, d, wti, i, p ) =>
        CommandConfig.Style.ComposeUntemplateMultiple( sn, un, s, d, wti, i, p )
    Command("compose-multiple",header=header)( opts )

  val confirm =
    val header = "Style a template that asks users to confirm a subscription."
    val opts =
      val subscribableName = CommonStyleOpts.SubscribableName
      val untemplateName = CommonStyleOpts.UntemplateName
      val destination = CommonStyleOpts.DestinationOrDefault
      val interface = CommonStyleOpts.Interface
      val port = CommonStyleOpts.Port
      ( subscribableName, untemplateName, destination, interface, port ) mapN: ( sn, un, d, i, p ) =>
        CommandConfig.Style.Confirm( sn, un, d, i, p )
    Command("confirm",header=header)( opts )

  val statusChange =
    val header = "Style a template that informs users of a subscription status change."
    val opts =
      val kind =
        val created   = Opts.flag("created",help="Inform user that a subscription has been created.").map( _ => SubscriptionStatusChange.Created )
        val confirmed = Opts.flag("confirmed",help="Inform user that a subscription has been confirmed.").map( _ => SubscriptionStatusChange.Confirmed )
        val removed   = Opts.flag("removed",help="Inform user that a subscription has been removed.").map( _ => SubscriptionStatusChange.Removed )
        (created orElse confirmed orElse removed)
      val subscribableName = CommonStyleOpts.SubscribableName
      val untemplateName = CommonStyleOpts.UntemplateName
      val destination = CommonStyleOpts.DestinationOrDefault
      val preconfirmed = Opts.flag("preconfirmed",help="Set to mark the styled subscription already confirmed, or not in need of a confirmation step.").orFalse
      val interface = CommonStyleOpts.Interface
      val port = CommonStyleOpts.Port
      ( kind, subscribableName, untemplateName, destination, preconfirmed, interface, port ) mapN: ( k, sn, un, d, prec, i, p ) =>
        CommandConfig.Style.StatusChange( k, sn, un, d, !prec, i, p ) // requiresConfirmation == !preconfirmed
    Command("status-change",header=header)( opts )

  val removalNotification =
    val header = "Style a template that notifies users that they have subscribed."
    val opts =
      val subscribableName = CommonStyleOpts.SubscribableName
      val untemplateName = CommonStyleOpts.UntemplateName
      val destination = CommonStyleOpts.DestinationOrDefault
      val interface = CommonStyleOpts.Interface
      val port = CommonStyleOpts.Port
      ( subscribableName, untemplateName, destination, interface, port ) mapN: ( sn, un, d, i, p ) =>
        CommandConfig.Style.RemovalNotification( sn, un, d, i, p )
    Command("removal-notification",header=header)( opts )

  val feedletterStyle =
    val header = "Iteratively edit and review the untemplates through which your posts will be notified."
    val opts : Opts[(Option[JPath], CommandConfig)] =
      val secrets = CommonOpts.Secrets
      val subcommands = Opts.subcommands( composeMultiple, composeSingle, confirm, removalNotification, statusChange )
      ( secrets, subcommands ) mapN( (sec,sub) => (sec,sub) )
    Command("feedletter-style", header=header)( opts )

  val baseCommand = feedletterStyle

end StyleMain
