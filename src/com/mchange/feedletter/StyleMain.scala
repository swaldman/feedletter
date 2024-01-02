package com.mchange.feedletter

import com.monovore.decline.*
import cats.implicits.* // for mapN
import cats.data.{NonEmptyList,Validated,ValidatedNel}

import java.nio.file.{Path as JPath}

object StyleMain extends AbstractMain:
  object CommonStyleOpts:
    val SubscriptionName =
      val help = "The name of an already defined subscription that will use this template."
      Opts.option[String]("subscription-name",help=help,metavar="name").map( SubscribableName.apply )
    val DestinationOrDefault = CommonOpts.AnyDestination.orNone
    val Port =
      val help = "The port on which to run a local HTTP server, which will serve the rendered untemplate."
      Opts.option[Int]("port",help=help,metavar="num").withDefault( Default.Style.StylePort )

  val composeSingle =
    val header = "Style a template that composes a single post."
    val opts =
      val subscriptionName = CommonStyleOpts.SubscriptionName
      val selection =
        val first  = Opts.flag("first",help="Display first item in feed.").map( _ => ComposeSelection.Single.First )
        val random = Opts.flag("random",help="Choose random item from feed to display").map( _ => ComposeSelection.Single.Random )
        val guid   = Opts.option[String]("guid",help="Choose guid of item to display.").map( s => ComposeSelection.Single.Guid(Guid(s)) )
        ( first orElse random orElse guid ).withDefault( ComposeSelection.Single.First )
      val destination = CommonStyleOpts.DestinationOrDefault
      val withinTypeId =
        val help = "A subscription-type specific sample within-type-id for the notification."
        Opts.option[String]("within-type-id",help=help,metavar="string").orNone
      val port = CommonStyleOpts.Port
      ( subscriptionName, selection, destination, withinTypeId, port ) mapN: ( sn, s, d, wti, p ) =>
        CommandConfig.Style.ComposeUntemplateSingle( sn, s, d, wti, p )
    Command("compose-single",header=header)( opts )

  val confirm =
    val header = "Style a template that asks users to composes a subscription."
    val opts =
      val subscriptionName = CommonStyleOpts.SubscriptionName
      val destination = CommonStyleOpts.DestinationOrDefault
      val port = CommonStyleOpts.Port
      ( subscriptionName, destination, port ) mapN: ( sn, d, p ) =>
        CommandConfig.Style.Confirm( sn, d, p )
    Command("confirm",header=header)( opts )

  val feedletterStyle =
    val header = "Iteratively edit and review the untemplates through which your posts will be notified."
    val opts : Opts[(Option[JPath], CommandConfig)] =
      val secrets = CommonOpts.Secrets
      val subcommands = Opts.subcommands( composeSingle, confirm )
      ( secrets, subcommands ) mapN( (sec,sub) => (sec,sub) )
    Command("feedletter-style", header=header)( opts )

  val baseCommand = feedletterStyle

end StyleMain
