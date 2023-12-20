package com.mchange.feedletter

import com.monovore.decline.*
import cats.implicits.* // for mapN
import cats.data.{NonEmptyList,Validated,ValidatedNel}

import java.nio.file.{Path as JPath}

object ComposeMain extends AbstractMain:

  val single =
    val header = "Compose a template for a single post."
    val opts =
      val subscriptionName =
        val help = "The name of an already defined subscription that will use this template."
        Opts.option[String]("subscription-name",help=help,metavar="name").map( SubscribableName.apply )
      val untemplateName =
        val help = "The fully-qualified name of the unteplate"
        Opts.option[String]("untemplate-name",help=help,metavar="fully-qualified-name")
      val selection =
        val first  = Opts.flag("first",help="Display first item in feed.").map( _ => ComposeSelection.Single.First )
        val random = Opts.flag("random",help="Choose random item from feed to display").map( _ => ComposeSelection.Single.Random )
        val guid   = Opts.option[String]("guid",help="Choose guid of item to display.").map( s => ComposeSelection.Single.Guid(Guid(s)) )
        ( first orElse random orElse guid ).withDefault( ComposeSelection.Single.First )
      val destination =  
        val help = "A subscription-type specific sample destination (e.g. email address) for the notification."
        Opts.option[String]("destination",help=help,metavar="string").map( Destination.apply ).orNone
      val withinTypeId =
        val help = "A subscription-type specific sample within-type-id for the notification."
        Opts.option[String]("within-type-id",help=help,metavar="string").orNone
      val port =
        val help = "The port on which to run a local HTTP server, which will serve the rendered untemplate."
        Opts.option[Int]("port",help=help,metavar="num").withDefault( Default.ComposePort )
      ( subscriptionName, untemplateName, selection, destination, withinTypeId, port ) mapN: ( sn, un, s, d, wti, p ) =>
        CommandConfig.Admin.ComposeUntemplateSingle( sn, un, s, d, wti, p )
    Command("single",header=header)( opts )

  val feedletterCompose =
    val header = "Iteratively edit and review the untemplates through which your posts will be notified."
    val opts : Opts[(Option[JPath], CommandConfig)] =
      val secrets = CommonOpts.Secrets
      val subcommands = Opts.subcommands( single )
      ( secrets, subcommands ) mapN( (sec,sub) => (sec,sub) )
    Command("feedletter-compose", header=header)( opts )

  val baseCommand = feedletterCompose

end ComposeMain
