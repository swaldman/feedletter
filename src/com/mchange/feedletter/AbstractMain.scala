package com.mchange.feedletter

import zio.*
import java.lang.System

import com.monovore.decline.*
import cats.implicits.* // for mapN
import cats.data.{NonEmptyList,Validated,ValidatedNel}

import java.nio.file.{Path as JPath}
import java.time.ZoneId
import java.util.{Properties, Map as JMap}
import javax.sql.DataSource

import com.mchange.mailutil.*

import com.mchange.conveniences.javautil.*

import com.mchange.v2.c3p0.DataSources

import LoggingApi.*

trait AbstractMain extends SelfLogging:
  object CommonOpts:
    val AnyDestination : Opts[Destination] =
      val email =
        val general = Opts.option[String]("e-mail",help="The e-mail address to subscribe.",metavar="address")
        val displayName = Opts.option[String]("display-name",help="A display name to wrap around the e-mail address.",metavar="name").orNone
        ( general, displayName ) mapN: (g, dn) =>
          val tmp = Destination.Email( Smtp.Address.parseSingle( g ) )
          dn.fold(tmp)( n => tmp.copy( displayNamePart = Some(n) ) )
      val sms = Opts.option[String]("sms",help="The number to which messages should be sent.",metavar="number").map( n => Destination.Sms(number=n) )
      val mastodon =
        val instanceName = Opts.option[String]("masto-instance-name",help="A private name for this Mastodon instance.",metavar="name")
        val instanceUrl = Opts.option[String]("masto-instance-url",help="The URL of the Mastodon instance",metavar="url")
        ( instanceName, instanceUrl ) mapN: (in, iu) =>
          Destination.Mastodon( name = in, instanceUrl = iu )
      val bluesky =
        val identifier  = Opts.option[String]("bsky-identifier",help="An account identifier, usually a DNS name or value beginning with 'did:'.",metavar="identifier")
        val entrywayUrl = Opts.option[String]("bsky-entryway-url",help="The base URL of the bluesky service.",metavar="name").withDefault("https://bsky.social/")
        ( identifier, entrywayUrl ) mapN: (id, eu) =>
          Destination.BlueSky(entrywayUrl = eu, identifier = id)
      email orElse sms orElse mastodon orElse bluesky
    val AwaitStabilizationMinutes =
      val help = "Period (in minutes) over which an item should not have changed before it is considered stable and can be notified."
      Opts.option[Int]("await-stabilization-minutes", help=help, metavar="minutes")
    val ExtraParams : Opts[Map[String,String]] =
      def validate( strings : List[String] ) : ValidatedNel[String,List[Tuple2[String,String]]] =
        strings.map{ s =>
          s.split(":", 2) match
            case Array(key, value) => Validated.valid(Tuple2(key, value))
            case _ => Validated.invalidNel(s"Invalid key:value pair: ${s}")
        }.sequence
      Opts.options[String]("extra-param", "An extra parameter your notification renderers might use.", metavar = "key:value")
        .map( _.toList)
        .withDefault(Nil)
        .mapValidated( validate )
        .map( Map.from )
    val ComposeUntemplateName =
      val help = "Fully qualified name of untemplate that will render notifications."
      Opts.option[String]("compose-untemplate",help=help,metavar="fully-qualified-name").orNone
    val ConfirmUntemplateName =
      val help = "Fully qualified name of untemplate that will ask for e-mail confirmations."
      Opts.option[String]("confirm-untemplate",help=help,metavar="fully-qualified-name").orNone
    val MaxDelayMinutes =
      val help = "Notwithstanding other settings, maximum period past which an item should be notified, regardless of its stability."
      Opts.option[Int]("max-delay-minutes", help=help, metavar="minutes")
    val MinDelayMinutes =
      val help = "Minimum wait (in miunutes) before a newly encountered item can be notified."
      Opts.option[Int]("min-delay-minutes", help=help, metavar="minutes")
    val RecheckEveryMinutes =
      val help = "Delay between refreshes of feeds, and redetermining items' availability for notification."
      Opts.option[Int]("recheck-every-minutes", help=help, metavar="minutes")
    val RemovalNotificationUntemplateName =
      val help = "Fully qualified name of untemplate that be mailed to users upon unsubscription."
      Opts.option[String]("removal-notification-untemplate",help=help,metavar="fully-qualified-name").orNone
    val StatusChangeUntemplateName =
      val help = "Fully qualified name of untemplate that will render results of GET request to the API."
      Opts.option[String]("status-change-untemplate",help=help,metavar="fully-qualified-name").orNone
    val Secrets = 
      val help = "Path to properties file containing SMTP, postgres, c3p0, and other configuration details."
      val opt  = Opts.option[JPath]("secrets",help=help,metavar="propsfile")
      val env  = Opts.env[JPath]("FEEDLETTER_SECRETS", help=help)
      (opt orElse env).orNone
    val SubscribableNameDefined =
        val help = "The name of an already-defined subscribable."
        Opts.option[String]("subscribable-name",help=help,metavar="name").map( SubscribableName.apply )
    val TimeZone = Opts.option[String]("time-zone",help="ID of a time zone for determining the beginning and end of the period.",metavar="id").map( ZoneId.of ) 
  end CommonOpts

  val LayerDataSource : ZLayer[AppSetup, Throwable, DataSource] =
    import com.mchange.v2.beans.BeansUtils
    import com.mchange.v2.c3p0.ComboPooledDataSource
    import scala.jdk.CollectionConverters.*

    def createDataSource( appSetup : AppSetup ) : DataSource =
      val nascent = new ComboPooledDataSource()
      DataSources.overwriteC3P0PrefixedProperties( nascent, appSetup.secrets.toProperties )
      appSetup.secrets.get( SecretsKey.FeedletterJdbcUrl ).foreach( nascent.setJdbcUrl )
      appSetup.secrets.get( SecretsKey.FeedletterJdbcUser ).foreach( nascent.setUser )
      appSetup.secrets.get( SecretsKey.FeedletterJdbcPassword ).foreach( nascent.setPassword )
      nascent

    ZLayer.fromFunction( createDataSource _ )

  def baseCommand : Command[(Option[JPath],CommandConfig)]

  def main( args : Array[String] ) : Unit =
    try
      baseCommand.parse(args.toIndexedSeq, sys.env) match
        case Left(help) =>
          println(help)
          System.exit(1)
        case Right( ( mbSecrets : Option[JPath], cc : CommandConfig ) ) =>
          val task =
            cc.zcommand.provide( AppSetup.live(mbSecrets), LayerDataSource )
              .tapError( t => SEVERE.zlog( "Application failed with an Exception.", t ) )
              .tapDefect( cause => SEVERE.zlog( "Application failed with cause: " + cause ) )
          val completionValue =
            Unsafe.unsafely:
              Runtime.default.unsafe.run(task).getOrThrow()
          if args.contains("daemon") then
            SEVERE.log(s"Feedletter process terminating unexpectedly with completion value: ${completionValue}")
          else
            TRACE.log(s"Feedletter process ended with completion value: ${completionValue}")
    catch
      case t : Throwable =>
        // yes this is superfluous, should happen anyway, but we are seeing daemon
        // processes that should run until killed exit unexpectedly, and I want to be
        // triply sure we see every way these processes can end
        System.err.println("Function main(...) is terminating with an Exception:")
        t.printStackTrace()
        throw t
