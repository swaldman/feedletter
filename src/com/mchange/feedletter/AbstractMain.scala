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

import MLevel.*

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
      email orElse sms orElse mastodon
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
      val c3p0PropsJMap =
        appSetup.secrets
          .filter( (k, _) => k.startsWith("c3p0.") )
          .map( (k, v) => (k.substring(5), v) )
          .asJava

      val nascent = new ComboPooledDataSource()
      BeansUtils.overwriteAccessiblePropertiesFromMap(
        c3p0PropsJMap, // sourceMap
        nascent,       // destination bean
        true,          // skip nulls
        null,          // props to ignore, null means none
        true,          // do coerce strings
        null,          // null means log to default (WARNING) level if can't write
        null,          // null means log to default (WARNING) level if can't coerce
        false          // don't die on failures, continue
      )
      appSetup.secrets.get( SecretsKey.FeedletterJdbcUrl ).foreach( nascent.setJdbcUrl )
      appSetup.secrets.get( SecretsKey.FeedletterJdbcUser ).foreach( nascent.setUser )
      appSetup.secrets.get( SecretsKey.FeedletterJdbcPassword ).foreach( nascent.setPassword )
      nascent

    ZLayer.fromFunction( createDataSource _ )

  def baseCommand : Command[(Option[JPath],CommandConfig)]

  def main( args : Array[String] ) : Unit =
    baseCommand.parse(args.toIndexedSeq, sys.env) match
      case Left(help) =>
        println(help)
        System.exit(1)
      case Right( ( mbSecrets : Option[JPath], cc : CommandConfig ) ) =>
        val task =
          cc.zcommand.provide( AppSetup.live(mbSecrets), LayerDataSource )
            .tapError( t => SEVERE.zlog( "Application failed with an Exception.", t ) )
            .tapDefect( cause => SEVERE.zlog( "Application failed with cause: " + cause ) )
        Unsafe.unsafely:
          Runtime.default.unsafe.run(task).getOrThrow()
