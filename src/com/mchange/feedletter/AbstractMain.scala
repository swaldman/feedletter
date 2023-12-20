package com.mchange.feedletter

import zio.*
import java.lang.System

import com.monovore.decline.*

import java.nio.file.{Path as JPath}
import java.util.{Properties, Map as JMap}
import javax.sql.DataSource
import org.postgresql.replication.fluent.CommonOptions


trait AbstractMain:
  object CommonOpts:
    val Secrets = 
      val help = "Path to properties file containing SMTP, postgres, c3p0, and other configuration details."
      val opt  = Opts.option[JPath]("secrets",help=help,metavar="propsfile")
      val env  = Opts.env[JPath]("FEEDLETTER_SECRETS", help=help)
      (opt orElse env).orNone
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
        val task = cc.zcommand.provide( AppSetup.live(mbSecrets), LayerDataSource )
        Unsafe.unsafely:
          Runtime.default.unsafe.run(task).getOrThrow()
