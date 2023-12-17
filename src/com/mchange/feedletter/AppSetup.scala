package com.mchange.feedletter

import zio.*
import java.io.BufferedInputStream
import java.nio.file.{Path as JPath}
import java.util.Properties
import scala.util.Using
import scala.jdk.CollectionConverters.*

import com.mchange.mailutil.Smtp
import com.mchange.conveniences.javautil.*

object AppSetup:

  val DefaultSecretsSearch = ("/etc/feedletter/feedletter-secrets.properties" :: "/usr/local/etc/feedletter/feedletter-secrets.properties" :: Nil).map( os.Path.apply )

  private val julAppSetup : Task[Unit] = ZIO.attemptBlocking:
    Using.resource( new BufferedInputStream( os.read.inputStream( os.resource / "logging.properties" ) ) ): is =>
      java.util.logging.LogManager.getLogManager().readConfiguration( is )

  def live( secrets : Option[JPath] ) : ZLayer[Any, Throwable, AppSetup] = ZLayer.fromZIO:
    for
      _ <- julAppSetup
    yield
      val (loc : Option[os.Path], props : Properties) =
        secrets match
          case Some( jpath ) => ( Some(os.Path(jpath)), loadProperties( jpath ) )
          case None =>
            DefaultSecretsSearch.find( os.exists ).fold( (None, new Properties()) )( path => (Some(path), loadProperties(path.toIO)) )
      val propsMap = props.toMap   
      AppSetup( loc, propsMap, Smtp.Context( (Smtp.Context.defaultProperties().asScala.toMap ++ propsMap).toProperties, sys.env ) )
      
case class AppSetup( secretsLoc : Option[os.Path], secrets : Map[String,String], smtpContext : Smtp.Context )
