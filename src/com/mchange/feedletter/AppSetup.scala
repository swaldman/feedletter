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

  val AcceptableSecretsPermStrings = Set("r--------","rw-------")
  val AcceptableSecretsPerms = AcceptableSecretsPermStrings.map( os.PermSet.fromString )

  val DefaultSecretsFileName = "feedletter-secrets.properties"

  val DefaultSecretsSearch =
    ((os.pwd / DefaultSecretsFileName).toString :: s"/etc/feedletter/${DefaultSecretsFileName}" :: s"/usr/local/etc/feedletter/${DefaultSecretsFileName}" :: Nil).map( os.Path.apply )

  def julConfigRawInputStream() =
    try
      os.read.inputStream( os.resource / "logging.properties" )
    catch
      case rnfe : os.ResourceNotFoundException =>
        os.read.inputStream( os.resource / "feedletter-default-logging.properties" )
    end try

  private val julAppSetup : Task[Unit] = ZIO.attemptBlocking:
    Using.resource( new BufferedInputStream( julConfigRawInputStream() ) ): is =>
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
      loc.foreach: p =>
        val perms : os.PermSet = os.perms(p)
        if (!AcceptableSecretsPerms(perms))
          val ap = AcceptableSecretsPermStrings.mkString(", ")
          throw new LeakySecrets(s"Secrets file '${p}' is not secret enough. Permission '${perms}'. Acceptable permissions: [$ap]")
      val propsMap = props.toMap
      AppSetup( loc, propsMap )

case class AppSetup( secretsLoc : Option[os.Path], secrets : Map[String,String] ):
  lazy val smtpContext : Smtp.Context = Smtp.Context( (Smtp.Context.defaultProperties().asScala.toMap ++ secrets).toProperties, sys.env )
  lazy val secretSalt : String = secrets.get("feedletter.secret.salt").getOrElse:
    throw new NoSecretSalt("Please set 'feedletter.secret.salt' to an arbitrary but consistent String in the feedletter-secrets file.")
