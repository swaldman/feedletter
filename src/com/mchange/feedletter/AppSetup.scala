package com.mchange.feedletter

import zio.*
import java.io.{BufferedInputStream,InputStream}
import java.nio.file.{Path as JPath}
import java.util.Properties
import scala.util.Using
import scala.util.control.NonFatal
import scala.jdk.CollectionConverters.*

import com.mchange.mailutil.Smtp
import com.mchange.conveniences.javautil.*

import MLevel.*

object AppSetup extends SelfLogging:

  val AcceptableSecretsPermStrings = Set("r--------","rw-------")
  val AcceptableSecretsPerms = AcceptableSecretsPermStrings.map( os.PermSet.fromString )

  val DefaultSecretsFileName = "feedletter-secrets.properties"

  val DefaultSecretsSearch =
    ((os.pwd / DefaultSecretsFileName).toString :: s"/etc/feedletter/${DefaultSecretsFileName}" :: s"/usr/local/etc/feedletter/${DefaultSecretsFileName}" :: Nil).map( os.Path.apply )

  def julConfigRawInputStreamAndSource() : (InputStream, LoggingConfig) =
    try
      (os.read.inputStream( os.resource / "logging.properties" ), LoggingConfig.User)
    catch
      case rnfe : os.ResourceNotFoundException =>
        (os.read.inputStream( os.resource / "feedletter-default-logging.properties" ), LoggingConfig.Default)
    end try

  private val julAppSetup : Task[LoggingConfig] = ZIO.attemptBlocking:
    val ( ris, source ) = julConfigRawInputStreamAndSource()
    Using.resource( new BufferedInputStream( ris ) ): is =>
      java.util.logging.LogManager.getLogManager().readConfiguration( is )
    source

  def setupAutoremovePidFile() : UIO[Unit] =
    val risky =
      ZIO.attempt:
        sys.env.get("MILL_DAEMON_PID_FILE").foreach: pidFileLoc =>
          val pidFilePath = os.Path( pidFileLoc )
          val onShutdown =
            new Thread:
              override def run() : Unit =
                try
                  if os.exists(pidFilePath) then
                    val myPid = ProcessHandle.current().pid()
                    val fromFilePid = os.read(pidFilePath).trim().toLong
                    if myPid == fromFilePid then
                      INFO.log(s"Shutdown Hook: Removing PID file '${pidFilePath}'")
                      os.remove( pidFilePath )
                catch
                  case NonFatal(t) => WARNING.log("Throwable while executing autoremove PID file shutdown hook.", t)
          java.lang.Runtime.getRuntime().addShutdownHook(onShutdown)
    risky.catchAll: t =>
      t match
        case NonFatal(t) => ZIO.succeed(WARNING.log("Throwable while setting up autoremove PID file shutdown hook.", t))
        case other => throw other

  def live( secrets : Option[JPath] ) : ZLayer[Any, Throwable, AppSetup] = ZLayer.fromZIO:
    for
      lcfg <- julAppSetup
      _    <- setupAutoremovePidFile()
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
      AppSetup( loc, propsMap, lcfg )

case class AppSetup( secretsLoc : Option[os.Path], secrets : Map[String,String], loggingConfig : LoggingConfig ):
  lazy val smtpContext : Smtp.Context = Smtp.Context( (Smtp.Context.defaultProperties().asScala.toMap ++ secrets).toProperties, sys.env )
  lazy val secretSalt : String = secrets.get("feedletter.secret.salt").getOrElse:
    throw new NoSecretSalt("Please set 'feedletter.secret.salt' to an arbitrary but consistent String in the feedletter-secrets file.")

  // we let the parent mill process write and sysd remove PID files now
  //lazy val pidFile : os.Path = secrets.get("feedletter.pid.file").map( os.Path.apply ).getOrElse( os.pwd / "feedletter.pid" )
