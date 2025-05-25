import $meta._

import mill._, scalalib._, publish._

import $ivy.`com.lihaoyi::mill-contrib-bloop:$MILL_VERSION`

import $ivy.`com.lihaoyi::mill-contrib-buildinfo:`
import mill.contrib.buildinfo.BuildInfo

// see https://github.com/lefou/mill-vcs-version
import $ivy.`de.tototec::de.tobiasroeser.mill.vcs.version::0.4.0`
import de.tobiasroeser.mill.vcs.version.VcsVersion

import $ivy.`com.mchange::mill-daemon:0.1.1`

import $ivy.`com.mchange::untemplate-mill:0.1.4`
import untemplate.mill._
import com.mchange.milldaemon.DaemonModule

import scala.util.control.NonFatal

object `package` extends RootModule with DaemonModule with UntemplateModule with PublishModule with BuildInfo {
  def scalaVersion = "3.3.6"

  override def scalacOptions = T{ Seq("-deprecation", "-explain-cyclic") }

  val TapirVersion      = "1.11.29"
  val LogadapterVersion = "0.0.1"

  def ivyDeps = Agg(
    ivy"com.mchange::logadapter-scala:${LogadapterVersion}",
    ivy"com.mchange::logadapter-scala-zio:${LogadapterVersion}",
    ivy"com.mchange::sqlutil-scala-zio:0.0.2",
    ivy"com.mchange::audiofluidity-rss:0.1.0",
    ivy"com.mchange::conveniences:0.0.5",
    ivy"com.mchange:c3p0:0.11.0",
    ivy"com.mchange::texttable:0.0.3",
    ivy"com.mchange::mailutil:0.0.5",
    ivy"com.mchange::cryptoutil:0.0.2",
    ivy"com.mchange::untemplate:0.1.4",
    ivy"dev.zio::zio:2.1.18",
    ivy"com.monovore::decline:2.4.1",
    ivy"org.postgresql:postgresql:42.7.3",
    ivy"org.scala-lang.modules::scala-xml:2.2.0",
    ivy"com.lihaoyi::os-lib:0.11.2",
    ivy"com.lihaoyi::requests:0.8.3", // holding back on upgrade to 0.9.0 because of HttpClient hangs observed in unify-rss, similar to https://developer.jboss.org/thread/274758, only under pre-21 JVMs
    ivy"com.lihaoyi::upickle:3.2.0",
    ivy"com.softwaremill.sttp.tapir::tapir-zio:${TapirVersion}",
    ivy"com.softwaremill.sttp.tapir::tapir-zio-http-server:${TapirVersion}",
    ivy"com.softwaremill.sttp.tapir::tapir-json-upickle:${TapirVersion}",
  )

  val pidFilePathFile = mill.api.WorkspaceRoot.workspaceRoot / ".feedletter-pid-file-path"

  override def runDaemonPidFile = {
    if ( os.exists( pidFilePathFile ) )
      try Some( os.Path( os.read( pidFilePathFile ).trim ) )
      catch {
        case NonFatal(t) =>
          throw new Exception( s"Could not parse absolute path of desired PID file from contents of ${pidFilePathFile}. Please repair or remove this file.", t )
      }
    else
      Some( mill.api.WorkspaceRoot.workspaceRoot / "feedletter.pid" )
  }

  def buildInfoMembers = Seq(
    BuildInfo.Value("version", VcsVersion.vcsState().format())
  )
  def buildInfoPackageName = "com.mchange.feedletter"

  // we'll build an index!
  override def untemplateIndexNameFullyQualified : Option[String] = Some("com.mchange.feedletter.IndexedUntemplates")

  override def untemplateSelectCustomizer: untemplate.Customizer.Selector = { key =>
    var out = untemplate.Customizer.empty

    // to customize, examine key and modify the customer
    // with out = out.copy=...
    //
    // e.g. out = out.copy(extraImports=Seq("draft.*"))

    out = out.copy(extraImports=Seq("com.mchange.feedletter.*","com.mchange.feedletter.style.*"))

    out
  }

  /**
   * Update the millw script.
   * modified from https://github.com/lefou/millw
   */
  def overwriteLatestMillw() = T.command {
    import java.nio.file.attribute.PosixFilePermission._
    val target = mill.util.Util.download("https://raw.githubusercontent.com/lefou/millw/main/millw")
    val millw = Task.workspace / "millw"
    os.copy.over(target.path, millw)
    os.perms.set(millw, os.perms(millw) + OWNER_EXECUTE + GROUP_EXECUTE + OTHERS_EXECUTE)
    target
  }

  override def artifactName = "feedletter"
  override def publishVersion =  T{ VcsVersion.vcsState().format() }
  //override def publishVersion =  T{ "0.0.1-SNAPSHOT" }
  override def pomSettings    = T{
    PomSettings(
      description = "A service that carefully watches RSS feeds and mails newsletters or sends notifications based on items that stablely appear.",
      organization = "com.mchange",
      url = "https://github.com/swaldman/feedletter",
      licenses = Seq(License.`AGPL-3.0-only`),
      versionControl = VersionControl.github("swaldman", "feedletter"),
      developers = Seq(
	Developer("swaldman", "Steve Waldman", "https://github.com/swaldman")
      )
    )
  }
}

