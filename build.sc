import $meta._

import mill._, scalalib._

import $ivy.`com.lihaoyi::mill-contrib-bloop:$MILL_VERSION`

import $ivy.`com.lihaoyi::mill-contrib-buildinfo:`
import mill.contrib.buildinfo.BuildInfo

// see https://github.com/lefou/mill-vcs-version
import $ivy.`de.tototec::de.tobiasroeser.mill.vcs.version::0.4.0`
import de.tobiasroeser.mill.vcs.version.VcsVersion

import $ivy.`com.mchange::untemplate-mill:0.1.2-SNAPSHOT`
import untemplate.mill._

object feedletter extends RootModule with UntemplateModule with BuildInfo {
  def scalaVersion = "3.3.1"

  //val UnstaticVersion = "0.2.1-SNAPSHOT"

  def ivyDeps = Agg(
    ivy"dev.zio::zio:2.0.18",
    ivy"com.monovore::decline:2.4.1",
    ivy"org.postgresql:postgresql:42.6.0",
    ivy"org.scala-lang.modules::scala-xml:2.2.0",
    ivy"com.lihaoyi::os-lib:0.9.1",
    ivy"com.lihaoyi::requests:0.8.0",
    ivy"com.mchange:c3p0:0.9.5.5",
    ivy"com.mchange::audiofluidity-rss:0.0.3",
    ivy"com.mchange::mlog-scala:0.3.15-SNAPSHOT",
    ivy"com.mchange::texttable:0.0.3",
    ivy"com.mchange::mailutil:0.0.2-SNAPSHOT",
    ivy"com.mchange::cryptoutil:0.0.2-SNAPSHOT",
    ivy"com.mchange::conveniences:0.0.2-SNAPSHOT",
    ivy"com.softwaremill.sttp.tapir::tapir-zio:1.9.5",
    ivy"com.softwaremill.sttp.tapir::tapir-zio-http-server:1.9.5",
    ivy"com.mchange::untemplate:0.1.2-SNAPSHOT",
    //ivy"com.mchange::unstatic:${UnstaticVersion}",
    //ivy"com.mchange::unstatic-ztapir:${UnstaticVersion}",
  )

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

    out = out.copy(extraImports=Seq("com.mchange.feedletter.*"))

    out
  }

  /**
   * Update the millw script.
   * modified from https://github.com/lefou/millw
   */
  def overwriteLatestMillw() = T.command {
    import java.nio.file.attribute.PosixFilePermission._
    val target = mill.util.Util.download("https://raw.githubusercontent.com/lefou/millw/main/millw")
    val millw = build.millSourcePath / "millw"
    os.copy.over(target.path, millw)
    os.perms.set(millw, os.perms(millw) + OWNER_EXECUTE + GROUP_EXECUTE + OTHERS_EXECUTE)
    target
  }
}

