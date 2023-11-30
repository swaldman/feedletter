import mill._, scalalib._

import $ivy.`com.lihaoyi::mill-contrib-buildinfo:`
import mill.contrib.buildinfo.BuildInfo

// see https://github.com/lefou/mill-vcs-version
import $ivy.`de.tototec::de.tobiasroeser.mill.vcs.version::0.4.0`
import de.tobiasroeser.mill.vcs.version.VcsVersion

object feedletter extends RootModule with ScalaModule with BuildInfo {
  def scalaVersion = "3.3.1"
  def ivyDeps = Agg(
    ivy"org.scala-lang.modules::scala-xml:2.2.0",
    ivy"com.lihaoyi::os-lib:0.9.1",
    ivy"com.lihaoyi::requests:0.8.0",
    ivy"dev.zio::zio:2.0.18",
    ivy"dev.zio::zio-cli:0.5.0",
    ivy"com.mchange::audiofluidity-rss:0.0.3",
    ivy"org.postgresql:postgresql:42.6.0",
    ivy"com.mchange:c3p0:0.9.5.5",
    ivy"com.mchange::mlog-scala:0.3.14",
    ivy"com.mchange::texttable:0.0.3"
  )

  def buildInfoMembers = Seq(
    BuildInfo.Value("version", VcsVersion.vcsState().format())
  )
  def buildInfoPackageName = "com.mchange.feedletter"

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

