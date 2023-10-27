import mill._, scalalib._

object feedletter extends RootModule with ScalaModule {
  def scalaVersion = "3.3.1"
  def ivyDeps = Agg(
    ivy"org.scala-lang.modules::scala-xml:2.2.0",
    ivy"com.lihaoyi::os-lib:0.9.1",
    ivy"dev.zio::zio:2.0.18"
  )
}
