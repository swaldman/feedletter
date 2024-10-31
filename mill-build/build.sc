import mill._, scalalib._

object `package` extends MillBuildRootModule {
  def scalacOptions = T {
    super.scalacOptions() ++ Seq("-Ytasty-reader")
  }
}

