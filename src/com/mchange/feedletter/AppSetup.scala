package com.mchange.feedletter

import zio.*
import java.io.BufferedInputStream
import scala.util.Using

object AppSetup:

  private val julAppSetup : Task[AppSetup] = ZIO.attemptBlocking:
    Using.resource( new BufferedInputStream( os.read.inputStream( os.resource / "logging.properties" ) ) ): is =>
      java.util.logging.LogManager.getLogManager().readConfiguration( is )
    new AppSetup{}

  val live : ZLayer[Any, Throwable, AppSetup] = ZLayer.fromZIO( julAppSetup )

sealed trait AppSetup
