package com.mchange.feedletter

import zio.ZIO
import com.mchange.sc.v1.log.MLogger

type SelfLogging = com.mchange.sc.v1.log.SelfLogging
val  MLevel      = com.mchange.sc.v1.log.MLevel
type MLevel      = com.mchange.sc.v1.log.MLevel

extension ( ml : MLevel )( using MLogger )
  def zlog( message : =>String )                      = ZIO.succeed( ml.log( message ) )
  def zlog( message : =>String, error : =>Throwable ) = ZIO.succeed( ml.log( message, error ) )

