package com.mchange.feedletter

import zio.{UIO,ZIO}
import com.mchange.sc.v1.log.MLogger

type SelfLogging = com.mchange.sc.v1.log.SelfLogging
val  MLevel      = com.mchange.sc.v1.log.MLevel
type MLevel      = com.mchange.sc.v1.log.MLevel

extension ( ml : MLevel )( using MLogger )
  def zlog( message : =>String ) : UIO[Unit]                      = ZIO.succeed( ml.log( message ) )
  def zlog( message : =>String, error : =>Throwable ) : UIO[Unit] = ZIO.succeed( ml.log( message, error ) )

extension[R,E,A] ( effect : ZIO[R,E,A] )( using MLogger )
  def zlogErrorDefect( ml : MLevel, what : => String = "Effect" ) : ZIO[R,E,A] =
    effect
      .tapError { e =>
        e match
          case t : Throwable => ml.zlog("${what} failed within error channel, on Throwable.", t)
          case other         => ml.zlog("${what} failed within error channel. Failure: " + other)
      }
      .tapDefect { cause =>
        ml.zlog("${what} failed outside of error channel with cause: " + cause)
      }  
