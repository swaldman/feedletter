package com.mchange.feedletter.style

import com.mchange.mailutil.Smtp

object StyleDest:
  case class Mail( from : Smtp.Address, to : Smtp.Address ) extends StyleDest
  case class Serve( interface : String, port : Int) extends StyleDest
sealed trait StyleDest


