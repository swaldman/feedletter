package com.mchange.feedletter

trait Jsonable:
  def json : Json
  def jsonPretty : Json

