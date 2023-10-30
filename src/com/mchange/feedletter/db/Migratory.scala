package com.mchange.feedletter.db

import zio.*
import javax.sql.DataSource
import com.mchange.feedletter.Config

trait Migratory:
  def targetDbVersion : Int
  def dump(config : Config, dataSource : DataSource) : Task[Unit]
  def dbVersionStatus(config : Config, dataSource : DataSource) : Task[DbVersionStatus]
  def upMigrate(config : Config, dataSource : DataSource, from : Option[Int]) : Task[Unit]


