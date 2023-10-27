package com.mchange.feedletter.db

import zio.*
import javax.sql.DataSource
import com.mchange.feedletter.Config

import scala.util.Using

class PgDatabase extends Migratory:
  val SchemaVersion = 1
  
  enum MetadataKey:
    case SchemaVersion
    case NextMailBatchTime
    case MailBatchSize
    case MailBatchDelaySecs

  def dump(config : Config, dataSource : DataSource) : Task[Unit] = ???
  def discoveredDbVersion(config : Config, dataSource : DataSource) : Task[Option[Int]] = ???
  def upMigrate(config : Config, dataSource : DataSource) : Task[Unit] = ???
  
