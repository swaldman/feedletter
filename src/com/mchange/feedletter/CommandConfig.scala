package com.mchange.feedletter

import zio.*
import com.mchange.feedletter.db.{DbVersionStatus,PgDatabase}

import com.mchange.sc.v1.log.*
import MLevel.*

import javax.sql.DataSource
import com.mchange.feedletter.Main.forceOption

object CommandConfig:
  private lazy given logger : MLogger = mlogger( this )
  
  case object DbDump extends CommandConfig:
    override def zcommand : ZCommand =
      for
        ds     <- ZIO.service[DataSource]
        out    <- PgDatabase.dump(ds)
      yield
        INFO.log(s"The database was successfully dumped to '${out}'.")
    end zcommand  
  case object DbInit extends CommandConfig:
    override def zcommand : ZCommand =
      def doInit( ds : DataSource, status : DbVersionStatus ) : Task[Unit] =
        if status == DbVersionStatus.SchemaMetadataNotFound then PgDatabase.migrate(ds)
        else
          status match
            case DbVersionStatus.Current(_) => ZIO.succeed( INFO.log("The database is already initialized and up-to-date." ) )
            case DbVersionStatus.OutOfDate( _, _) => ZIO.succeed( INFO.log("The database is already initialized, but out-of-date. Please migrate.") )
            case other => throw new FeedletterException(s"""${other}: ${other.errMessage.getOrElse("<no message available>")}""")
      for
        ds     <- ZIO.service[DataSource]
        status <- PgDatabase.dbVersionStatus(ds)
        _      <- doInit( ds, status )
      yield ()
    end zcommand  
  case class DbMigrate( force : Boolean ) extends CommandConfig:
    override def zcommand: ZCommand =
      def doMigrate( ds : DataSource ) = if force then PgDatabase.migrate(ds) else PgDatabase.cautiousMigrate(ds)
      for
        ds <- ZIO.service[DataSource]
        _ <- doMigrate( ds )
      yield ()
    end zcommand
  case object CrankAssign extends CommandConfig:
    override def zcommand: ZCommand =
      def doAssign( ds : DataSource ) = PgDatabase.updateAssignItems(ds)
      for
        ds <- ZIO.service[DataSource]
        _ <- PgDatabase.ensureDb( ds )
        _ <- doAssign( ds )
      yield ()
    end zcommand
  case object CrankComplete extends CommandConfig:
    override def zcommand: ZCommand =
      def doComplete( ds : DataSource ) = PgDatabase.completeAssignables( ds )
      for
        ds <- ZIO.service[DataSource]
        _ <- PgDatabase.ensureDb( ds )
        _ <- doComplete( ds )
      yield ()
    end zcommand
  case object ConfigList extends CommandConfig:
    override def zcommand : ZCommand =
      for
        ds   <- ZIO.service[DataSource]
        tups <- PgDatabase.reportConfigKeys(ds)
        _    <- printConfigurationTuplesTable(tups)
      yield ()
    end zcommand
  case class ConfigSet( settings : Map[ConfigKey,String] ) extends CommandConfig:
    override def zcommand : ZCommand =
      for
        ds <- ZIO.service[DataSource]
        ss <- PgDatabase.upsertConfigKeyMapAndReport( ds, settings )
        _  <- printConfigurationTuplesTable(ss)
      yield ()
    end zcommand
  case class ConfigAddFeed( fi : FeedInfo ) extends CommandConfig:
    override def zcommand : ZCommand =
      for
        ds  <- ZIO.service[DataSource]
        fis <- PgDatabase.addFeed( ds, fi )
        _   <- printFeedInfoTable(fis)
      yield ()
    end zcommand
  case object ConfigListFeeds extends CommandConfig:
    override def zcommand : ZCommand =
      for
        ds  <- ZIO.service[DataSource]
        fis <- PgDatabase.listFeeds( ds )
        _   <- printFeedInfoTable(fis)
      yield ()
    end zcommand
  case object Update extends CommandConfig
  case object Sendmail extends CommandConfig
  case object Daemon extends CommandConfig
sealed trait CommandConfig:
  def zcommand : ZCommand = ZIO.fail( new NotImplementedError("No zcommand has been implemented for this command") ) // XXX: temporary, make abstract when we stabilize
