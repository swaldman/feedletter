package com.mchange.feedletter

import zio.*
import com.mchange.feedletter.db.{DbVersionStatus,PgDatabase}
import com.mchange.feedletter.Config

import com.mchange.sc.v1.log.*
import MLevel.*

import javax.sql.DataSource

object CommandConfig:
  private lazy given logger : MLogger = mlogger( this )
  
  case object DbDump extends CommandConfig:
    override def zcommand : ZCommand =
      for
        config <- ZIO.service[Config]
        ds     <- ZIO.service[DataSource]
        out    <- PgDatabase.dump(config, ds)
      yield
        INFO.log(s"The database was successfully dumped to '${out}'.")
    end zcommand  
  case object DbInit extends CommandConfig:
    override def zcommand : ZCommand =
      def doInit( config : Config, ds : DataSource, status : DbVersionStatus ) : Task[Unit] =
        if status == DbVersionStatus.SchemaMetadataNotFound then PgDatabase.migrate(config, ds)
        else
          status match
            case DbVersionStatus.Current(_) => ZIO.succeed( INFO.log("The database is already initialized and up-to-date." ) )
            case DbVersionStatus.OutOfDate( _, _) => ZIO.succeed( INFO.log("The database is already initialized, but out-of-date. Please migrate.") )
            case other => throw new FeedletterException(s"""${other}: ${other.errMessage.getOrElse("<no message available>")}""")
      for
        config <- ZIO.service[Config]
        ds     <- ZIO.service[DataSource]
        status <- PgDatabase.dbVersionStatus(config, ds)
        _      <- doInit( config, ds, status )
      yield ()
    end zcommand  
  case class DbMigrate( force : Boolean ) extends CommandConfig:
    override def zcommand: ZCommand =
      def doMigrate( config : Config, ds : DataSource ) = if force then PgDatabase.migrate(config, ds) else PgDatabase.cautiousMigrate(config, ds)
      for
        config <- ZIO.service[Config]
        ds <- ZIO.service[DataSource]
        _ <- doMigrate( config, ds )
      yield ()
    end zcommand  
sealed trait CommandConfig:
  def zcommand : ZCommand = ZIO.unit
