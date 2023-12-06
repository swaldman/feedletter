package com.mchange.feedletter

import zio.*
import com.mchange.feedletter.db.{DbVersionStatus,PgDatabase}

import com.mchange.sc.v1.log.*
import MLevel.*

import javax.sql.DataSource

object CommandConfig:
  private lazy given logger : MLogger = mlogger( this )

  object Admin:
    case class AddFeed( fi : FeedInfo ) extends CommandConfig:
      override def zcommand : ZCommand =
        for
          ds  <- ZIO.service[DataSource]
          _   <- PgDatabase.ensureDb( ds )
          fis <- PgDatabase.addFeed( ds, fi )
          _   <- printFeedInfoTable(fis)
        yield ()
      end zcommand
    case class CreateSubscriptionTypeEmail( name : String, from : String, replyTo : Option[String], subtype : String, extraParams : Map[String,String]  ) extends CommandConfig:
      override def zcommand : ZCommand =
        val params = Seq( ("from", from) ) ++ replyTo.map( rt => ("replyTo",rt) ) ++ extraParams.toSeq
        val mbStype = SubscriptionType.dispatch( "Email", subtype, params )
        mbStype.fold( ZIO.fail( new InvalidSubscriptionType( "Failed to interpret command line args into valid subscription type." ) ) ): subscriptionType =>  
          for
            ds   <- ZIO.service[DataSource]
            _    <- PgDatabase.ensureDb( ds )
            _    <- PgDatabase.addSubscriptionType( ds, name, subscriptionType )
            tups <- PgDatabase.listSubscriptionTypes(ds)
            _    <- printSubscriptionTypeTable(tups)
            _    <- Console.printLine(s"SubscriptionType '${name}' created.")
          yield ()
      end zcommand
    case object ListConfig extends CommandConfig:
      override def zcommand : ZCommand =
        for
          ds   <- ZIO.service[DataSource]
          _    <- PgDatabase.ensureDb( ds )
          tups <- PgDatabase.reportConfigKeys(ds)
          _    <- printConfigurationTuplesTable(tups)
        yield ()
      end zcommand
    case object ListExcludedItems extends CommandConfig:
      override def zcommand : ZCommand =
        for
          ds  <- ZIO.service[DataSource]
          _   <- PgDatabase.ensureDb( ds )
          eis <- PgDatabase.fetchExcluded(ds)
          _   <- printExcludedItemsTable(eis)
        yield ()
      end zcommand
    case object ListFeeds extends CommandConfig:
      override def zcommand : ZCommand =
        for
          ds  <- ZIO.service[DataSource]
          _   <- PgDatabase.ensureDb( ds )
          fis <- PgDatabase.listFeeds( ds )
          _   <- printFeedInfoTable(fis)
        yield ()
      end zcommand
    case class SetConfig( settings : Map[ConfigKey,String] ) extends CommandConfig:
      override def zcommand : ZCommand =
        for
          ds <- ZIO.service[DataSource]
          _  <- PgDatabase.ensureDb( ds )
          ss <- PgDatabase.upsertConfigKeyMapAndReport( ds, settings )
          _  <- printConfigurationTuplesTable(ss)
        yield ()
      end zcommand
    case class Subscribe( aso : AdminSubscribeOptions ) extends CommandConfig:
      override def zcommand : ZCommand =
        for
          ds <- ZIO.service[DataSource]
          _  <- PgDatabase.ensureDb( ds )
          _  <- PgDatabase.addSubscription( ds, aso.stypeName, aso.destination, aso.feedUrl )
        yield ()
      end zcommand
  object Crank:
    case object Assign extends CommandConfig:
      override def zcommand: ZCommand =
        def doAssign( ds : DataSource ) = PgDatabase.updateAssignItems(ds)
        for
          ds <- ZIO.service[DataSource]
          _ <- PgDatabase.ensureDb( ds )
          _ <- doAssign( ds )
        yield ()
      end zcommand
    case object Complete extends CommandConfig:
      override def zcommand: ZCommand =
        def doComplete( ds : DataSource ) = PgDatabase.completeAssignables( ds )
        for
          ds <- ZIO.service[DataSource]
          _ <- PgDatabase.ensureDb( ds )
          _ <- doComplete( ds )
        yield ()
      end zcommand
  object Db:
    case object Dump extends CommandConfig:
      override def zcommand : ZCommand =
        for
          ds     <- ZIO.service[DataSource]
          out    <- PgDatabase.dump(ds)
        yield
          INFO.log(s"The database was successfully dumped to '${out}'.")
      end zcommand
    case object Init extends CommandConfig:
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
    case class Migrate( force : Boolean ) extends CommandConfig:
      override def zcommand: ZCommand =
        def doMigrate( ds : DataSource ) = if force then PgDatabase.migrate(ds) else PgDatabase.cautiousMigrate(ds)
        for
          ds <- ZIO.service[DataSource]
          _ <- doMigrate( ds )
        yield ()
      end zcommand
  case object Daemon extends CommandConfig
  case object Sendmail extends CommandConfig
sealed trait CommandConfig:
  def zcommand : ZCommand = ZIO.fail( new NotImplementedError("No zcommand has been implemented for this command") ) // XXX: temporary, make abstract when we stabilize
