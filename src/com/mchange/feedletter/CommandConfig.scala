package com.mchange.feedletter

import zio.*

import com.mchange.feedletter.db.{DbVersionStatus,PgDatabase}

import java.nio.file.{Path as JPath}
import javax.sql.DataSource

import com.mchange.mailutil.*

import MLevel.*

object CommandConfig extends SelfLogging:

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
    case class DefineEmailSubscription( feedUrl : FeedUrl, subscribableName : SubscribableName, from : String, replyTo : Option[String], subtype : String, extraParams : Map[String,String]  ) extends CommandConfig:
      override def zcommand : ZCommand =
        val params = Seq( ("from", from) ) ++ replyTo.map( rt => ("replyTo",rt) ) ++ extraParams.toSeq
        val mbStype = SubscriptionType.dispatch( "Email", subtype, params )
        mbStype.fold( ZIO.fail( new InvalidSubscriptionType( s"Failed to interpret command line args into valid subscription type. ('Email', '${subtype}', '${params}')" ) ) ): subscriptionType =>  
          for
            ds   <- ZIO.service[DataSource]
            _    <- PgDatabase.ensureDb( ds )
            _    <- PgDatabase.addSubscribable( ds, feedUrl, subscribableName, subscriptionType )
            tups <- PgDatabase.listSubscribables(ds)
            _    <- printSubscribablesTable(tups)
            _    <- Console.printLine(s"An email subscribable to '${feedUrl}' named '${subscribableName}' has been created.")
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
    case object ListSubscribables extends CommandConfig:
      override def zcommand : ZCommand =
        for
          ds  <- ZIO.service[DataSource]
          _   <- PgDatabase.ensureDb( ds )
          tups <- PgDatabase.listSubscribables(ds)
          _    <- printSubscribablesTable(tups)
        yield ()
      end zcommand
    case class SendTestEmail( from : String, to : String ) extends CommandConfig:
      override def zcommand : ZCommand =
        for
          as <- ZIO.service[AppSetup]
          _  <- ZIO.attemptBlocking:
                      given Smtp.Context = as.smtpContext
                      Smtp.sendSimplePlaintext("This is a test message from feedletter.", subject="FEEDLETTER TEST MESSAGE", from=from, to=to )
          _  <- Console.printLine( s"Test email sent from '$from' to '$to'." )
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
          _  <- PgDatabase.addSubscription( ds, aso.feedUrl, aso.subscribableName, aso.destination )
        yield ()
      end zcommand
  object Crank:
    case object Assign extends CommandConfig:
      override def zcommand : ZCommand =
        for
          ds <- ZIO.service[DataSource]
          _ <- PgDatabase.ensureDb( ds )
          _ <- PgDatabase.updateAssignItems(ds)
        yield ()
      end zcommand
    case object Complete extends CommandConfig:
      override def zcommand : ZCommand =
        for
          ds <- ZIO.service[DataSource]
          _ <- PgDatabase.ensureDb( ds )
          _ <- PgDatabase.completeAssignables( ds )
        yield ()
      end zcommand
    case object SendMailGroup extends CommandConfig:
      override def zcommand : ZCommand =
        for
          as <- ZIO.service[AppSetup]
          ds <- ZIO.service[DataSource]
          _ <- PgDatabase.ensureDb( ds )
          _ <- PgDatabase.forceMailNextGroup( ds, as.smtpContext )
        yield ()
      end zcommand
  object Db:
    case object Dump extends CommandConfig:
      override def zcommand : ZCommand =
        for
          ds  <- ZIO.service[DataSource]
          out <- PgDatabase.dump(ds)
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
      override def zcommand : ZCommand =
        def doMigrate( ds : DataSource ) = if force then PgDatabase.migrate(ds) else PgDatabase.cautiousMigrate(ds)
        for
          ds <- ZIO.service[DataSource]
          _  <- doMigrate( ds )
        yield ()
      end zcommand
  end Db
  case object Daemon extends CommandConfig:
      override def zcommand : ZCommand =
        for
          as <- ZIO.service[AppSetup]
          ds <- ZIO.service[DataSource]
          _  <- com.mchange.feedletter.Daemon.cyclingRetryingUpdateAssignComplete( ds ).fork
          _  <- com.mchange.feedletter.Daemon.cyclingRetryingMailNextGroupIfDue( ds, as.smtpContext ).fork
          _  <- ZIO.unit.forever
        yield ()
      end zcommand
sealed trait CommandConfig:
  def zcommand : ZCommand
