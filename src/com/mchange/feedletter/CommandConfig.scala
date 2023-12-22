package com.mchange.feedletter

import zio.*

import com.mchange.feedletter.db.{DbVersionStatus,PgDatabase}

import java.nio.file.{Path as JPath}
import javax.sql.DataSource

import com.mchange.conveniences.throwable.*

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
    case class ComposeUntemplateSingle(
      subscribableName : SubscribableName,
      selection        : ComposeSelection.Single,
      destination      : Option[Destination],
      withinTypeId     : Option[String],
      port             : Int ) extends CommandConfig:
      def digest( feedUrl : FeedUrl ) : FeedDigest =
        val digest = FeedDigest( feedUrl )
        if digest.isEmpty then
          throw new NoExampleItems(s"We can't compose against feed '$feedUrl', because it has no example items to render.")
        digest
      def guid( digest : FeedDigest ) : Guid = 
        selection match
          case ComposeSelection.Single.First  =>
            digest.orderedGuids.head
          case ComposeSelection.Single.Random =>
            val n = scala.util.Random.nextInt( digest.orderedGuids.size )
            digest.orderedGuids(n)
          case ComposeSelection.Single.Guid( guid ) =>
            guid
      def untemplateName( stype : SubscriptionType ) : String =
        stype match
          case stu : SubscriptionType.Untemplated => stu.untemplateName
          //case _ => // XXX: this gives an unreachable code warning, because for now all subscription types are Untemplated. But the may not always be!
          //  throw new InvalidSubscriptionType(s"Subscription '${subscribableName}' does not render through an untemplate, cannot compose: $stype")

      override def zcommand : ZCommand =
        for
          ds       <- ZIO.service[DataSource]
          _        <- PgDatabase.ensureDb( ds )
          pair     <- PgDatabase.feedUrlSubscriptionTypeForSubscribableName( ds, subscribableName )
          fu       =  pair(0)
          stype    =  pair(1)
          dig      =  digest( fu )
          g        =  guid( dig )
          un       = untemplateName(stype)
          _        <- serveComposeSingleUntemplate(
                        un,
                        subscribableName,
                        stype,
                        withinTypeId.getOrElse( stype.sampleWithinTypeId ),
                        destination.getOrElse( stype.sampleDestination ),
                        fu,
                        dig,
                        g,
                        port
                      ).fork
          _       <- INFO.zlog( s"HTTP Server started on port ${port}" )
          _       <- ZIO.unit.forever
        yield ()
      end zcommand
    case class DefineEmailSubscription(
      feedId           : FeedId,
      subscribableName : SubscribableName,
      from             : String,
      replyTo          : Option[String],
      mbUntemplateName : Option[String],
      stypeFactory     : SubscriptionType.Factory,
      extraParams      : Map[String,String]
    ) extends CommandConfig:
      override def zcommand : ZCommand =
        val untemplateName =
          stypeFactory match
            case SubscriptionType.Email.Each   => mbUntemplateName.getOrElse( Default.UntemplateSingle )
            case SubscriptionType.Email.Weekly => mbUntemplateName.getOrElse( Default.UntemplateMultiple )

        val params = (Seq( ("from", from) ) ++ replyTo.map( rt => ("replyTo",rt) ) :+ ("untemplateName", untemplateName)) ++ extraParams.toSeq
        val subscriptionType = stypeFactory(params)
        for
          ds   <- ZIO.service[DataSource]
          _    <- PgDatabase.ensureDb( ds )
          _    <- PgDatabase.addSubscribable( ds, subscribableName, feedId, subscriptionType )
          tups <- PgDatabase.listSubscribables(ds)
          _    <- printSubscribablesTable(tups)
          _    <- Console.printLine(s"An email subscribable to feed with ID '${feedId}' named '${subscribableName}' has been created.")
        yield ()
      end zcommand
    case class EditSubscriptionDefinition( name : SubscribableName ) extends CommandConfig:
      override def zcommand : ZCommand =
        def withTemp[T]( op : os.Path => Task[T] ) : Task[T] =
          ZIO.acquireReleaseWith( ZIO.attemptBlocking( os.temp() ) )( p => ZIO.succeedBlocking( try os.remove(p) catch NonFatals.PrintStackTrace ))(op)
        for
          ds         <- ZIO.service[DataSource]
          _          <- PgDatabase.ensureDb( ds )
          stypeRep   <- PgDatabase.subscriptionTypeRepForSubscribableName( ds, name )
          editFormat =  SubscriptionType.toEditFormatLenient( stypeRep )
          updated    <- withTemp { temp =>
                          for
                            _        <- ZIO.attemptBlocking( os.write.over(temp, editFormat) )
                            editor   =  sys.env.get("EDITOR").getOrElse:
                                          throw new EditorNotDefined("Please define environment variable EDITOR if you wish to edit a subscription.")
                            ec       <- ZIO.attemptBlocking( os.proc(List(editor,temp.toString)).call(stdin=os.Inherit,stdout=os.Inherit,stderr=os.Inherit).exitCode )
                            contents <- ZIO.attemptBlocking( os.read( temp ) )
                            updated  =  SubscriptionType.fromEditFormat( contents )
                          yield updated
                      }
          _           <- PgDatabase.updateSubscriptionType( ds, name, updated )
          _           <- Console.printLine(s"Subscription '$name' successfully updated.")
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
    case object ListComposeUntemplates extends CommandConfig:
      override def zcommand : ZCommand =
        for
          _ <- printUntemplatesTable( ComposeUntemplates )
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
          _  <- PgDatabase.addSubscription( ds, aso.subscribableName, aso.destination )
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
