package com.mchange.feedletter

import zio.*

import com.github.plokhotnyuk.jsoniter_scala.{core as jsoniter}

import com.mchange.feedletter.db.{DbVersionStatus,PgDatabase}

import java.nio.file.{Path as JPath}
import javax.sql.DataSource
import java.time.ZoneId

import com.mchange.conveniences.throwable.*

import com.mchange.mailutil.*

import MLevel.*

object CommandConfig extends SelfLogging:

  object Admin:
    case class AddFeed( nf : NascentFeed ) extends CommandConfig:
      override def zcommand : ZCommand =
        for
          ds  <- ZIO.service[DataSource]
          _   <- PgDatabase.ensureDb( ds )
          fis <- PgDatabase.addFeed( ds, nf )
          _   <- printFeedInfoTable(fis)
        yield ()
      end zcommand
    case class DefineEmailSubscription[T](
      feedId                               : FeedId,
      subscribableName                     : SubscribableName,
      from                                 : String,
      replyTo                              : Option[String],
      mbComposeUntemplateName              : Option[String],
      mbConfirmUntemplateName              : Option[String],
      mbRemovalNotificationUntemplateName  : Option[String],
      mbStatusChangeUntemplateName         : Option[String],
      emailCompanionAndArg                 : (SubscriptionManager.Email.Companion, Option[T]),
      extraParams                          : Map[String,String]
    ) extends CommandConfig:
      override def zcommand : ZCommand =
        val confirmUntemplateName             = mbConfirmUntemplateName.getOrElse( Default.Email.ConfirmUntemplate )
        val statusChangeUntemplateName        = mbStatusChangeUntemplateName.getOrElse( Default.Email.StatusChangeUntemplate )
        val removalNotificationUntemplateName = mbRemovalNotificationUntemplateName.getOrElse( Default.Email.RemovalNotificationUntemplate )

        val subscriptionManager =
          import SubscriptionManager.{Email as SMEM}
          emailCompanionAndArg match
            case (SMEM.Each, None) =>
              val composeUntemplateName = mbComposeUntemplateName.getOrElse( Default.Email.ComposeUntemplateSingle )
              SMEM.Each (
                from = Destination.Email(from),
                replyTo = replyTo.map( Destination.Email.apply ),
                composeUntemplateName = composeUntemplateName,
                confirmUntemplateName = confirmUntemplateName,
                statusChangeUntemplateName = statusChangeUntemplateName,
                removalNotificationUntemplateName = removalNotificationUntemplateName,
                extraParams = extraParams
              )
            case (SMEM.Weekly, tz : Option[ZoneId @unchecked]) => // we'll check ourselves
              tz.foreach( z => assert( z.isInstanceOf[ZoneId], "Only an Option[ZoneId] should be passed as the extra argument for Email.Weekly. Found '$z'." ) )
              val composeUntemplateName = mbComposeUntemplateName.getOrElse( Default.Email.ComposeUntemplateMultiple )
              SMEM.Weekly (
                from = Destination.Email(from),
                replyTo = replyTo.map( Destination.Email.apply ),
                composeUntemplateName = composeUntemplateName,
                confirmUntemplateName = confirmUntemplateName,
                statusChangeUntemplateName = statusChangeUntemplateName,
                removalNotificationUntemplateName = removalNotificationUntemplateName,
                timeZone = tz,
                extraParams = extraParams
              )
            case (SMEM.Daily, tz : Option[ZoneId @unchecked]) => // we'll check ourselves
              tz.foreach( z => assert( z.isInstanceOf[ZoneId], "Only an Option[ZoneId] should be passed as the extra argument for Email.Daily. Found '$z'." ) )
              val composeUntemplateName = mbComposeUntemplateName.getOrElse( Default.Email.ComposeUntemplateMultiple )
              SMEM.Daily (
                from = Destination.Email(from),
                replyTo = replyTo.map( Destination.Email.apply ),
                composeUntemplateName = composeUntemplateName,
                confirmUntemplateName = confirmUntemplateName,
                statusChangeUntemplateName = statusChangeUntemplateName,
                removalNotificationUntemplateName = removalNotificationUntemplateName,
                timeZone = tz,
                extraParams = extraParams
              )
            case (SMEM.Fixed, Some(nipl : Int)) =>
              val composeUntemplateName = mbComposeUntemplateName.getOrElse( Default.Email.ComposeUntemplateMultiple )
              SMEM.Fixed (
                from = Destination.Email(from),
                replyTo = replyTo.map( Destination.Email.apply ),
                composeUntemplateName = composeUntemplateName,
                confirmUntemplateName = confirmUntemplateName,
                statusChangeUntemplateName = statusChangeUntemplateName,
                removalNotificationUntemplateName = removalNotificationUntemplateName,
                numItemsPerLetter = nipl,
                extraParams = extraParams
              )
            case tup @ ( SMEM.Each, Some( whatev ) ) =>
              throw new AssertionError( s"Additional argument '$whatev' inconsistent with ${tup(0)}, which accepts no additional arguments." )
            case tup @ ( SMEM.Fixed | SMEM.Weekly | SMEM.Daily, Some( whatev ) ) =>
              throw new AssertionError( s"Additional argument '$whatev' inconsistent with ${tup(0)}, not of expected type." )
            case tup @ ( SMEM.Fixed, None ) =>
              throw new AssertionError( s"Missing additional argument (numItemsPerLetter : Int) expected for ${tup(0)}" )

        for
          ds   <- ZIO.service[DataSource]
          _    <- PgDatabase.ensureDb( ds )
          _    <- PgDatabase.addSubscribable( ds, subscribableName, feedId, subscriptionManager )
          tups <- PgDatabase.listSubscribables(ds)
          _    <- printSubscribablesTable(tups)
          _    <- Console.printLine(s"An email subscribable to feed with ID '${feedId}' named '${subscribableName}' has been created.")
        yield ()
      end zcommand
    case class DefineMastodonSubscription(
      feedId                        : FeedId,
      subscribableName              : SubscribableName,
      extraParams                   : Map[String,String]
    ) extends CommandConfig:
      override def zcommand : ZCommand =
        val subscriptionManager = SubscriptionManager.Mastodon.Announce( extraParams )
        for
          ds   <- ZIO.service[DataSource]
          _    <- PgDatabase.ensureDb( ds )
          _    <- PgDatabase.addSubscribable( ds, subscribableName, feedId, subscriptionManager )
          tups <- PgDatabase.listSubscribables(ds)
          _    <- printSubscribablesTable(tups)
          _    <- Console.printLine(s"An email subscribable to feed with ID '${feedId}' named '${subscribableName}' has been created.")
        yield ()
      end zcommand
    case class EditSubscriptionDefinition( name : SubscribableName ) extends CommandConfig:
      def prettifyJson( jsonStr : String ) : String =
        import upickle.default.*
        write( read[ujson.Value]( jsonStr ), indent = 4 )
      override def zcommand : ZCommand =
        def withTemp[T]( op : os.Path => Task[T] ) : Task[T] =
          ZIO.acquireReleaseWith( ZIO.attemptBlocking( os.temp() ) )( p => ZIO.succeedBlocking( try os.remove(p) catch NonFatals.PrintStackTrace ))(op)
        for
          ds         <- ZIO.service[DataSource]
          _          <- PgDatabase.ensureDb( ds )
          smanJson   <- PgDatabase.uninterpretedManagerJsonForSubscribableName( ds, name )
          updated    <- withTemp { temp =>
                          for
                            _        <- ZIO.attemptBlocking( os.write.over(temp, prettifyJson(smanJson)) )
                            editor   =  sys.env.get("EDITOR").getOrElse:
                                          throw new EditorNotDefined("Please define environment variable EDITOR if you wish to edit a subscription.")
                            ec       <- ZIO.attemptBlocking( os.proc(List(editor,temp.toString)).call(stdin=os.Inherit,stdout=os.Inherit,stderr=os.Inherit).exitCode )
                            contents <- ZIO.attemptBlocking( SubscriptionManager.Json( os.read( temp ) ) )
                            updated  <- ZIO.attempt( SubscriptionManager.materialize( contents ) )
                          yield updated
                      }
          _           <- PgDatabase.updateSubscriptionManagerJson( ds, name, updated )
          _           <- Console.printLine(s"Subscription definition '$name' successfully updated.")
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
          _ <- printUntemplatesTable( AllUntemplates.compose )
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
          ds   <- ZIO.service[DataSource]
          _    <- PgDatabase.ensureDb( ds )
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
          _  <- PgDatabase.addSubscription( ds, false, aso.subscribableName, aso.destination, aso.confirmed, aso.now )
        yield ()
      end zcommand
  object Crank:
    case object Assign extends CommandConfig:
      override def zcommand : ZCommand =
        for
          ds <- ZIO.service[DataSource]
          _  <- PgDatabase.ensureDb( ds )
          _  <- PgDatabase.updateAssignItems(ds)
        yield ()
      end zcommand
    case object Complete extends CommandConfig:
      override def zcommand : ZCommand =
        for
          ds <- ZIO.service[DataSource]
          as <- ZIO.service[AppSetup]
          _  <- PgDatabase.ensureDb( ds )
          tapirApi <- com.mchange.feedletter.Daemon.tapirApi( ds, as )
          _  <- PgDatabase.completeAssignables( ds, tapirApi )
        yield ()
      end zcommand
    case object SendMailGroup extends CommandConfig:
      override def zcommand : ZCommand =
        for
          as <- ZIO.service[AppSetup]
          ds <- ZIO.service[DataSource]
          _  <- PgDatabase.ensureDb( ds )
          _  <- PgDatabase.mailNextGroup( ds, as.smtpContext )
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
          _  <- com.mchange.feedletter.Daemon.startup( ds, as )
        yield ()  
      end zcommand
  object Style:
    def untemplateNameCompose( overrideUntemplateName : Option[String], sman : SubscriptionManager, subscribableName : SubscribableName ) : String =
      overrideUntemplateName.getOrElse:
        sman match
          case stu : SubscriptionManager.UntemplatedCompose => stu.composeUntemplateName
          case _ => // XXX: this gives an unreachable code warning, because for now all subscription types are Untemplated. But the may not always be!
            throw new InvalidSubscriptionManager(s"Subscription '${subscribableName}' does not compose through an untemplate, cannot style: $sman")
    def untemplateNameConfirm( overrideUntemplateName : Option[String], sman : SubscriptionManager, subscribableName : SubscribableName ) : String =
      overrideUntemplateName.getOrElse:
        sman match
          case stu : SubscriptionManager.UntemplatedConfirm => stu.confirmUntemplateName
          case _ => // XXX: this gives an unreachable code warning, because for now all subscription types are Untemplated. But the may not always be!
            throw new InvalidSubscriptionManager(s"Subscription '${subscribableName}' does not render confirmations through an untemplate, cannot style: $sman")
    def untemplateNameStatusChange( overrideUntemplateName : Option[String], sman : SubscriptionManager, subscribableName : SubscribableName ) : String =
      overrideUntemplateName.getOrElse:
        sman match
          case stu : SubscriptionManager.UntemplatedStatusChange => stu.statusChangeUntemplateName
          case _ => // XXX: this gives an unreachable code warning, because for now all subscription types are Untemplated. But the may not always be!
            throw new InvalidSubscriptionManager(s"Subscription '${subscribableName}' does not render status changes through an untemplate, cannot style: $sman")
    def untemplateNameRemovalNotification( overrideUntemplateName : Option[String], sman : SubscriptionManager, subscribableName : SubscribableName ) : String =
      overrideUntemplateName.getOrElse:
        sman match
          case stu : SubscriptionManager.UntemplatedRemovalNotification => stu.removalNotificationUntemplateName
          case _ => // XXX: this gives an unreachable code warning, because for now all subscription types are Untemplated. But the may not always be!
            throw new InvalidSubscriptionManager(s"Subscription '${subscribableName}' does not render removal notifications through an untemplate, cannot style: $sman")
    case class ComposeUntemplateSingle(
      subscribableName       : SubscribableName,
      overrideUntemplateName : Option[String],
      selection              : ComposeSelection.Single,
      destination            : Option[Destination],
      withinTypeId           : Option[String],
      interface              : String,
      port                   : Int
    ) extends CommandConfig:
      def digest( feedUrl : FeedUrl ) : FeedDigest =
        val digest = FeedDigest( feedUrl )
        if digest.isEmpty then
          throw new NoExampleItems(s"We can't compose against feed '$feedUrl', because it has no example items to render.")
        digest
      def guid( digest : FeedDigest ) : Guid = 
        selection match
          case ComposeSelection.Single.First  =>
            digest.fileOrderedGuids.head
          case ComposeSelection.Single.Random =>
            val n = scala.util.Random.nextInt( digest.fileOrderedGuids.size )
            digest.fileOrderedGuids(n)
          case ComposeSelection.Single.Guid( guid ) =>
            guid
      override def zcommand : ZCommand =
        for
          ds       <- ZIO.service[DataSource]
          _        <- PgDatabase.ensureDb( ds )
          pair     <- PgDatabase.feedUrlSubscriptionManagerForSubscribableName( ds, subscribableName )
          fu       =  pair(0)
          sman     =  pair(1)
          dig      =  digest( fu )
          g        =  guid( dig )
          un       = untemplateNameCompose(overrideUntemplateName, sman, subscribableName)
          _        <- styleComposeSingleUntemplate(
                        un,
                        subscribableName,
                        sman,
                        withinTypeId.getOrElse( sman.sampleWithinTypeId ),
                        destination.map(sman.narrowDestinationOrThrow).getOrElse(sman.sampleDestination),
                        fu,
                        dig,
                        g,
                        interface,
                        port
                      ).fork
          _       <- INFO.zlog( s"HTTP Server started on interface '${interface}', port ${port}" )
          _       <- ZIO.never
        yield ()
      end zcommand
    end ComposeUntemplateSingle
    case class ComposeUntemplateMultiple(
      subscribableName       : SubscribableName,
      overrideUntemplateName : Option[String],
      selection              : ComposeSelection.Multiple,
      destination            : Option[Destination],
      withinTypeId           : Option[String],
      interface              : String,
      port                   : Int ) extends CommandConfig:
      def digest( feedUrl : FeedUrl ) : FeedDigest =
        val digest = FeedDigest( feedUrl )
        if digest.isEmpty then
          throw new NoExampleItems(s"We can't compose against feed '$feedUrl', because it has no example items to render.")
        digest
      def guids( digest : FeedDigest ) : Set[Guid] = 
        selection match
          case ComposeSelection.Multiple.First(n)  =>
            digest.fileOrderedGuids.take(n).toSet
          case ComposeSelection.Multiple.Random(n) =>
            scala.util.Random.shuffle( digest.fileOrderedGuids ).take(n).toSet
          case ComposeSelection.Multiple.Guids( values ) =>
            values
      override def zcommand : ZCommand =
        for
          ds       <- ZIO.service[DataSource]
          _        <- PgDatabase.ensureDb( ds )
          pair     <- PgDatabase.feedUrlSubscriptionManagerForSubscribableName( ds, subscribableName )
          fu       =  pair(0)
          sman     =  pair(1)
          dig      =  digest( fu )
          _        <- if dig.fileOrderedGuids.isEmpty then ZIO.fail( new NoExampleItems( s"Feed currently contains no example items to render: ${fu}" ) ) else ZIO.unit
          gs       =  guids( dig )
          _        <- if gs.isEmpty then ZIO.fail( new NoExampleItems( s"${selection} yields no example items to render. Feed size: ${dig.fileOrderedGuids.size}" ) ) else ZIO.unit
          un       = untemplateNameCompose(overrideUntemplateName, sman, subscribableName)
          _        <- styleComposeMultipleUntemplate(
                        un,
                        subscribableName,
                        sman,
                        withinTypeId.getOrElse( sman.sampleWithinTypeId ),
                        destination.map(sman.narrowDestinationOrThrow).getOrElse(sman.sampleDestination),
                        fu,
                        dig,
                        gs,
                        interface,
                        port
                      ).fork
          _       <- INFO.zlog( s"HTTP Server started on interface '${interface}', port ${port}" )
          _       <- ZIO.never
        yield ()
      end zcommand
    end ComposeUntemplateMultiple
    case class Confirm( subscribableName : SubscribableName, overrideUntemplateName : Option[String], destination : Option[Destination], interface : String, port : Int ) extends CommandConfig:
      override def zcommand : ZCommand =
        for
          ds       <- ZIO.service[DataSource]
          _        <- PgDatabase.ensureDb( ds )
          pair     <- PgDatabase.feedUrlSubscriptionManagerForSubscribableName( ds, subscribableName )
          fu       =  pair(0)
          sman     =  pair(1)
          un       = untemplateNameConfirm(overrideUntemplateName, sman, subscribableName)
          ch       <- PgDatabase.confirmHours( ds )
          _        <- styleConfirmUntemplate(
                        un,
                        subscribableName,
                        sman,
                        destination.map(sman.narrowDestinationOrThrow).getOrElse(sman.sampleDestination),
                        fu,
                        ch,
                        interface,
                        port
                      ).fork
          _       <- INFO.zlog( s"HTTP Server started on interface '${interface}', port ${port}" )
          _       <- ZIO.never
        yield ()
      end zcommand
    case class RemovalNotification( subscribableName : SubscribableName, overrideUntemplateName : Option[String], destination : Option[Destination], interface : String, port : Int ) extends CommandConfig:
      override def zcommand : ZCommand =
        for
          ds       <- ZIO.service[DataSource]
          _        <- PgDatabase.ensureDb( ds )
          pair     <- PgDatabase.feedUrlSubscriptionManagerForSubscribableName( ds, subscribableName )
          fu       =  pair(0)
          sman     =  pair(1)
          un       =  untemplateNameRemovalNotification(overrideUntemplateName, sman, subscribableName)
          _        <- styleRemovalNotificationUntemplate(
                        un,
                        subscribableName,
                        sman,
                        destination.map(sman.narrowDestinationOrThrow).getOrElse(sman.sampleDestination),
                        interface,
                        port
                      ).fork
          _       <- INFO.zlog( s"HTTP Server started on interface '${interface}', port ${port}" )
          _       <- ZIO.never
        yield ()
      end zcommand
    case class StatusChange(
      statusChange           : SubscriptionStatusChange,
      subscribableName       : SubscribableName,
      overrideUntemplateName : Option[String],
      destination            : Option[Destination],
      requiresConfirmation   : Boolean,
      interface              : String,
      port                   : Int
    ) extends CommandConfig:
      override def zcommand : ZCommand =
        for
          ds       <- ZIO.service[DataSource]
          _        <- PgDatabase.ensureDb( ds )
          pair     <- PgDatabase.feedUrlSubscriptionManagerForSubscribableName( ds, subscribableName )
          fu       =  pair(0)
          sman     =  pair(1)
          un       = untemplateNameStatusChange(overrideUntemplateName, sman, subscribableName)
          _        <- styleStatusChangeUntemplate(
                        un,
                        statusChange,
                        subscribableName,
                        sman,
                        destination.map(sman.narrowDestinationOrThrow).getOrElse(sman.sampleDestination),
                        requiresConfirmation,
                        interface,
                        port
                      ).fork
          _       <- INFO.zlog( s"HTTP Server started on interface '${interface}', port ${port}" )
          _       <- ZIO.never
        yield ()
      end zcommand
sealed trait CommandConfig:
  def zcommand : ZCommand
