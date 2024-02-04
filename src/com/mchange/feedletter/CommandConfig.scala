package com.mchange.feedletter

import com.mchange.feedletter.style.*

import zio.*

import com.mchange.feedletter.db.{DbVersionStatus,PgDatabase}
import com.mchange.feedletter.style.AllUntemplates

import java.nio.file.{Path as JPath}
import javax.sql.DataSource
import java.time.ZoneId

import com.mchange.conveniences.throwable.*
import com.mchange.conveniences.collection.*

import com.mchange.mailutil.*

import untemplate.Untemplate

import MLevel.*

object CommandConfig extends SelfLogging:
  case class AddFeed( nf : NascentFeed ) extends CommandConfig:
    override def zcommand : ZCommand =
      for
        ds  <- ZIO.service[DataSource]
        _   <- PgDatabase.ensureDb( ds )
        fis <- PgDatabase.addFeed( ds, nf )
        _   <- printFeedInfoTable(fis)
      yield ()
    end zcommand
  case class AlterFeed( fts : FeedTimings ) extends CommandConfig:
    def ifSomethingToDo( ds : DataSource ) =
      val timings = fts._2 :: fts._3 :: fts._4 :: fts._5 :: Nil
      if timings.actuals.isEmpty then
        ZIO.attempt( println( s"Nothing to do, feed ${fts.feedId} is unchanged." ) )
      else
        PgDatabase.mergeFeedTimings( ds, fts ) *> ZIO.attempt( println( s"Feed ${fts.feedId} has been updated." ) )
    override def zcommand : ZCommand =
      for
        ds  <- ZIO.service[DataSource]
        _   <- PgDatabase.ensureDb( ds )
        _   <- ifSomethingToDo(ds)
      yield ()
    end zcommand
  case class Daemon( fork : Boolean ) extends CommandConfig:
    def logMore() =
      import java.util.logging.{Level,LogManager,Logger}
      val FeedletterLevel = Level.FINER // XXX: Someday, bring this up to Level.INFO
      val rootLogger = LogManager.getLogManager().getLogger("")
      val packageLogger = Logger.getLogger( "com.mchange.feedletter" )
      rootLogger.getHandlers().foreach( _.setLevel(Level.FINEST) )
      packageLogger.setLevel(FeedletterLevel)
    /*
    // we let the parent mill process write and sysd remove PID files now
    def writePidFile( pidf : os.Path ) =
      val contents = s"${ProcessHandle.current().pid()}${LineSep}"
      os.write(pidf, contents)
      val onShutdown =
        new Thread:
          override def run() : Unit =
            INFO.log(s"Shutdown Hook: Removing PID file '${pidf}'")
            os.remove( pidf )
      java.lang.Runtime.getRuntime().addShutdownHook(onShutdown)
    */
    override def zcommand : ZCommand =
      val task =
        for
          as <- ZIO.service[AppSetup]
          ds <- ZIO.service[DataSource]
          _  <- PgDatabase.ensureDb( ds )
          // _  <- if fork then ZIO.attempt( writePidFile(as.pidFile) ) else ZIO.unit // we let the parent mill process write and sysd remove PID files now
          _  <- if as.loggingConfig == LoggingConfig.Default then ZIO.attempt( logMore() ) else ZIO.unit
          _  <- com.mchange.feedletter.Daemon.startup( ds, as )
          _  <- SEVERE.zlog( "Unexpected successful completion of perpetual daemon!" ) // this is a bit ridiculous, but we're seeing unexpected daemon process exits
          _  <- ZIO.fail( new UnexpectedDaemonTermination( "Perpetual daemon task appears to have succeddfully terminated!" ) ) // this too!
        yield ()
      task.zlogErrorDefect(WARNING, what = "Main daemon")
    end zcommand
  case object DbDump extends CommandConfig:
    override def zcommand : ZCommand =
      for
        ds  <- ZIO.service[DataSource]
        out <- PgDatabase.dump(ds)
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
    override def zcommand : ZCommand =
      def doMigrate( ds : DataSource ) = if force then PgDatabase.migrate(ds) else PgDatabase.cautiousMigrate(ds)
      for
        ds <- ZIO.service[DataSource]
        _  <- doMigrate( ds )
      yield ()
    end zcommand
  case class DefineEmailSubscribable[T](
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
        tup  <- PgDatabase.addSubscribable( ds, subscribableName, feedId, subscriptionManager )
        _    <- printSubscribable(tup)
        _    <- Console.printLine(s"An email subscribable to feed with ID '${feedId}' named '${subscribableName}' has been created.")
      yield ()
    end zcommand
  case class DefineMastodonSubscribable(
    feedId                        : FeedId,
    subscribableName              : SubscribableName,
    extraParams                   : Map[String,String]
  ) extends CommandConfig:
    override def zcommand : ZCommand =
      val subscriptionManager = SubscriptionManager.Mastodon.Announce( extraParams )
      for
        ds   <- ZIO.service[DataSource]
        _    <- PgDatabase.ensureDb( ds )
        tup  <- PgDatabase.addSubscribable( ds, subscribableName, feedId, subscriptionManager )
        _    <- printSubscribable(tup)
        _    <- Console.printLine(s"An email subscribable to feed with ID '${feedId}' named '${subscribableName}' has been created.")
      yield ()
    end zcommand
  case class DropFeedAndSubscribables( feedId : FeedId, removeSubscriptions : Boolean ) extends CommandConfig:
    override def zcommand : ZCommand =
      for
        ds   <- ZIO.service[DataSource]
        _    <- PgDatabase.ensureDb( ds )
        _    <- PgDatabase.removeFeedAndSubscribables(ds, feedId, removeSubscriptions)
        _    <- Console.printLine(s"Feed with ID '${feedId.toInt}' and any subscribables perhaps defined upon it have been permanently removed.")
      yield ()
  case class DropSubscribable( name : SubscribableName, removeSubscriptions : Boolean ) extends CommandConfig:
    override def zcommand : ZCommand =
      for
        ds   <- ZIO.service[DataSource]
        _    <- PgDatabase.ensureDb( ds )
        _    <- if removeSubscriptions then PgDatabase.removeSubscribable( ds, name, true ) else PgDatabase.cautiousRemoveSubscribable(ds, name)
        _    <- Console.printLine(s"Subscribable '$name' has been permanently removed.")
      yield ()
  case class EditSubscribable( name : SubscribableName ) extends CommandConfig:
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
  case class ExportSubscribers(subscribableName : SubscribableName) extends CommandConfig:
    override def zcommand : ZCommand =
      for
        ds   <- ZIO.service[DataSource]
        _    <- PgDatabase.ensureDb( ds )
        sman <- PgDatabase.subscriptionManagerForSubscribableName( ds, subscribableName )
        tups <- PgDatabase.subscriptionsForSubscribableName(ds, subscribableName)
        _    <- printSubscriptionsCsv( sman, tups.map( _(1) ) )
      yield ()
    end zcommand
  case object ListConfig extends CommandConfig:
    override def zcommand : ZCommand =
      for
        ds   <- ZIO.service[DataSource]
        _    <- PgDatabase.ensureDb( ds )
        tups <- PgDatabase.reportAllConfigKeysStringified( ds )
        _    <- printConfigurationTuplesTable(tups)
      yield ()
    end zcommand
  case class ListUntemplates( group : Map[String,Untemplate.AnyUntemplate] ) extends CommandConfig:
    override def zcommand : ZCommand =
      for
        _ <- printUntemplatesTable( group )
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
  case class ListSubscribers(subscribableName : SubscribableName, fullDesc : Option[Boolean]) extends CommandConfig:
    def s( d : Destination ) = fullDesc.fold(d.defaultDesc)( fd => if fd then d.fullDesc else d.shortDesc )
    override def zcommand : ZCommand =
      for
        ds   <- ZIO.service[DataSource]
        _    <- PgDatabase.ensureDb( ds )
        tups <- PgDatabase.subscriptionsForSubscribableName(ds, subscribableName)
        _    <- printSubscriptions( tups.map( ( sid, d, c, a ) => ( sid, s(d), c, a ) ) ) 
      yield ()
    end zcommand
  case class ListSubscribables( verbose : Boolean ) extends CommandConfig:
    override def zcommand : ZCommand =
      for
        ds   <- ZIO.service[DataSource]
        _    <- PgDatabase.ensureDb( ds )
        tups <- PgDatabase.listSubscribables(ds)
        _    <- if verbose then printSubscribables(tups) else printSubscribableNamesTable( tups.map( _(0).str ) )
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
  case class SetExtraParams( subscribableName : SubscribableName, extraParams : Map[String,String], removals : List[String] ) extends CommandConfig:
    def updateSubscriptionManager( sman : SubscriptionManager ) : SubscriptionManager =
      val currentParams = sman.extraParams
      val newParams = (currentParams -- removals) ++ extraParams
      sman.withExtraParams(newParams)
    override def zcommand : ZCommand =
      for
        ds      <- ZIO.service[DataSource]
        _       <- PgDatabase.ensureDb( ds )
        sman    <- PgDatabase.subscriptionManagerForSubscribableName(ds, subscribableName)
        updated <- ZIO.attempt( updateSubscriptionManager( sman ) )
        _       <- PgDatabase.updateSubscriptionManagerJson( ds, subscribableName, updated )
        _       <- ZIO.attempt( println("Updated Subscription Manager: " + updated.jsonPretty) )
      yield ()
    end zcommand
  case class SetUntemplates(
    subscribableName                  : SubscribableName,
    composeUntemplateName             : Option[String],
    confirmUntemplateName             : Option[String],
    removalNotificationUntemplateName : Option[String],
    statusChangeUntemplateName        : Option[String]
  ) extends CommandConfig:
    override def zcommand : ZCommand =
      def updateSubscriptionManager( sman : SubscriptionManager ) : SubscriptionManager =
        val composed = composeUntemplateName.fold(sman): cun =>
          sman match
            case uc : SubscriptionManager.UntemplatedCompose =>
              // verify the named untemplate exists, throw if not found or inappropriate
              if uc.isComposeMultiple then AllUntemplates.findComposeUntemplateMultiple(cun) else AllUntemplates.findComposeUntemplateSingle(cun)
              uc.withComposeUntemplateName( cun )
            case _ =>
              val msg = s"Subscribable '${subscribableName}' does not make use of a compose untemplate, can't set its compose untemplate name to '${cun}'."
              throw new UnsupportedUntemplateRole(msg)  
        val confirmed = confirmUntemplateName.fold(composed): cun =>
          composed match
            case uc : SubscriptionManager.UntemplatedConfirm =>
              // verify the named untemplate exists, throw if not found or inappropriate
              AllUntemplates.findConfirmUntemplate( cun )
              uc.withConfirmUntemplateName( cun )
            case _ =>
              val msg = s"Subscribable '${subscribableName}' does not make use of a confirm untemplate, can't set its confirm untemplate name to '${cun}'."
              throw new UnsupportedUntemplateRole(msg)  
        val rned = removalNotificationUntemplateName.fold(confirmed): rnun =>
          confirmed match
            case urn : SubscriptionManager.UntemplatedRemovalNotification =>
              // verify the named untemplate exists, throw if not found or inappropriate
              AllUntemplates.findRemovalNotificationUntemplate( rnun )
              urn.withRemovalNotificationUntemplateName( rnun )
            case _ =>
              val msg = s"Subscribable '${subscribableName}' does not make use of a removal-notification untemplate, can't set its removal-notification untemplate name to '${rnun}'."
              throw new UnsupportedUntemplateRole(msg)  
        val sced = statusChangeUntemplateName.fold(rned): scun =>
          rned match
            case usc : SubscriptionManager.UntemplatedStatusChange =>
              // verify the named untemplate exists, throw if not found or inappropriate
              AllUntemplates.findStatusChangeUntemplate( scun )
              usc.withStatusChangeUntemplateName( scun )
            case _ =>
              val msg = s"Subscribable '${subscribableName}' does not make use of a status-change untemplate, can't set its status-change untemplate name to '${scun}'."
              throw new UnsupportedUntemplateRole(msg)
        sced
      end updateSubscriptionManager
      for
        ds      <- ZIO.service[DataSource]
        _       <- PgDatabase.ensureDb( ds )
        sman    <- PgDatabase.subscriptionManagerForSubscribableName(ds, subscribableName)
        updated <- ZIO.attempt( updateSubscriptionManager( sman ) )
        _       <- PgDatabase.updateSubscriptionManagerJson( ds, subscribableName, updated )
        _       <- ZIO.attempt( println("Updated Subscription Manager: " + updated.jsonPretty) )
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
      styleDest              : StyleDest
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
          as       <- ZIO.service[AppSetup]
          _        <- PgDatabase.ensureDb( ds )
          pair     <- PgDatabase.feedUrlSubscriptionManagerForSubscribableName( ds, subscribableName )
          fu       =  pair(0)
          sman     =  pair(1)
          dig      =  digest( fu )
          g        =  guid( dig )
          un       = untemplateNameCompose(overrideUntemplateName, sman, subscribableName)
          tz       <- db.withConnectionTransactional(ds)( conn => sman.bestTimeZone( conn ) )
          _        <- serveOrMailComposeSingleUntemplate(
                        un,
                        subscribableName,
                        sman,
                        withinTypeId.getOrElse( sman.sampleWithinTypeId ),
                        destination.map(sman.narrowDestinationOrThrow).getOrElse(sman.sampleDestination),
                        tz,
                        fu,
                        dig,
                        g,
                        styleDest,
                        as
                      )
        yield ()
      end zcommand
    end ComposeUntemplateSingle
    case class ComposeUntemplateMultiple(
      subscribableName       : SubscribableName,
      overrideUntemplateName : Option[String],
      selection              : ComposeSelection.Multiple,
      destination            : Option[Destination],
      withinTypeId           : Option[String],
      styleDest              : StyleDest
    ) extends CommandConfig:
      def digest( feedUrl : FeedUrl ) : FeedDigest =
        val digest = FeedDigest( feedUrl )
        if digest.isEmpty then
          throw new NoExampleItems(s"We can't compose against feed '$feedUrl', because it has no example items to render.")
        digest
      def guids( digest : FeedDigest ) : Seq[Guid] = 
        selection match
          case ComposeSelection.Multiple.First(n)  =>
            digest.fileOrderedGuids.take(n).toSeq
          case ComposeSelection.Multiple.Random(n) =>
            val keepers = scala.util.Random.shuffle( digest.fileOrderedGuids ).take(n).toSet
            digest.fileOrderedGuids.filter( keepers )
          case ComposeSelection.Multiple.Guids( values ) =>
            values
      override def zcommand : ZCommand =
        for
          ds       <- ZIO.service[DataSource]
          as       <- ZIO.service[AppSetup]
          _        <- PgDatabase.ensureDb( ds )
          pair     <- PgDatabase.feedUrlSubscriptionManagerForSubscribableName( ds, subscribableName )
          fu       =  pair(0)
          sman     =  pair(1)
          dig      =  digest( fu )
          _        <- if dig.fileOrderedGuids.isEmpty then ZIO.fail( new NoExampleItems( s"Feed currently contains no example items to render: ${fu}" ) ) else ZIO.unit
          gs       =  guids( dig )
          _        <- if gs.isEmpty then ZIO.fail( new NoExampleItems( s"${selection} yields no example items to render. Feed size: ${dig.fileOrderedGuids.size}" ) ) else ZIO.unit
          un       =  untemplateNameCompose(overrideUntemplateName, sman, subscribableName)
          tz       <- db.withConnectionTransactional(ds)( conn => sman.bestTimeZone( conn ) )
          _        <- serveOrMailComposeMultipleUntemplate(
                        un,
                        subscribableName,
                        sman,
                        withinTypeId.getOrElse( sman.sampleWithinTypeId ),
                        destination.map(sman.narrowDestinationOrThrow).getOrElse(sman.sampleDestination),
                        tz,
                        fu,
                        dig,
                        gs,
                        styleDest,
                        as
                      )
        yield ()
      end zcommand
    end ComposeUntemplateMultiple
    case class Confirm( subscribableName : SubscribableName, overrideUntemplateName : Option[String], destination : Option[Destination], styleDest : StyleDest ) extends CommandConfig:
      override def zcommand : ZCommand =
        for
          ds       <- ZIO.service[DataSource]
          as       <- ZIO.service[AppSetup]
          _        <- PgDatabase.ensureDb( ds )
          pair     <- PgDatabase.feedUrlSubscriptionManagerForSubscribableName( ds, subscribableName )
          fu       =  pair(0)
          sman     =  pair(1)
          un       = untemplateNameConfirm(overrideUntemplateName, sman, subscribableName)
          ch       <- PgDatabase.confirmHours( ds )
          _        <- serveOrMailConfirmUntemplate(
                        un,
                        subscribableName,
                        sman,
                        destination.map(sman.narrowDestinationOrThrow).getOrElse(sman.sampleDestination),
                        fu,
                        ch,
                        styleDest,
                        as
                      )
        yield ()
      end zcommand
    case class RemovalNotification( subscribableName : SubscribableName, overrideUntemplateName : Option[String], destination : Option[Destination], styleDest : StyleDest ) extends CommandConfig:
      override def zcommand : ZCommand =
        for
          ds       <- ZIO.service[DataSource]
          as       <- ZIO.service[AppSetup]
          _        <- PgDatabase.ensureDb( ds )
          pair     <- PgDatabase.feedUrlSubscriptionManagerForSubscribableName( ds, subscribableName )
          fu       =  pair(0)
          sman     =  pair(1)
          un       =  untemplateNameRemovalNotification(overrideUntemplateName, sman, subscribableName)
          _        <- serveOrMailRemovalNotificationUntemplate(
                        un,
                        subscribableName,
                        sman,
                        destination.map(sman.narrowDestinationOrThrow).getOrElse(sman.sampleDestination),
                        styleDest,
                        as
                      )
        yield ()
      end zcommand
    case class StatusChange(
      statusChange           : SubscriptionStatusChange,
      subscribableName       : SubscribableName,
      overrideUntemplateName : Option[String],
      destination            : Option[Destination],
      requiresConfirmation   : Boolean,
      styleDest              : StyleDest
    ) extends CommandConfig:
      override def zcommand : ZCommand =
        for
          ds       <- ZIO.service[DataSource]
          as       <- ZIO.service[AppSetup]
          _        <- PgDatabase.ensureDb( ds )
          pair     <- PgDatabase.feedUrlSubscriptionManagerForSubscribableName( ds, subscribableName )
          fu       =  pair(0)
          sman     =  pair(1)
          un       = untemplateNameStatusChange(overrideUntemplateName, sman, subscribableName)
          _        <- serveOrMailStatusChangeUntemplate(
                        un,
                        statusChange,
                        subscribableName,
                        sman,
                        destination.map(sman.narrowDestinationOrThrow).getOrElse(sman.sampleDestination),
                        requiresConfirmation,
                        styleDest,
                        as
                      )
        yield ()
      end zcommand
sealed trait CommandConfig:
  def zcommand : ZCommand
