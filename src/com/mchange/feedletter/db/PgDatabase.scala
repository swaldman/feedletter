package com.mchange.feedletter.db

import zio.*
import java.sql.{Connection,Timestamp}
import javax.sql.DataSource

import scala.collection.{immutable,mutable}
import scala.util.Using
import scala.util.control.NonFatal

import scala.math.Ordering.Implicits.infixOrderingOps

import java.sql.SQLException
import java.time.{Duration as JDuration,Instant,ZonedDateTime,ZoneId}
import java.time.temporal.ChronoUnit

import com.mchange.sc.v1.log.*
import MLevel.*

import audiofluidity.rss.util.formatPubDate
import com.mchange.feedletter.*
import com.mchange.mailutil.*
import com.mchange.cryptoutil.{*, given}

import com.mchange.feedletter.BuildInfo

object PgDatabase extends Migratory:
  private lazy given logger : MLogger = mlogger( this )

  val LatestSchema = PgSchema.V1
  override val targetDbVersion = LatestSchema.Version

  private def fetchMetadataValue( conn : Connection, key : MetadataKey ) : Option[String] =
    PgSchema.Unversioned.Table.Metadata.select( conn, key )

  private def zfetchMetadataValue( conn : Connection, key : MetadataKey ) : Task[Option[String]] =
    ZIO.attemptBlocking( PgSchema.Unversioned.Table.Metadata.select( conn, key ) )

  private def fetchDbName(conn : Connection) : Task[String] =
    ZIO.attemptBlocking:
      Using.resource(conn.createStatement()): stmt =>
        Using.resource(stmt.executeQuery("SELECT current_database()")): rs =>
          uniqueResult("select-current-database-name", rs)( _.getString(1) )

  private def fetchDumpDir(conn : Connection) : Task[os.Path] =
    for
      mbDumpDir <- Config.zfetchValue(conn, ConfigKey.DumpDbDir)
    yield
      os.Path( mbDumpDir.getOrElse( throw new ConfigurationMissing(ConfigKey.DumpDbDir) ) )

  override def fetchDumpDir( ds : DataSource ) : Task[os.Path] =
    withConnectionZIO(ds)( fetchDumpDir )

  override def dump(ds : DataSource) : Task[os.Path] =
    def runDump( dbName : String, dumpFile : os.Path ) : Task[Unit] =
      ZIO.attemptBlocking:
        val parsedCommand = List("pg_dump", dbName)
        os.proc( parsedCommand ).call( stdout = dumpFile )
    withConnectionZIO( ds ): conn =>
      for
        dbName   <- fetchDbName(conn)
        dumpDir  <- fetchDumpDir(conn)
        dumpFile <- Migratory.prepareDumpFileForInstant(dumpDir, java.time.Instant.now)
        _        <- runDump( dbName, dumpFile )
      yield dumpFile

  override def dbVersionStatus(ds : DataSource) : Task[DbVersionStatus] =
    withConnectionZIO( ds ): conn =>
      val okeyDokeyIsh =
        for
          mbDbVersion <- zfetchMetadataValue(conn, MetadataKey.SchemaVersion)
          mbCreatorAppVersion <- zfetchMetadataValue(conn, MetadataKey.CreatorAppVersion)
        yield
          try
            mbDbVersion.map( _.toInt ) match
              case Some( version ) if version == LatestSchema.Version => DbVersionStatus.Current( version )
              case Some( version ) if version < LatestSchema.Version => DbVersionStatus.OutOfDate( version, LatestSchema.Version )
              case Some( version ) => DbVersionStatus.UnexpectedVersion( Some(version.toString), mbCreatorAppVersion, Some( BuildInfo.version ), Some(LatestSchema.Version.toString()) )
              case None => DbVersionStatus.SchemaMetadataDisordered( s"Expected key '${MetadataKey.SchemaVersion}' was not found in schema metadata!" )
          catch
            case nfe : NumberFormatException =>
              DbVersionStatus.UnexpectedVersion( mbDbVersion, mbCreatorAppVersion, Some( BuildInfo.version ), Some(LatestSchema.Version.toString()) )
      okeyDokeyIsh.catchSome:
        case sqle : SQLException =>
          val dbmd = conn.getMetaData()
          try
            val rs = dbmd.getTables(null,null,PgSchema.Unversioned.Table.Metadata.Name,null)
            if !rs.next() then // the metadata table does not exist
              ZIO.succeed( DbVersionStatus.SchemaMetadataNotFound )
            else
              ZIO.succeed( DbVersionStatus.SchemaMetadataDisordered(s"Metadata table found, but an Exception occurred while accessing it: ${sqle.toString()}") )
          catch
            case t : SQLException =>
              WARNING.log("Exception while connecting to database.", t)
              ZIO.succeed( DbVersionStatus.ConnectionFailed )
  end dbVersionStatus

  override def upMigrate(ds : DataSource, from : Option[Int]) : Task[Unit] =
    def upMigrateFrom_New() : Task[Unit] =
      TRACE.log( "upMigrateFrom_New()" )
      withConnectionTransactional( ds ): conn =>
        PgSchema.Unversioned.Table.Metadata.create( conn )
        insertMetadataKeys(
          conn,
          (MetadataKey.SchemaVersion, "0"),
          (MetadataKey.CreatorAppVersion, BuildInfo.version)
        )

    def upMigrateFrom_0() : Task[Unit] =
      TRACE.log( "upMigrateFrom_0()" )
      withConnectionTransactional( ds ): conn =>
        Using.resource( conn.createStatement() ): stmt =>
          PgSchema.V1.Table.Config.create( stmt )
          PgSchema.V1.Table.Times.create( stmt )
          PgSchema.V1.Table.Feed.create( stmt )
          PgSchema.V1.Table.Feed.Sequence.FeedSeq.create( stmt )
          PgSchema.V1.Table.Item.Type.ItemAssignability.create( stmt )
          PgSchema.V1.Table.Item.create( stmt )
          PgSchema.V1.Table.Item.Index.ItemAssignability.create( stmt )
          PgSchema.V1.Table.Subscribable.create( stmt )
          PgSchema.V1.Table.Assignable.create( stmt )
          PgSchema.V1.Table.Assignment.create( stmt )
          PgSchema.V1.Table.Subscription.create( stmt )
          PgSchema.V1.Table.MailableTemplate.create( stmt )
          PgSchema.V1.Table.Mailable.create( stmt )
          PgSchema.V1.Table.Mailable.Sequence.MailableSeq.create( stmt )
        insertConfigKeys(
          conn,
          (ConfigKey.MailBatchSize, Default.MailBatchSize.toString()),
          (ConfigKey.MailBatchDelaySeconds, Default.MailBatchDelaySeconds.toString()),
          (ConfigKey.MailMaxRetries, Default.MailMaxRetries.toString())
        )
        updateMetadataKeys(
          conn,
          (MetadataKey.SchemaVersion, "1"),
          (MetadataKey.CreatorAppVersion, BuildInfo.version)
        )

    TRACE.log( s"upMigrate( from=${from} )" )
    from match
      case None      => upMigrateFrom_New()
      case Some( 0 ) => upMigrateFrom_0()
      case Some( `targetDbVersion` ) =>
        ZIO.fail( new CannotUpMigrate( s"Cannot upmigrate from current target DB version: V${targetDbVersion}" ) )
      case Some( other ) =>
        ZIO.fail( new CannotUpMigrate( s"Cannot upmigrate from unknown DB version: V${other}" ) )

  private def insertMetadataKeys( conn : Connection, pairs : (MetadataKey,String)* ) : Unit =
    pairs.foreach( ( mdkey, value ) => PgSchema.Unversioned.Table.Metadata.insert(conn, mdkey, value) )

  private def updateMetadataKeys( conn : Connection, pairs : (MetadataKey,String)* ) : Unit =
    pairs.foreach( ( mdkey, value ) => PgSchema.Unversioned.Table.Metadata.update(conn, mdkey, value) )

  private def insertConfigKeys( conn : Connection, pairs : (ConfigKey,String)* ) : Unit =
    pairs.foreach( ( cfgkey, value ) => LatestSchema.Table.Config.insert(conn, cfgkey, value) )

  private def updateConfigKeys( conn : Connection, pairs : (ConfigKey,String)* ) : Unit =
    pairs.foreach( ( cfgkey, value ) => LatestSchema.Table.Config.update(conn, cfgkey, value) )

  private def upsertConfigKeys( conn : Connection, pairs : (ConfigKey,String)* ) : Unit =
    pairs.foreach( ( cfgkey, value ) => LatestSchema.Table.Config.upsert(conn, cfgkey, value) )

  private def sort( tups : Set[Tuple2[ConfigKey,String]] ) : immutable.SortedSet[Tuple2[ConfigKey,String]] =
    immutable.SortedSet.from( tups )( using Ordering.by( tup => (tup(0).toString().toUpperCase, tup(1) ) ) )

  private def upsertConfigKeyMapAndReport( conn : Connection, map : Map[ConfigKey,String] ) : immutable.SortedSet[Tuple2[ConfigKey,String]] =
    upsertConfigKeys( conn, map.toList* )
    sort( LatestSchema.Table.Config.selectTuples(conn) )

  def upsertConfigKeyMapAndReport( ds : DataSource, map : Map[ConfigKey,String] ) : Task[immutable.SortedSet[Tuple2[ConfigKey,String]]] =
    withConnectionTransactional( ds )( conn => upsertConfigKeyMapAndReport( conn, map ) )

  def reportConfigKeys( ds : DataSource ): Task[immutable.SortedSet[Tuple2[ConfigKey,String]]] =
    withConnectionTransactional( ds )( conn => sort( LatestSchema.Table.Config.selectTuples( conn ) ) )

  private def lastCompletedAssignableWithinTypeStatus( conn : Connection, feedId : FeedId, subscribableName : SubscribableName ) : Option[AssignableWithinTypeStatus] =
    val withinTypeId = LatestSchema.Table.Assignable.selectWithinTypeIdLastCompleted( conn, subscribableName )
    withinTypeId.map: wti =>
      val count = LatestSchema.Table.Assignment.selectCountWithinAssignable( conn, subscribableName, wti )
      AssignableWithinTypeStatus( wti, count )

  private def mostRecentOpenAssignableWithinTypeStatus( conn : Connection, feedId : FeedId, subscribableName : SubscribableName ) : Option[AssignableWithinTypeStatus] =
    val withinTypeId = LatestSchema.Table.Assignable.selectWithinTypeIdMostRecentOpen( conn, subscribableName )
    withinTypeId.map: wti =>
      val count = LatestSchema.Table.Assignment.selectCountWithinAssignable( conn, subscribableName, wti )
      AssignableWithinTypeStatus( wti, count )

  private def ensureOpenAssignable( conn : Connection, feedId : FeedId, subscribableName : SubscribableName, withinTypeId : String, forGuid : Option[Guid]) : Unit =
    LatestSchema.Table.Assignable.selectIsCompleted( conn, subscribableName, withinTypeId ) match
      case Some( false ) => /* okay, ignore */
      case Some( true )  => throw new AssignableCompleted( feedId, subscribableName, withinTypeId, forGuid ) /* uh oh! */
      case None =>
        LatestSchema.Table.Assignable.insert( conn, subscribableName, withinTypeId, Instant.now, None )

  private def assignForSubscriptionType( conn : Connection, subscribableName : SubscribableName, feedId : FeedId, guid : Guid, content : ItemContent, status : ItemStatus ) : Unit =
    TRACE.log( s"assignForSubscriptionType( $conn, $subscribableName, $feedId, $guid, $content, $status )" )
    val lastCompleted = lastCompletedAssignableWithinTypeStatus( conn, feedId, subscribableName )
    val mostRecentOpen = mostRecentOpenAssignableWithinTypeStatus( conn, feedId, subscribableName )
    val subscriptionType = LatestSchema.Table.Subscribable.selectType( conn, subscribableName )
    subscriptionType.withinTypeId( conn, feedId, guid, content, status, lastCompleted, mostRecentOpen ).foreach: wti =>
      ensureOpenAssignable( conn, feedId, subscribableName, wti, Some(guid) )
      LatestSchema.Table.Assignment.insert( conn, subscribableName, wti, guid )

  private def assign( conn : Connection, feedId : FeedId, guid : Guid, content : ItemContent, status : ItemStatus ) : Unit =
    TRACE.log( s"assign( $conn, $feedId, $guid, $content, $status )" )
    val subscriptionTypeNames = LatestSchema.Table.Subscribable.selectSubscriptionTypeNamesByFeedId( conn, feedId )
    subscriptionTypeNames.foreach( stypeName => assignForSubscriptionType( conn, stypeName, feedId, guid, content, status ) )
    LatestSchema.Table.Item.updateAssignability( conn, feedId, guid, ItemAssignability.Assigned )

  private def updateAssignItem( conn : Connection, fi : FeedInfo, guid : Guid, dbStatus : Option[ItemStatus], freshContent : ItemContent, now : Instant ) : Unit =
    TRACE.log( s"updateAssignItem( $conn, $fi, $guid, $dbStatus, $freshContent, $now )" )
    dbStatus match
      case Some( prev @ ItemStatus( contentHash, firstSeen, lastChecked, stableSince, ItemAssignability.Unassigned ) ) =>
        val newContentHash = freshContent.##
        /* don't update empty over actual content, treat empty content (rare, since guid would still have been found) as just stability */
        if newContentHash == contentHash || newContentHash == ItemContent.EmptyHashCode then
          val newLastChecked = now
          LatestSchema.Table.Item.updateStable( conn, fi.assertFeedId, guid, newLastChecked )
          val seenMinutes = JDuration.between(firstSeen, now).get( ChronoUnit.SECONDS ) / 60     // ChronoUnite.Minutes fails, "UnsupportedTemporalTypeException: Unsupported unit: Minutes"
          val stableMinutes = JDuration.between(stableSince, now).get( ChronoUnit.SECONDS ) / 60 // ChronoUnite.Minutes fails, "UnsupportedTemporalTypeException: Unsupported unit: Minutes"
          def afterMinDelay = seenMinutes > fi.minDelayMinutes
          def sufficientlyStable = stableMinutes > fi.awaitStabilizationMinutes
          def pastMaxDelay = seenMinutes > fi.maxDelayMinutes
          if (afterMinDelay && sufficientlyStable) || pastMaxDelay then
            assign( conn, fi.assertFeedId, guid, freshContent, prev.copy( lastChecked = newLastChecked ) )
        else
          val newStableSince = now
          val newLastChecked = now
          LatestSchema.Table.Item.updateChanged( conn, fi.assertFeedId, guid, freshContent, ItemStatus( newContentHash, firstSeen, newLastChecked, newStableSince, ItemAssignability.Unassigned ) )
      case Some( prev @ ItemStatus( contentHash, firstSeen, lastChecked, stableSince, ItemAssignability.Assigned ) ) => /* update cache only */
        val newContentHash = freshContent.##
        if newContentHash != contentHash && newContentHash != ItemContent.EmptyHashCode then
          LatestSchema.Table.Item.updateChanged( conn, fi.assertFeedId, guid, freshContent, prev )
      case Some( ItemStatus( _, _, _, _, ItemAssignability.Cleared ) )  => /* ignore, we're done with this one */
      case Some( ItemStatus( _, _, _, _, ItemAssignability.Excluded ) ) => /* ignore, we don't assign  */
      case None =>
        def doInsert() =
          LatestSchema.Table.Item.insertNew(conn, fi.assertFeedId, guid, freshContent, ItemAssignability.Unassigned)
          val dbStatus = LatestSchema.Table.Item.checkStatus( conn, fi.assertFeedId, guid ).getOrElse:
            throw new AssertionError("Just inserted row is not found???")
          if fi.minDelayMinutes <= 0 && fi.awaitStabilizationMinutes <= 0 then // if eligible for immediate assignment...
            assign( conn, fi.assertFeedId, guid, freshContent, dbStatus )
        freshContent.pubDate match
          case Some( pd ) =>
            if pd > fi.added then doInsert() // skip items known to be published prior to subscription
          case None =>
            doInsert()

  private def updateAssignItems( conn : Connection, fi : FeedInfo ) : Unit =
    val FeedDigest( guidToItemContent, timestamp ) = FeedDigest( fi.feedUrl )
    guidToItemContent.foreach: ( guid, freshContent ) =>
      val dbStatus = LatestSchema.Table.Item.checkStatus( conn, fi.assertFeedId, guid )
      updateAssignItem( conn, fi, guid, dbStatus, freshContent, timestamp )
    val deleted = LatestSchema.Table.Item.deleteDisappearedUnassigned( conn, guidToItemContent.keySet ) // so that if a post is deleted before it has been assigned, it won't be notified
    DEBUG.log( s"Deleted ${deleted} disappeared unassigned items." )
    LatestSchema.Table.Feed.updateLastAssigned(conn, fi.assertFeedId, timestamp)

  // TODO: USe latest from feed, rather than cached values, if available
  private def materializeAssignable( conn : Connection, assignableKey : AssignableKey ) : Set[ItemContent] =
    LatestSchema.Join.ItemAssignment.selectItemContentsForAssignable( conn, assignableKey.subscribableName, assignableKey.withinTypeId )

  private def route( conn : Connection, assignableKey : AssignableKey ) : Unit =
    val subscriptionType = LatestSchema.Table.Subscribable.selectType( conn, assignableKey.subscribableName )
    route( conn, assignableKey, subscriptionType )

  private def route( conn : Connection, assignableKey : AssignableKey, stype : SubscriptionType ) : Unit =
    val AssignableKey( subscribableName, withinTypeId ) = assignableKey
    val contents = materializeAssignable(conn, assignableKey)
    val destinations = LatestSchema.Table.Subscription.selectDestination( conn, subscribableName )
    stype.route(conn, assignableKey, contents, destinations )

  def queueForMailing( conn : Connection, contents : String, from : String, replyTo : Option[String], tosWithParams : Set[(Destination,TemplateParams)], subject : String ) : Unit = 
    val hash = Hash.SHA3_256.hash( contents.getBytes( scala.io.Codec.UTF8.charSet ) )
    LatestSchema.Table.MailableTemplate.ensure( conn, hash, contents )
    LatestSchema.Table.Mailable.insertBatch( conn, hash, from, replyTo, tosWithParams, subject, 0 )

  def updateAssignItems( ds : DataSource ) : Task[Unit] =
    withConnectionTransactional( ds ): conn =>
      LatestSchema.Table.Feed.selectAll( conn ).foreach( updateAssignItems( conn, _ ) )

  def completeAssignables( ds : DataSource ) : Task[Unit] =
    withConnectionTransactional( ds ): conn =>
      LatestSchema.Table.Assignable.selectOpen( conn ).foreach: ak =>
        val AssignableKey( subscribableName, withinTypeId ) = ak
        val count = LatestSchema.Table.Assignment.selectCountWithinAssignable( conn, subscribableName, withinTypeId )
        val ( feedId, subscriptionType ) = LatestSchema.Table.Subscribable.selectFeedIdAndType( conn, subscribableName )
        val lastAssigned = LatestSchema.Table.Feed.selectLastAssigned( conn, feedId ).getOrElse:
          throw new AssertionError( s"DB constraints should have ensured a row for feed with ID '${feedId}' with a NOT NULL lastAssigned, but did not?" )
        if subscriptionType.isComplete( conn, withinTypeId, count, lastAssigned ) then
          route( conn, ak, subscriptionType )
          cleanUpPreviouslyCompleted( conn, subscribableName ) // we have to do this BEFORE updateCompleted, or we'll clean away the latest last completed.
          LatestSchema.Table.Assignable.updateCompleted( conn, subscribableName, withinTypeId, Some(Instant.now) )

  def cleanUpPreviouslyCompleted( conn : Connection, subscribableName : SubscribableName ) : Unit =
    val mbLastWithinTypeId = LatestSchema.Table.Assignable.selectWithinTypeIdLastCompleted( conn, subscribableName )
    mbLastWithinTypeId.foreach: wti =>
      LatestSchema.Table.Assignment.cleanAwayAssignable( conn, subscribableName, wti)
      LatestSchema.Table.Assignable.delete( conn, subscribableName, wti )
      LatestSchema.Join.ItemAssignableAssignment.clearOldCache( conn )

  def addFeed( ds : DataSource, fi : FeedInfo ) : Task[Set[FeedInfo]] =
    withConnectionTransactional( ds ): conn =>
      val newFeedId = LatestSchema.Table.Feed.Sequence.FeedSeq.selectNext(conn)
      LatestSchema.Table.Feed.insert(conn, newFeedId, fi)
      try
        val fd = FeedDigest( fi.feedUrl )
        fd.guidToItemContent.foreach: (guid, itemContent) =>
          LatestSchema.Table.Item.insertNew( conn, newFeedId, guid, ItemContent.Empty, ItemAssignability.Excluded ) // don't cache items we're excluding anyway
      catch
        case NonFatal(t) =>
          WARNING.log(s"Failed to exclude existing content from assignment when adding feed '${fi.feedUrl}'. Existing content may be distributed.", t)
      LatestSchema.Table.Feed.selectAll(conn)

  def listFeeds( ds : DataSource ) : Task[Set[FeedInfo]] =
    withConnectionTransactional( ds ): conn =>
      LatestSchema.Table.Feed.selectAll(conn)

  def fetchExcluded( ds : DataSource ) : Task[Set[ExcludedItem]] =
    withConnectionTransactional( ds ): conn =>
      LatestSchema.Table.Item.selectExcluded(conn)

  def addSubscribable( ds : DataSource, subscribableName : SubscribableName, feedId : FeedId, subscriptionType : SubscriptionType ) : Task[Unit] =
    withConnectionTransactional( ds ): conn =>
      LatestSchema.Table.Subscribable.insert( conn, subscribableName, feedId, subscriptionType  )

  def addSubscription( ds : DataSource, subscribableName : SubscribableName, destination : Destination ) : Task[Unit] =
    withConnectionTransactional( ds ): conn =>
      val subscriptionType = LatestSchema.Table.Subscribable.selectType( conn, subscribableName )
      subscriptionType.validateDestination( conn, destination, subscribableName )
      LatestSchema.Table.Subscription.insert( conn, destination, subscribableName )

  def listSubscribables( ds : DataSource ) : Task[Set[(SubscribableName,FeedId,SubscriptionType)]] =
    withConnectionTransactional( ds ): conn =>
      LatestSchema.Table.Subscribable.select( conn )

  object Config:
    def fetchValue( conn : Connection, key : ConfigKey ) : Option[String] = LatestSchema.Table.Config.select( conn, key )
    def zfetchValue( conn : Connection, key : ConfigKey ) : Task[Option[String]] = ZIO.attemptBlocking( LatestSchema.Table.Config.select( conn, key ) )
    def timeZone( conn : Connection ) : ZoneId = fetchValue( conn, ConfigKey.TimeZone ).map( str => ZoneId.of(str) ).getOrElse( ZoneId.systemDefault() )
    def mailBatchDelaySeconds( conn : Connection ) : Int = fetchValue( conn, ConfigKey.MailBatchDelaySeconds ).map( _.toInt ).getOrElse( Default.MailBatchDelaySeconds )
    def mailMaxRetries( conn : Connection ) : Int = fetchValue( conn, ConfigKey.MailMaxRetries ).map( _.toInt ).getOrElse( Default.MailMaxRetries )

  def pullMailGroup( conn : Connection ) : Set[MailSpec.WithTemplate] =
    val batchSize = Config.fetchValue( conn, ConfigKey.MailBatchSize ).map( _.toInt ).getOrElse( Default.MailBatchSize )
    val withHashes : Set[MailSpec.WithHash] = LatestSchema.Table.Mailable.selectForDelivery(conn, batchSize)
    val contentMap = mutable.Map.empty[Hash.SHA3_256,String]
    def templateFromHash( hash : Hash.SHA3_256 ) : String =
      LatestSchema.Table.MailableTemplate.selectByHash( conn, hash ).getOrElse:
        throw new AssertionError(s"Database consistency issue, we should only be trying to load contents from extant hashes. (${hash.hex})")
    withHashes.foreach: mswh =>
      contentMap.getOrElseUpdate( mswh.templateHash, templateFromHash(mswh.templateHash) )
    withHashes.map( mswh => MailSpec.WithTemplate( mswh.seqnum, mswh.templateHash, templateFromHash( mswh.templateHash ), mswh.from, mswh.replyTo, mswh.to, mswh.subject, mswh.templateParams, mswh.retried ) )

  def attemptMail( conn : Connection, maxRetries : Int, mswt : MailSpec.WithTemplate, smtpContext : Smtp.Context ) : Unit =
    LatestSchema.Table.Mailable.deleteSingle( conn, mswt.seqnum )
    var attemptDeleteContents = true
    try
      val contents = mswt.templateParams.fill( mswt.template )
      given Smtp.Context = smtpContext
      Smtp.sendSimpleHtmlOnly( contents, subject = mswt.subject, from = mswt.from, to = mswt.to.toString(), replyTo = mswt.replyTo.toSeq )
    catch
      case NonFatal(t) =>
        val lastRetryMessage = if mswt.retried == maxRetries then "(last retry, will drop)" else s"(maxRetries: ${maxRetries})"
        WARNING.log( s"Failed email attempt: subject = ${mswt.subject}, from = ${mswt.from}, to = ${mswt.to}, replyTo = ${mswt.replyTo} ), retried = ${mswt.retried} ${lastRetryMessage}", t )
        LatestSchema.Table.Mailable.insert( conn, mswt.templateHash, mswt.from, mswt.replyTo, mswt.to, mswt.subject, mswt.templateParams, mswt.retried + 1)
        attemptDeleteContents = false
    if attemptDeleteContents then
      LatestSchema.Table.MailableTemplate.deleteIfUnreferenced( conn, mswt.templateHash )

  def mailNextGroup( conn : Connection, smtpContext : Smtp.Context ) =
    val retries = Config.mailMaxRetries( conn )
    val mswts   = pullMailGroup( conn )
    mswts.foreach( mswt => attemptMail( conn, retries, mswt, smtpContext ) )

  def forceMailNextGroup( ds : DataSource, smtpContext : Smtp.Context ) : Task[Unit] =
    withConnectionTransactional( ds ): conn =>
      mailNextGroup( conn, smtpContext )

  def mailNextGroupIfDue( conn : Connection, smtpContext : Smtp.Context ) =
    val now = Instant.now()
    val nextMailingTime = LatestSchema.Table.Times.select( conn, TimesKey.MailNextBatch ).getOrElse( now )
    if now >= nextMailingTime then
      mailNextGroup( conn, smtpContext )
      val delay = Config.mailBatchDelaySeconds( conn )
      LatestSchema.Table.Times.upsert( conn, TimesKey.MailNextBatch, now.plusSeconds( delay ) )

  def mailNextGroupIfDue( ds : DataSource, smtpContext : Smtp.Context ) : Task[Unit] = withConnectionTransactional( ds ): conn =>
    mailNextGroupIfDue(conn, smtpContext )

  def hasMetadataTable( conn : Connection ) : Boolean =
    Using.resource( conn.getMetaData().getTables(null,null,PgSchema.Unversioned.Table.Metadata.Name,null) )( _.next() )

  def ensureMetadataTable( conn : Connection ) : Task[Unit] =
    ZIO.attemptBlocking:
      if !hasMetadataTable(conn) then throw new DbNotInitialized("Please initialize the database. (No metadata table found.)")

  def ensureDb( ds : DataSource ) : Task[Unit] =
    withConnectionZIO( ds ): conn =>
      for
        _ <- ensureMetadataTable(conn)
        mbSchemaVersion <- zfetchMetadataValue(conn, MetadataKey.SchemaVersion).map( option => option.map( _.toInt ) )
        mbAppVersion <- zfetchMetadataValue(conn, MetadataKey.CreatorAppVersion)
      yield
        mbSchemaVersion match
          case Some( schemaVersion ) =>
            if schemaVersion > LatestSchema.Version then
              throw new MoreRecentFeedletterVersionRequired(
                s"The database schema version is ${schemaVersion}. " +
                mbAppVersion.fold("")( appVersion => s"It was created by app version '${appVersion}'. " ) +
                s"The latest version known by this version of the app is ${LatestSchema.Version}. " +
                s"You are running app version '${BuildInfo.version}'."
              )
            else if schemaVersion < LatestSchema.Version then
              throw new SchemaMigrationRequired(
                s"The database schema version is ${schemaVersion}. " +
                mbAppVersion.fold("")( appVersion => s"It was created by app version '${appVersion}'. " ) +
                s"The current schema this version of the app (${BuildInfo.version}) is ${LatestSchema.Version}. " +
                "Please migrate."
              )
            // else schemaVersion == LatestSchema.version and we're good
          case None =>
            throw new DbNotInitialized("Please initialize the database.")
  end ensureDb

  def feedUrl( conn : Connection, feedId : FeedId ) : Option[FeedUrl] =
    LatestSchema.Table.Feed.selectUrl(conn, feedId)

  def assertFeedUrl( conn : Connection, feedId : FeedId ) : FeedUrl =
    LatestSchema.Table.Feed.selectUrl(conn, feedId).getOrElse:
      throw new FeedletterException( s"Expected a URL assigned for Feed ID '$feedId', none found.")

  def feedIdUrlForSubscribableName( conn : Connection, subscribableName : SubscribableName ) : ( FeedId, FeedUrl ) =
    LatestSchema.Join.ItemSubscribable.selectFeedIdUrlForSubscribableName( conn, subscribableName )

