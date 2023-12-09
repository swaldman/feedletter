package com.mchange.feedletter.db

import zio.*
import java.sql.{Connection,Timestamp}
import javax.sql.DataSource

import scala.collection.{immutable,mutable}
import scala.util.Using
import scala.util.control.NonFatal

import java.sql.SQLException
import java.time.{Duration as JDuration,Instant,ZonedDateTime,ZoneId}
import java.time.temporal.ChronoUnit

import com.mchange.sc.v1.log.*
import MLevel.*

import audiofluidity.rss.util.formatPubDate
import com.mchange.feedletter.{BuildInfo, ConfigKey, ExcludedItem, FeedDigest, FeedInfo, ItemContent, SubscriptionType}
import com.mchange.mailutil.*
import com.mchange.cryptoutil.{*, given}

object PgDatabase extends Migratory:
  private lazy given logger : MLogger = mlogger( this )

  val LatestSchema = PgSchema.V1
  override val targetDbVersion = LatestSchema.Version

  val DefaultMailBatchSize         = 100
  val DefaultMailBatchDelaySeconds = 15 * 60 // 15 mins
  val DefaultMailMaxRetries        = 5

  private def fetchMetadataValue( conn : Connection, key : MetadataKey ) : Option[String] =
    PgSchema.Unversioned.Table.Metadata.select( conn, key )

  private def fetchConfigValue( conn : Connection, key : ConfigKey ) : Option[String] =
    LatestSchema.Table.Config.select( conn, key )

  private def zfetchMetadataValue( conn : Connection, key : MetadataKey ) : Task[Option[String]] =
    ZIO.attemptBlocking( PgSchema.Unversioned.Table.Metadata.select( conn, key ) )

  private def zfetchConfigValue( conn : Connection, key : ConfigKey ) : Task[Option[String]] =
    ZIO.attemptBlocking( LatestSchema.Table.Config.select( conn, key ) )

  private def fetchDbName(conn : Connection) : Task[String] =
    ZIO.attemptBlocking:
      Using.resource(conn.createStatement()): stmt =>
        Using.resource(stmt.executeQuery("SELECT current_database()")): rs =>
          uniqueResult("select-current-database-name", rs)( _.getString(1) )

  private def fetchDumpDir(conn : Connection) : Task[os.Path] =
    for
      mbDumpDir <- zfetchConfigValue(conn, ConfigKey.DumpDbDir)
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
          PgSchema.V1.Table.Feed.create( stmt )
          PgSchema.V1.Table.Item.Type.ItemAssignability.create( stmt )
          PgSchema.V1.Table.Item.create( stmt )
          PgSchema.V1.Table.SubscriptionType.create( stmt )
          PgSchema.V1.Table.Assignable.create( stmt )
          PgSchema.V1.Table.Assignment.create( stmt )
          PgSchema.V1.Table.Subscription.create( stmt )
          PgSchema.V1.Table.MailableContents.create( stmt )
          PgSchema.V1.Table.Mailable.create( stmt )
          PgSchema.V1.Table.Mailable.Sequence.MailableSeq.create( stmt )
        insertConfigKeys(
          conn,
          (ConfigKey.MailNextBatchTime, formatPubDate(ZonedDateTime.now)),
          (ConfigKey.MailBatchSize, DefaultMailBatchSize.toString()),
          (ConfigKey.MailBatchDelaySecs, DefaultMailBatchDelaySeconds.toString()),
          (ConfigKey.MailMaxRetries, DefaultMailMaxRetries.toString())
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

  private def lastCompletedAssignableWithinTypeStatus( conn : Connection, feedUrl : String, stypeName : String ) : Option[AssignableWithinTypeStatus] =
    val withinTypeId = LatestSchema.Table.Assignable.selectWithinTypeIdLastCompleted( conn, feedUrl, stypeName )
    withinTypeId.map: wti =>
      val count = LatestSchema.Table.Assignment.selectCountWithinAssignable( conn, feedUrl, stypeName, wti )
      AssignableWithinTypeStatus( wti, count )

  private def mostRecentOpenAssignableWithinTypeStatus( conn : Connection, feedUrl : String, stypeName : String ) : Option[AssignableWithinTypeStatus] =
    val withinTypeId = LatestSchema.Table.Assignable.selectWithinTypeIdMostRecentOpen( conn, feedUrl, stypeName )
    withinTypeId.map: wti =>
      val count = LatestSchema.Table.Assignment.selectCountWithinAssignable( conn, feedUrl, stypeName, wti )
      AssignableWithinTypeStatus( wti, count )

  private def ensureOpenAssignable( conn : Connection, feedUrl : String, stypeName : String, withinTypeId : String, forGuid : Option[String]) : Unit =
    LatestSchema.Table.Assignable.selectIsCompleted( conn, feedUrl, stypeName, withinTypeId) match
      case Some( false ) => /* okay, ignore */
      case Some( true )  => throw new AssignableCompleted( feedUrl, stypeName, withinTypeId, forGuid ) /* uh oh! */
      case None =>
        LatestSchema.Table.Assignable.insert( conn, feedUrl, stypeName, withinTypeId, Instant.now, None )

  private def assignForSubscriptionType( conn : Connection, stypeName : String, feedUrl : String, guid : String, content : ItemContent, status : ItemStatus ) : Unit =
    val lastCompleted = lastCompletedAssignableWithinTypeStatus( conn, feedUrl, stypeName )
    val mostRecentOpen = mostRecentOpenAssignableWithinTypeStatus( conn, feedUrl, stypeName )
    val stype = LatestSchema.Table.SubscriptionType.selectByName( conn, stypeName )
    stype.withinTypeId( feedUrl, lastCompleted, mostRecentOpen, guid, content, status ).foreach: wti =>
      ensureOpenAssignable( conn, feedUrl, stypeName, wti, Some(guid) )
      LatestSchema.Table.Assignment.insert( conn, feedUrl, stypeName, wti, guid )

  private def assign( conn : Connection, feedUrl : String, guid : String, content : ItemContent, status : ItemStatus ) : Unit =
    val subscriptionTypeNames = LatestSchema.Table.Subscription.selectSubscriptionTypeNamesByFeedUrl( conn, feedUrl )
    subscriptionTypeNames.foreach( stypeName => assignForSubscriptionType( conn, stypeName, feedUrl, guid, content, status ) )
    LatestSchema.Table.Item.updateAssignability( conn, feedUrl, guid, ItemAssignability.Assigned )

  private def updateAssignItem( conn : Connection, fi : FeedInfo, guid : String, dbStatus : Option[ItemStatus], freshContent : ItemContent, now : Instant ) : Unit =
    dbStatus match
      case Some( prev @ ItemStatus( contentHash, firstSeen, lastChecked, stableSince, ItemAssignability.Unassigned ) ) =>
        val newContentHash = freshContent.##
        /* don't update empty over actual content, treat empty content (rare, since guid would still have been found) as just stability */
        if newContentHash == contentHash || newContentHash == ItemContent.EmptyHashCode then
          val newLastChecked = now
          LatestSchema.Table.Item.updateStable( conn, fi.feedUrl, guid, newLastChecked )
          val seenMinutes = JDuration.between(firstSeen, now).get( ChronoUnit.MINUTES )
          val stableMinutes = JDuration.between(stableSince, now).get( ChronoUnit.MINUTES )
          def afterMinDelay = seenMinutes > fi.minDelayMinutes
          def sufficientlyStable = stableMinutes > fi.awaitStabilizationMinutes
          def pastMaxDelay = seenMinutes > fi.maxDelayMinutes
          if (afterMinDelay && sufficientlyStable) || pastMaxDelay then
            assign( conn, fi.feedUrl, guid, freshContent, prev.copy( lastChecked = newLastChecked ) )
        else
          val newStableSince = now
          val newLastChecked = now
          LatestSchema.Table.Item.updateChanged( conn, fi.feedUrl, guid, freshContent, ItemStatus( newContentHash, firstSeen, newLastChecked, newStableSince, ItemAssignability.Unassigned ) )
      case Some( ItemStatus( _, _, _, _, ItemAssignability.Assigned ) ) => /* ignore, already assigned */
      case Some( ItemStatus( _, _, _, _, ItemAssignability.Excluded ) ) => /* ignore, we don't assign  */
      case None =>
        def doInsert() = LatestSchema.Table.Item.insertNew(conn, fi.feedUrl, guid, freshContent, ItemAssignability.Unassigned)
        freshContent.pubDate match
          case Some( pd ) =>
            if fi.subscribed.compareTo(pd) <= 0 then doInsert() // skip items known to be published prior to subscription
          case None =>
            doInsert()

  private def updateAssignItems( conn : Connection, fi : FeedInfo ) : Unit =
    val FeedDigest( guidToItemContent, timestamp ) = FeedDigest( fi.feedUrl )
    guidToItemContent.foreach: ( guid, freshContent ) =>
      val dbStatus = LatestSchema.Table.Item.checkStatus( conn, fi.feedUrl, guid )
      updateAssignItem( conn, fi, guid, dbStatus, freshContent, timestamp )
    LatestSchema.Table.Feed.updateLastAssigned(conn, fi.feedUrl, timestamp)

  private def materializeAssignable( conn : Connection, assignableKey : AssignableKey ) : Set[ItemContent] =
    LatestSchema.Join.ItemAssignment.selectItemContentsForAssignable( conn, assignableKey.feedUrl, assignableKey.stypeName, assignableKey.withinTypeId )

  private def route( conn : Connection, assignableKey : AssignableKey ) : Unit =
    val stype = LatestSchema.Table.SubscriptionType.selectByName( conn, assignableKey.stypeName )
    route( conn, assignableKey, stype )

  private def route( conn : Connection, assignableKey : AssignableKey, stype : SubscriptionType ) : Unit =
    val AssignableKey( feedUrl, stypeName, withinTypeId ) = assignableKey
    val contents = materializeAssignable(conn, assignableKey)
    val destinations = LatestSchema.Table.Subscription.selectDestination( conn, feedUrl, stypeName )
    stype.route(conn, assignableKey, contents, destinations )

  def queueForMailing( conn : Connection, contents : String, from : String, replyTo : Option[String], tos : Set[String], subject : String ) : Unit = 
    val hash = Hash.SHA3_256.hash( contents.getBytes( scala.io.Codec.UTF8.charSet ) )
    LatestSchema.Table.MailableContents.ensure( conn, hash, contents )
    LatestSchema.Table.Mailable.insertBatch( conn, hash, from, replyTo, tos, subject, 0 )

  def updateAssignItems( ds : DataSource ) : Task[Unit] =
    withConnectionTransactional( ds ): conn =>
      LatestSchema.Table.Feed.selectAll( conn ).foreach( updateAssignItems( conn, _ ) )

  def completeAssignables( ds : DataSource ) : Task[Unit] =
    withConnectionTransactional( ds ): conn =>
      LatestSchema.Table.Assignable.selectOpen( conn ).foreach: ak =>
        val AssignableKey( feedUrl, stypeName, withinTypeId ) = ak
        val count = LatestSchema.Table.Assignment.selectCountWithinAssignable( conn, feedUrl, stypeName, withinTypeId )
        val stype = LatestSchema.Table.SubscriptionType.selectByName( conn, stypeName )
        val lastAssigned = LatestSchema.Table.Feed.selectLastAssigned( conn, feedUrl ).getOrElse:
          throw new AssertionError( s"DB constraints should have ensured a row for feed '${feedUrl}' with a NOT NULL lastAssigned, but did not?" )
        if stype.isComplete( conn, withinTypeId, count, lastAssigned ) then
          route( conn, ak, stype )
          LatestSchema.Table.Assignable.updateCompleted( conn, feedUrl, stypeName, withinTypeId, Some(Instant.now) )

  def addFeed( ds : DataSource, fi : FeedInfo ) : Task[Set[FeedInfo]] =
    withConnectionTransactional( ds ): conn =>
      LatestSchema.Table.Feed.upsert(conn, fi)
      try
        val fd = FeedDigest( fi.feedUrl )
        fd.guidToItemContent.foreach: (guid, itemContent) =>
          LatestSchema.Table.Item.insertNew( conn, fi.feedUrl, guid, itemContent, ItemAssignability.Excluded )
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

  def addSubscriptionType( ds : DataSource, stypeName : String, stype : SubscriptionType ) : Task[Unit] =
    withConnectionTransactional( ds ): conn =>
      LatestSchema.Table.SubscriptionType.insert( conn, stypeName, stype )

  def addSubscription( ds : DataSource, stypeName : String, destination : String, feedUrl : String ) : Task[Unit] =
    withConnectionTransactional( ds ): conn =>
      val stype = LatestSchema.Table.SubscriptionType.selectByName( conn, stypeName )
      stype.validateDestination( conn, stypeName, destination, feedUrl )
      LatestSchema.Table.Subscription.insert( conn, destination, feedUrl, stypeName )

  def listSubscriptionTypes( ds : DataSource ) : Task[Set[(String,SubscriptionType)]] =
    withConnectionTransactional( ds ): conn =>
      LatestSchema.Table.SubscriptionType.select( conn )

  def timeZone( conn : Connection ) : ZoneId =
    fetchConfigValue( conn, ConfigKey.TimeZone ).map( str => ZoneId.of(str) ).getOrElse( ZoneId.systemDefault() )

  def pullMailGroup( conn : Connection ) : Set[MailSpec.WithContents] =
    val batchSize = fetchConfigValue( conn, ConfigKey.MailBatchSize ).map( _.toInt ).getOrElse( DefaultMailBatchSize )
    val withHashes : Set[MailSpec.WithHash] = LatestSchema.Table.Mailable.selectForDelivery(conn, batchSize)
    val contentMap = mutable.Map.empty[Hash.SHA3_256,String]
    def contentsFromHash( hash : Hash.SHA3_256 ) : String =
      LatestSchema.Table.MailableContents.selectByHash( conn, hash ).getOrElse:
        throw new AssertionError(s"Database consistency issue, we should only be trying to load contents from extant hashes. (${hash.hex})")
    withHashes.foreach: mswh =>
      contentMap.getOrElseUpdate( mswh.contentsHash, contentsFromHash(mswh.contentsHash) )
    withHashes.map( mswh => MailSpec.WithContents( mswh.seqnum, mswh.contentsHash, contentsFromHash( mswh.contentsHash ), mswh.from, mswh.replyTo, mswh.to, mswh.subject, mswh.retried ) )

  def maxRetries( conn : Connection ) : Int = fetchConfigValue( conn, ConfigKey.MailMaxRetries ).map( _.toInt ).getOrElse( DefaultMailMaxRetries )

  def attemptMail( conn : Connection, maxRetries : Int, mswc : MailSpec.WithContents ) : Unit =
    LatestSchema.Table.Mailable.deleteSingle( conn, mswc.seqnum )
    try
      Smtp.sendSimpleHtmlOnly( mswc.contents, subject = mswc.subject, from = mswc.from, to = mswc.to, replyTo = mswc.replyTo.toSeq )
    catch
      case NonFatal(t) =>
        val lastRetryMessage = if mswc.retried == maxRetries then "(last retry, will drop)" else s"(maxRetries: ${maxRetries})"
        WARNING.log( s"Failed email attempt: subject = ${mswc.subject}, from = ${mswc.from}, to = ${mswc.to}, replyTo = ${mswc.replyTo} ), retried = ${mswc.retried} ${lastRetryMessage}", t )
        LatestSchema.Table.Mailable.insert( conn, mswc.contentsHash, mswc.from, mswc.replyTo, mswc.to, mswc.subject, mswc.retried + 1)

  def mailNextGroup( conn : Connection ) =
    val retries = maxRetries( conn )
    val mswcs   = pullMailGroup( conn )
    mswcs.foreach( mswc => attemptMail( conn, retries, mswc ) )

  def ensureDb( ds : DataSource ) : Task[Unit] =
    withConnectionZIO( ds ): conn =>
      for
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

