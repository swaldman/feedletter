package com.mchange.feedletter.db

import zio.*
import java.sql.{Connection,Timestamp}
import javax.sql.DataSource

import scala.collection.immutable
import scala.util.Using
import java.sql.SQLException
import java.time.{Duration as JDuration,Instant,ZonedDateTime}

import com.mchange.sc.v1.log.*
import MLevel.*

import audiofluidity.rss.util.formatPubDate
import com.mchange.feedletter.{doDigestFeed, BuildInfo, ConfigKey, FeedDigest, FeedInfo, ItemContent, SubscriptionType}

object PgDatabase extends Migratory:
  private lazy given logger : MLogger = mlogger( this )

  val LatestSchema = PgSchema.V1
  override val targetDbVersion = LatestSchema.Version

  val DefaultMailBatchSize = 100
  val DefaultMailBatchDelaySeconds = 15 * 60 // 15 mins

  private def fetchMetadataValue( conn : Connection, key : MetadataKey ) : Task[Option[String]] =
    ZIO.attemptBlocking( PgSchema.Unversioned.Table.Metadata.select( conn, key ) )

  private def fetchConfigValue( conn : Connection, key : ConfigKey ) : Task[Option[String]] =
    ZIO.attemptBlocking( LatestSchema.Table.Config.select( conn, key ) )

  private def fetchDbName(conn : Connection) : Task[String] =
    ZIO.attemptBlocking:
      Using.resource(conn.createStatement()): stmt =>
        Using.resource(stmt.executeQuery("SELECT current_database()")): rs =>
          uniqueResult("select-current-database-name", rs)( _.getString(1) )

  private def fetchDumpDir(conn : Connection) : Task[os.Path] =
    for
      mbDumpDir <- fetchConfigValue(conn, ConfigKey.DumpDbDir)
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
          mbDbVersion <- fetchMetadataValue(conn, MetadataKey.SchemaVersion)
          mbCreatorAppVersion <- fetchMetadataValue(conn, MetadataKey.CreatorAppVersion)
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
          PgSchema.V1.Table.Item.create( stmt )
          PgSchema.V1.Table.SubscriptionType.create( stmt )
          PgSchema.V1.Table.Assignable.create( stmt )
          PgSchema.V1.Table.Assignment.create( stmt )
          PgSchema.V1.Table.Subscription.create( stmt )
          PgSchema.V1.Table.Mailable.create( stmt )
          PgSchema.V1.Table.Mailable.Sequence.MailableSeq.create( stmt )
        insertConfigKeys(
          conn,
          (ConfigKey.MailNextBatchTime, formatPubDate(ZonedDateTime.now)),
          (ConfigKey.MailBatchSize, DefaultMailBatchSize.toString()),
          (ConfigKey.MailBatchDelaySecs, DefaultMailBatchDelaySeconds.toString())
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
    withConnection( ds )( conn => sort( LatestSchema.Table.Config.selectTuples( conn ) ) )

  private def lastCompletedAssignableWithinTypeInfo( conn : Connection, feedUrl : String, stype : SubscriptionType ) : Option[AssignableWithinTypeInfo] =
    val withinTypeId = LatestSchema.Table.Assignable.selectWithinTypeIdLastCompleted( conn, feedUrl, stype )
    withinTypeId.map: wti =>
      val count = LatestSchema.Table.Assignment.selectCountWithinAssignable( conn, feedUrl, stype, wti )
      AssignableWithinTypeInfo( wti, count )

  private def mostRecentOpenAssignableWithinTypeInfo( conn : Connection, feedUrl : String, stype : SubscriptionType ) : Option[AssignableWithinTypeInfo] =
    val withinTypeId = LatestSchema.Table.Assignable.selectWithinTypeIdMostRecentOpen( conn, feedUrl, stype )
    withinTypeId.map: wti =>
      val count = LatestSchema.Table.Assignment.selectCountWithinAssignable( conn, feedUrl, stype, wti )
      AssignableWithinTypeInfo( wti, count )

  private def ensureOpenAssignable( conn : Connection, feedUrl : String, stype : SubscriptionType, withinTypeId : String, forGuid : Option[String]) : Unit =
    LatestSchema.Table.Assignable.selectIsCompleted( conn, feedUrl, stype, withinTypeId) match
      case Some( false ) => /* okay, ignore */
      case Some( true )  => throw new AssignableCompleted( feedUrl, stype, withinTypeId, forGuid ) /* uh oh! */
      case None =>
        LatestSchema.Table.Assignable.insert( conn, feedUrl, stype, withinTypeId, Instant.now, None )

  private def assignForSubscriptionType( conn : Connection, stype : SubscriptionType, feedUrl : String, guid : String, content : ItemContent, status : ItemStatus ) : Unit =
    val lastCompleted = lastCompletedAssignableWithinTypeInfo( conn, feedUrl, stype )
    val mostRecentOpen = mostRecentOpenAssignableWithinTypeInfo( conn, feedUrl, stype )
    stype.withinTypeId( feedUrl, lastCompleted, mostRecentOpen, guid, content, status ).foreach: wti =>
      ensureOpenAssignable( conn, feedUrl, stype, wti, Some(guid) )
      LatestSchema.Table.Assignment.insert( conn, feedUrl, stype, wti, guid )

  private def assign( conn : Connection, feedUrl : String, guid : String, content : ItemContent, status : ItemStatus ) : Unit =
    val subscriptionTypes = LatestSchema.Table.Subscription.selectSubscriptionTypeByFeedUrl( conn, feedUrl )
    subscriptionTypes.foreach( stype => assignForSubscriptionType( conn, stype, feedUrl, guid, content, status ) )
    LatestSchema.Table.Item.updateAssigned( conn, feedUrl, guid, true )

  private def updateAssignItem( conn : Connection, fi : FeedInfo, guid : String, dbStatus : Option[ItemStatus], freshContent : ItemContent, now : Instant ) : Unit =
    dbStatus match
      case Some( prev @ ItemStatus( contentHash, firstSeen, lastChecked, stableSince, false ) ) =>
        val newContentHash = freshContent.##
        if newContentHash == contentHash then
          val newLastChecked = now
          LatestSchema.Table.Item.updateStable( conn, fi.feedUrl, guid, newLastChecked )
          val seenSeconds = JDuration.between(firstSeen, now).getSeconds()
          val stableSeconds = JDuration.between(stableSince, now).getSeconds()
          if seenSeconds > fi.minDelaySeconds && stableSeconds > fi.awaitStabilizationSeconds then
            assign( conn, fi.feedUrl, guid, freshContent, prev.copy( lastChecked = newLastChecked ) )
        else
          val newStableSince = now
          val newLastChecked = now
          LatestSchema.Table.Item.updateChanged( conn, fi.feedUrl, guid, freshContent, ItemStatus( newContentHash, firstSeen, newLastChecked, newStableSince, false ) )
      case Some( ItemStatus( _, _, _, _, true ) ) => /* ignore, already assigned */
      case None =>
        LatestSchema.Table.Item.insertNew(conn, fi.feedUrl, guid, freshContent)

  private def updateAssignItems( conn : Connection, fi : FeedInfo ) : Task[Unit] =
    ZIO.attemptBlocking:
      conn.setAutoCommit( false )
      val FeedDigest( guidToItemContent, timestamp ) = doDigestFeed( fi.feedUrl )
      guidToItemContent.foreach: ( guid, freshContent ) =>
        val dbStatus = LatestSchema.Table.Item.checkStatus( conn, fi.feedUrl, guid )
        updateAssignItem( conn, fi, guid, dbStatus, freshContent, timestamp )
      conn.commit()

  private def populateMailable( conn : Connection, assignableKey : AssignableKey ) : Unit =
    val AssignableKey( feedUrl, stype, withinTypeId ) = assignableKey
    LatestSchema.Table.Subscription.selectEmail( conn, feedUrl, stype ).foreach: email =>
      LatestSchema.Table.Mailable.insert( conn, email, feedUrl, stype, withinTypeId, false )

  def ensureDb( ds : DataSource ) : Task[Unit] =
    withConnectionZIO( ds ): conn =>
      for
        mbSchemaVersion <- fetchMetadataValue(conn, MetadataKey.SchemaVersion).map( option => option.map( _.toInt ) )
        mbAppVersion <- fetchMetadataValue(conn, MetadataKey.CreatorAppVersion)
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

  def updateAssignItems( ds : DataSource ) : Task[Unit] =
    withConnectionTransactionalZIO( ds ): conn =>
      for
        _ <-  ZIO.collectAllDiscard( LatestSchema.Table.Feed.select( conn ).map( updateAssignItems( conn, _ ) ) )
      yield ()

  def completeAssignables( ds : DataSource ) : Task[Unit] =
    withConnectionTransactional( ds ): conn =>
      LatestSchema.Table.Assignable.selectOpen( conn ).foreach: ak =>
        val AssignableKey( feedUrl, stype, withinTypeId ) = ak
        val count = LatestSchema.Table.Assignment.selectCountWithinAssignable( conn, feedUrl, stype, withinTypeId )
        if stype.isComplete( withinTypeId, count, Instant.now ) then
          populateMailable( conn, ak )
          LatestSchema.Table.Assignable.updateCompleted( conn, feedUrl, stype, withinTypeId, true )

  def addFeed( ds : DataSource, fi : FeedInfo ) : Task[Set[FeedInfo]] =
    withConnectionTransactional( ds ): conn =>
      LatestSchema.Table.Feed.upsert(conn, fi)
      LatestSchema.Table.Feed.select(conn)

  def listFeeds( ds : DataSource ) : Task[Set[FeedInfo]] =
    withConnectionTransactional( ds ): conn =>
      LatestSchema.Table.Feed.select(conn)


